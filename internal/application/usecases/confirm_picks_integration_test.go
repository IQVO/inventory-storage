//go:build integration

// TaskCompleted -> confirm-pick (ADR 0035) end-to-end proof on REAL
// infrastructure (testcontainers Kafka via the shared kafkatc helper, plus the
// package's shared testcontainers Postgres with every migration applied; never
// an external broker, never t.Skip):
//
//	fulfillment-execution-shaped TaskCompleted on a Kafka topic (one PICK task
//	per order LINE, all with the same order_ref)
//	  -> TaskCompletedConsumer -> ConfirmPicksForOrder (counts the order's picks
//	     in order_pick_progress, atomically with the processed-event claim)
//	  -> NOTHING is confirmed until the LAST pick; then every reservation is
//	     CONFIRMED, stock decremented exactly once, bin capacity released,
//	     StockPicked in the outbox exactly once
//	  -> a redelivered event (same CloudEvents id) and an extra PICK event for
//	     the same order change nothing, and never advance the counter.
//
// Every id carries a per-run suffix, so the tests are -count=N safe on the
// shared database.
package usecases_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// cpLine is one seeded order line: a stocked unit and its reservation.
type cpLine struct {
	sku           string
	unitID        string
	binID         string
	reservationID string
	stowed        int
	reserved      int
}

// cpSeedLine stows `stowed` units of a fresh SKU through the real StowStock and
// reserves `reserved` of them for demandRef through the real ReserveStock. A
// clock in the past makes the reservation already expired (still ACTIVE in the
// database, exactly what lazy expiry leaves behind).
func cpSeedLine(t *testing.T, env *transferTestEnv, run, label, demandRef string, stowed, reserved int, clock *memory.FixedClock) cpLine {
	t.Helper()
	return cpSeedLineNo(t, env, run, label, demandRef, nil, stowed, reserved, clock)
}

// cpSeedLineNo is cpSeedLine for a reservation made as order line lineNo
// (decision 18); nil seeds a legacy reservation without a line.
func cpSeedLineNo(t *testing.T, env *transferTestEnv, run, label, demandRef string, lineNo *int, stowed, reserved int, clock *memory.FixedClock) cpLine {
	t.Helper()
	ctx := context.Background()
	line := cpLine{sku: fmt.Sprintf("SKU-CP-%s-%s", label, run), stowed: stowed, reserved: reserved}
	binID := saveBin(t, env, fmt.Sprintf("BIN-CP-%s-%s", label, run))
	line.binID = binID.String()

	uow := postgres.NewUnitOfWork(env.pool)
	stockRepo := postgres.NewStockRepo(env.pool)
	reservations := postgres.NewReservationRepo(env.pool)

	stow := &usecases.StowStock{
		Stock: stockRepo, Locations: postgres.NewLocationRepo(env.pool), Events: events.NewBufferedPublisher(),
		Clock: memory.SystemClock{}, Classifications: postgres.NewProductClassificationRepo(env.pool),
		LocationLookup: &fakeLocationLookup{}, UnitOfWork: uow,
	}
	unit, err := stow.Execute(ctx, shared.SKU(line.sku), mustQty(t, stowed), binID)
	if err != nil {
		t.Fatalf("stow %s: %v", line.sku, err)
	}
	line.unitID = unit.ID()

	var clk ports.Clock = memory.SystemClock{}
	if clock != nil {
		clk = clock
	}
	reserve := &usecases.ReserveStock{Stock: stockRepo, Reservations: reservations, Events: events.NewBufferedPublisher(), Clock: clk, UnitOfWork: uow}
	res, err := reserve.ExecuteForLine(ctx, shared.SKU(line.sku), mustQty(t, reserved), demandRef, lineNo)
	if err != nil {
		t.Fatalf("reserve %s: %v", line.sku, err)
	}
	line.reservationID = res.ID()

	// The shared database's outbox is drained by other tests' relay passes; the
	// rows this test queues (analytics-topic StockPicked / ReservationExpired,
	// whose topic does not exist on the shared broker) must not become the
	// "oldest unpublished row" those passes trip over. Mark them published when
	// the test ends: the row's existence was asserted by then.
	reservationID := res.ID()
	t.Cleanup(func() {
		if _, err := env.pool.Exec(context.Background(), `
			UPDATE outbox_events SET published_at = now()
			WHERE published_at IS NULL
			  AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'reservation_id' = $1`, reservationID); err != nil {
			t.Errorf("mark outbox rows of %s published: %v", reservationID, err)
		}
	})
	return line
}

func cpReservationStatus(t *testing.T, env *transferTestEnv, id string) string {
	t.Helper()
	var status string
	if err := env.pool.QueryRow(context.Background(), `SELECT status FROM reservations WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read reservation %s: %v", id, err)
	}
	return status
}

func cpUnitQuantities(t *testing.T, env *transferTestEnv, unitID string) (quantity, reserved int) {
	t.Helper()
	if err := env.pool.QueryRow(context.Background(), `SELECT quantity, reserved FROM stock_units WHERE id = $1`, unitID).Scan(&quantity, &reserved); err != nil {
		t.Fatalf("read stock unit %s: %v", unitID, err)
	}
	return quantity, reserved
}

func cpBinOccupied(t *testing.T, env *transferTestEnv, binID string) int {
	t.Helper()
	var occupied int
	if err := env.pool.QueryRow(context.Background(), `SELECT occupied FROM bins WHERE id = $1`, binID).Scan(&occupied); err != nil {
		t.Fatalf("read bin %s: %v", binID, err)
	}
	return occupied
}

// cpOutboxRows counts the outbox rows of one event name for one reservation
// (any topic: the row's existence is the durable fact).
func cpOutboxRows(t *testing.T, env *transferTestEnv, eventName, reservationID string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM outbox_events
		WHERE event_type = $1
		  AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'reservation_id' = $2`,
		"com.warehouse.wms.inventory-storage.reservation."+eventName, reservationID,
	).Scan(&n); err != nil {
		t.Fatalf("count outbox %s for %s: %v", eventName, reservationID, err)
	}
	return n
}

func cpClaims(t *testing.T, env *transferTestEnv, eventID string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE consumer = $1 AND event_id = $2`,
		usecases.ConfirmPicksConsumer, eventID).Scan(&n); err != nil {
		t.Fatalf("count claims for %s: %v", eventID, err)
	}
	return n
}

// cpPublish writes one raw message to topic (retrying while a fresh topic's
// metadata propagates).
func cpPublish(t *testing.T, env *transferTestEnv, key string, value []byte) {
	t.Helper()
	w := kafkago.Writer{Addr: kafkago.TCP(env.brokers...), Topic: env.topic, Balancer: &kafkago.Hash{}, BatchTimeout: 50 * time.Millisecond}
	defer func() { _ = w.Close() }()
	msg := kafkago.Message{
		Key: []byte(key), Value: value,
		Headers: []kafkago.Header{{Key: "content-type", Value: []byte("application/cloudevents+json; charset=UTF-8")}},
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := w.WriteMessages(context.Background(), msg)
		if err == nil {
			return
		}
		if !errors.Is(err, kafkago.UnknownTopicOrPartition) || time.Now().After(deadline) {
			t.Fatalf("publish: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// cpTaskCompleted is a TaskCompleted CloudEvent exactly as fulfillment-execution
// emits it (its source, the full type, key = subject = task id).
func cpTaskCompleted(t *testing.T, id, taskID, taskType, orderRef string) []byte {
	t.Helper()
	data := map[string]any{
		"task_id": taskID, "station_id": "station-03", "work_unit_id": "wu-" + taskID,
		"associate_id": "worker-42", "duration_seconds": 245, "task_type": taskType,
	}
	if orderRef != "" {
		data["order_ref"] = orderRef
	}
	payload, _ := json.Marshal(data)
	value, err := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/fulfillment-execution",
		"type":            "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject":         taskID,
		"time":            "2026-10-06T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":            json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

// cpEnv hands a test its own topic on the shared broker, so consumers in
// different tests never share a group or a stream.
func cpEnv(t *testing.T, run string) *transferTestEnv {
	t.Helper()
	base := transferSharedEnv(t) // boots the shared Postgres and broker once
	topic := fmt.Sprintf("%s.itest-cp-%s", inboundkafka.FulfillmentTopic, run)
	createTransferTopic(t, base.brokers[0], topic, 2)
	return &transferTestEnv{pool: base.pool, brokers: base.brokers, topic: topic}
}

func cpUseCase(env *transferTestEnv, events ports.EventPublisher) *usecases.ConfirmPicksForOrder {
	stockRepo := postgres.NewStockRepo(env.pool)
	locations := postgres.NewLocationRepo(env.pool)
	reservations := postgres.NewReservationRepo(env.pool)
	uow := postgres.NewUnitOfWork(env.pool)
	return &usecases.ConfirmPicksForOrder{
		Stock: stockRepo, Reservations: reservations, Events: events, Clock: memory.SystemClock{},
		Confirm: &usecases.ConfirmPick{
			Stock: stockRepo, Locations: locations, Reservations: reservations,
			Events: events, Clock: memory.SystemClock{}, UnitOfWork: uow,
		},
		ProcessedEvents: postgres.NewProcessedEventRepo(env.pool),
		PickProgress:    postgres.NewOrderPickProgressRepo(env.pool),
		UnitOfWork:      uow,
	}
}

// cpProgress reads the order's counter row; ok is false when it has none.
func cpProgress(t *testing.T, env *transferTestEnv, demandRef string) (picked int, ok bool) {
	t.Helper()
	err := env.pool.QueryRow(context.Background(),
		`SELECT picked_tasks FROM order_pick_progress WHERE demand_ref = $1`, demandRef).Scan(&picked)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read order_pick_progress %s: %v", demandRef, err)
	}
	return picked, true
}

// cpOutboxPublisher is the production outbox wiring: StockPicked is an
// analytics-topic event, so the analytics encoder is what queues its row.
func cpOutboxPublisher(t *testing.T, env *transferTestEnv) ports.EventPublisher {
	t.Helper()
	reservations := postgres.NewReservationRepo(env.pool)
	analytics := outboundkafka.NewAnalyticsPublisher(env.brokers, reservations, nil)
	t.Cleanup(func() { _ = analytics.Close() })
	return postgres.NewOutboxPublisher(env.pool, outboundkafka.NewPublisher(nil, reservations), analytics)
}

// cpRunUntil runs the consumer until settled() is true AND the group has no
// lag (so a message published after the last assertion window was consumed, not
// merely not yet fetched), then stops it.
func cpRunUntil(t *testing.T, env *transferTestEnv, consumer *inboundkafka.TaskCompletedConsumer, group string, settled func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	deadline := time.Now().Add(60 * time.Second)
	for !(settled() && groupLag(env, group) == 0) {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("consumer did not settle: lag=%d", groupLag(env, group))
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
}

func cpConsumer(t *testing.T, env *transferTestEnv, group string, uc inboundkafka.PickCompletionHandler) *inboundkafka.TaskCompletedConsumer {
	t.Helper()
	consumer := inboundkafka.NewTaskCompletedConsumerForTopic(env.topic, env.brokers, group, uc, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = consumer.Close() })
	return consumer
}

// pickFixtureOrder seeds a three-line order (one reservation per line, as
// order-management reserves) plus an unrelated order's reservation.
type pickFixtureOrder struct {
	order string
	lines []cpLine
	other cpLine
}

func cpSeedThreeLineOrder(t *testing.T, env *transferTestEnv, run string) pickFixtureOrder {
	t.Helper()
	order := "order-cp-" + run
	return pickFixtureOrder{
		order: order,
		lines: []cpLine{
			cpSeedLine(t, env, run, "A", order, 10, 4, nil),
			cpSeedLine(t, env, run, "B", order, 8, 3, nil),
			cpSeedLine(t, env, run, "C", order, 6, 2, nil),
		},
		other: cpSeedLine(t, env, run, "O", "order-other-"+run, 5, 2, nil),
	}
}

func cpAssertAllUntouched(t *testing.T, env *transferTestEnv, f pickFixtureOrder, when string) {
	t.Helper()
	for _, l := range append(append([]cpLine{}, f.lines...), f.other) {
		if got := cpReservationStatus(t, env, l.reservationID); got != "ACTIVE" {
			t.Errorf("%s: %s reservation = %s, want ACTIVE", when, l.sku, got)
		}
		if q, r := cpUnitQuantities(t, env, l.unitID); q != l.stowed || r != l.reserved {
			t.Errorf("%s: %s unit quantity/reserved = %d/%d, want %d/%d (untouched)", when, l.sku, q, r, l.stowed, l.reserved)
		}
		if occ := cpBinOccupied(t, env, l.binID); occ != l.stowed {
			t.Errorf("%s: %s bin occupied = %d, want %d (untouched)", when, l.sku, occ, l.stowed)
		}
		if n := cpOutboxRows(t, env, "StockPicked", l.reservationID); n != 0 {
			t.Errorf("%s: %s raised StockPicked %d times, want 0", when, l.sku, n)
		}
	}
}

// ADR 0035: a PICK task is per order LINE, so with three lines the first two
// TaskCompleted events must confirm NOTHING and only the third confirms all
// three, with the stock decremented exactly once even when events are
// redelivered or extra ones arrive.
func TestIntegration_TaskCompleted_ConfirmsTheOrderOnlyOnTheLastPickExactlyOnce(t *testing.T) {
	run := uniqueRun()
	env := cpEnv(t, run)
	f := cpSeedThreeLineOrder(t, env, run)

	// The use case runs against the real outbox publisher, so StockPicked
	// lands in outbox_events in the confirming transaction.
	uc := cpUseCase(env, cpOutboxPublisher(t, env))
	group := "confirm-pick-itest-" + run
	consumer := cpConsumer(t, env, group, uc)

	// Picks 1 and 2, plus: a duplicate of pick 1 (same CloudEvents id), a PACK
	// task for the order, a PICK without order_ref (a producer that predates the
	// field), a PICK for an order that holds no reservation, and garbage.
	cpPublish(t, env, "task-1", cpTaskCompleted(t, "ce-1-"+run, "task-1", "PICK", f.order))
	cpPublish(t, env, "task-1", cpTaskCompleted(t, "ce-1-"+run, "task-1", "PICK", f.order))
	cpPublish(t, env, "task-p", cpTaskCompleted(t, "ce-pack-"+run, "task-p", "PACK", f.order))
	cpPublish(t, env, "task-n", cpTaskCompleted(t, "ce-noref-"+run, "task-n", "PICK", ""))
	cpPublish(t, env, "task-x", cpTaskCompleted(t, "ce-noresv-"+run, "task-x", "PICK", "order-no-reservations-"+run))
	cpPublish(t, env, "garbage", []byte(`{"event":"TaskCompleted","legacy":true}`))
	cpPublish(t, env, "task-2", cpTaskCompleted(t, "ce-2-"+run, "task-2", "PICK", f.order))

	cpRunUntil(t, env, consumer, group, func() bool {
		return cpClaims(t, env, "ce-1-"+run) == 1 && cpClaims(t, env, "ce-2-"+run) == 1 && cpClaims(t, env, "ce-noresv-"+run) == 1
	})

	// After TWO of three picks nothing is confirmed, nothing decremented.
	cpAssertAllUntouched(t, env, f, "after 2 of 3 picks")
	if n, ok := cpProgress(t, env, f.order); !ok || n != 2 {
		t.Fatalf("order_pick_progress = %d (row %v), want 2 (the duplicate and the PACK must not count)", n, ok)
	}
	if _, ok := cpProgress(t, env, "order-no-reservations-"+run); ok {
		t.Error("an order with no reservations left an order_pick_progress row")
	}
	for _, id := range []string{"ce-pack-" + run, "ce-noref-" + run} {
		if n := cpClaims(t, env, id); n != 0 {
			t.Errorf("an ignored event claimed %s (%d rows)", id, n)
		}
	}

	// The third (last) pick confirms the whole order.
	cpPublish(t, env, "task-3", cpTaskCompleted(t, "ce-3-"+run, "task-3", "PICK", f.order))
	cpRunUntil(t, env, consumer, group, func() bool { return cpClaims(t, env, "ce-3-"+run) == 1 })

	for _, l := range f.lines {
		if got := cpReservationStatus(t, env, l.reservationID); got != "CONFIRMED" {
			t.Errorf("%s reservation = %s, want CONFIRMED", l.sku, got)
		}
		// Physically decremented exactly once: stowed - reserved, nothing still reserved.
		if q, r := cpUnitQuantities(t, env, l.unitID); q != l.stowed-l.reserved || r != 0 {
			t.Errorf("%s unit quantity/reserved = %d/%d, want %d/0", l.sku, q, r, l.stowed-l.reserved)
		}
		if occ := cpBinOccupied(t, env, l.binID); occ != l.stowed-l.reserved {
			t.Errorf("%s bin occupied = %d, want %d (capacity released)", l.sku, occ, l.stowed-l.reserved)
		}
		if n := cpOutboxRows(t, env, "StockPicked", l.reservationID); n != 1 {
			t.Errorf("%s StockPicked outbox rows = %d, want exactly 1", l.sku, n)
		}
	}
	if n, _ := cpProgress(t, env, f.order); n != 3 {
		t.Errorf("order_pick_progress = %d, want 3", n)
	}
	// A different order is untouched.
	if got := cpReservationStatus(t, env, f.other.reservationID); got != "ACTIVE" {
		t.Errorf("other order reservation = %s, want ACTIVE", got)
	}
	if q, r := cpUnitQuantities(t, env, f.other.unitID); q != f.other.stowed || r != f.other.reserved {
		t.Errorf("other order unit quantity/reserved = %d/%d, want %d/%d", q, r, f.other.stowed, f.other.reserved)
	}

	// Redelivery of the 3rd, then an extra PICK event with a NEW id for the same
	// order: nothing changes (the claim short-circuits the first; the second
	// finds no ACTIVE reservation), so stock is decremented exactly once.
	pickedRows := cpOutboxRows(t, env, "StockPicked", f.lines[0].reservationID)
	cpPublish(t, env, "task-3", cpTaskCompleted(t, "ce-3-"+run, "task-3", "PICK", f.order))
	cpPublish(t, env, "task-5", cpTaskCompleted(t, "ce-5-"+run, "task-5", "PICK", f.order))
	cpRunUntil(t, env, consumer, group, func() bool { return cpClaims(t, env, "ce-5-"+run) == 1 })

	for _, l := range f.lines {
		if got := cpReservationStatus(t, env, l.reservationID); got != "CONFIRMED" {
			t.Errorf("%s reservation = %s after redelivery, want CONFIRMED", l.sku, got)
		}
		if q, r := cpUnitQuantities(t, env, l.unitID); q != l.stowed-l.reserved || r != 0 {
			t.Errorf("%s after redelivery quantity/reserved = %d/%d, want %d/0 (decremented exactly once)", l.sku, q, r, l.stowed-l.reserved)
		}
		if n := cpOutboxRows(t, env, "StockPicked", l.reservationID); n != 1 {
			t.Errorf("%s StockPicked outbox rows = %d after redelivery, want 1", l.sku, n)
		}
	}
	if n := cpOutboxRows(t, env, "StockPicked", f.lines[0].reservationID); n != pickedRows {
		t.Errorf("StockPicked outbox rows %d -> %d after redelivery, want unchanged", pickedRows, n)
	}
	if n, _ := cpProgress(t, env, f.order); n != 4 {
		t.Errorf("order_pick_progress = %d, want 4 (3 distinct picks + 1 extra; the redelivered 3rd did not count)", n)
	}
}

// A reservation that expired before the last pick is skipped, never confirmed:
// its stock was already returned to usable by lazy expiry.
func TestIntegration_TaskCompleted_ExpiredLineIsSkippedOnTheLastPick(t *testing.T) {
	ctx := context.Background()
	run := uniqueRun()
	env := cpEnv(t, run)
	order := "order-cp-exp-" + run

	live := cpSeedLine(t, env, run, "EL", order, 10, 4, nil)
	// Already past its timeout but still ACTIVE in the database (what lazy
	// expiry leaves behind), so it counts as a pending pick.
	expired := cpSeedLine(t, env, run, "EX", order, 6, 2, memory.NewFixedClock(time.Now().Add(-2*time.Hour)))

	uc := cpUseCase(env, cpOutboxPublisher(t, env))
	first, err := uc.Execute(ctx, usecases.PickCompletion{EventID: "ce-e1-" + run, TaskID: "t1", TaskType: "PICK", OrderRef: order})
	if err != nil || first.Outcome != usecases.PicksAwaiting {
		t.Fatalf("first pick = %+v, %v; want AWAITING_LAST_PICK", first, err)
	}
	if got := cpReservationStatus(t, env, live.reservationID); got != "ACTIVE" {
		t.Fatalf("live line = %s after the first of two picks, want ACTIVE", got)
	}
	last, err := uc.Execute(ctx, usecases.PickCompletion{EventID: "ce-e2-" + run, TaskID: "t2", TaskType: "PICK", OrderRef: order})
	if err != nil || last.Outcome != usecases.PicksProcessed || last.Confirmed != 1 || last.Expired != 1 {
		t.Fatalf("last pick = %+v, %v; want PROCESSED with 1 confirmed and 1 expired", last, err)
	}

	if got := cpReservationStatus(t, env, live.reservationID); got != "CONFIRMED" {
		t.Errorf("live line = %s, want CONFIRMED", got)
	}
	if got := cpReservationStatus(t, env, expired.reservationID); got != "EXPIRED" {
		t.Errorf("expired line = %s, want EXPIRED", got)
	}
	if q, r := cpUnitQuantities(t, env, expired.unitID); q != expired.stowed || r != 0 {
		t.Errorf("expired line unit quantity/reserved = %d/%d, want %d/0 (returned to usable, never picked)", q, r, expired.stowed)
	}
	if n := cpOutboxRows(t, env, "StockPicked", expired.reservationID); n != 0 {
		t.Errorf("expired line raised StockPicked %d times, want 0", n)
	}
}

// A failure on the LAST pick rolls back the claim, the counter increment AND
// every confirmation together; the retried delivery then converges.
func TestIntegration_TaskCompleted_FailureRollsBackEverythingAndRetryConverges(t *testing.T) {
	ctx := context.Background()
	run := uniqueRun()
	env := cpEnv(t, run)
	order := "order-cp-rb-" + run

	lineA := cpSeedLine(t, env, run, "RA", order, 10, 4, nil)
	lineB := cpSeedLine(t, env, run, "RB", order, 8, 3, nil)
	firstID, eventID := "ce-rb1-"+run, "ce-rb-"+run

	// Pick 1 of 2 commits normally.
	healthy := cpUseCase(env, cpOutboxPublisher(t, env))
	if got, err := healthy.Execute(ctx, usecases.PickCompletion{EventID: firstID, TaskID: "task-rb1", TaskType: "PICK", OrderRef: order}); err != nil || got.Outcome != usecases.PicksAwaiting {
		t.Fatalf("first pick = %+v, %v; want AWAITING_LAST_PICK", got, err)
	}

	// The outbox encode step fails on the LAST write of the last pick's
	// confirmation, after the counter/stock/bin/reservation writes: exactly what
	// the rollback must undo.
	broken := cpUseCase(env, postgres.NewOutboxPublisher(env.pool, failingTransferEncoder{}))
	var outboxBefore int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxBefore); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	_, err := broken.Execute(ctx, usecases.PickCompletion{EventID: eventID, TaskID: "task-rb", TaskType: "PICK", OrderRef: order})
	if err == nil || errors.Is(err, usecases.ErrMalformedPickCompletion) {
		t.Fatalf("err = %v, want a transient failure so the consumer retries", err)
	}

	for _, l := range []cpLine{lineA, lineB} {
		if got := cpReservationStatus(t, env, l.reservationID); got != "ACTIVE" {
			t.Errorf("%s reservation = %s after a rolled-back handling, want ACTIVE", l.sku, got)
		}
		if q, r := cpUnitQuantities(t, env, l.unitID); q != l.stowed || r != l.reserved {
			t.Errorf("%s unit quantity/reserved = %d/%d, want %d/%d (untouched)", l.sku, q, r, l.stowed, l.reserved)
		}
		if occ := cpBinOccupied(t, env, l.binID); occ != l.stowed {
			t.Errorf("%s bin occupied = %d, want %d (untouched)", l.sku, occ, l.stowed)
		}
	}
	if n := cpClaims(t, env, eventID); n != 0 {
		t.Fatalf("claim survived the rollback (%d rows): the redelivery would be skipped as a duplicate", n)
	}
	if n, _ := cpProgress(t, env, order); n != 1 {
		t.Fatalf("order_pick_progress = %d after the rolled-back pick, want 1 (the failed pick must not stay counted)", n)
	}
	var outboxAfter int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxAfter); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxAfter != outboxBefore {
		t.Fatalf("outbox rows %d -> %d, want unchanged", outboxBefore, outboxAfter)
	}

	// The redelivery of the SAME event now converges: counted once, confirmed.
	got, err := healthy.Execute(ctx, usecases.PickCompletion{EventID: eventID, TaskID: "task-rb", TaskType: "PICK", OrderRef: order})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got.Outcome != usecases.PicksProcessed || got.Confirmed != 2 || got.PicksSeen != 2 {
		t.Fatalf("retry result = %+v, want PROCESSED with 2 confirmed on pick 2 of 2", got)
	}
	for _, l := range []cpLine{lineA, lineB} {
		if s := cpReservationStatus(t, env, l.reservationID); s != "CONFIRMED" {
			t.Errorf("%s reservation = %s after the retry, want CONFIRMED", l.sku, s)
		}
		if q, r := cpUnitQuantities(t, env, l.unitID); q != l.stowed-l.reserved || r != 0 {
			t.Errorf("%s unit quantity/reserved = %d/%d, want %d/0", l.sku, q, r, l.stowed-l.reserved)
		}
	}
	if n := cpClaims(t, env, eventID); n != 1 {
		t.Errorf("claims after the retry = %d, want 1", n)
	}
	if n, _ := cpProgress(t, env, order); n != 2 {
		t.Errorf("order_pick_progress = %d after the retry, want 2", n)
	}
}

// A malformed TaskCompleted is dead-lettered with its raw bytes and committed,
// and the message behind it is still handled (the partition is not wedged).
func TestIntegration_TaskCompleted_PoisonIsDeadLetteredAndTheStreamContinues(t *testing.T) {
	run := uniqueRun()
	env := cpEnv(t, run)
	order := "order-cp-dlq-" + run
	line := cpSeedLine(t, env, run, "D", order, 10, 4, nil)

	uc := cpUseCase(env, cpOutboxPublisher(t, env))
	group := "confirm-pick-dlq-itest-" + run
	consumer := cpConsumer(t, env, group, uc)

	poison := fulfillmentPoison(t, "ce-poison-"+run)
	// Same key => same partition => the poison sits strictly BEFORE the good one.
	cpPublish(t, env, "task-p", poison)
	cpPublish(t, env, "task-p", cpTaskCompleted(t, "ce-good-"+run, "task-p", "PICK", order))

	cpRunUntil(t, env, consumer, group, func() bool {
		return cpReservationStatus(t, env, line.reservationID) == "CONFIRMED"
	})

	dlq := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: env.brokers, Topic: env.topic + ".dlq", Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 500 * time.Millisecond,
	})
	defer func() { _ = dlq.Close() }()
	readCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msg, err := dlq.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("read the dead-letter topic: %v", err)
	}
	if string(msg.Value) != string(poison) {
		t.Fatalf("dead-lettered value differs from the original message")
	}
	headers := map[string]string{}
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["x-dlq-source-topic"] != env.topic || headers["x-dlq-error"] == "" {
		t.Fatalf("DLQ headers = %v", headers)
	}
}

// fulfillmentPoison is a valid CloudEvent of the TaskCompleted type whose data
// can not be decoded into the contract (task_type is an object).
func fulfillmentPoison(t *testing.T, id string) []byte {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": id, "source": "/warehouse/fulfillment-execution",
		"type":    "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject": "task-p", "time": "2026-10-06T12:00:00Z", "datacontenttype": "application/json",
		"dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":       map[string]any{"task_id": "task-p", "task_type": map[string]any{"not": "a string"}},
	})
	if err != nil {
		t.Fatalf("marshal poison: %v", err)
	}
	return value
}
