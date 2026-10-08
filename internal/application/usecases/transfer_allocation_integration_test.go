//go:build integration

// Transfer-safe site-scoped allocation (Phase 2) end-to-end proof, on
// REAL infrastructure this file boots itself via testcontainers (never
// an external DATABASE_URL/KAFKA_BROKERS, never t.Skip):
//
//   - Postgres with every migration applied, the real postgres repos,
//     the real UnitOfWork, and the real transactional outbox publisher
//     (OutboxPublisher + kafka.Publisher encoder).
//   - Kafka carrying network-inventory-planning's command topic.
//
// The four scenarios required by the Phase 2 contract:
//
//  1. duplicate command => ONE reservation, ONE decrement, stable outcome;
//  2. other-site stock untouched;
//  3. insufficient stock => exactly one rejection;
//  4. forced outbox failure rolls EVERYTHING back (stock, reservation,
//     ledger, outbox row).
//
// Settling is observed via the LEDGER and the reply events in the
// OUTBOX (both durable outcomes of the consumer's transaction), not via
// consumer-group offsets — the database is the source of truth the
// contract cares about.
package usecases_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

// transferTestEnv owns the Postgres pool and Kafka broker for the
// transfer integration tests. ONE broker and ONE database are booted
// for the whole package run (mirroring the facilitycache consumer's
// proven TestMain pattern — booting a container per test both wastes
// minutes and has hit container-start flakiness); test isolation comes
// from each test using its own SKUs/line ids and its own COMMAND TOPIC,
// so consumers in different tests never share a group or a stream.
type transferTestEnv struct {
	pool    *pgxpool.Pool
	brokers []string
	topic   string
}

var (
	transferSharedBrokers   []string
	transferSharedContainer *kafkatc.Broker
	transferSharedPool      *pgxpool.Pool
)

// TestTransferMain boots ONE Postgres and ONE Kafka for the whole
// transfer test set (mirroring the facilitycache consumer's proven
// TestMain pattern) and tears them down after the last test — a pool or
// broker booted per-test both wastes minutes and has hit container-start
// flakiness. Per-test isolation comes from unique command topics and
// unique SKUs/line ids, never from separate infrastructure.
func TestMain(m *testing.M) {
	code := m.Run()
	if transferSharedPool != nil {
		transferSharedPool.Close()
	}
	if transferSharedContainer != nil {
		if err := transferSharedContainer.Terminate(); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	if transferPostgresContainer != nil {
		if err := testcontainers.TerminateContainer(transferPostgresContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

// transferSharedEnv lazily boots the shared broker+pool (from the first
// test that needs them) and hands every test a fresh unique command
// topic on it. Cleanup is owned by TestTransferMain, NOT t.Cleanup, so
// the infrastructure outlives every test in the set.
func transferSharedEnv(t *testing.T) *transferTestEnv {
	t.Helper()
	ctx := context.Background()

	if transferSharedPool == nil {
		// One retry: the very first container boot of a test binary has
		// hit Docker Desktop's ryuk reaper startup race ("Started"
		// matched 0 times) — transient, and a fresh attempt clears it.
		var err error
		transferSharedPool, err = bootSharedPostgresForTransfers(ctx, t)
		if err != nil {
			t.Logf("first postgres boot failed (%v); retrying once", err)
			transferSharedPool, err = bootSharedPostgresForTransfers(ctx, t)
			if err != nil {
				t.Fatalf("start postgres container (after retry): %v", err)
			}
		}
	}
	if transferSharedBrokers == nil {
		// kafkatc.Start returns only once the broker is a usable group
		// coordinator (a cold broker answers [15]
		// GroupCoordinatorNotAvailable and kafka-go then sleeps a fixed
		// 5 s JoinGroupBackoff per attempt — the "timed out waiting for
		// the transfer consumer to settle" flake), so that cost is paid
		// here, once, not inside the first test's timed window.
		broker, err := kafkatc.Start(ctx, "transfer-alloc-itest")
		if err != nil {
			t.Fatalf("%v", err)
		}
		transferSharedContainer = broker
		transferSharedBrokers = broker.Addrs
	}

	topic := fmt.Sprintf("%s.itest-%d", inboundkafka.Topic, time.Now().UnixNano())
	createTransferTopic(t, transferSharedBrokers[0], topic, 4)
	return &transferTestEnv{pool: transferSharedPool, brokers: transferSharedBrokers, topic: topic}
}

// bootSharedPostgresForTransfers is ocDB's recipe WITHOUT the
// t.Cleanup binding: the shared pool's lifetime is owned by
// TestTransferMain, not by whichever test booted it first.
func bootSharedPostgresForTransfers(ctx context.Context, t *testing.T) (*pgxpool.Pool, error) {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, err
	}
	// The container outlives the booting test: TestMain terminates it
	// after the last test (the pool is shared).
	transferPostgresContainer = container

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, err
	}
	if err := postgres.RunMigrations(url, migrationsDirForUsecases(t)); err != nil {
		return nil, err
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

var transferPostgresContainer testcontainers.Container

// createTransferTopic creates topic with numPartitions partitions on
// broker and waits until the metadata is propagated. Creation errors
// that mean "already exists" (a broker racing auto-creation) are
// tolerated; the wait still enforces the partition count.
func createTransferTopic(t *testing.T, broker, topic string, numPartitions int) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial %s: %v", broker, err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{
		Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1,
	}); err != nil && !isTopicExistsErr(err) {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) == numPartitions {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never became readable with %d partitions", topic, numPartitions)
}

// isTopicExistsErr reports whether err is Kafka's TopicAlreadyExists /
// UnknownTopicOrPartition-metadata-race shape.
func isTopicExistsErr(err error) bool {
	return strings.Contains(err.Error(), "Topic with this name already exists") ||
		errors.Is(err, kafkago.UnknownTopicOrPartition)
}

// publishTransferCommand writes one TransferAllocationRequested
// CloudEvent onto the command topic, keyed by transfer_line_id (the
// natural per-line ordering key), exactly as network-inventory-planning
// would.
func publishTransferCommand(t *testing.T, topic string, brokers []string, id, transferID, lineID, site, sku string, qty int) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"transfer_id":      transferID,
		"transfer_line_id": lineID,
		"origin_site_id":   site,
		"sku":              sku,
		"quantity":         qty,
	})
	value, _ := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/network-inventory-planning",
		"type":            "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested",
		"subject":         lineID,
		"time":            "2026-10-06T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:network-inventory-planning:events:TransferAllocationRequested:v1",
		"data":            json.RawMessage(payload),
	})

	w := kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		BatchTimeout: 50 * time.Millisecond,
	}
	defer func() { _ = w.Close() }()
	msg := kafkago.Message{
		Key:     []byte(lineID),
		Value:   value,
		Headers: []kafkago.Header{{Key: "content-type", Value: []byte("application/cloudevents+json; charset=UTF-8")}},
	}
	// A freshly created topic's metadata can lag on the broker the
	// writer first dials (UnknownTopicOrPartition); retry briefly.
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := w.WriteMessages(context.Background(), msg)
		if err == nil {
			return
		}
		if !errors.Is(err, kafkago.UnknownTopicOrPartition) || time.Now().After(deadline) {
			t.Fatalf("publish command %s: %v", id, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// seedTransferStock stows qty of sku at site through the real repos.
func seedTransferStock(t *testing.T, env *transferTestEnv, id, sku string, site shared.SiteID, qty int) {
	t.Helper()
	ctx := context.Background()
	locations := postgres.NewLocationRepo(env.pool)
	binID, _ := shared.NewBinId("BIN-" + id)
	capacity, _ := shared.NewQuantity(qty + 10)
	bin, err := location.NewBin(binID, capacity)
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}
	stockRepo := postgres.NewStockRepo(env.pool)
	skuVO, _ := shared.NewSKU(sku)
	unit, err := stock.NewStockUnitAtSite(id, skuVO, binID, mustQty(t, qty), site)
	if err != nil {
		t.Fatalf("build unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}
}

// wireTransferConsumer builds the full inbound path: real Postgres
// repos, UnitOfWork, outbox publisher with the real Kafka encoder, and
// the consumer itself under a unique group.
func wireTransferConsumer(t *testing.T, env *transferTestEnv, groupID string) *inboundkafka.Consumer {
	t.Helper()
	stockRepo := postgres.NewStockRepo(env.pool)
	reservations := postgres.NewReservationRepo(env.pool)
	transfers := postgres.NewTransferAllocationRepo(env.pool)
	uow := postgres.NewUnitOfWork(env.pool)

	encoder := outboundkafka.NewPublisher(nil, reservations)
	publisher := postgres.NewOutboxPublisher(env.pool, encoder)

	consumer := inboundkafka.NewConsumerForTopic(env.topic, env.brokers, groupID, &usecases.AllocateTransferStock{
		Stock:        stockRepo,
		Reservations: reservations,
		Transfers:    transfers,
		Events:       publisher,
		Clock:        memory.SystemClock{},
		UnitOfWork:   uow,
	}, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = consumer.Close() })
	return consumer
}

// runConsumerUntil runs the consumer until settled() returns true, then
// cancels it. settled is polled against the DATABASE (the ledger and the
// outbox are the transaction's durable outcomes). On timeout the Run
// loop's return error (if any) is reported alongside the failure.
func runConsumerUntil(t *testing.T, consumer *inboundkafka.Consumer, settled func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	deadline := time.After(45 * time.Second)
	for {
		if settled() {
			// Give the commit a moment to land, then stop.
			time.Sleep(500 * time.Millisecond)
			cancel()
			<-done
			return
		}
		select {
		case <-deadline:
			cancel()
			runErr := "(still running)"
			select {
			case err := <-done:
				if err == nil {
					runErr = "(returned nil)"
				} else {
					runErr = err.Error()
				}
			default:
			}
			t.Fatalf("timed out waiting for the transfer consumer to settle; Run: %s", runErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Scenario 1: a duplicate command yields ONE reservation, ONE decrement,
// and a stable outcome — nothing double-decides.
func TestIntegration_TransferAllocation_DuplicateCommand_OneReservationOneDecrement(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()

	// Every identifier carries a per-run suffix: the Postgres container
	// is shared by the whole package run, so fixed ids made the test
	// non-repeatable (-count=N) and let other scenarios' rows leak into
	// its global counts.
	run := uniqueRun()
	unitID, sku, transferID, lineID := "su-dup-"+run, "SKU-DUP-"+run, "tr-dup-"+run, "tl-dup-"+run
	ce1, ce2 := "ce-dup-1-"+run, "ce-dup-2-"+run

	site, _ := shared.NewSiteID("SITE-A")
	seedTransferStock(t, env, unitID, sku, site, 10)

	group := "transfer-itest-dup-" + run
	consumer := wireTransferConsumer(t, env, group)

	// The SAME CloudEvent id twice (a genuine broker redelivery), plus a
	// NEW id for the same line (a planning retry): all must collapse to
	// one decision.
	publishTransferCommand(t, env.topic, env.brokers, ce1, transferID, lineID, "SITE-A", sku, 6)
	publishTransferCommand(t, env.topic, env.brokers, ce1, transferID, lineID, "SITE-A", sku, 6)
	publishTransferCommand(t, env.topic, env.brokers, ce2, transferID, lineID, "SITE-A", sku, 6)

	// Settled when the ledger row exists, the reply event hit the
	// outbox, AND the group has committed past all three commands — the
	// assertions below are about what the DUPLICATES did, so they must
	// have been consumed (not merely the first command) before we look.
	runConsumerUntil(t, consumer, func() bool {
		return transferLedgerOutcome(t, env, lineID) != "" &&
			outboxEventsForLine(t, env, "TransferStockAllocated", lineID) >= 1 &&
			groupLag(env, group) == 0
	})

	// Ledger: exactly one row, ALLOCATED.
	var ledgerRows int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM transfer_allocations WHERE transfer_line_id = $1`, lineID).Scan(&ledgerRows); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if ledgerRows != 1 {
		t.Fatalf("ledger rows = %d, want exactly 1", ledgerRows)
	}

	// Reservations: exactly one for the transfer's demandRef.
	var resRows int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM reservations WHERE demand_ref = $1`, usecases.TransferDemandRef(transferID, lineID)).Scan(&resRows); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	if resRows != 1 {
		t.Fatalf("reservation rows = %d, want exactly 1", resRows)
	}

	// Stock: usable fell by exactly 6, once.
	if got := usableAt(t, env, unitID); got != 4 {
		t.Fatalf("usable after duplicate commands = %d, want 4 (one decrement of 6)", got)
	}

	// Exactly ONE reply event for the line: the duplicates republished nothing.
	if allocatedEvents := outboxEventsForLine(t, env, "TransferStockAllocated", lineID); allocatedEvents != 1 {
		t.Fatalf("TransferStockAllocated outbox rows = %d, want exactly 1 (replays republish nothing)", allocatedEvents)
	}
}

// Scenario 2: only the origin site's stock is drawn; another site's
// identical SKU is untouched.
func TestIntegration_TransferAllocation_OtherSiteStockUntouched(t *testing.T) {
	env := transferSharedEnv(t)

	run := uniqueRun()
	unitA, unitB, sku, lineID := "su-a-"+run, "su-b-"+run, "SKU-SITES-"+run, "tl-sites-"+run
	siteA, _ := shared.NewSiteID("SITE-A")
	siteB, _ := shared.NewSiteID("SITE-B")
	seedTransferStock(t, env, unitA, sku, siteA, 10)
	seedTransferStock(t, env, unitB, sku, siteB, 10)

	consumer := wireTransferConsumer(t, env, "transfer-itest-sites-"+run)
	publishTransferCommand(t, env.topic, env.brokers, "ce-sites-1-"+run, "tr-sites-"+run, lineID, "SITE-A", sku, 7)

	runConsumerUntil(t, consumer, func() bool { return transferLedgerOutcome(t, env, lineID) != "" })

	if got := usableAt(t, env, unitA); got != 3 {
		t.Fatalf("origin site usable = %d, want 3", got)
	}
	if got := usableAt(t, env, unitB); got != 10 {
		t.Fatalf("other site usable = %d, want 10 (untouched)", got)
	}
}

// Scenario 3: insufficient stock produces EXACTLY ONE rejection with the
// closed reason — and a replay of it adds nothing.
func TestIntegration_TransferAllocation_InsufficientStock_OneRejection(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()

	run := uniqueRun()
	unitID, sku, transferID, lineID := "su-ins-"+run, "SKU-INS-"+run, "tr-ins-"+run, "tl-ins-"+run
	siteA, _ := shared.NewSiteID("SITE-A")
	seedTransferStock(t, env, unitID, sku, siteA, 3)

	consumer := wireTransferConsumer(t, env, "transfer-itest-ins-"+run)
	publishTransferCommand(t, env.topic, env.brokers, "ce-ins-1-"+run, transferID, lineID, "SITE-A", sku, 6)

	runConsumerUntil(t, consumer, func() bool { return transferLedgerOutcome(t, env, lineID) != "" })

	var outcome, reason string
	if err := env.pool.QueryRow(ctx,
		`SELECT outcome, rejection_reason FROM transfer_allocations WHERE transfer_line_id = $1`, lineID,
	).Scan(&outcome, &reason); err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if outcome != "REJECTED" || reason != "INSUFFICIENT_USABLE" {
		t.Fatalf("outcome/reason = %s/%s, want REJECTED/INSUFFICIENT_USABLE", outcome, reason)
	}

	// Exactly one rejection reply event.
	if rejectedEvents := outboxEventsForLine(t, env, "TransferStockAllocationRejected", lineID); rejectedEvents != 1 {
		t.Fatalf("TransferStockAllocationRejected outbox rows = %d, want exactly 1", rejectedEvents)
	}

	// A replay of the same command (new CE id, same line) changes
	// nothing. The ledger is already decided so its outcome cannot
	// signal consumption; give the consumer a bounded window instead.
	publishTransferCommand(t, env.topic, env.brokers, "ce-ins-2-"+run, transferID, lineID, "SITE-A", sku, 6)
	time.Sleep(3 * time.Second)

	var rows int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM transfer_allocations WHERE transfer_line_id = $1`, lineID,
	).Scan(&rows); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if rows != 1 {
		t.Fatalf("ledger rows after replay = %d, want exactly 1", rows)
	}
	if got := usableAt(t, env, unitID); got != 3 {
		t.Fatalf("usable after rejection = %d, want 3 (untouched)", got)
	}
	if rejectedEvents := outboxEventsForLine(t, env, "TransferStockAllocationRejected", lineID); rejectedEvents != 1 {
		t.Fatalf("rejection events after replay = %d, want still exactly 1", rejectedEvents)
	}
}

// Scenario 4: a forced outbox failure rolls EVERYTHING back — the stock
// decrement, the reservation, and the ledger row. This is the
// one-UnitOfWork proof, driven directly at the use case (the consumer is
// not involved: the point is the transaction shape, not the transport).
func TestIntegration_TransferAllocation_OutboxFailureRollsEverythingBack(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()

	stockRepo := postgres.NewStockRepo(env.pool)
	reservations := postgres.NewReservationRepo(env.pool)
	transfers := postgres.NewTransferAllocationRepo(env.pool)
	uow := postgres.NewUnitOfWork(env.pool)

	run := uniqueRun()
	unitID, skuStr, transferID, lineID := "su-rb-"+run, "SKU-RB-"+run, "tr-rb-"+run, "tl-rb-"+run
	siteA, _ := shared.NewSiteID("SITE-A")
	seedTransferStock(t, env, unitID, skuStr, siteA, 10)

	// failingTransferEncoder forces the outbox encode step to fail so
	// the transaction's LAST write aborts after the earlier ones — the
	// exact shape the rollback must undo.
	publisher := postgres.NewOutboxPublisher(env.pool, failingTransferEncoder{})

	uc := &usecases.AllocateTransferStock{
		Stock:        stockRepo,
		Reservations: reservations,
		Transfers:    transfers,
		Events:       publisher,
		Clock:        memory.SystemClock{},
		UnitOfWork:   uow,
	}
	// The outbox baseline is taken BEFORE the command: the aborted
	// transaction must leave the count unchanged (see the delta
	// assertion at the end for why a global zero-count is wrong on the
	// shared database).
	var outboxBefore int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxBefore); err != nil {
		t.Fatalf("count outbox before: %v", err)
	}

	sku, _ := shared.NewSKU(skuStr)
	if _, err := uc.Execute(ctx, usecases.TransferCommand{
		TransferID: transferID, TransferLineID: lineID, OriginSiteID: siteA, SKU: sku, Quantity: mustQty(t, 6),
	}); err == nil {
		t.Fatal("expected the outbox failure to surface as an error")
	}

	// Stock: no decrement survived.
	if got := usableAt(t, env, unitID); got != 10 {
		t.Fatalf("usable after rolled-back allocation = %d, want 10", got)
	}
	// Reservation: none was created.
	var resRows int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM reservations WHERE demand_ref = $1`, usecases.TransferDemandRef(transferID, lineID)).Scan(&resRows); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	if resRows != 0 {
		t.Fatalf("reservation rows = %d, want 0", resRows)
	}
	// Ledger: no row was written.
	var ledgerRows int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM transfer_allocations WHERE transfer_line_id = $1`, lineID).Scan(&ledgerRows); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if ledgerRows != 0 {
		t.Fatalf("ledger rows = %d, want 0", ledgerRows)
	}
	// Outbox: nothing queued by the aborted command. The database is
	// shared across the whole scenario set (one container, TestMain-
	// owned), so earlier scenarios' reply events legitimately sit in
	// the outbox — assert on the DELTA across the command, which is the
	// actual rollback property, not on a global count.
	var outboxAfter int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxAfter); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxAfter != outboxBefore {
		t.Fatalf("outbox rows before = %d, after = %d, want unchanged (nothing queued by the rolled-back allocation)", outboxBefore, outboxAfter)
	}
}

// --- helpers ------------------------------------------------------------

type failingTransferEncoder struct{}

func (failingTransferEncoder) Encode(context.Context, shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	return nil, errForcedOutbox
}

var errForcedOutbox = fmt.Errorf("forced outbox encode failure")

// usableAt reads a unit's usable quantity straight from the DB.
func usableAt(t *testing.T, env *transferTestEnv, unitID string) int {
	t.Helper()
	var qty, reserved int
	var state string
	if err := env.pool.QueryRow(context.Background(),
		`SELECT quantity, reserved, state FROM stock_units WHERE id = $1`, unitID,
	).Scan(&qty, &reserved, &state); err != nil {
		t.Fatalf("read stock %s: %v", unitID, err)
	}
	if state == "UNLOCATED" || state == "REMOVED" {
		return 0
	}
	return qty - reserved
}

// transferLedgerOutcome returns the decided outcome for a line, "" while
// undecided. Tolerant of pgx.ErrNoRows because it doubles as the poll
// condition inside runConsumerUntil — a missing row simply means "not
// settled yet". A non-ErrNoRows error is real and fatal.
func transferLedgerOutcome(t *testing.T, env *transferTestEnv, lineID string) string {
	t.Helper()
	var outcome *string
	err := env.pool.QueryRow(context.Background(),
		`SELECT outcome FROM transfer_allocations WHERE transfer_line_id = $1`, lineID,
	).Scan(&outcome)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatalf("read ledger %s: %v", lineID, err)
	}
	if outcome == nil {
		return ""
	}
	return *outcome
}

// uniqueRun returns a per-invocation suffix for ids, so a test never
// collides with its own earlier run (-count=N) or with another test's
// rows on the shared database.
func uniqueRun() string { return fmt.Sprintf("%d", time.Now().UnixNano()) }

// outboxEventsForLine counts the outbox rows of one transfer reply event
// type that belong to ONE transfer line (published or not — the row's
// existence is the durable fact). Scoping by line, not by type alone,
// keeps the count independent of every other scenario on the shared
// database. The CloudEvent sits in outbox_events.value as JSON bytes.
func outboxEventsForLine(t *testing.T, env *transferTestEnv, eventName, lineID string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM outbox_events
		WHERE event_type = $1
		  AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'transfer_line_id' = $2`,
		"com.warehouse.wms.inventory-storage.reservation."+eventName, lineID,
	).Scan(&n); err != nil {
		t.Fatalf("count outbox %s events for %s: %v", eventName, lineID, err)
	}
	return n
}

// groupLag returns how many messages of env.topic the consumer group has
// not yet committed past (sum over partitions of end offset minus the
// committed offset). Any lookup error returns -1 ("not known to be
// caught up"), so a caller polling for 0 simply keeps waiting.
func groupLag(env *transferTestEnv, group string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addr := kafkago.TCP(env.brokers[0])
	client := &kafkago.Client{Addr: addr, Timeout: 5 * time.Second}

	meta, err := client.Metadata(ctx, &kafkago.MetadataRequest{Addr: addr, Topics: []string{env.topic}})
	if err != nil || len(meta.Topics) != 1 || meta.Topics[0].Error != nil {
		return -1
	}
	var parts []int
	ends := map[string][]kafkago.OffsetRequest{}
	for _, p := range meta.Topics[0].Partitions {
		parts = append(parts, p.ID)
		ends[env.topic] = append(ends[env.topic], kafkago.LastOffsetOf(p.ID))
	}
	end, err := client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{Addr: addr, Topics: ends})
	if err != nil {
		return -1
	}
	committed, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		Addr: addr, GroupID: group, Topics: map[string][]int{env.topic: parts},
	})
	if err != nil {
		return -1
	}
	done := map[int]int64{}
	for _, p := range committed.Topics[env.topic] {
		if p.Error != nil {
			return -1
		}
		if p.CommittedOffset > 0 {
			done[p.Partition] = p.CommittedOffset
		}
	}
	var lag int64
	for _, p := range end.Topics[env.topic] {
		if p.Error != nil {
			return -1
		}
		if d := p.LastOffset - done[p.Partition]; d > 0 {
			lag += d
		}
	}
	return lag
}
