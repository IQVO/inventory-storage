//go:build integration

// TaskCompleted RELOCATE -> ApplyRelocationTask (ADR 0039) end-to-end proof on
// REAL infrastructure (testcontainers Kafka via the shared kafkatc helper,
// plus the package's shared testcontainers Postgres with every migration
// applied; never an external broker, never t.Skip):
//
//	fulfillment-execution-shaped TaskCompleted with task_type RELOCATE and
//	the additive fields sku, from_bin, to_bin, quantity, relocation_ref
//	  -> TaskCompletedConsumer (RELOCATE branch) -> ApplyRelocationTask
//	     (CloudEvents id claimed in processed_events in the move's UnitOfWork)
//	  -> RelocateStock: both bins, the source unit and the new destination
//	     unit, the relocations row and the StockRelocated outbox row, all
//	     committed together
//	  -> a redelivery (same CloudEvents id) moves nothing again; a RELOCATE
//	     the ledger can never apply (unknown destination bin) is
//	     dead-lettered with its raw bytes and does not wedge the partition.
//
// Every id carries a per-run suffix, so the test is -count=N safe on the
// shared database.
package usecases_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// relTaskCompleted is a RELOCATE TaskCompleted CloudEvent exactly as
// fulfillment-execution emits it (decision E1: the additive fields ride only
// on RELOCATE tasks).
func relTaskCompleted(t *testing.T, id, taskID, sku, fromBin, toBin string, qty int, ref string) []byte {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"task_id": taskID, "station_id": "station-03", "work_unit_id": "wu-" + taskID,
		"associate_id": "worker-42", "duration_seconds": 61, "task_type": usecases.RelocateTaskType,
		"sku": sku, "from_bin": fromBin, "to_bin": toBin, "quantity": qty, "relocation_ref": ref,
	})
	value, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": id, "source": "/warehouse/fulfillment-execution",
		"type":    "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject": taskID, "time": "2026-10-13T12:00:00Z", "datacontenttype": "application/json",
		"dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":       json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

// relWorld is one seeded relocation fixture: a SKU stowed in a source bin of
// site WH1 and an empty destination bin of the same site.
type relWorld struct {
	sku, from, to, unitID string
}

func relSeed(t *testing.T, env *transferTestEnv, run string, stowed int) relWorld {
	t.Helper()
	// Seven-segment location codes, so the bins resolve to site WH1 (the
	// RELOCATE path derives the site from the source bin).
	w := relWorld{
		sku:  "SKU-RELIT-" + run,
		from: saveBin(t, env, "WH1-STOR-AMB-R"+run+"-01-01-A").String(),
		to:   saveBin(t, env, "WH1-STOR-AMB-R"+run+"-01-01-B").String(),
	}
	stow := &usecases.StowStock{
		Stock: postgres.NewStockRepo(env.pool), Locations: postgres.NewLocationRepo(env.pool),
		Events: events.NewBufferedPublisher(), Clock: memory.SystemClock{},
		Classifications: postgres.NewProductClassificationRepo(env.pool),
		LocationLookup:  &fakeLocationLookup{}, UnitOfWork: postgres.NewUnitOfWork(env.pool),
	}
	unit, err := stow.Execute(context.Background(), shared.SKU(w.sku), mustQty(t, stowed), shared.BinId(w.from))
	if err != nil {
		t.Fatalf("stow %s: %v", w.sku, err)
	}
	w.unitID = unit.ID()
	// The shared database's outbox is drained by other tests' relay passes:
	// leave no unpublished StockRelocated row of this run behind.
	t.Cleanup(func() {
		if _, err := env.pool.Exec(context.Background(), `
			UPDATE outbox_events SET published_at = now()
			WHERE published_at IS NULL
			  AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'sku' = $1`, w.sku); err != nil {
			t.Errorf("mark outbox rows of %s published: %v", w.sku, err)
		}
	})
	return w
}

// relApplyUseCase is the production wiring of the RELOCATE branch (see
// cmd/inventory newApplyRelocationTask) over the real Postgres adapters and
// the real outbox publisher.
func relApplyUseCase(env *transferTestEnv) *usecases.ApplyRelocationTask {
	uow := postgres.NewUnitOfWork(env.pool)
	reservations := postgres.NewReservationRepo(env.pool)
	return &usecases.ApplyRelocationTask{
		Relocate: &usecases.RelocateStock{
			Stock: postgres.NewStockRepo(env.pool), Locations: postgres.NewLocationRepo(env.pool),
			Relocations: postgres.NewRelocationRepo(env.pool),
			Events:      postgres.NewOutboxPublisher(env.pool, outboundkafka.NewPublisher(nil, reservations)),
			Clock:       memory.SystemClock{}, UnitOfWork: uow,
		},
		ProcessedEvents: postgres.NewProcessedEventRepo(env.pool),
		UnitOfWork:      uow,
	}
}

func relClaims(t *testing.T, env *transferTestEnv, eventID string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE consumer = $1 AND event_id = $2`,
		usecases.RelocateTaskConsumer, eventID).Scan(&n); err != nil {
		t.Fatalf("count claims for %s: %v", eventID, err)
	}
	return n
}

func relOnHand(t *testing.T, env *transferTestEnv, sku, bin string) int {
	t.Helper()
	var total int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT COALESCE(SUM(quantity), 0) FROM stock_units WHERE sku = $1 AND bin_id = $2`, sku, bin).Scan(&total); err != nil {
		t.Fatalf("sum stock of %s at %s: %v", sku, bin, err)
	}
	return total
}

// relOutboxData returns the data payloads of every StockRelocated outbox row
// of one SKU.
func relOutboxData(t *testing.T, env *transferTestEnv, sku string) []map[string]any {
	t.Helper()
	rows, err := env.pool.Query(context.Background(), `
		SELECT value FROM outbox_events
		WHERE event_type = 'com.warehouse.wms.inventory-storage.stock.StockRelocated'
		  AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'sku' = $1
		ORDER BY id`, sku)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		var ce struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(value, &ce); err != nil {
			t.Fatalf("decode outbox value: %v", err)
		}
		out = append(out, ce.Data)
	}
	return out
}

func relRelocationRows(t *testing.T, env *transferTestEnv, sku string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM relocations WHERE sku = $1`, sku).Scan(&n); err != nil {
		t.Fatalf("count relocations of %s: %v", sku, err)
	}
	return n
}

// relReadDLQ reads the first dead-lettered message of the env's topic.
func relReadDLQ(t *testing.T, env *transferTestEnv) kafkago.Message {
	t.Helper()
	dlq := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: env.brokers, Topic: env.topic + ".dlq", Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 500 * time.Millisecond,
	})
	defer func() { _ = dlq.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msg, err := dlq.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read the dead-letter topic: %v", err)
	}
	return msg
}

func TestIntegration_TaskCompleted_RelocateMovesTheLedgerExactlyOnceAndEnqueuesStockRelocated(t *testing.T) {
	run := uniqueRun()
	env := cpEnv(t, run)
	w := relSeed(t, env, run, 10)

	group := "relocate-itest-" + run
	consumer := cpConsumer(t, env, group, cpUseCase(env, cpOutboxPublisher(t, env)))
	consumer.Relocate = relApplyUseCase(env)

	good := relTaskCompleted(t, "ce-rel-"+run, "task-rel-"+run, w.sku, w.from, w.to, 4, "plan-"+run+"#1")
	// A move the ledger can never apply: the destination bin does not exist.
	bad := relTaskCompleted(t, "ce-rel-bad-"+run, "task-rel-bad-"+run, w.sku, w.from, "WH1-STOR-AMB-R"+run+"-09-09-Z", 2, "")
	// Same key => same partition: the poison sits BEFORE the good one and the
	// good one is redelivered after it.
	cpPublish(t, env, "task-rel", bad)
	cpPublish(t, env, "task-rel", good)
	cpPublish(t, env, "task-rel", good) // same CloudEvents id: redelivery

	cpRunUntil(t, env, consumer, group, func() bool { return relClaims(t, env, "ce-rel-"+run) == 1 })

	// The ledger moved exactly once: 6 left at the source, 4 at the destination.
	if got := relOnHand(t, env, w.sku, w.from); got != 6 {
		t.Errorf("on hand at the source = %d, want 6", got)
	}
	if got := relOnHand(t, env, w.sku, w.to); got != 4 {
		t.Errorf("on hand at the destination = %d, want 4 (moved exactly once)", got)
	}
	if occ := cpBinOccupied(t, env, w.from); occ != 6 {
		t.Errorf("source bin occupied = %d, want 6", occ)
	}
	if occ := cpBinOccupied(t, env, w.to); occ != 4 {
		t.Errorf("destination bin occupied = %d, want 4", occ)
	}
	if q, r := cpUnitQuantities(t, env, w.unitID); q != 6 || r != 0 {
		t.Errorf("source unit quantity/reserved = %d/%d, want 6/0", q, r)
	}
	if n := relRelocationRows(t, env, w.sku); n != 1 {
		t.Errorf("relocations rows = %d, want 1", n)
	}

	// Exactly one StockRelocated outbox row, carrying the task's fields.
	data := relOutboxData(t, env, w.sku)
	if len(data) != 1 {
		t.Fatalf("StockRelocated outbox rows = %d, want exactly 1", len(data))
	}
	want := map[string]any{
		"site_code": "WH1", "sku": w.sku, "from_bin": w.from, "to_bin": w.to,
		"quantity": float64(4), "reason": "SLOTTING", "source_ref": "plan-" + run + "#1",
	}
	for k, v := range want {
		if data[0][k] != v {
			t.Errorf("StockRelocated data[%s] = %v, want %v", k, data[0][k], v)
		}
	}
	if id, _ := data[0]["relocation_id"].(string); id == "" {
		t.Error("StockRelocated data carries no relocation_id")
	}

	// The un-appliable RELOCATE is dead-lettered with its raw bytes, never
	// claimed (its transaction rolled back).
	if n := relClaims(t, env, "ce-rel-bad-"+run); n != 0 {
		t.Errorf("the rejected RELOCATE left %d claims, want 0", n)
	}
	msg := relReadDLQ(t, env)
	if string(msg.Value) != string(bad) {
		t.Fatal("dead-lettered value differs from the rejected RELOCATE completion")
	}
	headers := map[string]string{}
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["x-dlq-source-topic"] != env.topic || headers["x-dlq-error"] == "" {
		t.Fatalf("DLQ headers = %v", headers)
	}
}
