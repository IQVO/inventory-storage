//go:build integration

// TaskCompleted REPLENISH -> ApplyReplenishTask (ADR 0041) on REAL
// infrastructure (testcontainers Kafka and Postgres, never an external
// broker, never t.Skip):
//
//	TaskCompleted task_type REPLENISH + sku, from_bin, to_bin, quantity,
//	request_ref -> TaskCompletedConsumer (REPLENISH branch) ->
//	ApplyReplenishTask (id claimed under "task-completed-replenish" in the
//	move's UnitOfWork) -> RelocateStock with reason REPLENISHMENT and
//	source_ref = request_ref: both bins, the stock units, the relocations
//	row, the StockRelocated outbox row and the two BinStockLevelChanged
//	outbox rows (reason REPLENISHMENT, source bin then destination bin), all
//	committed together; a redelivery moves nothing; a REPLENISH without
//	request_ref is dead-lettered. A failure inside the unit of work rolls the
//	claim, the move and the outbox back together.
//
// Every id carries a per-run suffix, so the tests are -count=N safe on the
// shared database. Run with -timeout 300s.
package usecases_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

const replLevelType = "com.warehouse.wms.inventory-storage.stock.BinStockLevelChanged"

// replTaskCompleted is a REPLENISH TaskCompleted CloudEvent as
// fulfillment-execution emits it: the RELOCATE shape with request_ref in
// place of relocation_ref (an empty ref omits the field).
func replTaskCompleted(t *testing.T, id, taskID, sku, fromBin, toBin string, qty int, requestRef string) []byte {
	t.Helper()
	data := map[string]any{
		"task_id": taskID, "station_id": "station-03", "work_unit_id": "wu-" + taskID,
		"associate_id": "worker-42", "duration_seconds": 61, "task_type": usecases.ReplenishTaskType,
		"sku": sku, "from_bin": fromBin, "to_bin": toBin, "quantity": qty,
	}
	if requestRef != "" {
		data["request_ref"] = requestRef
	}
	payload, _ := json.Marshal(data)
	value, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": id, "source": "/warehouse/fulfillment-execution",
		"type":    "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject": taskID, "time": "2026-10-17T12:00:00Z", "datacontenttype": "application/json",
		"dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":       json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

// replApplyUseCase is the production wiring of the REPLENISH branch (see
// cmd/inventory newApplyReplenishTask) over the real Postgres adapters, the
// real outbox publisher and the bin-level emitter. encoders default to the
// production integration-topic encoder.
func replApplyUseCase(env *transferTestEnv, encoders ...outboundkafka.Encoder) *usecases.ApplyReplenishTask {
	uow := postgres.NewUnitOfWork(env.pool)
	if len(encoders) == 0 {
		encoders = []outboundkafka.Encoder{outboundkafka.NewPublisher(nil, postgres.NewReservationRepo(env.pool))}
	}
	events := postgres.NewOutboxPublisher(env.pool, encoders...)
	locations := postgres.NewLocationRepo(env.pool)
	return &usecases.ApplyReplenishTask{
		Relocate: &usecases.RelocateStock{
			Stock: postgres.NewStockRepo(env.pool), Locations: locations,
			Relocations: postgres.NewRelocationRepo(env.pool), Events: events,
			Clock: memory.SystemClock{}, UnitOfWork: uow,
			Levels: &usecases.BinLevelEmitter{
				Ledger: postgres.NewBinLevelLedger(env.pool), Locations: locations, Events: events, Clock: memory.SystemClock{},
			},
		},
		ProcessedEvents: postgres.NewProcessedEventRepo(env.pool),
		UnitOfWork:      uow,
	}
}

func replClaims(t *testing.T, env *transferTestEnv, consumer, eventID string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE consumer = $1 AND event_id = $2`, consumer, eventID).Scan(&n); err != nil {
		t.Fatalf("count claims for %s: %v", eventID, err)
	}
	return n
}

// replOutboxData returns the data payloads of every outbox row of one event
// type and SKU, oldest first.
func replOutboxData(t *testing.T, env *transferTestEnv, eventType, sku string) []map[string]any {
	t.Helper()
	rows, err := env.pool.Query(context.Background(), `
		SELECT value FROM outbox_events
		WHERE event_type = $1 AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'sku' = $2
		ORDER BY id`, eventType, sku)
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

func replRelocationReason(t *testing.T, env *transferTestEnv, sku string) (reason, sourceRef string) {
	t.Helper()
	if err := env.pool.QueryRow(context.Background(),
		`SELECT reason, source_ref FROM relocations WHERE sku = $1`, sku).Scan(&reason, &sourceRef); err != nil {
		t.Fatalf("read the relocation of %s: %v", sku, err)
	}
	return reason, sourceRef
}

func replAssertMatches(t *testing.T, label string, got map[string]any, want map[string]any) {
	t.Helper()
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s data[%s] = %v, want %v", label, k, got[k], v)
		}
	}
}

func TestIntegration_TaskCompleted_ReplenishMovesTheLedgerExactlyOnceAsReplenishment(t *testing.T) {
	run := uniqueRun()
	env := cpEnv(t, run)
	w := relSeed(t, env, run, 10)
	requestRef := "repl-" + run

	group := "replenish-itest-" + run
	consumer := cpConsumer(t, env, group, cpUseCase(env, cpOutboxPublisher(t, env)))
	consumer.Replenish = replApplyUseCase(env)

	good := replTaskCompleted(t, "ce-repl-"+run, "task-repl-"+run, w.sku, w.from, w.to, 4, requestRef)
	// No request_ref: deterministic poison, there is no task-id fallback.
	bad := replTaskCompleted(t, "ce-repl-bad-"+run, "task-repl-bad-"+run, w.sku, w.from, w.to, 2, "")
	// Same key => same partition: the poison sits BEFORE the good one and the
	// good one is redelivered after it.
	cpPublish(t, env, "task-repl", bad)
	cpPublish(t, env, "task-repl", good)
	cpPublish(t, env, "task-repl", good) // same CloudEvents id: redelivery

	cpRunUntil(t, env, consumer, group, func() bool {
		return replClaims(t, env, usecases.ReplenishTaskConsumer, "ce-repl-"+run) == 1
	})

	if got := relOnHand(t, env, w.sku, w.from); got != 6 {
		t.Errorf("on hand at the source = %d, want 6", got)
	}
	if got := relOnHand(t, env, w.sku, w.to); got != 4 {
		t.Errorf("on hand at the destination = %d, want 4 (moved exactly once)", got)
	}
	if n := relRelocationRows(t, env, w.sku); n != 1 {
		t.Errorf("relocations rows = %d, want 1", n)
	}
	if reason, ref := replRelocationReason(t, env, w.sku); reason != "REPLENISHMENT" || ref != requestRef {
		t.Errorf("persisted relocation reason %q source_ref %q, want REPLENISHMENT / %s", reason, ref, requestRef)
	}
	if n := replClaims(t, env, usecases.RelocateTaskConsumer, "ce-repl-"+run); n != 0 {
		t.Errorf("the REPLENISH id was claimed under the RELOCATE consumer %d times, want 0", n)
	}

	replAssertOutbox(t, env, w, requestRef)

	if n := replClaims(t, env, usecases.ReplenishTaskConsumer, "ce-repl-bad-"+run); n != 0 {
		t.Errorf("the rejected REPLENISH left %d claims, want 0", n)
	}
	if msg := relReadDLQ(t, env); string(msg.Value) != string(bad) {
		t.Fatal("dead-lettered value differs from the REPLENISH completion without request_ref")
	}
}

// replAssertOutbox checks the three events of the one applied replenishment.
func replAssertOutbox(t *testing.T, env *transferTestEnv, w relWorld, requestRef string) {
	t.Helper()
	moved := replOutboxData(t, env, "com.warehouse.wms.inventory-storage.stock.StockRelocated", w.sku)
	if len(moved) != 1 {
		t.Fatalf("StockRelocated outbox rows = %d, want exactly 1", len(moved))
	}
	replAssertMatches(t, "StockRelocated", moved[0], map[string]any{
		"site_code": "WH1", "sku": w.sku, "from_bin": w.from, "to_bin": w.to,
		"quantity": float64(4), "reason": "REPLENISHMENT", "source_ref": requestRef,
	})

	levels := replOutboxData(t, env, replLevelType, w.sku)
	if len(levels) != 2 {
		t.Fatalf("BinStockLevelChanged outbox rows = %d, want 2 (source then destination)", len(levels))
	}
	replAssertMatches(t, "source level", levels[0], map[string]any{
		"bin": w.from, "on_hand": float64(6), "reserved": float64(0), "reason": "REPLENISHMENT",
	})
	replAssertMatches(t, "destination level", levels[1], map[string]any{
		"bin": w.to, "on_hand": float64(4), "reserved": float64(0), "reason": "REPLENISHMENT",
	})
}

// failingMoveEncoder fails to encode the destination bin's level event: the
// outbox insert is the LAST write of the unit of work, so by then the id
// claim, the stock move, the bins and the relocations row have all been
// written and must roll back with it.
type failingMoveEncoder struct{ bin string }

var errReplEncode = errors.New("replenish encode failed")

func (f failingMoveEncoder) Encode(_ context.Context, e shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	if ev, ok := e.(shared.BinStockLevelChanged); ok && ev.BinID.String() == f.bin {
		return nil, errReplEncode
	}
	return nil, nil
}

func TestIntegration_ApplyReplenishTask_ClaimMoveAndOutboxRollBackTogether(t *testing.T) {
	run := uniqueRun()
	env := transferSharedEnv(t)
	w := relSeed(t, env, run, 10)
	in := usecases.ReplenishTaskCompletion{
		EventID: "ce-repl-atomic-" + run, TaskID: "task-atomic-" + run, TaskType: usecases.ReplenishTaskType,
		SKU: w.sku, FromBin: w.from, ToBin: w.to, Quantity: intPtr(4), RequestRef: "repl-atomic-" + run,
	}

	failing := replApplyUseCase(env, failingMoveEncoder{bin: w.to}, outboundkafka.NewPublisher(nil, postgres.NewReservationRepo(env.pool)))
	if _, err := failing.Execute(context.Background(), in); !errors.Is(err, errReplEncode) {
		t.Fatalf("err = %v, want the encoder's error (a transient failure, not a malformed task)", err)
	}
	assertReplenishRolledBack(t, env, w, in.EventID)

	// The un-claimed id is applied normally on the retry: nothing is stuck.
	res, err := replApplyUseCase(env).Execute(context.Background(), in)
	if err != nil || res.Outcome != usecases.ReplenishApplied {
		t.Fatalf("retry: outcome %q err %v, want APPLIED", res.Outcome, err)
	}
	if relOnHand(t, env, w.sku, w.from) != 6 || relOnHand(t, env, w.sku, w.to) != 4 {
		t.Fatal("the retry did not move the stock")
	}
	if n := replClaims(t, env, usecases.ReplenishTaskConsumer, in.EventID); n != 1 {
		t.Fatalf("claims after the retry = %d, want 1", n)
	}
}

func assertReplenishRolledBack(t *testing.T, env *transferTestEnv, w relWorld, eventID string) {
	t.Helper()
	if n := replClaims(t, env, usecases.ReplenishTaskConsumer, eventID); n != 0 {
		t.Errorf("claims after the rollback = %d, want 0 (the id must stay un-claimed)", n)
	}
	if relOnHand(t, env, w.sku, w.from) != 10 || relOnHand(t, env, w.sku, w.to) != 0 {
		t.Error("stock moved although the transaction failed")
	}
	if occ := cpBinOccupied(t, env, w.from); occ != 10 {
		t.Errorf("source bin occupied = %d after the rollback, want 10", occ)
	}
	if occ := cpBinOccupied(t, env, w.to); occ != 0 {
		t.Errorf("destination bin occupied = %d after the rollback, want 0", occ)
	}
	if n := relRelocationRows(t, env, w.sku); n != 0 {
		t.Errorf("relocations rows after the rollback = %d, want 0", n)
	}
	if n := len(replOutboxData(t, env, "com.warehouse.wms.inventory-storage.stock.StockRelocated", w.sku)); n != 0 {
		t.Errorf("StockRelocated outbox rows after the rollback = %d, want 0", n)
	}
	if n := len(replOutboxData(t, env, replLevelType, w.sku)); n != 0 {
		t.Errorf("BinStockLevelChanged outbox rows after the rollback = %d, want 0", n)
	}
}
