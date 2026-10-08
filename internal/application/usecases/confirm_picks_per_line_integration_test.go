//go:build integration

// Per-line confirm-pick (decision 18, ADR 0036) end-to-end proof on REAL
// infrastructure (testcontainers Kafka via the shared kafkatc helper, and the
// package's shared testcontainers Postgres with every migration applied; never
// an external broker, never t.Skip):
//
//	order-management-shaped reservations, one per order LINE, each stored with
//	its line_no through the real ReserveStock + Postgres
//	  -> fulfillment-execution-shaped TaskCompleted events that carry line_no
//	  -> TaskCompletedConsumer -> ConfirmPicksForOrder (per-line path)
//	  -> ONLY the picked lines are CONFIRMED (stock decremented once, bin
//	     capacity released, StockPicked in the outbox once); a line whose pick
//	     has not arrived stays ACTIVE and reserved; no order_pick_progress row.
//
// Every id carries a per-run suffix, so the tests are -count=N safe on the
// shared database.
package usecases_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// cpTaskCompletedLine is cpTaskCompleted with the additive line_no.
func cpTaskCompletedLine(t *testing.T, id, taskID, taskType, orderRef string, lineNo int) []byte {
	t.Helper()
	base := cpTaskCompleted(t, id, taskID, taskType, orderRef)
	var ce map[string]json.RawMessage
	if err := json.Unmarshal(base, &ce); err != nil {
		t.Fatalf("unmarshal CE: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(ce["data"], &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	data["line_no"] = lineNo
	patched, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	ce["data"] = patched
	out, err := json.Marshal(ce)
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return out
}

// cpAssertPicked checks one line ended up physically picked exactly once.
func cpAssertPicked(t *testing.T, env *transferTestEnv, l cpLine, when string) {
	t.Helper()
	if got := cpReservationStatus(t, env, l.reservationID); got != "CONFIRMED" {
		t.Errorf("%s: %s reservation = %s, want CONFIRMED", when, l.sku, got)
	}
	if q, r := cpUnitQuantities(t, env, l.unitID); q != l.stowed-l.reserved || r != 0 {
		t.Errorf("%s: %s unit quantity/reserved = %d/%d, want %d/0 (decremented exactly once)", when, l.sku, q, r, l.stowed-l.reserved)
	}
	if occ := cpBinOccupied(t, env, l.binID); occ != l.stowed-l.reserved {
		t.Errorf("%s: %s bin occupied = %d, want %d (capacity released)", when, l.sku, occ, l.stowed-l.reserved)
	}
	if n := cpOutboxRows(t, env, "StockPicked", l.reservationID); n != 1 {
		t.Errorf("%s: %s StockPicked outbox rows = %d, want exactly 1", when, l.sku, n)
	}
}

// cpAssertStillReserved checks one line is untouched: ACTIVE, stock reserved,
// no StockPicked.
func cpAssertStillReserved(t *testing.T, env *transferTestEnv, l cpLine, when string) {
	t.Helper()
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

// The mixed order: lines 1 and 3 arrive, line 2 does not. ONLY 1 and 3 are
// confirmed; line 2 stays ACTIVE (this is the whole point of decision 18: under
// ADR 0035's counting the second pick would have confirmed all three). Then
// line 2's own pick confirms it, and redelivery plus an extra event change
// nothing.
func TestIntegration_TaskCompleted_PerLine_MixedOrderConfirmsOnlyThePickedLines(t *testing.T) {
	run := uniqueRun()
	env := cpEnv(t, run)
	order := "order-cp-line-" + run
	line1 := cpSeedLineNo(t, env, run, "L1", order, ptr(1), 10, 4, nil)
	line2 := cpSeedLineNo(t, env, run, "L2", order, ptr(2), 8, 3, nil)
	line3 := cpSeedLineNo(t, env, run, "L3", order, ptr(3), 6, 2, nil)
	other := cpSeedLineNo(t, env, run, "OL", "order-other-"+run, ptr(1), 5, 2, nil)

	uc := cpUseCase(env, cpOutboxPublisher(t, env))
	group := "confirm-pick-line-itest-" + run
	consumer := cpConsumer(t, env, group, uc)

	// Lines 1 and 3 complete (line 1 redelivered), plus a PACK task and a
	// PICK for a line the order never reserved.
	cpPublish(t, env, "task-1", cpTaskCompletedLine(t, "ce-l1-"+run, "task-1", "PICK", order, 1))
	cpPublish(t, env, "task-1", cpTaskCompletedLine(t, "ce-l1-"+run, "task-1", "PICK", order, 1))
	cpPublish(t, env, "task-p", cpTaskCompletedLine(t, "ce-pack-"+run, "task-p", "PACK", order, 2))
	cpPublish(t, env, "task-9", cpTaskCompletedLine(t, "ce-l9-"+run, "task-9", "PICK", order, 9))
	cpPublish(t, env, "task-3", cpTaskCompletedLine(t, "ce-l3-"+run, "task-3", "PICK", order, 3))
	cpRunUntil(t, env, consumer, group, func() bool {
		return cpClaims(t, env, "ce-l1-"+run) == 1 && cpClaims(t, env, "ce-l3-"+run) == 1 && cpClaims(t, env, "ce-l9-"+run) == 1
	})

	cpAssertPicked(t, env, line1, "after lines 1 and 3")
	cpAssertPicked(t, env, line3, "after lines 1 and 3")
	cpAssertStillReserved(t, env, line2, "after lines 1 and 3")
	cpAssertStillReserved(t, env, other, "after lines 1 and 3")
	if n, ok := cpProgress(t, env, order); ok {
		t.Errorf("the per-line path left an order_pick_progress row (%d)", n)
	}
	if n := cpClaims(t, env, "ce-pack-"+run); n != 0 {
		t.Errorf("an ignored PACK event claimed its id (%d rows)", n)
	}

	// Line 2's own pick arrives later and confirms exactly line 2.
	cpPublish(t, env, "task-2", cpTaskCompletedLine(t, "ce-l2-"+run, "task-2", "PICK", order, 2))
	cpRunUntil(t, env, consumer, group, func() bool { return cpClaims(t, env, "ce-l2-"+run) == 1 })
	for _, l := range []cpLine{line1, line2, line3} {
		cpAssertPicked(t, env, l, "after all three lines")
	}
	cpAssertStillReserved(t, env, other, "after all three lines")

	// Redelivery of line 3, then an extra event for line 1 under a NEW id: nothing changes.
	cpPublish(t, env, "task-3", cpTaskCompletedLine(t, "ce-l3-"+run, "task-3", "PICK", order, 3))
	cpPublish(t, env, "task-1", cpTaskCompletedLine(t, "ce-l1x-"+run, "task-1", "PICK", order, 1))
	cpRunUntil(t, env, consumer, group, func() bool { return cpClaims(t, env, "ce-l1x-"+run) == 1 })
	for _, l := range []cpLine{line1, line2, line3} {
		cpAssertPicked(t, env, l, "after redelivery and an extra event")
	}
	if n, ok := cpProgress(t, env, order); ok {
		t.Errorf("order_pick_progress = %d, want no row on the pure per-line path", n)
	}
}

// Reservations made before line_no existed (NULL) keep the ADR 0035 last-pick
// counting even when the events carry line_no: the backward-compatible
// fallback, end to end.
func TestIntegration_TaskCompleted_PerLine_LegacyReservationsFallBackToCounting(t *testing.T) {
	run := uniqueRun()
	env := cpEnv(t, run)
	order := "order-cp-legacy-" + run
	a := cpSeedLine(t, env, run, "LA", order, 10, 4, nil)
	b := cpSeedLine(t, env, run, "LB", order, 8, 3, nil)

	uc := cpUseCase(env, cpOutboxPublisher(t, env))
	group := "confirm-pick-legacy-itest-" + run
	consumer := cpConsumer(t, env, group, uc)

	cpPublish(t, env, "task-1", cpTaskCompletedLine(t, "ce-a-"+run, "task-1", "PICK", order, 1))
	cpRunUntil(t, env, consumer, group, func() bool { return cpClaims(t, env, "ce-a-"+run) == 1 })
	cpAssertStillReserved(t, env, a, "after 1 of 2 picks")
	cpAssertStillReserved(t, env, b, "after 1 of 2 picks")
	if n, ok := cpProgress(t, env, order); !ok || n != 1 {
		t.Fatalf("order_pick_progress = %d (row %v), want 1 (the fallback counts)", n, ok)
	}

	cpPublish(t, env, "task-2", cpTaskCompletedLine(t, "ce-b-"+run, "task-2", "PICK", order, 2))
	cpRunUntil(t, env, consumer, group, func() bool { return cpClaims(t, env, "ce-b-"+run) == 1 })
	cpAssertPicked(t, env, a, "after the last pick")
	cpAssertPicked(t, env, b, "after the last pick")
}

// A failure while confirming the line rolls the claim, the stock/bin/
// reservation writes and the outbox row back together; the redelivery of the
// same event converges.
func TestIntegration_TaskCompleted_PerLine_FailureRollsBackEverythingAndRetryConverges(t *testing.T) {
	ctx := context.Background()
	run := uniqueRun()
	env := cpEnv(t, run)
	order := "order-cp-line-rb-" + run
	line1 := cpSeedLineNo(t, env, run, "R1", order, ptr(1), 10, 4, nil)
	line2 := cpSeedLineNo(t, env, run, "R2", order, ptr(2), 8, 3, nil)
	eventID := "ce-rb-" + run
	in := usecases.PickCompletion{EventID: eventID, TaskID: "task-rb", TaskType: "PICK", OrderRef: order, LineNo: ptr(1)}

	broken := cpUseCase(env, postgres.NewOutboxPublisher(env.pool, failingTransferEncoder{}))
	var outboxBefore int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxBefore); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	_, err := broken.Execute(ctx, in)
	if err == nil || errors.Is(err, usecases.ErrMalformedPickCompletion) {
		t.Fatalf("err = %v, want a transient failure so the consumer retries", err)
	}
	cpAssertStillReserved(t, env, line1, "after a rolled-back handling")
	cpAssertStillReserved(t, env, line2, "after a rolled-back handling")
	if n := cpClaims(t, env, eventID); n != 0 {
		t.Fatalf("claim survived the rollback (%d rows): the redelivery would be skipped as a duplicate", n)
	}
	var outboxAfter int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxAfter); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxAfter != outboxBefore {
		t.Fatalf("outbox rows %d -> %d, want unchanged", outboxBefore, outboxAfter)
	}

	healthy := cpUseCase(env, cpOutboxPublisher(t, env))
	got, err := healthy.Execute(ctx, in)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got.Outcome != usecases.PicksLineSettled || got.Confirmed != 1 {
		t.Fatalf("retry = %+v, want LINE_SETTLED with 1 confirmed", got)
	}
	cpAssertPicked(t, env, line1, "after the retry")
	cpAssertStillReserved(t, env, line2, "after the retry")
	if n := cpClaims(t, env, eventID); n != 1 {
		t.Errorf("claims after the retry = %d, want 1", n)
	}
}
