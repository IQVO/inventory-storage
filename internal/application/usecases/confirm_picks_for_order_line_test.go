package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// Per-line confirm-pick (decision 18, ADR 0036): a TaskCompleted that names
// its line_no confirms exactly the ACTIVE reservation(s) of
// (demand_ref = order_ref, line_no). Events or reservations without a line keep
// the ADR 0035 last-pick counting.

// reserveNumbered stows a fresh SKU named label and reserves 4 of its 10 units
// for demandRef as order line lineNo (nil = a legacy reservation without one).
func (f *confirmPicksFixture) reserveNumbered(t *testing.T, label, demandRef string, lineNo *int, timeout time.Duration) {
	t.Helper()
	stowUnit(t, f.e, label, "BIN-"+label, 20, 10)
	reserve := &usecases.ReserveStock{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock, Timeout: timeout}
	res, err := reserve.ExecuteForLine(context.Background(), mustSKU(t, label), mustQty(t, 4), demandRef, lineNo)
	if err != nil {
		t.Fatalf("reserve %s: %v", label, err)
	}
	f.ids[label] = res.ID()
}

// reserveLines reserves one reservation per line 1..n, labelled "<demandRef>-L<n>".
func (f *confirmPicksFixture) reserveLines(t *testing.T, demandRef string, n int) []string {
	t.Helper()
	labels := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		label := fmt.Sprintf("%s-L%d", demandRef, i)
		f.reserveNumbered(t, label, demandRef, ptr(i), time.Hour)
		labels = append(labels, label)
	}
	return labels
}

func lineCompleted(eventID, orderRef string, lineNo int) usecases.PickCompletion {
	return usecases.PickCompletion{EventID: eventID, TaskID: "task-" + eventID, TaskType: "PICK", OrderRef: orderRef, LineNo: ptr(lineNo)}
}

func (f *confirmPicksFixture) pickLine(t *testing.T, orderRef string, lineNo int) usecases.ConfirmPicksResult {
	t.Helper()
	f.nextID++
	got, err := f.uc.Execute(context.Background(), lineCompleted(fmt.Sprintf("ce-%d", f.nextID), orderRef, lineNo))
	if err != nil {
		t.Fatalf("pick of line %d for %s: %v", lineNo, orderRef, err)
	}
	return got
}

func (f *confirmPicksFixture) assertStatuses(t *testing.T, want map[string]reservation.Status) {
	t.Helper()
	for label, s := range want {
		if got := f.status(t, label); got != s {
			t.Errorf("%s status = %s, want %s", label, got, s)
		}
	}
}

// The whole point of decision 18: lines 1 and 3 are picked, line 2 is not.
// ONLY 1 and 3 are confirmed; line 2 stays ACTIVE with its stock still
// reserved. Under ADR 0035's counting the second pick would have confirmed
// every line, including the unpicked one.
func TestConfirmPicksForOrder_PerLine_MixedOrderConfirmsOnlyThePickedLines(t *testing.T) {
	f := newConfirmPicksFixture(t)
	labels := f.reserveLines(t, "order-1", 3)
	f.reserveNumbered(t, "OTHER-ORDER", "order-2", ptr(1), time.Hour)
	pickedBefore := f.countEvents("StockPicked")

	first := f.pickLine(t, "order-1", 1)
	if first.Outcome != usecases.PicksLineSettled || first.Confirmed != 1 {
		t.Fatalf("line 1 result = %+v, want LINE_SETTLED with 1 confirmed", first)
	}
	third := f.pickLine(t, "order-1", 3)
	if third.Outcome != usecases.PicksLineSettled || third.Confirmed != 1 {
		t.Fatalf("line 3 result = %+v, want LINE_SETTLED with 1 confirmed", third)
	}

	f.assertStatuses(t, map[string]reservation.Status{
		labels[0]: reservation.StatusConfirmed, labels[1]: reservation.StatusActive,
		labels[2]: reservation.StatusConfirmed, "OTHER-ORDER": reservation.StatusActive,
	})
	if n := f.countEvents("StockPicked") - pickedBefore; n != 2 {
		t.Errorf("StockPicked raised %d times, want 2 (lines 1 and 3)", n)
	}
	if u := f.usable(t, labels[1]); u != 6 {
		t.Errorf("line 2 usable = %d, want 6 (still merely reserved)", u)
	}
	for _, i := range []int{0, 2} {
		stock, _ := f.e.Stock.FindBySKU(context.Background(), mustSKU(t, labels[i]))
		if len(stock) != 1 || stock[0].Quantity().Int() != 6 {
			t.Errorf("%s stock = %+v, want one unit of 6 (10 - 4 picked)", labels[i], stock)
		}
	}
	if len(f.progress.counts) != 0 {
		t.Errorf("the pure per-line path wrote order_pick_progress: %v", f.progress.counts)
	}
	if f.metrics.counts["confirmed"] != 2 {
		t.Errorf("metrics = %v, want confirmed=2", f.metrics.counts)
	}
}

// Order of arrival does not matter, and each line is confirmed by its own pick
// only: 3 then 1 then 2.
func TestConfirmPicksForOrder_PerLine_AnyArrivalOrder(t *testing.T) {
	f := newConfirmPicksFixture(t)
	labels := f.reserveLines(t, "order-1", 3)

	f.pickLine(t, "order-1", 3)
	f.assertStatuses(t, map[string]reservation.Status{
		labels[0]: reservation.StatusActive, labels[1]: reservation.StatusActive, labels[2]: reservation.StatusConfirmed,
	})
	f.pickLine(t, "order-1", 1)
	f.pickLine(t, "order-1", 2)
	f.assertAllStatus(t, labels, reservation.StatusConfirmed)
}

// A line whose first attempt was revoked and then re-reserved has two
// reservations for the same line; the pick confirms the ACTIVE retry and
// skips (counts) the REVOKED attempt.
func TestConfirmPicksForOrder_PerLine_RevokedEarlierAttemptAndActiveRetry(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	f.reserveNumbered(t, "ATTEMPT-1", "order-1", ptr(3), time.Hour)
	revoke := &usecases.RevokeReservation{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock}
	if err := revoke.Execute(ctx, f.ids["ATTEMPT-1"]); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	f.reserveNumbered(t, "ATTEMPT-2", "order-1", ptr(3), time.Hour)
	f.reserveNumbered(t, "LINE-1", "order-1", ptr(1), time.Hour)

	got := f.pickLine(t, "order-1", 3)
	want := usecases.ConfirmPicksResult{Outcome: usecases.PicksLineSettled, Confirmed: 1, Revoked: 1}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	f.assertStatuses(t, map[string]reservation.Status{
		"ATTEMPT-1": reservation.StatusRevoked, "ATTEMPT-2": reservation.StatusConfirmed, "LINE-1": reservation.StatusActive,
	})
}

// A defensive case: should a line ever hold two ACTIVE reservations, the pick
// confirms every ACTIVE one for that line (contract), and still nothing else.
func TestConfirmPicksForOrder_PerLine_EveryActiveReservationOfTheLineIsConfirmed(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveNumbered(t, "DUP-A", "order-1", ptr(2), time.Hour)
	f.reserveNumbered(t, "DUP-B", "order-1", ptr(2), time.Hour)
	f.reserveNumbered(t, "LINE-1", "order-1", ptr(1), time.Hour)

	got := f.pickLine(t, "order-1", 2)
	if got.Confirmed != 2 {
		t.Fatalf("result = %+v, want 2 confirmed", got)
	}
	f.assertStatuses(t, map[string]reservation.Status{
		"DUP-A": reservation.StatusConfirmed, "DUP-B": reservation.StatusConfirmed, "LINE-1": reservation.StatusActive,
	})
}

// Reservations made before line_no existed (NULL) keep the ADR 0035 last-pick
// counting when the event names a line none of them carries.
func TestConfirmPicksForOrder_PerLine_LegacyReservationsFallBackToCounting(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveNumbered(t, "LEGACY-A", "order-1", nil, time.Hour)
	f.reserveNumbered(t, "LEGACY-B", "order-1", nil, time.Hour)

	first := f.pickLine(t, "order-1", 1)
	if want := (usecases.ConfirmPicksResult{Outcome: usecases.PicksAwaiting, PicksSeen: 1, PicksNeeded: 2}); first != want {
		t.Fatalf("first = %+v, want %+v", first, want)
	}
	f.assertAllStatus(t, []string{"LEGACY-A", "LEGACY-B"}, reservation.StatusActive)

	second := f.pickLine(t, "order-1", 2)
	if second.Outcome != usecases.PicksProcessed || second.Confirmed != 2 || second.PicksSeen != 2 {
		t.Fatalf("second = %+v, want PROCESSED, 2 confirmed on pick 2 of 2", second)
	}
	f.assertAllStatus(t, []string{"LEGACY-A", "LEGACY-B"}, reservation.StatusConfirmed)
}

// A mixed order (one line-aware reservation, two legacy ones): the line-aware
// pick is purely per-line and counts nothing; a pick for a line no reservation
// carries counts against the LEGACY reservations only, and confirms only them.
func TestConfirmPicksForOrder_PerLine_MixedLegacyAndLineAwareOrder(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveNumbered(t, "AWARE-1", "order-1", ptr(1), time.Hour)
	f.reserveNumbered(t, "LEGACY-A", "order-1", nil, time.Hour)
	f.reserveNumbered(t, "LEGACY-B", "order-1", nil, time.Hour)

	if got := f.pickLine(t, "order-1", 1); got.Outcome != usecases.PicksLineSettled || got.Confirmed != 1 {
		t.Fatalf("line 1 = %+v, want LINE_SETTLED with 1 confirmed", got)
	}
	if len(f.progress.counts) != 0 {
		t.Fatalf("the line-aware pick counted: %v", f.progress.counts)
	}
	f.assertStatuses(t, map[string]reservation.Status{
		"AWARE-1": reservation.StatusConfirmed, "LEGACY-A": reservation.StatusActive, "LEGACY-B": reservation.StatusActive,
	})

	// needed counts the two legacy reservations, not the already confirmed line-aware one.
	if got := f.pickLine(t, "order-1", 2); got.Outcome != usecases.PicksAwaiting || got.PicksSeen != 1 || got.PicksNeeded != 2 {
		t.Fatalf("line 2 = %+v, want AWAITING 1 of 2 (legacy reservations only)", got)
	}
	if got := f.pickLine(t, "order-1", 3); got.Outcome != usecases.PicksProcessed || got.Confirmed != 2 {
		t.Fatalf("line 3 = %+v, want PROCESSED with the 2 legacy reservations confirmed", got)
	}
	f.assertAllStatus(t, []string{"AWARE-1", "LEGACY-A", "LEGACY-B"}, reservation.StatusConfirmed)
}

// A line the order never reserved (all of its reservations carry a line, none
// this one) confirms nothing and writes nothing.
func TestConfirmPicksForOrder_PerLine_UnknownLineConfirmsNothing(t *testing.T) {
	f := newConfirmPicksFixture(t)
	labels := f.reserveLines(t, "order-1", 2)

	got := f.pickLine(t, "order-1", 9)
	if got.Outcome != usecases.PicksLineNotFound || got.Confirmed != 0 {
		t.Fatalf("result = %+v, want LINE_NOT_FOUND and nothing confirmed", got)
	}
	f.assertAllStatus(t, labels, reservation.StatusActive)
	if len(f.progress.counts) != 0 {
		t.Fatalf("progress rows = %v, want none", f.progress.counts)
	}
	if len(f.claims.claimed) != 1 {
		t.Fatalf("the event must still be claimed (settled), got %v", f.claims.claimed)
	}
}

func TestConfirmPicksForOrder_PerLine_NoReservationsIsASuccessfulNoOp(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveNumbered(t, "LINE-1", "order-1", ptr(1), time.Hour)

	got := f.pickLine(t, "transfer-or-non-inventory-order", 1)
	if got.Outcome != usecases.PicksNoReservations {
		t.Fatalf("outcome = %q, want %q", got.Outcome, usecases.PicksNoReservations)
	}
	if s := f.status(t, "LINE-1"); s != reservation.StatusActive {
		t.Fatalf("an unrelated order's reservation changed: %s", s)
	}
}

// An expired line is skipped, never confirmed, never an error: here the
// reservation storage has already resolved to EXPIRED (lazy expiry, ADR 0003).
// The next test covers one that is past its timeout but still ACTIVE.
func TestConfirmPicksForOrder_PerLine_ExpiredLineIsSkippedAndCounted(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	f.reserveNumbered(t, "STORED-EXPIRED", "order-1", ptr(1), time.Minute)
	f.reserveNumbered(t, "LIVE", "order-1", ptr(3), time.Hour)
	f.e.Clock.Advance(2 * time.Minute)
	// A read resolves line 1's timeout to EXPIRED in storage.
	resolve := &usecases.GetReservationsByDemandRef{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock}
	if _, err := resolve.Execute(ctx, "order-1"); err != nil {
		t.Fatalf("lazy expiry: %v", err)
	}

	got1 := f.pickLine(t, "order-1", 1)
	if got1.Outcome != usecases.PicksLineSettled || got1.Expired != 1 || got1.Confirmed != 0 {
		t.Fatalf("line 1 = %+v, want LINE_SETTLED with 1 expired", got1)
	}
	got3 := f.pickLine(t, "order-1", 3)
	if got3.Confirmed != 1 {
		t.Fatalf("line 3 = %+v, want 1 confirmed", got3)
	}
	f.assertStatuses(t, map[string]reservation.Status{
		"STORED-EXPIRED": reservation.StatusExpired, "LIVE": reservation.StatusConfirmed,
	})
	if f.metrics.counts["expired"] != 1 || f.metrics.counts["confirmed"] != 1 {
		t.Errorf("metrics = %v, want expired=1 confirmed=1", f.metrics.counts)
	}
}

func TestConfirmPicksForOrder_PerLine_ActiveLinePastItsTimeoutIsExpiredNotConfirmed(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveNumbered(t, "LATE", "order-1", ptr(1), time.Minute)
	f.e.Clock.Advance(2 * time.Minute)

	got := f.pickLine(t, "order-1", 1)
	if got.Outcome != usecases.PicksLineSettled || got.Expired != 1 || got.Confirmed != 0 {
		t.Fatalf("result = %+v, want LINE_SETTLED with 1 expired, 0 confirmed", got)
	}
	if s := f.status(t, "LATE"); s != reservation.StatusExpired {
		t.Fatalf("status = %s, want EXPIRED (stock returned to usable, not picked)", s)
	}
	if u := f.usable(t, "LATE"); u != 10 {
		t.Fatalf("usable = %d, want 10 (expiry returns the stock)", u)
	}
}

// Redelivery of one event id is a no-op; a different event id for an already
// confirmed line finds nothing ACTIVE and changes nothing, so stock is
// decremented exactly once.
func TestConfirmPicksForOrder_PerLine_RedeliveryAndExtraEventsChangeNothing(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	labels := f.reserveLines(t, "order-1", 2)

	if _, err := f.uc.Execute(ctx, lineCompleted("ce-A", "order-1", 1)); err != nil {
		t.Fatalf("first: %v", err)
	}
	stockedAfter := f.countEvents("StockPicked")
	for i := 0; i < 3; i++ {
		dup, err := f.uc.Execute(ctx, lineCompleted("ce-A", "order-1", 1))
		if err != nil || dup.Outcome != usecases.PicksDuplicate {
			t.Fatalf("redelivery = %+v, %v; want DUPLICATE", dup, err)
		}
	}
	extra, err := f.uc.Execute(ctx, lineCompleted("ce-B", "order-1", 1))
	if err != nil {
		t.Fatalf("extra: %v", err)
	}
	if extra.Outcome != usecases.PicksLineSettled || extra.Confirmed != 0 || extra.AlreadyPicked != 1 {
		t.Fatalf("extra = %+v, want LINE_SETTLED with 1 already picked", extra)
	}
	if n := f.countEvents("StockPicked"); n != stockedAfter {
		t.Fatalf("StockPicked %d -> %d, want unchanged", stockedAfter, n)
	}
	stock, _ := f.e.Stock.FindBySKU(ctx, mustSKU(t, labels[0]))
	if len(stock) != 1 || stock[0].Quantity().Int() != 6 {
		t.Fatalf("stock = %+v, want one unit of 6 (decremented once)", stock)
	}
	if s := f.status(t, labels[1]); s != reservation.StatusActive {
		t.Fatalf("line 2 = %s, want ACTIVE", s)
	}
}

// An event WITHOUT a line keeps the ADR 0035 counting, even on an order whose
// reservations carry lines: the legacy-producer path is unchanged.
func TestConfirmPicksForOrder_NoLineInTheEvent_StillCountsPicks(t *testing.T) {
	f := newConfirmPicksFixture(t)
	labels := f.reserveLines(t, "order-1", 2)

	first := f.pick(t, "order-1")
	if want := (usecases.ConfirmPicksResult{Outcome: usecases.PicksAwaiting, PicksSeen: 1, PicksNeeded: 2}); first != want {
		t.Fatalf("first = %+v, want %+v", first, want)
	}
	f.assertAllStatus(t, labels, reservation.StatusActive)
	second := f.pick(t, "order-1")
	if second.Outcome != usecases.PicksProcessed || second.Confirmed != 2 {
		t.Fatalf("second = %+v, want PROCESSED with 2 confirmed", second)
	}
}

// A line_no below 1 can never be a real line: deterministic, dead-lettered by
// the consumer, and nothing is claimed or written.
func TestConfirmPicksForOrder_PerLine_NonPositiveLineIsMalformed(t *testing.T) {
	for _, n := range []int{0, -1, reservation.MaxLineNo + 1} {
		f := newConfirmPicksFixture(t)
		f.reserveNumbered(t, "LINE-1", "order-1", ptr(1), time.Hour)

		_, err := f.uc.Execute(context.Background(), lineCompleted("ce-1", "order-1", n))
		if !errors.Is(err, usecases.ErrMalformedPickCompletion) {
			t.Fatalf("line_no %d: err = %v, want ErrMalformedPickCompletion", n, err)
		}
		if len(f.claims.claimed) != 0 || f.status(t, "LINE-1") != reservation.StatusActive {
			t.Fatalf("line_no %d: a malformed event changed state", n)
		}
	}
}

func TestConfirmPicksForOrder_PerLine_NonPickAndNoOrderRefAreStillIgnored(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveNumbered(t, "LINE-1", "order-1", ptr(1), time.Hour)
	for _, in := range []usecases.PickCompletion{
		{EventID: "ce-1", TaskType: "PACK", OrderRef: "order-1", LineNo: ptr(1)},
		{EventID: "ce-2", TaskType: "PICK", OrderRef: "", LineNo: ptr(1)},
	} {
		got, err := f.uc.Execute(context.Background(), in)
		if err != nil || got.Outcome != usecases.PicksIgnored {
			t.Fatalf("%+v = %+v, %v; want IGNORED", in, got, err)
		}
	}
	if s := f.status(t, "LINE-1"); s != reservation.StatusActive || len(f.claims.claimed) != 0 {
		t.Fatalf("an ignored event changed state: %s %v", s, f.claims.claimed)
	}
}

// A failure while confirming the line rolls the claim back (the in-memory
// stand-in for the transaction), classifies as transient, and the redelivery
// converges.
func TestConfirmPicksForOrder_PerLine_FailureRollsBackTheClaimAndRetryConverges(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	f.reserveNumbered(t, "LINE-1", "order-1", ptr(1), time.Hour)
	healthy := f.uc.Confirm
	f.uc.Confirm = failingConfirmer{}

	_, err := f.uc.Execute(ctx, lineCompleted("ce-1", "order-1", 1))
	if !errors.Is(err, errFake) || errors.Is(err, usecases.ErrMalformedPickCompletion) {
		t.Fatalf("err = %v, want the transient error propagated", err)
	}
	if len(f.claims.claimed) != 0 || len(f.progress.counts) != 0 {
		t.Fatalf("claim/counter survived a failed handling: %v %v", f.claims.claimed, f.progress.counts)
	}
	if f.metrics.counts["confirmed"] != 0 {
		t.Fatalf("metrics recorded for a failed attempt: %v", f.metrics.counts)
	}

	f.uc.Confirm = healthy
	got, err := f.uc.Execute(ctx, lineCompleted("ce-1", "order-1", 1))
	if err != nil || got.Outcome != usecases.PicksLineSettled || got.Confirmed != 1 {
		t.Fatalf("retry = %+v, %v; want LINE_SETTLED with 1 confirmed", got, err)
	}
	if s := f.status(t, "LINE-1"); s != reservation.StatusConfirmed {
		t.Fatalf("status = %s, want CONFIRMED", s)
	}
}

func TestConfirmPicksForOrder_PerLine_LookupFailureRollsBackTheClaim(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	f.reserveNumbered(t, "LINE-1", "order-1", ptr(1), time.Hour)
	healthy := f.uc.Reservations
	f.uc.Reservations = &failingReservationRepo{delegate: f.e.Reservations, failFindByDemandRef: true}

	if _, err := f.uc.Execute(ctx, lineCompleted("ce-1", "order-1", 1)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want errFake", err)
	}
	if len(f.claims.claimed) != 0 {
		t.Fatalf("claim survived a failed lookup: %v", f.claims.claimed)
	}
	f.uc.Reservations = healthy
	if got, err := f.uc.Execute(ctx, lineCompleted("ce-1", "order-1", 1)); err != nil || got.Confirmed != 1 {
		t.Fatalf("retry = %+v, %v; want 1 confirmed", got, err)
	}
}
