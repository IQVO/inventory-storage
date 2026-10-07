package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// recordingPickMetrics counts ports.PickConfirmationMetrics calls by outcome.
type recordingPickMetrics struct {
	counts map[string]int
}

func newRecordingPickMetrics() *recordingPickMetrics {
	return &recordingPickMetrics{counts: map[string]int{}}
}

func (m *recordingPickMetrics) PickConfirmation(_ context.Context, outcome string, n int) {
	m.counts[outcome] += n
}

var _ ports.PickConfirmationMetrics = (*recordingPickMetrics)(nil)

// snapshotClaims is a ProcessedEventRepo that a snapshotUnitOfWork can roll
// back, standing in for Postgres' transactional un-claim.
type snapshotClaims struct{ claimed map[string]bool }

func (r *snapshotClaims) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	key := consumer + "/" + eventID
	if r.claimed[key] {
		return false, nil
	}
	r.claimed[key] = true
	return true, nil
}

// snapshotProgress is an OrderPickProgressRepo a snapshotUnitOfWork can roll
// back, standing in for the transactional un-count.
type snapshotProgress struct{ counts map[string]int }

func (r *snapshotProgress) RecordPick(_ context.Context, demandRef string, _ time.Time) (int, error) {
	r.counts[demandRef]++
	return r.counts[demandRef], nil
}

// snapshotUnitOfWork restores the claims and the progress counters when fn
// fails, like a rolled-back transaction would. Nested calls join the outer
// scope.
type snapshotUnitOfWork struct {
	claims   *snapshotClaims
	progress *snapshotProgress
	depth    int
}

func (u *snapshotUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if u.depth > 0 {
		return fn(ctx)
	}
	claimsBefore := map[string]bool{}
	for k, v := range u.claims.claimed {
		claimsBefore[k] = v
	}
	progressBefore := map[string]int{}
	for k, v := range u.progress.counts {
		progressBefore[k] = v
	}
	u.depth++
	defer func() { u.depth-- }()
	if err := fn(ctx); err != nil {
		u.claims.claimed = claimsBefore
		u.progress.counts = progressBefore
		return err
	}
	return nil
}

type confirmPicksFixture struct {
	e        env
	claims   *snapshotClaims
	progress *snapshotProgress
	metrics  *recordingPickMetrics
	uc       *usecases.ConfirmPicksForOrder
	ids      map[string]string // line label -> reservation id
	nextID   int
}

func newConfirmPicksFixture(t *testing.T) *confirmPicksFixture {
	t.Helper()
	e := newEnv()
	claims := &snapshotClaims{claimed: map[string]bool{}}
	progress := &snapshotProgress{counts: map[string]int{}}
	uow := &snapshotUnitOfWork{claims: claims, progress: progress}
	metrics := newRecordingPickMetrics()
	f := &confirmPicksFixture{e: e, claims: claims, progress: progress, metrics: metrics, ids: map[string]string{}}
	f.uc = f.buildUseCase(e.Events, uow)
	return f
}

func (f *confirmPicksFixture) buildUseCase(events ports.EventPublisher, uow ports.UnitOfWork) *usecases.ConfirmPicksForOrder {
	confirm := &usecases.ConfirmPick{Stock: f.e.Stock, Locations: f.e.Locations, Reservations: f.e.Reservations, Events: events, Clock: f.e.Clock, UnitOfWork: uow}
	return &usecases.ConfirmPicksForOrder{
		Stock:           f.e.Stock,
		Reservations:    f.e.Reservations,
		Events:          events,
		Clock:           f.e.Clock,
		Confirm:         confirm,
		ProcessedEvents: f.claims,
		PickProgress:    f.progress,
		UnitOfWork:      uow,
		Metrics:         f.metrics,
	}
}

// reserveLine stows 10 units of sku in its own bin and reserves 4 of them
// for demandRef with the given timeout, remembering the reservation id.
func (f *confirmPicksFixture) reserveLine(t *testing.T, label, demandRef string, timeout time.Duration) {
	t.Helper()
	stowUnit(t, f.e, label, "BIN-"+label, 20, 10)
	reserve := &usecases.ReserveStock{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock, Timeout: timeout}
	res, err := reserve.Execute(context.Background(), mustSKU(t, label), mustQty(t, 4), demandRef)
	if err != nil {
		t.Fatalf("reserve %s: %v", label, err)
	}
	f.ids[label] = res.ID()
}

// reserveOrder reserves n one-line-per-SKU lines (L1..Ln) for demandRef.
func (f *confirmPicksFixture) reserveOrder(t *testing.T, demandRef string, n int) []string {
	t.Helper()
	labels := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		label := fmt.Sprintf("%s-L%d", demandRef, i)
		f.reserveLine(t, label, demandRef, time.Hour)
		labels = append(labels, label)
	}
	return labels
}

// pick delivers one PICK completion with a fresh CloudEvents id.
func (f *confirmPicksFixture) pick(t *testing.T, orderRef string) usecases.ConfirmPicksResult {
	t.Helper()
	f.nextID++
	got, err := f.uc.Execute(context.Background(), pickCompleted(fmt.Sprintf("ce-%d", f.nextID), orderRef))
	if err != nil {
		t.Fatalf("pick %d for %s: %v", f.nextID, orderRef, err)
	}
	return got
}

func (f *confirmPicksFixture) status(t *testing.T, label string) reservation.Status {
	t.Helper()
	res, err := f.e.Reservations.FindByID(context.Background(), f.ids[label])
	if err != nil || res == nil {
		t.Fatalf("find reservation %s: %v", label, err)
	}
	return res.Status()
}

func (f *confirmPicksFixture) usable(t *testing.T, label string) int {
	t.Helper()
	got, err := (&usecases.GetUsable{Stock: f.e.Stock}).Execute(context.Background(), mustSKU(t, label))
	if err != nil {
		t.Fatalf("usable %s: %v", label, err)
	}
	return got.Usable.Int()
}

func (f *confirmPicksFixture) countEvents(name string) int {
	n := 0
	for _, ev := range f.e.Events.Events() {
		if ev.EventName() == name {
			n++
		}
	}
	return n
}

func (f *confirmPicksFixture) assertAllStatus(t *testing.T, labels []string, want reservation.Status) {
	t.Helper()
	for _, label := range labels {
		if got := f.status(t, label); got != want {
			t.Errorf("%s status = %s, want %s", label, got, want)
		}
	}
}

func pickCompleted(eventID, orderRef string) usecases.PickCompletion {
	return usecases.PickCompletion{EventID: eventID, TaskID: "task-1", TaskType: "PICK", OrderRef: orderRef}
}

// The core of ADR 0035: a PICK task is per order LINE, so with three lines
// the first two completions must leave everything ACTIVE and only the third
// confirms all three, exactly once.
func TestConfirmPicksForOrder_ThreeLinesConfirmOnlyOnTheLastPick(t *testing.T) {
	f := newConfirmPicksFixture(t)
	labels := f.reserveOrder(t, "order-1", 3)
	f.reserveLine(t, "OTHER-ORDER", "order-2", time.Hour)
	pickedBefore := f.countEvents("StockPicked")

	for i := 1; i <= 2; i++ {
		got := f.pick(t, "order-1")
		want := usecases.ConfirmPicksResult{Outcome: usecases.PicksAwaiting, PicksSeen: i, PicksNeeded: 3}
		if got != want {
			t.Fatalf("pick %d result = %+v, want %+v", i, got, want)
		}
		f.assertAllStatus(t, labels, reservation.StatusActive)
		if n := f.countEvents("StockPicked") - pickedBefore; n != 0 {
			t.Fatalf("after pick %d StockPicked raised %d times, want 0 (lines not yet all picked)", i, n)
		}
		if u := f.usable(t, labels[0]); u != 6 {
			t.Fatalf("after pick %d usable = %d, want 6 (still merely reserved)", i, u)
		}
	}

	got := f.pick(t, "order-1")
	want := usecases.ConfirmPicksResult{Outcome: usecases.PicksProcessed, PicksSeen: 3, PicksNeeded: 3, Confirmed: 3}
	if got != want {
		t.Fatalf("pick 3 result = %+v, want %+v", got, want)
	}
	f.assertAllStatus(t, labels, reservation.StatusConfirmed)
	if n := f.countEvents("StockPicked") - pickedBefore; n != 3 {
		t.Errorf("StockPicked raised %d times, want 3", n)
	}
	for _, label := range labels {
		stock, _ := f.e.Stock.FindBySKU(context.Background(), mustSKU(t, label))
		if len(stock) != 1 || stock[0].Quantity().Int() != 6 {
			t.Errorf("%s stock after confirm = %+v, want one unit of 6 (10 - 4 picked, decremented once)", label, stock)
		}
	}
	if s := f.status(t, "OTHER-ORDER"); s != reservation.StatusActive {
		t.Errorf("another order's reservation = %s, want ACTIVE", s)
	}
	if f.metrics.counts["confirmed"] != 3 {
		t.Errorf("metrics = %v, want confirmed=3", f.metrics.counts)
	}
}

// seedMixedOrder builds order-1 with one line in each state: two ACTIVE, one
// CONFIRMED (PICKED), one REVOKED, one EXPIRED in storage, and one ACTIVE line
// already past its timeout (LATE-EXPIRING, resolved only when next read), plus
// an unrelated order-2 line.
func (f *confirmPicksFixture) seedMixedOrder(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	f.reserveLine(t, "ACTIVE-1", "order-1", time.Hour)
	f.reserveLine(t, "ACTIVE-2", "order-1", time.Hour)
	f.reserveLine(t, "PICKED", "order-1", time.Hour)
	f.reserveLine(t, "REVOKED", "order-1", time.Hour)
	f.reserveLine(t, "STORED-EXPIRED", "order-1", time.Minute)
	f.reserveLine(t, "OTHER-ORDER", "order-2", time.Hour)

	if err := f.uc.Confirm.Execute(ctx, f.ids["PICKED"]); err != nil {
		t.Fatalf("pre-confirm: %v", err)
	}
	revoke := &usecases.RevokeReservation{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock}
	if err := revoke.Execute(ctx, f.ids["REVOKED"]); err != nil {
		t.Fatalf("pre-revoke: %v", err)
	}
	// STORED-EXPIRED times out and a read resolves it to EXPIRED in storage.
	f.e.Clock.Advance(2 * time.Minute)
	lookup := &usecases.GetReservationsByDemandRef{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock}
	if _, err := lookup.Execute(ctx, "order-1"); err != nil {
		t.Fatalf("lazy expiry: %v", err)
	}
	if s := f.status(t, "STORED-EXPIRED"); s != reservation.StatusExpired {
		t.Fatalf("STORED-EXPIRED = %s, want EXPIRED in storage", s)
	}
	// LATE-EXPIRING is past its timeout but still ACTIVE in storage: it
	// counts as a pending pick and is resolved (skipped) on the last pick.
	f.reserveLine(t, "LATE-EXPIRING", "order-1", time.Minute)
	f.e.Clock.Advance(2 * time.Minute)
}

// ACTIVE and CONFIRMED lines count towards the number of picks needed;
// REVOKED and EXPIRED (stored or lazily expired on this very lookup) do not
// confirm and the expired one is skipped and counted.
func TestConfirmPicksForOrder_MixedStatesNeedPicksForActiveAndConfirmedOnly(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.seedMixedOrder(t)
	pickedEventsBefore := f.countEvents("StockPicked")
	// needed = ACTIVE-1, ACTIVE-2, LATE-EXPIRING (ACTIVE) + PICKED = 4.
	for i := 1; i <= 3; i++ {
		got := f.pick(t, "order-1")
		want := usecases.ConfirmPicksResult{Outcome: usecases.PicksAwaiting, PicksSeen: i, PicksNeeded: 4}
		if got != want {
			t.Fatalf("pick %d result = %+v, want %+v", i, got, want)
		}
	}
	got := f.pick(t, "order-1")
	want := usecases.ConfirmPicksResult{
		Outcome: usecases.PicksProcessed, PicksSeen: 4, PicksNeeded: 4,
		Confirmed: 2, AlreadyPicked: 1, Revoked: 1, Expired: 2,
	}
	if got != want {
		t.Fatalf("last pick result = %+v, want %+v", got, want)
	}
	wantStatus := map[string]reservation.Status{
		"ACTIVE-1": reservation.StatusConfirmed, "ACTIVE-2": reservation.StatusConfirmed,
		"PICKED": reservation.StatusConfirmed, "REVOKED": reservation.StatusRevoked,
		"STORED-EXPIRED": reservation.StatusExpired, "LATE-EXPIRING": reservation.StatusExpired,
		"OTHER-ORDER": reservation.StatusActive,
	}
	for label, s := range wantStatus {
		if gotS := f.status(t, label); gotS != s {
			t.Errorf("%s status = %s, want %s", label, gotS, s)
		}
	}
	if n := f.countEvents("StockPicked") - pickedEventsBefore; n != 2 {
		t.Errorf("StockPicked raised %d times, want 2 (only the two ACTIVE lines)", n)
	}
	if u := f.usable(t, "ACTIVE-1"); u != 6 {
		t.Errorf("ACTIVE-1 usable = %d, want 6", u)
	}
	if u := f.usable(t, "LATE-EXPIRING"); u != 10 {
		t.Errorf("LATE-EXPIRING usable = %d, want 10 (expiry returns stock)", u)
	}
	if u := f.usable(t, "OTHER-ORDER"); u != 6 {
		t.Errorf("OTHER-ORDER usable = %d, want 6 (still reserved)", u)
	}
	if f.metrics.counts["confirmed"] != 2 || f.metrics.counts["expired"] != 2 {
		t.Errorf("metrics = %v, want confirmed=2 expired=2", f.metrics.counts)
	}
}

// A revoked line will never be picked for, so the order needs one pick fewer.
func TestConfirmPicksForOrder_RevokedLineNeedsOneFewerPick(t *testing.T) {
	f := newConfirmPicksFixture(t)
	labels := f.reserveOrder(t, "order-1", 3)
	revoke := &usecases.RevokeReservation{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock}
	if err := revoke.Execute(context.Background(), f.ids[labels[2]]); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	first := f.pick(t, "order-1")
	if first.Outcome != usecases.PicksAwaiting || first.PicksNeeded != 2 {
		t.Fatalf("first pick = %+v, want AWAITING with 2 needed", first)
	}
	f.assertAllStatus(t, labels[:2], reservation.StatusActive)

	second := f.pick(t, "order-1")
	if second.Outcome != usecases.PicksProcessed || second.Confirmed != 2 || second.Revoked != 1 {
		t.Fatalf("second pick = %+v, want PROCESSED, 2 confirmed, 1 revoked", second)
	}
	f.assertAllStatus(t, labels[:2], reservation.StatusConfirmed)
	if s := f.status(t, labels[2]); s != reservation.StatusRevoked {
		t.Errorf("revoked line = %s, want REVOKED", s)
	}
}

// Redelivery of one event id, however often, never advances the counter: the
// processed-event claim short-circuits before the counter is touched.
func TestConfirmPicksForOrder_DuplicateEventDoesNotAdvanceTheCounter(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	labels := f.reserveOrder(t, "order-1", 3)

	if _, err := f.uc.Execute(ctx, pickCompleted("ce-A", "order-1")); err != nil {
		t.Fatalf("first: %v", err)
	}
	for i := 0; i < 3; i++ {
		dup, err := f.uc.Execute(ctx, pickCompleted("ce-A", "order-1"))
		if err != nil {
			t.Fatalf("duplicate: %v", err)
		}
		if dup.Outcome != usecases.PicksDuplicate {
			t.Fatalf("duplicate outcome = %q, want %q", dup.Outcome, usecases.PicksDuplicate)
		}
	}
	if n := f.progress.counts["order-1"]; n != 1 {
		t.Fatalf("progress counter = %d after 1 distinct event + 3 redeliveries, want 1", n)
	}
	// A second distinct event is the 2nd pick, not the 5th: still not the last.
	second, err := f.uc.Execute(ctx, pickCompleted("ce-B", "order-1"))
	if err != nil || second.Outcome != usecases.PicksAwaiting || second.PicksSeen != 2 {
		t.Fatalf("second distinct event = %+v, %v; want AWAITING with 2 seen", second, err)
	}
	f.assertAllStatus(t, labels, reservation.StatusActive)
}

// Once the order is confirmed, further events (a redelivered 3rd under a new
// id, say) find nothing ACTIVE and change nothing.
func TestConfirmPicksForOrder_ExtraPickAfterConfirmationChangesNothing(t *testing.T) {
	f := newConfirmPicksFixture(t)
	labels := f.reserveOrder(t, "order-1", 2)
	f.pick(t, "order-1")
	if got := f.pick(t, "order-1"); got.Confirmed != 2 {
		t.Fatalf("last pick = %+v, want 2 confirmed", got)
	}
	eventsAfter := f.countEvents("StockPicked")
	usableAfter := f.usable(t, labels[0])

	extra := f.pick(t, "order-1")
	if extra.Outcome != usecases.PicksNothingToConfirm || extra.Confirmed != 0 {
		t.Fatalf("extra pick = %+v, want NOTHING_TO_CONFIRM", extra)
	}
	if n := f.countEvents("StockPicked"); n != eventsAfter {
		t.Fatalf("StockPicked count %d -> %d, want unchanged", eventsAfter, n)
	}
	if u := f.usable(t, labels[0]); u != usableAfter {
		t.Fatalf("usable %d -> %d, want unchanged (stock decremented exactly once)", usableAfter, u)
	}
	stock, _ := f.e.Stock.FindBySKU(context.Background(), mustSKU(t, labels[0]))
	if len(stock) != 1 || stock[0].Quantity().Int() != 6 {
		t.Fatalf("stock quantity wrong after the extra pick: %+v", stock)
	}
}

func TestConfirmPicksForOrder_Ignored(t *testing.T) {
	tests := []struct {
		name string
		in   usecases.PickCompletion
	}{
		{"non-PICK task type", usecases.PickCompletion{EventID: "ce-1", TaskID: "t", TaskType: "PACK", OrderRef: "order-1"}},
		{"empty task type", usecases.PickCompletion{EventID: "ce-1", TaskID: "t", TaskType: "", OrderRef: "order-1"}},
		{"lower-case pick is not the contract value", usecases.PickCompletion{EventID: "ce-1", TaskID: "t", TaskType: "pick", OrderRef: "order-1"}},
		{"missing order_ref", usecases.PickCompletion{EventID: "ce-1", TaskID: "t", TaskType: "PICK", OrderRef: ""}},
		{"blank order_ref", usecases.PickCompletion{EventID: "ce-1", TaskID: "t", TaskType: "PICK", OrderRef: "   "}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newConfirmPicksFixture(t)
			f.reserveLine(t, "SKU-1", "order-1", time.Hour)

			got, err := f.uc.Execute(context.Background(), tt.in)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got.Outcome != usecases.PicksIgnored {
				t.Fatalf("outcome = %q, want %q", got.Outcome, usecases.PicksIgnored)
			}
			if s := f.status(t, "SKU-1"); s != reservation.StatusActive {
				t.Fatalf("reservation status = %s, want ACTIVE (nothing to do)", s)
			}
			if len(f.claims.claimed) != 0 {
				t.Fatalf("an ignored event must not claim an id, got %v", f.claims.claimed)
			}
			if len(f.progress.counts) != 0 {
				t.Fatalf("an ignored event must not count a pick, got %v", f.progress.counts)
			}
		})
	}
}

// An order with no reservation (a transfer, a non-inventory order) is a
// successful no-op that leaves no progress row behind.
func TestConfirmPicksForOrder_NoReservationsIsSuccessfulNoOpWithoutProgressRow(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveLine(t, "SKU-1", "order-1", time.Hour)

	got, err := f.uc.Execute(context.Background(), pickCompleted("ce-1", "transfer-or-non-inventory-order"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.Outcome != usecases.PicksNoReservations || got.Confirmed != 0 {
		t.Fatalf("result = %+v, want NO_RESERVATIONS and nothing confirmed", got)
	}
	if s := f.status(t, "SKU-1"); s != reservation.StatusActive {
		t.Fatalf("an unrelated order's reservation changed: %s", s)
	}
	if len(f.progress.counts) != 0 {
		t.Fatalf("progress rows = %v, want none for an order with no reservations", f.progress.counts)
	}
}

// An order whose reservations are all REVOKED/EXPIRED will never be
// confirmed, so it must not leave a progress row either.
func TestConfirmPicksForOrder_OnlyRevokedReservationsLeaveNoProgressRow(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveLine(t, "SKU-1", "order-1", time.Hour)
	revoke := &usecases.RevokeReservation{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock}
	if err := revoke.Execute(context.Background(), f.ids["SKU-1"]); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	got := f.pick(t, "order-1")
	if got.Outcome != usecases.PicksNothingToConfirm {
		t.Fatalf("outcome = %q, want %q", got.Outcome, usecases.PicksNothingToConfirm)
	}
	if len(f.progress.counts) != 0 {
		t.Fatalf("progress rows = %v, want none (nothing could ever be confirmed)", f.progress.counts)
	}
}

func TestConfirmPicksForOrder_MalformedEventIDIsDeterministic(t *testing.T) {
	f := newConfirmPicksFixture(t)
	_, err := f.uc.Execute(context.Background(), usecases.PickCompletion{EventID: "", TaskType: "PICK", OrderRef: "order-1"})
	if !errors.Is(err, usecases.ErrMalformedPickCompletion) {
		t.Fatalf("err = %v, want ErrMalformedPickCompletion", err)
	}
}

// A failure after the claim and the counter increment rolls both back with the
// rest, so the retried delivery is counted and applied in full rather than
// skipped as a duplicate. (The in-memory repos cannot roll aggregate writes
// back; the Postgres integration test proves the whole transaction, this one
// proves the claim/counter ordering and the error classification.)
func TestConfirmPicksForOrder_FailureRollsBackClaimAndCounterAndRetryConverges(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	labels := f.reserveOrder(t, "order-1", 2)
	f.pick(t, "order-1") // pick 1 of 2 committed

	// The LAST pick fails while confirming (after claim + increment).
	f.uc.Confirm = failingConfirmer{}
	_, err := f.uc.Execute(ctx, pickCompleted("ce-last", "order-1"))
	if !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the transient error propagated (so the consumer retries)", err)
	}
	if errors.Is(err, usecases.ErrMalformedPickCompletion) {
		t.Fatalf("a transient failure must not be classified as malformed: %v", err)
	}
	if _, kept := f.claims.claimed[usecases.ConfirmPicksConsumer+"/ce-last"]; kept {
		t.Fatalf("claim survived a failed handling: %v", f.claims.claimed)
	}
	if n := f.progress.counts["order-1"]; n != 1 {
		t.Fatalf("progress counter = %d after the rolled-back pick, want 1 (not 2)", n)
	}
	if f.metrics.counts["confirmed"] != 0 {
		t.Fatalf("metrics recorded for a failed attempt: %v", f.metrics.counts)
	}

	// Healthy retry of the same event id converges: counted once, confirmed.
	f.uc = f.buildUseCase(f.e.Events, f.uc.UnitOfWork)
	got, err := f.uc.Execute(ctx, pickCompleted("ce-last", "order-1"))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got.Outcome != usecases.PicksProcessed || got.Confirmed != 2 || got.PicksSeen != 2 {
		t.Fatalf("retry result = %+v, want PROCESSED with 2 confirmed on pick 2", got)
	}
	f.assertAllStatus(t, labels, reservation.StatusConfirmed)
}

// A failure while loading the order's reservations rolls the claim back too.
func TestConfirmPicksForOrder_LookupFailureRollsBackClaimAndRetryConverges(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	f.reserveLine(t, "SKU-1", "order-1", time.Hour)

	healthyReservations := f.uc.Reservations
	f.uc.Reservations = &failingReservationRepo{delegate: f.e.Reservations, failFindByDemandRef: true}

	_, err := f.uc.Execute(ctx, pickCompleted("ce-1", "order-1"))
	if !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the transient error propagated", err)
	}
	if len(f.claims.claimed) != 0 || len(f.progress.counts) != 0 {
		t.Fatalf("claim/counter survived a failed handling: %v %v", f.claims.claimed, f.progress.counts)
	}

	f.uc.Reservations = healthyReservations
	got, err := f.uc.Execute(ctx, pickCompleted("ce-1", "order-1"))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got.Outcome != usecases.PicksProcessed || got.Confirmed != 1 {
		t.Fatalf("retry result = %+v, want PROCESSED with 1 confirmed", got)
	}
}

func TestConfirmPicksForOrder_ConfirmFailureIsPropagatedAsTransient(t *testing.T) {
	f := newConfirmPicksFixture(t)
	f.reserveLine(t, "SKU-1", "order-1", time.Hour)
	f.uc.Confirm = failingConfirmer{}

	_, err := f.uc.Execute(context.Background(), pickCompleted("ce-1", "order-1"))
	if !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want errFake", err)
	}
	if len(f.claims.claimed) != 0 || len(f.progress.counts) != 0 {
		t.Fatalf("claim/counter survived a failed confirm: %v %v", f.claims.claimed, f.progress.counts)
	}
}

type failingConfirmer struct{}

func (failingConfirmer) Execute(context.Context, string) error { return errFake }
