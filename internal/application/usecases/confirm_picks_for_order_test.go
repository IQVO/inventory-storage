package usecases_test

import (
	"context"
	"errors"
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

// snapshotUnitOfWork restores the claims when fn fails, like a rolled-back
// transaction would. Nested calls join the outer scope.
type snapshotUnitOfWork struct {
	claims *snapshotClaims
	depth  int
}

func (u *snapshotUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if u.depth > 0 {
		return fn(ctx)
	}
	before := map[string]bool{}
	for k, v := range u.claims.claimed {
		before[k] = v
	}
	u.depth++
	defer func() { u.depth-- }()
	if err := fn(ctx); err != nil {
		u.claims.claimed = before
		return err
	}
	return nil
}

type confirmPicksFixture struct {
	e       env
	claims  *snapshotClaims
	metrics *recordingPickMetrics
	uc      *usecases.ConfirmPicksForOrder
	ids     map[string]string // line label -> reservation id
}

func newConfirmPicksFixture(t *testing.T) *confirmPicksFixture {
	t.Helper()
	e := newEnv()
	claims := &snapshotClaims{claimed: map[string]bool{}}
	uow := &snapshotUnitOfWork{claims: claims}
	metrics := newRecordingPickMetrics()
	f := &confirmPicksFixture{e: e, claims: claims, metrics: metrics, ids: map[string]string{}}
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

func pickCompleted(eventID, orderRef string) usecases.PickCompletion {
	return usecases.PickCompletion{EventID: eventID, TaskID: "task-1", TaskType: "PICK", OrderRef: orderRef}
}

func TestConfirmPicksForOrder_ActivePickedRevokedExpiredMix(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()

	f.reserveLine(t, "ACTIVE-1", "order-1", time.Hour)
	f.reserveLine(t, "ACTIVE-2", "order-1", time.Hour)
	f.reserveLine(t, "PICKED", "order-1", time.Hour)
	f.reserveLine(t, "REVOKED", "order-1", time.Hour)
	f.reserveLine(t, "EXPIRING", "order-1", time.Minute)
	f.reserveLine(t, "OTHER-ORDER", "order-2", time.Hour)

	if err := f.uc.Confirm.Execute(ctx, f.ids["PICKED"]); err != nil {
		t.Fatalf("pre-confirm: %v", err)
	}
	revoke := &usecases.RevokeReservation{Stock: f.e.Stock, Reservations: f.e.Reservations, Events: f.e.Events, Clock: f.e.Clock}
	if err := revoke.Execute(ctx, f.ids["REVOKED"]); err != nil {
		t.Fatalf("pre-revoke: %v", err)
	}
	f.e.Clock.Advance(2 * time.Minute) // EXPIRING is now past its timeout, still ACTIVE in storage
	pickedEventsBefore := f.countEvents("StockPicked")

	got, err := f.uc.Execute(ctx, pickCompleted("ce-1", "order-1"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	want := usecases.ConfirmPicksResult{Outcome: usecases.PicksProcessed, Confirmed: 2, AlreadyPicked: 1, Revoked: 1, Expired: 1}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	wantStatus := map[string]reservation.Status{
		"ACTIVE-1": reservation.StatusConfirmed, "ACTIVE-2": reservation.StatusConfirmed,
		"PICKED": reservation.StatusConfirmed, "REVOKED": reservation.StatusRevoked,
		"EXPIRING": reservation.StatusExpired, "OTHER-ORDER": reservation.StatusActive,
	}
	for label, s := range wantStatus {
		if gotS := f.status(t, label); gotS != s {
			t.Errorf("%s status = %s, want %s", label, gotS, s)
		}
	}
	if n := f.countEvents("StockPicked") - pickedEventsBefore; n != 2 {
		t.Errorf("StockPicked raised %d times, want 2 (only the two ACTIVE lines)", n)
	}
	// ACTIVE-1: 10 stowed, 4 picked => 6 usable and physically gone; an
	// expired line returns its 4 to usable (10), a different order is untouched.
	if u := f.usable(t, "ACTIVE-1"); u != 6 {
		t.Errorf("ACTIVE-1 usable = %d, want 6", u)
	}
	if u := f.usable(t, "EXPIRING"); u != 10 {
		t.Errorf("EXPIRING usable = %d, want 10 (expiry returns stock)", u)
	}
	if u := f.usable(t, "OTHER-ORDER"); u != 6 {
		t.Errorf("OTHER-ORDER usable = %d, want 6 (still reserved)", u)
	}
	if f.metrics.counts["confirmed"] != 2 || f.metrics.counts["expired"] != 1 {
		t.Errorf("metrics = %v, want confirmed=2 expired=1", f.metrics.counts)
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
		})
	}
}

func TestConfirmPicksForOrder_NoReservationsIsSuccessfulNoOp(t *testing.T) {
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
}

func TestConfirmPicksForOrder_MalformedEventIDIsDeterministic(t *testing.T) {
	f := newConfirmPicksFixture(t)
	_, err := f.uc.Execute(context.Background(), usecases.PickCompletion{EventID: "", TaskType: "PICK", OrderRef: "order-1"})
	if !errors.Is(err, usecases.ErrMalformedPickCompletion) {
		t.Fatalf("err = %v, want ErrMalformedPickCompletion", err)
	}
}

func TestConfirmPicksForOrder_RedeliveryConfirmsNothingNew(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	f.reserveLine(t, "SKU-1", "order-1", time.Hour)

	first, err := f.uc.Execute(ctx, pickCompleted("ce-1", "order-1"))
	if err != nil || first.Confirmed != 1 {
		t.Fatalf("first = %+v, %v; want 1 confirmed", first, err)
	}
	eventsAfterFirst := f.countEvents("StockPicked")
	usableAfterFirst := f.usable(t, "SKU-1")

	// Same CloudEvents id: deduplicated.
	dup, err := f.uc.Execute(ctx, pickCompleted("ce-1", "order-1"))
	if err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if dup.Outcome != usecases.PicksDuplicate {
		t.Fatalf("duplicate outcome = %q, want %q", dup.Outcome, usecases.PicksDuplicate)
	}
	// A NEW id for the same order (e.g. a second PICK task): the reservation is
	// already picked, so nothing is confirmed twice.
	again, err := f.uc.Execute(ctx, pickCompleted("ce-2", "order-1"))
	if err != nil {
		t.Fatalf("second event: %v", err)
	}
	if again.Confirmed != 0 || again.AlreadyPicked != 1 {
		t.Fatalf("second event result = %+v, want 0 confirmed, 1 already picked", again)
	}
	if n := f.countEvents("StockPicked"); n != eventsAfterFirst {
		t.Fatalf("StockPicked count %d -> %d, want unchanged", eventsAfterFirst, n)
	}
	if u := f.usable(t, "SKU-1"); u != usableAfterFirst {
		t.Fatalf("usable %d -> %d, want unchanged (stock decremented exactly once)", usableAfterFirst, u)
	}
	stockUnits, _ := f.e.Stock.FindBySKU(ctx, mustSKU(t, "SKU-1"))
	if len(stockUnits) != 1 || stockUnits[0].Quantity().Int() != 6 {
		t.Fatalf("stock quantity wrong after redelivery: %+v", stockUnits)
	}
}

// A failure after the claim rolls the claim back with the rest, so the
// retried delivery is applied in full rather than skipped as a duplicate.
// (The in-memory repos cannot roll aggregate writes back; the Postgres
// integration test proves the whole transaction, this one proves the claim
// ordering and the error classification.)
func TestConfirmPicksForOrder_FailureRollsBackClaimAndRetryConverges(t *testing.T) {
	f := newConfirmPicksFixture(t)
	ctx := context.Background()
	f.reserveLine(t, "SKU-1", "order-1", time.Hour)
	f.reserveLine(t, "SKU-2", "order-1", time.Hour)

	healthyReservations := f.uc.Reservations
	f.uc.Reservations = &failingReservationRepo{delegate: f.e.Reservations, failFindByDemandRef: true}

	_, err := f.uc.Execute(ctx, pickCompleted("ce-1", "order-1"))
	if !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the transient error propagated (so the consumer retries)", err)
	}
	if errors.Is(err, usecases.ErrMalformedPickCompletion) {
		t.Fatalf("a transient failure must not be classified as malformed: %v", err)
	}
	if len(f.claims.claimed) != 0 {
		t.Fatalf("claim survived a failed handling: %v", f.claims.claimed)
	}
	if f.metrics.counts["confirmed"] != 0 {
		t.Fatalf("metrics recorded for a failed attempt: %v", f.metrics.counts)
	}

	f.uc.Reservations = healthyReservations
	got, err := f.uc.Execute(ctx, pickCompleted("ce-1", "order-1"))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got.Outcome != usecases.PicksProcessed || got.Confirmed != 2 {
		t.Fatalf("retry result = %+v, want PROCESSED with 2 confirmed", got)
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
	if len(f.claims.claimed) != 0 {
		t.Fatalf("claim survived a failed confirm: %v", f.claims.claimed)
	}
}

type failingConfirmer struct{}

func (failingConfirmer) Execute(context.Context, string) error { return errFake }
