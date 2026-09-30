package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// TestGetReservationsByDemandRef_BeforeExpiry_StaysActiveUntouched pins the
// "no-op on an unexpired lookup" branch: reading a reservation before its
// timeout must not touch it at all — status, usable inventory, and the
// published-event stream are all unchanged.
func TestGetReservationsByDemandRef_BeforeExpiry_StaysActiveUntouched(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	// Look the reservation up well before its expiry (clock unchanged).
	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	found, err := getUC.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(found) != 1 || found[0].Status() != reservation.StatusActive {
		t.Fatalf("expected 1 Active reservation, got %+v", found)
	}
	if found[0].ID() != res.ID() {
		t.Fatalf("expected the same reservation id, got %s want %s", found[0].ID(), res.ID())
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 4 {
		t.Fatalf("expected usable unchanged at 4 (10 - 6 reserved), got %d", usable.Usable.Int())
	}

	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ReservationExpired" {
			t.Fatalf("did not expect ReservationExpired before the timeout, got events=%v", e.Events.Events())
		}
	}
}

// TestGetReservationsByDemandRef_AfterExpiry_TransitionsAndRaisesEvent is
// the core lazy-expiry behaviour: a read AFTER the timeout must discover
// the expiry, return quantity to usable, persist the transition, and raise
// ReservationExpired — all synchronously within the read.
func TestGetReservationsByDemandRef_AfterExpiry_TransitionsAndRaisesEvent(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	// Advance the clock past the reservation's timeout, then read it.
	e.Clock.Advance(time.Hour + time.Second)

	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	found, err := getUC.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(found) != 1 || found[0].Status() != reservation.StatusExpired {
		t.Fatalf("expected 1 Expired reservation, got %+v", found)
	}

	// The transition must be durable, not just returned in-memory.
	persisted, err := e.Reservations.FindByID(context.Background(), res.ID())
	if err != nil {
		t.Fatalf("unexpected error re-finding reservation: %v", err)
	}
	if persisted.Status() != reservation.StatusExpired {
		t.Fatalf("expected persisted status Expired, got %v", persisted.Status())
	}

	// Quantity returns to usable, exactly like a revoke would.
	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 10 {
		t.Fatalf("expected usable restored to 10 after lazy expiry, got %d", usable.Usable.Int())
	}

	var sawExpired bool
	for _, ev := range e.Events.Events() {
		if exp, ok := ev.(shared.ReservationExpired); ok {
			sawExpired = true
			if exp.ReservationID != res.ID() {
				t.Fatalf("expected ReservationExpired for %s, got %s", res.ID(), exp.ReservationID)
			}
		}
	}
	if !sawExpired {
		t.Fatalf("expected a ReservationExpired event, got events=%v", e.Events.Events())
	}
}

// TestGetReservationsByDemandRef_AlreadyExpired_NotReprocessed pins the
// idempotency guarantee: a SECOND read after the reservation is already
// Expired must not raise ReservationExpired again or touch usable a second
// time.
func TestGetReservationsByDemandRef_AlreadyExpired_NotReprocessed(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	if _, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if _, err := getUC.Execute(context.Background(), "order-1"); err != nil {
		t.Fatalf("unexpected error on first (expiring) read: %v", err)
	}
	expiredEventCount := 0
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ReservationExpired" {
			expiredEventCount++
		}
	}
	if expiredEventCount != 1 {
		t.Fatalf("expected exactly 1 ReservationExpired after first read, got %d", expiredEventCount)
	}

	// Advance further and read again: nothing new should happen.
	e.Clock.Advance(2 * time.Hour)
	found, err := getUC.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error on second read: %v", err)
	}
	if len(found) != 1 || found[0].Status() != reservation.StatusExpired {
		t.Fatalf("expected the reservation to remain Expired, got %+v", found)
	}

	expiredEventCount = 0
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ReservationExpired" {
			expiredEventCount++
		}
	}
	if expiredEventCount != 1 {
		t.Fatalf("expected ReservationExpired to be raised exactly once total, got %d", expiredEventCount)
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 10 {
		t.Fatalf("expected usable to stay at 10 (not double-released), got %d", usable.Usable.Int())
	}
}

// TestGetReservationsByDemandRef_ConfirmedReservation_NotReprocessed covers
// the other two terminal states explicitly named in the task: a Confirmed
// reservation past its original expiresAt must never be flipped to Expired
// by a later lazy-expiry read.
func TestGetReservationsByDemandRef_ConfirmedReservation_NotReprocessed(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	pickUC := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := pickUC.Execute(context.Background(), res.ID()); err != nil {
		t.Fatalf("unexpected error confirming pick: %v", err)
	}

	// Now advance well past the original expiresAt and read again.
	e.Clock.Advance(2 * time.Hour)
	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	found, err := getUC.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(found) != 1 || found[0].Status() != reservation.StatusConfirmed {
		t.Fatalf("expected the Confirmed reservation to remain Confirmed, got %+v", found)
	}
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ReservationExpired" {
			t.Fatalf("did not expect ReservationExpired for an already-Confirmed reservation, got events=%v", e.Events.Events())
		}
	}
}

// TestGetReservationsByDemandRef_RevokedReservation_NotReprocessed is the
// Revoked-state analogue of the Confirmed test above.
func TestGetReservationsByDemandRef_RevokedReservation_NotReprocessed(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := revokeUC.Execute(context.Background(), res.ID()); err != nil {
		t.Fatalf("unexpected error revoking: %v", err)
	}

	e.Clock.Advance(2 * time.Hour)
	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	found, err := getUC.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(found) != 1 || found[0].Status() != reservation.StatusRevoked {
		t.Fatalf("expected the Revoked reservation to remain Revoked, got %+v", found)
	}
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ReservationExpired" {
			t.Fatalf("did not expect ReservationExpired for an already-Revoked reservation, got events=%v", e.Events.Events())
		}
	}
}

// TestRevokeReservation_AfterExpiry_Rejected proves the lazy-expiry check
// on the RevokeReservation read path: attempting to revoke a reservation
// that has already timed out resolves it to Expired first, so the Revoke()
// call itself fails with ErrAlreadyResolved, exactly as a real double-revoke
// would.
func TestRevokeReservation_AfterExpiry_Rejected(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := revokeUC.Execute(context.Background(), res.ID()); err != reservation.ErrAlreadyResolved {
		t.Fatalf("expected ErrAlreadyResolved (already lazily expired), got %v", err)
	}

	persisted, _ := e.Reservations.FindByID(context.Background(), res.ID())
	if persisted.Status() != reservation.StatusExpired {
		t.Fatalf("expected the reservation to have been lazily expired, got %v", persisted.Status())
	}
	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 10 {
		t.Fatalf("expected usable restored to 10 by the lazy expiry, got %d", usable.Usable.Int())
	}
}

// TestConfirmPick_AfterExpiry_Rejected is the ConfirmPick analogue: a pick
// confirmed after the reservation's timeout must fail, because the lazy
// expiry check resolves it to Expired before Confirm(now) runs.
func TestConfirmPick_AfterExpiry_Rejected(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	pickUC := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := pickUC.Execute(context.Background(), res.ID()); err != reservation.ErrAlreadyResolved {
		t.Fatalf("expected ErrAlreadyResolved (already lazily expired), got %v", err)
	}
}

// TestReserveStock_ExpiredExistingReservation_TreatedAsNewAttempt covers
// ReserveStock's own idempotency lookup: if the only "existing" reservation
// for a demandRef has actually timed out, a new ReserveStock call must NOT
// treat it as the live retry target (that branch is reserved for a
// genuinely still-Active reservation) — it must lazily expire the stale one
// and then allocate a brand-new reservation.
func TestReserveStock_ExpiredExistingReservation_TreatedAsNewAttempt(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}

	first, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error on first reserve: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	second, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), "order-1")
	if err != nil {
		t.Fatalf("unexpected error on second reserve: %v", err)
	}
	if second.ID() == first.ID() {
		t.Fatalf("expected a genuinely new reservation, got the same expired one back: %s", second.ID())
	}
	if second.Quantity().Int() != 5 {
		t.Fatalf("expected the new reservation's quantity=5, got %d", second.Quantity().Int())
	}

	firstPersisted, _ := e.Reservations.FindByID(context.Background(), first.ID())
	if firstPersisted.Status() != reservation.StatusExpired {
		t.Fatalf("expected the stale reservation to have been lazily expired, got %v", firstPersisted.Status())
	}
}

// --------------------------------------------------------------------
// expireIfDue error paths: every dependency failure inside the lazy-expiry
// release loop and persistence tail must abort the read with the exact
// error — never a half-expired reservation presented as a successful read.
// --------------------------------------------------------------------

// seedExpiredActiveReservation saves a rehydrated ACTIVE reservation whose
// timeout has already elapsed under demandRef, with exactly the given
// allocations. This is the persisted-invariant-bypassing seam the
// inconsistent-state tests below need: a healthy ReserveStock flow can
// never produce an allocation referencing a missing unit or one exceeding
// the unit's reserved quantity, but corrupted storage could.
func seedExpiredActiveReservation(t *testing.T, e env, demandRef string, allocations []reservation.Allocation) *reservation.Reservation {
	t.Helper()
	createdAt := e.Clock.Now().Add(-2 * time.Hour)
	res, err := reservation.New("res-seed-"+demandRef, mustSKU(t, "SKU-1"), mustQty(t, 5), demandRef, allocations, createdAt, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error building reservation: %v", err)
	}
	if err := e.Reservations.Save(context.Background(), res); err != nil {
		t.Fatalf("unexpected error saving reservation: %v", err)
	}
	return res
}

func TestGetReservationsByDemandRef_AfterExpiry_StockFindByIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	if _, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	getUC := &usecases.GetReservationsByDemandRef{
		Stock:        &failingStockRepo{delegate: e.Stock, failFindByID: true},
		Reservations: e.Reservations, Events: e.Events, Clock: e.Clock,
	}
	if _, err := getUC.Execute(context.Background(), "order-1"); err != errFake {
		t.Fatalf("expected errFake from stock FindByID inside the release loop, got %v", err)
	}
}

func TestGetReservationsByDemandRef_AfterExpiry_MissingStockUnit_Rejected(t *testing.T) {
	e := newEnv()
	seedExpiredActiveReservation(t, e, "order-missing-unit", []reservation.Allocation{
		{StockUnitID: "no-such-unit", Quantity: mustQty(t, 5)},
	})
	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	if _, err := getUC.Execute(context.Background(), "order-missing-unit"); err != usecases.ErrStockUnitNotFound {
		t.Fatalf("expected ErrStockUnitNotFound for an allocation pointing at a missing unit, got %v", err)
	}
}

func TestGetReservationsByDemandRef_AfterExpiry_ReleaseFails_PropagatesReleaseError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	units, err := e.Stock.FindBySKU(context.Background(), mustSKU(t, "SKU-1"))
	if err != nil || len(units) != 1 {
		t.Fatalf("expected exactly one stowed unit, got %d (err=%v)", len(units), err)
	}
	// The allocation claims more of the unit than it ever had reserved
	// (unit has reserved=0), so ReleaseReservation fails with the domain's
	// own ErrInsufficientReserved rather than driving usable negative.
	seedExpiredActiveReservation(t, e, "order-overallocated", []reservation.Allocation{
		{StockUnitID: units[0].ID(), Quantity: mustQty(t, 99)},
	})
	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	if _, err := getUC.Execute(context.Background(), "order-overallocated"); err != stock.ErrInsufficientReserved {
		t.Fatalf("expected ErrInsufficientReserved from ReleaseReservation, got %v", err)
	}
}

func TestGetReservationsByDemandRef_AfterExpiry_StockSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	if _, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	getUC := &usecases.GetReservationsByDemandRef{
		Stock:        &failingStockRepo{delegate: e.Stock, failSave: true},
		Reservations: e.Reservations,
		Events:       e.Events,
		Clock:        e.Clock,
	}
	if _, err := getUC.Execute(context.Background(), "order-1"); err != errFake {
		t.Fatalf("expected errFake from stock Save inside the release loop, got %v", err)
	}
}

func TestGetReservationsByDemandRef_AfterExpiry_ReservationSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	if _, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	getUC := &usecases.GetReservationsByDemandRef{
		Stock:        e.Stock,
		Reservations: &failingReservationRepo{delegate: e.Reservations, failSave: true},
		Events:       e.Events, Clock: e.Clock,
	}
	if _, err := getUC.Execute(context.Background(), "order-1"); err != errFake {
		t.Fatalf("expected errFake from reservation Save after Expire(), got %v", err)
	}

	// ReservationExpired must not be raised when the read aborted before
	// the publish step (the release loop and Expire() ran, but the
	// transition was never durably saved).
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ReservationExpired" {
			t.Fatalf("did not expect ReservationExpired on an aborted expiry, got events=%v", e.Events.Events())
		}
	}
}

func TestGetReservationsByDemandRef_AfterExpiry_EventPublishFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	if _, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	getUC := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: failingEvents{}, Clock: e.Clock}
	if _, err := getUC.Execute(context.Background(), "order-1"); err != errFake {
		t.Fatalf("expected errFake from ReservationExpired publish, got %v", err)
	}
}

// A demandRef holding BOTH a timed-out reservation and a still-live one:
// when lazy expiry of the timed-out one hits a dependency failure,
// expireAllIfDue must abort the entire read (nil results + the error)
// instead of returning a partially-resolved slice.
func TestGetReservationsByDemandRef_MixedResults_ExpiryFailure_AbortsWholeRead(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}
	if _, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 4), "order-mixed"); err != nil {
		t.Fatalf("unexpected error on first reserve: %v", err)
	}
	// The still-live second reservation is rehydrated directly because
	// ReserveStock's own demandRef-idempotency guard would just return the
	// first reservation (still ACTIVE in storage, merely timed out) rather
	// than create a second one for the same demandRef.
	live, err := reservation.New(
		"res-live", mustSKU(t, "SKU-1"), mustQty(t, 3), "order-mixed",
		[]reservation.Allocation{{StockUnitID: firstUnitID(t, e), Quantity: mustQty(t, 3)}},
		e.Clock.Now(), 3*time.Hour,
	)
	if err != nil {
		t.Fatalf("unexpected error building live reservation: %v", err)
	}
	if err := e.Reservations.Save(context.Background(), live); err != nil {
		t.Fatalf("unexpected error saving live reservation: %v", err)
	}
	e.Clock.Advance(time.Hour + time.Second)

	getUC := &usecases.GetReservationsByDemandRef{
		Stock:        &failingStockRepo{delegate: e.Stock, failSave: true},
		Reservations: e.Reservations,
		Events:       e.Events,
		Clock:        e.Clock,
	}
	results, err := getUC.Execute(context.Background(), "order-mixed")
	if err != errFake {
		t.Fatalf("expected errFake to abort the whole read, got %v", err)
	}
	if results != nil {
		t.Fatalf("expected nil results on aborted read, got %+v", results)
	}
}

// firstUnitID returns the id of the single stocked unit for SKU-1.
func firstUnitID(t *testing.T, e env) string {
	t.Helper()
	units, err := e.Stock.FindBySKU(context.Background(), mustSKU(t, "SKU-1"))
	if err != nil || len(units) != 1 {
		t.Fatalf("expected exactly one stowed unit, got %d (err=%v)", len(units), err)
	}
	return units[0].ID()
}
