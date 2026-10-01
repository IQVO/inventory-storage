package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

func stowUnit(t *testing.T, e env, sku string, binID string, capacity, qty int) {
	t.Helper()
	seedBin(t, e, mustBinID(t, binID), capacity)
	stow := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}
	if _, err := stow.Execute(context.Background(), mustSKU(t, sku), mustQty(t, qty), mustBinID(t, binID)); err != nil {
		t.Fatalf("unexpected error stowing: %v", err)
	}
}

func TestReserveStock_WithinUsable_Succeeds(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	res, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Quantity().Int() != 6 {
		t.Fatalf("expected reserved quantity=6, got %d", res.Quantity().Int())
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 4 {
		t.Fatalf("expected usable=4 after reserve, got %d", usable.Usable.Int())
	}
}

// Named invariant: "reservation <= usable" — enforced end-to-end through the use case.
func TestReserveStock_ExceedsUsable_Rejected(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 5)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != usecases.ErrInsufficientUsable {
		t.Fatalf("expected ErrInsufficientUsable, got %v", err)
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 5 {
		t.Fatalf("usable must be unchanged on rejected reserve, got %d", usable.Usable.Int())
	}
}

func TestReserveStock_SpansMultipleStockUnits(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 5, 5)
	stowUnit(t, e, "SKU-1", "A-1-2", 5, 5)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	res, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 8), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Allocations()) != 2 {
		t.Fatalf("expected allocations across 2 stock units, got %d", len(res.Allocations()))
	}
	// Pick location (ADR 0025): each allocation carries the bin of the
	// StockUnit it drew from, captured at allocation time.
	bins := map[string]bool{}
	for _, a := range res.Allocations() {
		unit, _ := e.Stock.FindByID(context.Background(), a.StockUnitID)
		if unit == nil || a.BinID != unit.BinID() {
			t.Fatalf("expected allocation BinID to match its stock unit's bin, got %+v (unit %v)", a, unit)
		}
		bins[a.BinID.String()] = true
	}
	if !bins["A-1-1"] || !bins["A-1-2"] {
		t.Fatalf("expected pick locations A-1-1 and A-1-2, got %v", bins)
	}
}

func TestReserveStock_RejectsZeroQuantity(t *testing.T) {
	e := newEnv()
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), 0, "order-1")
	if err == nil {
		t.Fatalf("expected error for zero quantity")
	}
}

func TestReserveStock_NoStockForSKU_Rejected(t *testing.T) {
	e := newEnv()
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), "order-1")
	if err != usecases.ErrInsufficientUsable {
		t.Fatalf("expected ErrInsufficientUsable, got %v", err)
	}
}

func TestReserveStock_StockFindBySKUFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stockRepo := &failingStockRepo{delegate: e.Stock, failFindBySKU: true}
	uc := &usecases.ReserveStock{Stock: stockRepo, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), "order-1")
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestReserveStock_StockSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	stockRepo := &failingStockRepo{delegate: e.Stock, failSave: true}
	uc := &usecases.ReserveStock{Stock: stockRepo, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestReserveStock_ReservationsNextIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	resRepo := &failingReservationRepo{delegate: e.Reservations, failNextID: true}
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: resRepo, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestReserveStock_ReservationsSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	resRepo := &failingReservationRepo{delegate: e.Reservations, failSave: true}
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: resRepo, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestReserveStock_EventPublishFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: failingEvents{}, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

// Named invariant: a retried request against the same demandRef must not
// double-reserve stock or create a second Reservation row — this is the
// idempotency guard flagged as a follow-up in REST_AUDIT.md and fixed here.
func TestReserveStock_SameDemandRefRetried_ReturnsSameReservation_NoDoubleReserve(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	first, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-retry-1")
	if err != nil {
		t.Fatalf("unexpected error on first call: %v", err)
	}

	// Simulate a client-side retry after a dropped response: same
	// demandRef, same call, no knowledge the first one succeeded.
	second, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-retry-1")
	if err != nil {
		t.Fatalf("unexpected error on retry: %v", err)
	}

	if second.ID() != first.ID() {
		t.Fatalf("expected retry to return the same reservation ID, got first=%s second=%s", first.ID(), second.ID())
	}

	all, err := e.Reservations.FindByDemandRef(context.Background(), "order-retry-1")
	if err != nil {
		t.Fatalf("unexpected error listing reservations: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected exactly 1 reservation to exist for the demandRef after a retry, got %d", len(all))
	}

	// Usable must reflect only ONE reservation of 6, not two (i.e. not
	// double-reserved): 10 stowed - 6 reserved = 4 usable.
	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, err := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usable.Usable.Int() != 4 {
		t.Fatalf("expected usable=4 (stock reserved only once), got %d", usable.Usable.Int())
	}
}

// A demandRef with only a REVOKED reservation must be treated as a genuine
// new attempt, not a retry — the idempotency guard only short-circuits on
// an unresolved (Active) reservation.
func TestReserveStock_SameDemandRefAfterRevoke_CreatesNewReservation(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	first, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-revoke-retry")
	if err != nil {
		t.Fatalf("unexpected error on first call: %v", err)
	}

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := revokeUC.Execute(context.Background(), first.ID()); err != nil {
		t.Fatalf("unexpected error revoking: %v", err)
	}

	second, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 3), "order-revoke-retry")
	if err != nil {
		t.Fatalf("unexpected error on new reserve after revoke: %v", err)
	}
	if second.ID() == first.ID() {
		t.Fatalf("expected a genuinely new reservation after revoke, got the same ID %s", second.ID())
	}

	all, err := e.Reservations.FindByDemandRef(context.Background(), "order-revoke-retry")
	if err != nil {
		t.Fatalf("unexpected error listing reservations: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 reservations (revoked + new), got %d", len(all))
	}
}

func TestReserveStock_FindByDemandRefFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	resRepo := &failingReservationRepo{delegate: e.Reservations, failFindByDemandRef: true}
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: resRepo, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestReserveStock_CustomTimeout_Applied(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour}

	res, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.ExpiresAt().Equal(res.CreatedAt().Add(time.Hour)) {
		t.Fatalf("expected custom timeout of 1h to apply, got expiresAt=%v createdAt=%v", res.ExpiresAt(), res.CreatedAt())
	}
}
