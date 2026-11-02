package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// Named invariant: "revoke returns to usable" — enforced end-to-end through the use case.
func TestRevokeReservation_ReturnsQuantityToUsable(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := revokeUC.Execute(context.Background(), res.ID()); err != nil {
		t.Fatalf("unexpected error revoking: %v", err)
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 10 {
		t.Fatalf("expected usable restored to 10, got %d", usable.Usable.Int())
	}
}

func TestRevokeReservation_Twice_Rejected(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	_ = revokeUC.Execute(context.Background(), res.ID())

	if err := revokeUC.Execute(context.Background(), res.ID()); err == nil {
		t.Fatalf("expected error revoking an already-revoked reservation")
	}
}

func TestRevokeReservation_ThenReserveAgain_CanDrawDifferentHolding(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	first, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 10), "order-1")

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := revokeUC.Execute(context.Background(), first.ID()); err != nil {
		t.Fatalf("unexpected error revoking: %v", err)
	}

	stowUnit(t, e, "SKU-1", "A-1-2", 10, 10)
	second, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 15), "order-2")
	if err != nil {
		t.Fatalf("unexpected error re-reserving against a different holding: %v", err)
	}
	if second.Quantity().Int() != 15 {
		t.Fatalf("expected reserved quantity=15, got %d", second.Quantity().Int())
	}
}

func TestRevokeReservation_UnknownReservation_Rejected(t *testing.T) {
	e := newEnv()
	uc := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	if err := uc.Execute(context.Background(), "unknown-id"); !errors.Is(err, usecases.ErrReservationNotFound) {
		t.Fatalf("expected ErrReservationNotFound, got %v", err)
	}
}

func TestRevokeReservation_StockUnitNotFound_Rejected(t *testing.T) {
	e := newEnv()
	sku := mustSKU(t, "SKU-1")
	allocs := []reservation.Allocation{{StockUnitID: "missing-unit", Quantity: mustQty(t, 5)}}
	res, err := reservation.New("res-1", sku, mustQty(t, 5), "order-1", allocs, e.Clock.Now(), time.Hour)
	if err != nil {
		t.Fatalf("unexpected error building reservation: %v", err)
	}
	if err := e.Reservations.Save(context.Background(), res); err != nil {
		t.Fatalf("unexpected error saving reservation: %v", err)
	}

	uc := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, usecases.ErrStockUnitNotFound) {
		t.Fatalf("expected ErrStockUnitNotFound, got %v", err)
	}
}

func TestRevokeReservation_ReservationsFindByIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	resRepo := &failingReservationRepo{delegate: e.Reservations, failFindByID: true}
	uc := &usecases.RevokeReservation{Stock: e.Stock, Reservations: resRepo, Events: e.Events, Clock: e.Clock}

	if err := uc.Execute(context.Background(), "any-id"); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestRevokeReservation_StockFindByIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	stockRepo := &failingStockRepo{delegate: e.Stock, failFindByID: true}
	uc := &usecases.RevokeReservation{Stock: stockRepo, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestRevokeReservation_StockSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	stockRepo := &failingStockRepo{delegate: e.Stock, failSave: true}
	uc := &usecases.RevokeReservation{Stock: stockRepo, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestRevokeReservation_ReservationsSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	resRepo := &failingReservationRepo{delegate: e.Reservations, failSave: true}
	uc := &usecases.RevokeReservation{Stock: e.Stock, Reservations: resRepo, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestRevokeReservation_EventPublishFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	uc := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: failingEvents{}, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}
