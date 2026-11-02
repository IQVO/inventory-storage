package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

func TestConfirmPick_ConsumesReservationAndFreesBin(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	pickUC := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := pickUC.Execute(context.Background(), res.ID()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 4 {
		t.Fatalf("expected usable=4 after pick, got %d", usable.Usable.Int())
	}

	bin, _ := e.Locations.FindByID(context.Background(), mustBinID(t, "A-1-1"))
	if bin.Occupied().Int() != 4 {
		t.Fatalf("expected bin occupied=4 after pick freed capacity, got %d", bin.Occupied().Int())
	}
}

func TestConfirmPick_AfterRevoke_Rejected(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	_ = revokeUC.Execute(context.Background(), res.ID())

	pickUC := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := pickUC.Execute(context.Background(), res.ID()); err == nil {
		t.Fatalf("expected error confirming pick on a revoked reservation")
	}
}

func TestConfirmPick_UnknownReservation_Rejected(t *testing.T) {
	e := newEnv()
	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	if err := uc.Execute(context.Background(), "unknown-id"); !errors.Is(err, usecases.ErrReservationNotFound) {
		t.Fatalf("expected ErrReservationNotFound, got %v", err)
	}
}

func TestConfirmPick_StockUnitNotFound_Rejected(t *testing.T) {
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

	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, usecases.ErrStockUnitNotFound) {
		t.Fatalf("expected ErrStockUnitNotFound, got %v", err)
	}
}

func TestConfirmPick_BinNotFound_Rejected(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)

	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	// Swap in a fresh, empty location repo: the stock unit's BinID no longer
	// resolves, even though the stow+reserve above succeeded against the
	// original one.
	emptyLocations := memory.NewLocationRepo()
	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: emptyLocations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, usecases.ErrBinNotFound) {
		t.Fatalf("expected ErrBinNotFound, got %v", err)
	}
}

func TestConfirmPick_ReservationsFindByIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	resRepo := &failingReservationRepo{delegate: e.Reservations, failFindByID: true}
	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: resRepo, Events: e.Events, Clock: e.Clock}

	if err := uc.Execute(context.Background(), "any-id"); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestConfirmPick_StockFindByIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	stockRepo := &failingStockRepo{delegate: e.Stock, failFindByID: true}
	uc := &usecases.ConfirmPick{Stock: stockRepo, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestConfirmPick_StockSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	stockRepo := &failingStockRepo{delegate: e.Stock, failSave: true}
	uc := &usecases.ConfirmPick{Stock: stockRepo, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestConfirmPick_LocationsFindByIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	locs := &failingLocationRepo{delegate: e.Locations, failFindByID: true}
	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: locs, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestConfirmPick_LocationsSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	locs := &failingLocationRepo{delegate: e.Locations, failSave: true}
	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: locs, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestConfirmPick_ReservationsSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	resRepo := &failingReservationRepo{delegate: e.Reservations, failSave: true}
	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: resRepo, Events: e.Events, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestConfirmPick_EventPublishFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, _ := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")

	uc := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: failingEvents{}, Clock: e.Clock}
	if err := uc.Execute(context.Background(), res.ID()); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}
