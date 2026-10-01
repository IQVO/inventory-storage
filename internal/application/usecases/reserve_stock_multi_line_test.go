package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// Regression: one demand (an order) holds one ACTIVE reservation PER LINE.
// The replay guard used to match on demandRef alone, so the second line's
// reserve (a different SKU) was handed back the first line's reservation
// and nothing was reserved for it -- observed live in the warehouse-day
// simulation, where every line of a multi-line order carried the same
// reservation id and cancelling the order revoked it twice (409 -> 500).
func TestReserveStock_SameDemandRefDifferentSKU_ReservesEachLine(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	stowUnit(t, e, "SKU-2", "A-1-2", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	line1, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-multi-line")
	if err != nil {
		t.Fatalf("line 1: %v", err)
	}
	line2, err := uc.Execute(context.Background(), mustSKU(t, "SKU-2"), mustQty(t, 3), "order-multi-line")
	if err != nil {
		t.Fatalf("line 2: %v", err)
	}
	if line1.ID() == line2.ID() {
		t.Fatalf("line 2 was handed line 1's reservation %s", line1.ID())
	}
	if got := line2.SKU().String(); got != "SKU-2" {
		t.Fatalf("line 2 reservation SKU = %s, want SKU-2", got)
	}
	assertUsable(t, e, "SKU-1", 8)
	assertUsable(t, e, "SKU-2", 7)
}

// A same-SKU line of the same demand still short of stock must not be
// silently satisfied by another line's reservation: it is a real reserve
// that can fail with insufficient stock (i.e. a backorder upstream).
func TestReserveStock_SameDemandRefDifferentSKU_InsufficientStockIsReported(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	stowUnit(t, e, "SKU-SCARCE", "A-1-2", 1, 1)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-backorder"); err != nil {
		t.Fatalf("line 1: %v", err)
	}
	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-SCARCE"), mustQty(t, 2), "order-backorder")
	if !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("scarce line err = %v, want ErrInsufficientUsable", err)
	}
}

// Same SKU, different quantity under one demandRef is not a replay either.
func TestReserveStock_SameDemandRefSameSKUDifferentQuantity_IsNotAReplay(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	first, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-qty")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 3), "order-qty")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.ID() == second.ID() {
		t.Fatal("a different quantity must not be treated as a replay")
	}
	assertUsable(t, e, "SKU-1", 5)
}

func assertUsable(t *testing.T, e env, sku string, want int) {
	t.Helper()
	usable, err := (&usecases.GetUsable{Stock: e.Stock}).Execute(context.Background(), mustSKU(t, sku))
	if err != nil {
		t.Fatalf("GetUsable(%s): %v", sku, err)
	}
	if usable.Usable.Int() != want {
		t.Fatalf("usable %s = %d, want %d", sku, usable.Usable.Int(), want)
	}
}
