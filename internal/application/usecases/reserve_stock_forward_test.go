package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// forwardEnv stows SKU-1 into the given bins (in the given order, so a naive
// first-fit would drain them in that order) and declares forwardBin the SKU's
// forward pick face in the approved slot plan of site WH1.
func forwardEnv(t *testing.T, forwardBin string, stows [][2]any) (env, *usecases.ReserveStock) {
	t.Helper()
	e := newEnv()
	for _, s := range stows {
		stowUnit(t, e, "SKU-1", s[0].(string), 100, s[1].(int))
	}
	faces := memory.NewForwardFaceRepo()
	m, err := allocation.NewSiteMap("WH1", "plan-1", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		[]allocation.RawAssignment{{SKU: "SKU-1", Slot: forwardBin}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := faces.ReplaceSiteMap(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return e, &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Forward: faces}
}

func drawn(res *reservation.Reservation) map[string]int {
	out := map[string]int{}
	for _, a := range res.Allocations() {
		out[a.BinID.String()] += a.Quantity.Int()
	}
	return out
}

func assertDrawn(t *testing.T, res *reservation.Reservation, want map[string]int) {
	t.Helper()
	got := drawn(res)
	if len(got) != len(want) {
		t.Fatalf("drawn from %v, want %v", got, want)
	}
	for bin, q := range want {
		if got[bin] != q {
			t.Fatalf("drawn from %v, want %v", got, want)
		}
	}
}

func reserve(t *testing.T, uc *usecases.ReserveStock, qty int, demand string) *reservation.Reservation {
	t.Helper()
	res, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, qty), demand)
	if err != nil {
		t.Fatalf("reserve %d: %v", qty, err)
	}
	return res
}

// Slotting has an effect: with a full forward bin and a full reserve bin, a
// pick consumes the forward bin first, even though the reserve bin was
// stowed first (first-fit would have drained it).
func TestReserveStock_FullForwardAndFullReserve_ConsumesForwardFirst(t *testing.T) {
	_, uc := forwardEnv(t, "F-1", [][2]any{{"R-1", 20}, {"F-1", 20}})
	res := reserve(t, uc, 6, "order-1")
	assertDrawn(t, res, map[string]int{"F-1": 6})
}

// Replenishment stays meaningful: once the forward bin is exhausted the
// reserve bin serves the pick, so BinStockLevelChanged on the forward bin
// can still fall under its minimum and raise replenishment demand.
func TestReserveStock_ForwardExhausted_FallsBackToReserve(t *testing.T) {
	_, uc := forwardEnv(t, "F-1", [][2]any{{"R-1", 20}, {"F-1", 5}})
	assertDrawn(t, reserve(t, uc, 5, "order-1"), map[string]int{"F-1": 5})
	assertDrawn(t, reserve(t, uc, 4, "order-2"), map[string]int{"R-1": 4})
}

// Partial forward: the forward remainder is taken first, then the request
// spills to reserve in the same deterministic order.
func TestReserveStock_PartialForward_TakesForwardRemainderThenSpillsToReserve(t *testing.T) {
	_, uc := forwardEnv(t, "F-1", [][2]any{{"R-2", 20}, {"R-1", 30}, {"F-1", 3}})
	res := reserve(t, uc, 8, "order-1")
	assertDrawn(t, res, map[string]int{"F-1": 3, "R-2": 5})
	if first := res.Allocations()[0].BinID.String(); first != "F-1" {
		t.Fatalf("first allocation = %s, want the forward bin", first)
	}
}

// Ties between reserve bins resolve by usable then bin id, never by the
// order the rows happen to come back from storage.
func TestReserveStock_EqualReserveBins_ResolveByBinID(t *testing.T) {
	_, uc := forwardEnv(t, "F-1", [][2]any{{"R-9", 10}, {"R-2", 10}, {"R-5", 10}})
	assertDrawn(t, reserve(t, uc, 4, "order-1"), map[string]int{"R-2": 4})
}

// A forward face held only by ANOTHER SKU's plan assignment is just reserve
// storage for this SKU.
func TestReserveStock_BinForwardForAnotherSKU_IsReserveForThisOne(t *testing.T) {
	e, _ := forwardEnv(t, "F-1", nil)
	stowUnit(t, e, "SKU-1", "R-1", 100, 10)
	stowUnit(t, e, "SKU-1", "F-9", 100, 10)
	faces := memory.NewForwardFaceRepo()
	m, _ := allocation.NewSiteMap("WH1", "plan-1", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		[]allocation.RawAssignment{{SKU: "SKU-OTHER", Slot: "F-9"}})
	if _, err := faces.ReplaceSiteMap(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Forward: faces}
	// Equal usable, both reserve for SKU-1: bin id order picks F-9 < R-1.
	assertDrawn(t, reserve(t, uc, 4, "order-1"), map[string]int{"F-9": 4})
}

// The invariants hold: never more than usable, and a rejection changes nothing.
func TestReserveStock_ForwardFirst_NeverOverReserves(t *testing.T) {
	e, uc := forwardEnv(t, "F-1", [][2]any{{"R-1", 5}, {"F-1", 5}})
	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 11), "order-1")
	if !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("err = %v, want ErrInsufficientUsable", err)
	}
	usable, _ := (&usecases.GetUsable{Stock: e.Stock}).Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 10 {
		t.Fatalf("usable = %d after rejected reserve, want 10", usable.Usable.Int())
	}
	assertDrawn(t, reserve(t, uc, 10, "order-2"), map[string]int{"F-1": 5, "R-1": 5})
}
