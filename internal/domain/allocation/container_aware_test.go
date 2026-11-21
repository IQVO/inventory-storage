package allocation_test

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

const (
	fwdBin  = shared.BinId("WH1-STOR-FWD-A01-01-01-A")
	rsvBin  = shared.BinId("WH1-STOR-RSV-A01-01-01-A")
	asrsBin = shared.BinId("WH1-AUTO-ASRS-ASRS1-00-00-S")
)

func loose(t *testing.T, id string, bin shared.BinId, qty int) *stock.StockUnit {
	t.Helper()
	u, err := stock.NewLooseUnit(id, "SKU-1", bin, shared.Quantity(qty), "WH1", "")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func inTote(t *testing.T, id, lpn string, qty int) *stock.StockUnit {
	t.Helper()
	u, err := stock.NewToteUnit(id, "SKU-1", asrsBin, shared.Quantity(qty), "WH1", "", lpn)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func unitIDs(units []*stock.StockUnit) []string {
	out := make([]string, 0, len(units))
	for _, u := range units {
		out = append(out, u.ID())
	}
	return out
}

func assertOrder(t *testing.T, got []*stock.StockUnit, want ...string) {
	t.Helper()
	g := unitIDs(got)
	if len(g) != len(want) {
		t.Fatalf("order = %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("order = %v, want %v", g, want)
		}
	}
}

func TestOrderForWithoutContainerStockIsADR0050Order(t *testing.T) {
	units := []*stock.StockUnit{loose(t, "su-b", rsvBin, 9), loose(t, "su-a", rsvBin, 2), loose(t, "su-f", fwdBin, 5)}
	faces := allocation.NewForwardFaces(fwdBin)
	assertOrder(t, allocation.OrderFor(units, faces, 12), unitIDs(allocation.Order(units, faces))...)
	assertOrder(t, allocation.OrderFor(units, faces, 12), "su-f", "su-a", "su-b")
}

func TestOrderForDrawsOneContainerThatCoversTheRequest(t *testing.T) {
	units := []*stock.StockUnit{loose(t, "su-a", rsvBin, 3), inTote(t, "su-t1", "TOTE-1", 4), inTote(t, "su-t2", "TOTE-2", 10)}
	// ADR 0050 alone: su-a(3), su-t1(4), su-t2(10) -> three holdings, two retrievals.
	assertOrder(t, allocation.OrderFor(units, allocation.ForwardFaces{}, 8), "su-t2", "su-a", "su-t1")
}

func TestOrderForPicksTheSmallestContainerThatCovers(t *testing.T) {
	units := []*stock.StockUnit{inTote(t, "su-t9", "TOTE-9", 9), inTote(t, "su-t5", "TOTE-5", 5), inTote(t, "su-t3", "TOTE-3", 3)}
	assertOrder(t, allocation.OrderFor(units, allocation.ForwardFaces{}, 5), "su-t5", "su-t3", "su-t9")
}

func TestOrderForKeepsForwardFacesFirst(t *testing.T) {
	units := []*stock.StockUnit{loose(t, "su-f", fwdBin, 2), inTote(t, "su-t1", "TOTE-1", 3), inTote(t, "su-t2", "TOTE-2", 5)}
	faces := allocation.NewForwardFaces(fwdBin)
	// Forward covers 2 of 6; the remainder 4 fits su-t2 (5) but not su-t1 (3).
	assertOrder(t, allocation.OrderFor(units, faces, 6), "su-f", "su-t2", "su-t1")
}

func TestOrderForLeavesTheOrderWhenForwardCovers(t *testing.T) {
	units := []*stock.StockUnit{loose(t, "su-f", fwdBin, 6), inTote(t, "su-t1", "TOTE-1", 9)}
	faces := allocation.NewForwardFaces(fwdBin)
	assertOrder(t, allocation.OrderFor(units, faces, 6), "su-f", "su-t1")
}

func TestOrderForLeavesTheOrderWhenNoSingleContainerCovers(t *testing.T) {
	units := []*stock.StockUnit{inTote(t, "su-t1", "TOTE-1", 3), inTote(t, "su-t2", "TOTE-2", 4)}
	assertOrder(t, allocation.OrderFor(units, allocation.ForwardFaces{}, 6), "su-t1", "su-t2")
}

func TestOrderForBoundaryAnExactFitCovers(t *testing.T) {
	units := []*stock.StockUnit{loose(t, "su-a", rsvBin, 1), inTote(t, "su-t1", "TOTE-1", 4)}
	assertOrder(t, allocation.OrderFor(units, allocation.ForwardFaces{}, 4), "su-t1", "su-a")
	assertOrder(t, allocation.OrderFor(units, allocation.ForwardFaces{}, 5), "su-a", "su-t1")
}

func TestOrderForDoesNotModifyItsInput(t *testing.T) {
	units := []*stock.StockUnit{loose(t, "su-a", rsvBin, 3), inTote(t, "su-t2", "TOTE-2", 10)}
	_ = allocation.OrderFor(units, allocation.ForwardFaces{}, 8)
	assertOrder(t, units, "su-a", "su-t2")
}

// Every unit in a forward face and short of qty: forwardPrefix walks to the
// end of the slice and no container is drawn (kills the loop-bound mutant).
func TestOrderForAllForwardFacesShortIsOrder(t *testing.T) {
	units := []*stock.StockUnit{loose(t, "su-f3", fwdBin, 3), loose(t, "su-f2", fwdBin, 2)}
	faces := allocation.NewForwardFaces(fwdBin)
	assertOrder(t, allocation.OrderFor(units, faces, 9), "su-f2", "su-f3")
}
