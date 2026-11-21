package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// twoTotesInTheASRS stores TOTE-1 (3 x SKU-1) and TOTE-2 (10 x SKU-1) in the
// AS/RS location, plus 3 loose SKU-1 in a reserve bin.
func (w *tw) twoTotesInTheASRS(t *testing.T) {
	t.Helper()
	w.bin(t, decant, "WH1", 100, container.HandlingUnknown)
	w.bin(t, asrs, "WH1", 100000, container.HandlingUnknown)
	w.bin(t, "WH1-STOR-RSV-A01-01-01-A", "WH1", 100, container.HandlingUnknown)
	for lpn, qty := range map[string]int{"TOTE-1": 3, "TOTE-2": 10} {
		if _, _, err := w.Register.Execute(context.Background(), lpn); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Stow.Execute(context.Background(), lpn, decant); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Fill.Execute(context.Background(), usecases.FillToteCommand{LPN: lpn, SKU: "SKU-1", Quantity: sharedQty(qty)}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Stow.Execute(context.Background(), lpn, asrs); err != nil {
			t.Fatal(err)
		}
	}
	stow := &usecases.StowStock{Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock}
	if _, err := stow.Execute(context.Background(), "SKU-1", 3, "WH1-STOR-RSV-A01-01-01-A"); err != nil {
		t.Fatal(err)
	}
}

func TestReserveFromAutomatedStorageNamesOneCoveringContainer(t *testing.T) {
	w := newTW()
	w.twoTotesInTheASRS(t)
	res, err := w.Reserve.ExecuteFor(context.Background(), usecases.ReserveRequest{SKU: "SKU-1", Quantity: 8, DemandRef: "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	a := res.Allocations()
	if len(a) != 1 || a[0].ContainerLPN != "TOTE-2" || string(a[0].BinID) != asrs || a[0].Quantity.Int() != 8 {
		t.Fatalf("allocations = %+v, want one of 8 from TOTE-2 in the AS/RS location", a)
	}
}

func TestReserveLooseStockNamesNoContainer(t *testing.T) {
	w := newTW()
	w.twoTotesInTheASRS(t)
	res, err := w.Reserve.ExecuteFor(context.Background(), usecases.ReserveRequest{SKU: "SKU-1", Quantity: 3, DemandRef: "order-2"})
	if err != nil {
		t.Fatal(err)
	}
	// 3 fits TOTE-1 (3) exactly: the smallest covering container comes first.
	if a := res.Allocations(); len(a) != 1 || a[0].ContainerLPN != "TOTE-1" {
		t.Fatalf("allocations = %+v", a)
	}
	res, err = w.Reserve.ExecuteFor(context.Background(), usecases.ReserveRequest{SKU: "SKU-1", Quantity: 13, DemandRef: "order-3"})
	if err != nil {
		t.Fatal(err)
	}
	loose := 0
	for _, a := range res.Allocations() {
		if a.ContainerLPN == "" {
			loose += a.Quantity.Int()
		}
	}
	if loose != 3 {
		t.Fatalf("loose share = %d, want the 3 reserve-bin eaches with no container", loose)
	}
}

func TestFullCaseAllocationNamesTheCase(t *testing.T) {
	w := newTW()
	w.bin(t, "S1-A-Z-1-1-1-1", "S1", 100, container.HandlingCase)
	w.registered(t, "003400000000000017")
	if _, err := w.cw.Stow.Execute(context.Background(), "003400000000000017", "S1-A-Z-1-1-1-1"); err != nil {
		t.Fatal(err)
	}
	res, err := w.Cases.Execute(context.Background(), usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D-1", OwnerRef: "seller-a"})
	if err != nil {
		t.Fatal(err)
	}
	if a := res.Allocations(); len(a) != 1 || a[0].ContainerLPN != "003400000000000017" {
		t.Fatalf("allocations = %+v", a)
	}
}

func sharedQty(n int) shared.Quantity { return shared.Quantity(n) }
