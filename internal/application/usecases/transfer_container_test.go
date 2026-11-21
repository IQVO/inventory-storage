package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// carry (Task 7 review): a transfer drawn from a tote names the tote (D7:
// every allocation drawn from a unit in a tote or a sealed case names its
// container); one drawn from a loose unit names none.

// transferFrom wires AllocateTransferStock over the tote world's stock.
func (w *tw) transferFrom() *usecases.AllocateTransferStock {
	return &usecases.AllocateTransferStock{
		Stock: w.Stock, Reservations: w.Reservations, Transfers: memory.NewTransferAllocationRepo(),
		Events: w.Events, Clock: w.Clock,
	}
}

func transferAllocations(t *testing.T, uc *usecases.AllocateTransferStock, lineID string, qty int) []reservation.Allocation {
	t.Helper()
	res, err := uc.Execute(context.Background(), cmd("tr-1", lineID, "WH1", "SKU-1", qty))
	if err != nil {
		t.Fatal(err)
	}
	if res.Reservation == nil {
		t.Fatalf("transfer %s was not allocated: %s", lineID, res.Allocation.Outcome())
	}
	return res.Reservation.Allocations()
}

func TestTransferDrawnFromAToteNamesTheTote(t *testing.T) {
	w := newTW()
	w.decanted(t)
	if _, err := w.Stow.Execute(context.Background(), "TOTE-1", asrs); err != nil {
		t.Fatalf("stow into the AS/RS: %v", err)
	}
	a := transferAllocations(t, w.transferFrom(), "tl-1", 4)
	if len(a) != 1 || a[0].ContainerLPN != "TOTE-1" || string(a[0].BinID) != asrs {
		t.Fatalf("allocations = %+v, want one from TOTE-1 in the AS/RS location", a)
	}
}

func TestTransferDrawnFromLooseStockNamesNoContainer(t *testing.T) {
	w := newTW()
	w.bin(t, "WH1-STOR-RSV-A01-01-01-A", "WH1", 100, container.HandlingUnknown)
	unit, err := stock.NewStockUnitAtSite("su-loose", "SKU-1", "WH1-STOR-RSV-A01-01-01-A", sharedQty(5), "WH1")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Stock.Save(context.Background(), unit); err != nil {
		t.Fatal(err)
	}
	a := transferAllocations(t, w.transferFrom(), "tl-2", 3)
	if len(a) != 1 || a[0].ContainerLPN != "" || a[0].StockUnitID != "su-loose" {
		t.Fatalf("allocations = %+v, want one from the loose unit naming no container", a)
	}
}
