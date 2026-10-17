package memory_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

const ledgerBin = shared.BinId("WH1-STOR-AMB-A01-01-01-A")

func mustUnit(t *testing.T, id string, sku shared.SKU, qty int) *stock.StockUnit {
	t.Helper()
	unit, err := stock.NewStockUnit(id, sku, ledgerBin, shared.Quantity(qty))
	if err != nil {
		t.Fatal(err)
	}
	return unit
}

// seedLedgerWorld stores, for SKU-1: a live unit (10, 4 reserved), an
// unlocated unit (3) and a fully picked-out REMOVED unit; plus 9 of SKU-2.
func seedLedgerWorld(t *testing.T) *memory.StockRepo {
	t.Helper()
	repo := memory.NewStockRepo()
	live := mustUnit(t, "su-1", "SKU-1", 10)
	lost := mustUnit(t, "su-2", "SKU-1", 3)
	gone := mustUnit(t, "su-3", "SKU-1", 2)
	other := mustUnit(t, "su-4", "SKU-2", 9)
	lost.MarkUnlocated()
	for _, step := range []error{live.Reserve(4), gone.Reserve(2), gone.Pick(2)} { // gone: REMOVED, quantity 0
		if step != nil {
			t.Fatal(step)
		}
	}
	for _, u := range []*stock.StockUnit{live, lost, gone, other} {
		if err := repo.Save(context.Background(), u); err != nil {
			t.Fatal(err)
		}
	}
	return repo
}

func TestBinLevelLedger_LevelsExcludeUnlocatedAndRemovedUnits(t *testing.T) {
	ledger := memory.NewBinLevelLedger(seedLedgerWorld(t))

	level, err := ledger.Advance(context.Background(), ledgerBin, "SKU-1")
	if err != nil {
		t.Fatal(err)
	}

	if level.OnHand != 10 || level.Reserved != 4 {
		t.Fatalf("level = %+v, want on_hand 10 (the unlocated 3 and the removed unit excluded) reserved 4", level)
	}
}

func TestBinLevelLedger_VersionsRisePerBinSKU(t *testing.T) {
	ledger := memory.NewBinLevelLedger(seedLedgerWorld(t))
	ctx := context.Background()

	first, _ := ledger.Advance(ctx, ledgerBin, "SKU-1")
	second, _ := ledger.Advance(ctx, ledgerBin, "SKU-1")
	otherSKU, _ := ledger.Advance(ctx, ledgerBin, "SKU-2")

	if first.Version != 1 || second.Version != 2 {
		t.Fatalf("versions %d, %d, want 1, 2", first.Version, second.Version)
	}
	if otherSKU.OnHand != 9 || otherSKU.Version != 1 {
		t.Fatalf("other sku = %+v, want its own counter starting at 1", otherSKU)
	}
}
