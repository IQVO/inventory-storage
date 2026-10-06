//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func TestPostgres_StockUnitRoundTrip(t *testing.T) {
	databaseURL := migratedDB(t)

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("IT-STOCK-BIN")
	bin, err := location.NewBin(binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error building bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("unexpected error saving bin: %v", err)
	}

	repo := postgres.NewStockRepo(pool)
	id, err := repo.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating ID: %v", err)
	}

	sku, _ := shared.NewSKU("IT-SKU-1")
	unit, err := stock.NewStockUnit(id, sku, binID, mustQty(t, 7))
	if err != nil {
		t.Fatalf("unexpected error building stock unit: %v", err)
	}
	if err := unit.Reserve(mustQty(t, 3)); err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	if err := repo.Save(ctx, unit); err != nil {
		t.Fatalf("unexpected error saving stock unit: %v", err)
	}

	found, err := repo.FindByID(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error finding stock unit: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find the saved stock unit")
	}
	if found.Quantity().Int() != 7 || found.Reserved().Int() != 3 || found.State() != stock.StateReserved {
		t.Fatalf("expected round-tripped fields to match, got quantity=%d reserved=%d state=%v",
			found.Quantity().Int(), found.Reserved().Int(), found.State())
	}

	// Query-specific: FindBySKU and FindByBin both surface the same unit.
	bySKU, err := repo.FindBySKU(ctx, sku)
	if err != nil {
		t.Fatalf("unexpected error finding by SKU: %v", err)
	}
	if !containsStockUnitID(bySKU, id) {
		t.Fatalf("expected FindBySKU to include %s, got %d units", id, len(bySKU))
	}

	byBin, err := repo.FindByBin(ctx, binID)
	if err != nil {
		t.Fatalf("unexpected error finding by bin: %v", err)
	}
	if !containsStockUnitID(byBin, id) {
		t.Fatalf("expected FindByBin to include %s, got %d units", id, len(byBin))
	}
}

func TestPostgres_StockUnit_FindByID_UnknownReturnsNil(t *testing.T) {
	databaseURL := migratedDB(t)

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()

	repo := postgres.NewStockRepo(pool)
	found, err := repo.FindByID(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found != nil {
		t.Fatalf("expected nil for an unknown stock unit ID, got %+v", found)
	}
}

func containsStockUnitID(units []*stock.StockUnit, id string) bool {
	for _, u := range units {
		if u.ID() == id {
			return true
		}
	}
	return false
}
