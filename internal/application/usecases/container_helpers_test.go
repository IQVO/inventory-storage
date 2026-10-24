package usecases_test

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func usecasesBinFull() error { return location.ErrBinFull }

// unitsOf lists every stock unit of the in-memory repo for SKU-1.
type unitLister struct{ repo *memory.StockRepo }

func (u unitLister) mustUnits(t *testing.T) []*stock.StockUnit {
	t.Helper()
	units, err := u.repo.FindBySKU(ctx, "SKU-1")
	if err != nil {
		t.Fatal(err)
	}
	return units
}
