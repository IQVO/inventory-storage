package stock_test

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// CanReserveWhole is the gate ReserveFullCases uses: a sealed case of the
// requesting owner that no reservation touches and that still holds eaches.
func TestCanReserveWhole(t *testing.T) {
	a := shared.OwnerRef("seller-a")
	b := shared.OwnerRef("seller-b")
	reservedCase := stock.RehydrateStockUnit("su-r", "SKU-1", "BIN-C", 12, 3, stock.StateAvailable, 1).
		WithOwnership(a, stock.PackSealedCase)
	emptyCase := stock.RehydrateStockUnit("su-e", "SKU-1", "BIN-C", 0, 0, stock.StateAvailable, 1).
		WithOwnership(a, stock.PackSealedCase)
	unlocatedCase := stock.RehydrateStockUnit("su-q", "SKU-1", "BIN-C", 12, 0, stock.StateUnlocated, 1).
		WithOwnership(a, stock.PackSealedCase)

	tests := []struct {
		name  string
		unit  *stock.StockUnit
		owner shared.OwnerRef
		want  bool
	}{
		{"untouched sealed case of the same owner", caseUnit(t, a, 12), a, true},
		{"sealed case of another owner", caseUnit(t, a, 12), b, false},
		{"fleet owner cannot take a seller case", caseUnit(t, a, 12), shared.OwnerRef(""), false},
		{"loose stock is never a whole case", looseUnit(t, a, 12), a, false},
		{"sealed case with a reservation on it", reservedCase, a, false},
		{"sealed case with no eaches", emptyCase, a, false},
		{"unlocated sealed case", unlocatedCase, a, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.unit.CanReserveWhole(tc.owner); got != tc.want {
				t.Fatalf("CanReserveWhole = %v, want %v", got, tc.want)
			}
		})
	}
}
