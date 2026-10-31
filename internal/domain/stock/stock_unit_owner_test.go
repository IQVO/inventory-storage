package stock_test

import (
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func looseUnit(t *testing.T, owner shared.OwnerRef, qty int) *stock.StockUnit {
	t.Helper()
	u, err := stock.NewLooseUnit("su-1", "SKU-1", "BIN-1", shared.Quantity(qty), "S1", owner)
	if err != nil {
		t.Fatalf("NewLooseUnit: %v", err)
	}
	return u
}

func caseUnit(t *testing.T, owner shared.OwnerRef, qty int) *stock.StockUnit {
	t.Helper()
	u, err := stock.NewSealedCaseUnit("su-c", "SKU-1", "BIN-C", shared.Quantity(qty), "S1", owner)
	if err != nil {
		t.Fatalf("NewSealedCaseUnit: %v", err)
	}
	return u
}

func TestPackStateDefaultsToLoose(t *testing.T) {
	legacy := stock.RehydrateStockUnit("su-0", "SKU-1", "BIN-1", 5, 0, stock.StateAvailable, 1)
	if legacy.PackState() != stock.PackLoose || !legacy.IsLoose() || !legacy.OwnerRef().IsFleet() {
		t.Fatalf("a legacy row must read as loose fleet-owned stock, got %q owner %q", legacy.PackState(), legacy.OwnerRef())
	}
	if got := stock.ParsePackState(""); got != stock.PackLoose {
		t.Errorf("ParsePackState(\"\") = %q, want LOOSE", got)
	}
	if got := stock.ParsePackState("SEALED_CASE"); got != stock.PackSealedCase {
		t.Errorf("ParsePackState(SEALED_CASE) = %q", got)
	}
}

func TestReservableByRequiresSameOwnerAndLooseStock(t *testing.T) {
	a := shared.OwnerRef("seller-a")
	cases := []struct {
		name  string
		unit  *stock.StockUnit
		owner shared.OwnerRef
		want  bool
	}{
		{"same owner loose", looseUnit(t, a, 5), a, true},
		{"other owner loose", looseUnit(t, a, 5), "seller-b", false},
		{"owned stock is not fleet stock", looseUnit(t, a, 5), "", false},
		{"fleet stock is not owned stock", looseUnit(t, "", 5), a, false},
		{"fleet stock for fleet", looseUnit(t, "", 5), "", true},
		{"sealed case is never quantity-reservable", caseUnit(t, a, 12), a, false},
	}
	for _, tc := range cases {
		if got := tc.unit.ReservableBy(tc.owner); got != tc.want {
			t.Errorf("%s: ReservableBy = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestReserveForRejectsAnotherOwner(t *testing.T) {
	u := looseUnit(t, "seller-a", 5)
	if err := u.ReserveFor("seller-b", 1); !errors.Is(err, shared.ErrOwnerMismatch) {
		t.Fatalf("ReserveFor other owner = %v, want ErrOwnerMismatch", err)
	}
	if u.Reserved() != 0 {
		t.Fatalf("a rejected reservation must not move reserved, got %d", u.Reserved())
	}
	if err := u.ReserveFor("seller-a", 2); err != nil || u.Reserved() != 2 {
		t.Fatalf("ReserveFor same owner = %v reserved %d", err, u.Reserved())
	}
}

func TestReserveWholeBindsTheWholeCaseOnly(t *testing.T) {
	c := caseUnit(t, "seller-a", 12)
	if err := c.ReserveWhole("seller-a"); err != nil {
		t.Fatalf("ReserveWhole: %v", err)
	}
	if c.Reserved() != 12 || c.State() != stock.StateReserved {
		t.Fatalf("whole case reserved = %d state %s", c.Reserved(), c.State())
	}
	if err := c.ReserveWhole("seller-a"); !errors.Is(err, stock.ErrInsufficientUsable) {
		t.Fatalf("second ReserveWhole = %v, want ErrInsufficientUsable", err)
	}
	if err := looseUnit(t, "", 12).ReserveWhole(""); !errors.Is(err, stock.ErrNotSealedCase) {
		t.Fatalf("ReserveWhole on loose = %v, want ErrNotSealedCase", err)
	}
	if err := caseUnit(t, "seller-a", 12).ReserveWhole("seller-b"); !errors.Is(err, shared.ErrOwnerMismatch) {
		t.Fatalf("ReserveWhole other owner = %v, want ErrOwnerMismatch", err)
	}
}

func TestAddQuantity(t *testing.T) {
	u := looseUnit(t, "", 5)
	if err := u.AddQuantity(7); err != nil || u.Quantity() != 12 {
		t.Fatalf("AddQuantity = %v qty %d", err, u.Quantity())
	}
	if err := u.AddQuantity(0); !errors.Is(err, shared.ErrZeroQuantity) {
		t.Fatalf("AddQuantity(0) = %v", err)
	}
	u.MarkUnlocated()
	if err := u.AddQuantity(1); !errors.Is(err, stock.ErrUnitUnlocated) {
		t.Fatalf("AddQuantity on unlocated = %v", err)
	}
}

func TestMoveTo(t *testing.T) {
	c := caseUnit(t, "", 12)
	if err := c.MoveTo("BIN-X", "S1"); err != nil || c.BinID() != "BIN-X" {
		t.Fatalf("MoveTo = %v bin %s", err, c.BinID())
	}
	if err := c.ReserveWhole(""); err != nil {
		t.Fatal(err)
	}
	if err := c.MoveTo("BIN-Y", "S1"); !errors.Is(err, stock.ErrInsufficientUsable) {
		t.Fatalf("MoveTo reserved = %v, want ErrInsufficientUsable", err)
	}
	gone := caseUnit(t, "", 12)
	gone.MarkUnlocated()
	if err := gone.MoveTo("BIN-Y", "S1"); !errors.Is(err, stock.ErrUnitUnlocated) {
		t.Fatalf("MoveTo unlocated = %v", err)
	}
}

func TestWithOwnershipRoundTrip(t *testing.T) {
	u := stock.RehydrateStockUnit("su-9", "SKU-1", "BIN-1", 12, 0, stock.StateAvailable, 3).
		WithOwnership("seller-a", stock.PackSealedCase)
	if u.OwnerRef() != "seller-a" || u.PackState() != stock.PackSealedCase {
		t.Fatalf("WithOwnership lost data: %q %q", u.OwnerRef(), u.PackState())
	}
}
