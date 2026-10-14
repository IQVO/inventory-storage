package stock

import (
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Named invariant: "relocation-moves-usable-only" (ADR 0039).

func TestRelocateOut_TakesPartOfTheUsableQuantity(t *testing.T) {
	unit, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 10))

	if err := unit.RelocateOut(mustQty(t, 4)); err != nil {
		t.Fatalf("RelocateOut: %v", err)
	}
	if unit.Quantity() != 6 {
		t.Fatalf("quantity = %d, want 6", unit.Quantity())
	}
	if unit.State() != StateAvailable {
		t.Fatalf("state = %s, want AVAILABLE (quantity remains)", unit.State())
	}
	if unit.Usable() != 6 {
		t.Fatalf("usable = %d, want 6", unit.Usable())
	}
}

func TestRelocateOut_TakingEverythingRemovesTheUnit(t *testing.T) {
	unit, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))

	if err := unit.RelocateOut(mustQty(t, 5)); err != nil {
		t.Fatalf("RelocateOut: %v", err)
	}
	if unit.Quantity() != 0 {
		t.Fatalf("quantity = %d, want 0", unit.Quantity())
	}
	if unit.State() != StateRemoved {
		t.Fatalf("state = %s, want REMOVED", unit.State())
	}
}

func TestRelocateOut_NeverMovesReservedStock(t *testing.T) {
	unit, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 10))
	if err := unit.Reserve(mustQty(t, 7)); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	err := unit.RelocateOut(mustQty(t, 4))
	if !errors.Is(err, ErrInsufficientUsable) {
		t.Fatalf("err = %v, want ErrInsufficientUsable (only 3 usable)", err)
	}
	if unit.Quantity() != 10 || unit.Reserved() != 7 {
		t.Fatalf("a rejected relocation changed the unit: quantity %d reserved %d", unit.Quantity(), unit.Reserved())
	}
}

func TestRelocateOut_ExactlyTheUsableQuantityOfAReservedUnitKeepsItReserved(t *testing.T) {
	unit, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 10))
	if err := unit.Reserve(mustQty(t, 7)); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	if err := unit.RelocateOut(mustQty(t, 3)); err != nil {
		t.Fatalf("RelocateOut: %v", err)
	}
	if unit.Quantity() != 7 || unit.Reserved() != 7 {
		t.Fatalf("quantity %d reserved %d, want 7/7", unit.Quantity(), unit.Reserved())
	}
	if unit.State() != StateReserved {
		t.Fatalf("state = %s, want RESERVED", unit.State())
	}
	if unit.Usable() != 0 {
		t.Fatalf("usable = %d, want 0", unit.Usable())
	}
}

func TestRelocateOut_RejectsZeroQuantity(t *testing.T) {
	unit, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))

	if err := unit.RelocateOut(mustQty(t, 0)); !errors.Is(err, shared.ErrZeroQuantity) {
		t.Fatalf("err = %v, want ErrZeroQuantity", err)
	}
	if unit.Quantity() != 5 {
		t.Fatalf("quantity = %d, want 5", unit.Quantity())
	}
}

func TestRelocateOut_RejectsMoreThanUsable(t *testing.T) {
	unit, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))

	if err := unit.RelocateOut(mustQty(t, 6)); !errors.Is(err, ErrInsufficientUsable) {
		t.Fatalf("err = %v, want ErrInsufficientUsable", err)
	}
	if unit.Quantity() != 5 || unit.State() != StateAvailable {
		t.Fatalf("a rejected relocation changed the unit: quantity %d state %s", unit.Quantity(), unit.State())
	}
}

func TestRelocateOut_RejectsUnlocatedAndRemovedUnits(t *testing.T) {
	for _, state := range []State{StateUnlocated, StateRemoved} {
		unit := RehydrateStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), 0, state, 1)

		if err := unit.RelocateOut(mustQty(t, 1)); !errors.Is(err, ErrUnitUnlocated) {
			t.Fatalf("state %s: err = %v, want ErrUnitUnlocated", state, err)
		}
		if unit.Quantity() != 5 {
			t.Fatalf("state %s: quantity = %d, want 5", state, unit.Quantity())
		}
	}
}

func TestRelocateOut_PickedUnitKeepsPickedStateWhileQuantityRemains(t *testing.T) {
	unit := RehydrateStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), 0, StatePicked, 1)

	if err := unit.RelocateOut(mustQty(t, 2)); err != nil {
		t.Fatalf("RelocateOut: %v", err)
	}
	if unit.Quantity() != 3 || unit.State() != StatePicked {
		t.Fatalf("quantity %d state %s, want 3/PICKED", unit.Quantity(), unit.State())
	}
}
