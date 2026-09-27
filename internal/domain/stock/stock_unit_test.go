package stock

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func mustQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("unexpected error building quantity: %v", err)
	}
	return q
}

func mustSKU(t *testing.T) shared.SKU {
	t.Helper()
	sku, _ := shared.NewSKU("SKU-1")
	return sku
}

func mustBin(t *testing.T) shared.BinId {
	t.Helper()
	bin, _ := shared.NewBinId("A-1-1")
	return bin
}

// Named invariant: "stow-requires-item-and-location".
func TestNewStockUnit_RequiresSKU(t *testing.T) {
	_, err := NewStockUnit("su-1", "", mustBin(t), mustQty(t, 5))
	if err != ErrStowRequiresItemAndLocation {
		t.Fatalf("expected ErrStowRequiresItemAndLocation, got %v", err)
	}
}

func TestNewStockUnit_RequiresBin(t *testing.T) {
	_, err := NewStockUnit("su-1", mustSKU(t), "", mustQty(t, 5))
	if err != ErrStowRequiresItemAndLocation {
		t.Fatalf("expected ErrStowRequiresItemAndLocation, got %v", err)
	}
}

func TestNewStockUnit_RequiresBothMissing_StillRejected(t *testing.T) {
	_, err := NewStockUnit("su-1", "", "", mustQty(t, 5))
	if err != ErrStowRequiresItemAndLocation {
		t.Fatalf("expected ErrStowRequiresItemAndLocation, got %v", err)
	}
}

func TestNewStockUnit_ValidItemAndLocation_Succeeds(t *testing.T) {
	u, err := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.State() != StateAvailable {
		t.Fatalf("expected StateAvailable, got %v", u.State())
	}
	if u.Usable().Int() != 5 {
		t.Fatalf("expected usable=5, got %d", u.Usable().Int())
	}
}

func TestNewStockUnit_RejectsZeroQuantity(t *testing.T) {
	if _, err := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 0)); err != shared.ErrZeroQuantity {
		t.Fatalf("expected ErrZeroQuantity, got %v", err)
	}
}

// Named invariant: "reservation <= usable".
func TestStockUnit_Reserve_ExceedsUsable_Rejected(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))

	err := u.Reserve(mustQty(t, 6))
	if err != ErrInsufficientUsable {
		t.Fatalf("expected ErrInsufficientUsable, got %v", err)
	}
	if u.State() != StateAvailable {
		t.Fatalf("state must not change on rejected reserve, got %v", u.State())
	}
	if u.Usable().Int() != 5 {
		t.Fatalf("usable must be unchanged on rejected reserve, got %d", u.Usable().Int())
	}
}

func TestStockUnit_Reserve_WithinUsable_Succeeds(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))

	if err := u.Reserve(mustQty(t, 3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.State() != StateReserved {
		t.Fatalf("expected StateReserved, got %v", u.State())
	}
	if u.Usable().Int() != 2 {
		t.Fatalf("expected usable=2, got %d", u.Usable().Int())
	}
	if u.Quantity().Int() != 5 {
		t.Fatalf("on-hand quantity must be unchanged by reserve, got %d", u.Quantity().Int())
	}
}

// No double-consume: a second reserve cannot dip below zero usable.
func TestStockUnit_Reserve_NoDoubleConsume(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 5))

	if err := u.Reserve(mustQty(t, 1)); err != ErrInsufficientUsable {
		t.Fatalf("expected ErrInsufficientUsable on double-consume, got %v", err)
	}
}

// Named invariant: "revoke returns to usable".
func TestStockUnit_ReleaseReservation_ReturnsToUsable(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 4))

	if err := u.ReleaseReservation(mustQty(t, 4)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Usable().Int() != 5 {
		t.Fatalf("expected usable restored to 5, got %d", u.Usable().Int())
	}
	if u.State() != StateAvailable {
		t.Fatalf("expected StateAvailable after full release, got %v", u.State())
	}
}

func TestStockUnit_ReleaseReservation_ExceedsReserved_Rejected(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 2))

	if err := u.ReleaseReservation(mustQty(t, 3)); err != ErrInsufficientReserved {
		t.Fatalf("expected ErrInsufficientReserved, got %v", err)
	}
}

func TestStockUnit_Pick_FullyDepletes_TransitionsToRemoved(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 5))

	if err := u.Pick(mustQty(t, 5)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.State() != StateRemoved {
		t.Fatalf("expected StateRemoved, got %v", u.State())
	}
	if u.Quantity().Int() != 0 {
		t.Fatalf("expected quantity=0, got %d", u.Quantity().Int())
	}
}

func TestStockUnit_Pick_PartialQuantityRemains_TransitionsToPicked(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 3))

	if err := u.Pick(mustQty(t, 3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.State() != StatePicked {
		t.Fatalf("expected StatePicked, got %v", u.State())
	}
	if u.Quantity().Int() != 2 {
		t.Fatalf("expected remaining quantity=2, got %d", u.Quantity().Int())
	}
}

func TestStockUnit_Pick_ExceedsReserved_Rejected(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 2))

	if err := u.Pick(mustQty(t, 3)); err != ErrInsufficientReserved {
		t.Fatalf("expected ErrInsufficientReserved, got %v", err)
	}
}

func TestStockUnit_MarkUnlocated_ZerosUsable(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	u.MarkUnlocated()

	if u.State() != StateUnlocated {
		t.Fatalf("expected StateUnlocated, got %v", u.State())
	}
	if u.Usable().Int() != 0 {
		t.Fatalf("expected usable=0 for unlocated unit, got %d", u.Usable().Int())
	}
}

func TestStockUnit_Reserve_UnlocatedUnit_Rejected(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	u.MarkUnlocated()

	if err := u.Reserve(mustQty(t, 1)); err != ErrUnitUnlocated {
		t.Fatalf("expected ErrUnitUnlocated, got %v", err)
	}
}

func TestStockUnit_Accessors(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 2))

	if u.ID() != "su-1" {
		t.Fatalf("expected ID=su-1, got %s", u.ID())
	}
	if u.SKU() != mustSKU(t) {
		t.Fatalf("expected SKU to round-trip, got %v", u.SKU())
	}
	if u.BinID() != mustBin(t) {
		t.Fatalf("expected BinID to round-trip, got %v", u.BinID())
	}
	if u.Reserved().Int() != 2 {
		t.Fatalf("expected Reserved()=2, got %d", u.Reserved().Int())
	}
}

func TestRehydrateStockUnit_ReconstructsWithoutValidation(t *testing.T) {
	u := RehydrateStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), mustQty(t, 2), StateReserved, 4)

	if u.ID() != "su-1" || u.Quantity().Int() != 5 || u.Reserved().Int() != 2 || u.State() != StateReserved {
		t.Fatalf("expected rehydrated fields to round-trip, got id=%s qty=%d reserved=%d state=%v",
			u.ID(), u.Quantity().Int(), u.Reserved().Int(), u.State())
	}
	if u.Version() != 4 {
		t.Fatalf("expected rehydrated version to round-trip, got %d", u.Version())
	}
}

func TestStockUnit_Usable_RemovedState_IsZero(t *testing.T) {
	u, _ := NewStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5))
	_ = u.Reserve(mustQty(t, 5))
	_ = u.Pick(mustQty(t, 5))

	if u.State() != StateRemoved {
		t.Fatalf("expected StateRemoved precondition, got %v", u.State())
	}
	if u.Usable().Int() != 0 {
		t.Fatalf("expected usable=0 for removed unit, got %d", u.Usable().Int())
	}
}

// Usable must never go negative: a rehydrated unit whose persisted reserved
// exceeds its on-hand quantity (only possible via corrupted storage — the
// Reserve path enforces reserved <= on-hand) reports usable 0, not a
// negative value.
func TestStockUnit_Usable_ReservedExceedsOnHand_IsZero(t *testing.T) {
	u := RehydrateStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), mustQty(t, 7), StateReserved, 1)

	if got := u.Usable().Int(); got != 0 {
		t.Fatalf("expected usable=0 when reserved exceeds on-hand, got %d", got)
	}
}

func TestStockUnit_Pick_ExceedsOnHandQuantity_Rejected(t *testing.T) {
	u := RehydrateStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 3), mustQty(t, 5), StateReserved, 1)

	if err := u.Pick(mustQty(t, 5)); err != ErrInsufficientReserved {
		t.Fatalf("expected ErrInsufficientReserved, got %v", err)
	}
}
