package location

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

func TestNewBin_RejectsInvalidCapacity(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	if _, err := NewBin(binID, mustQty(t, 0)); err != ErrInvalidCapacity {
		t.Fatalf("expected ErrInvalidCapacity, got %v", err)
	}
}

func TestBin_Occupy_WithinCapacity_Succeeds(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))

	if err := bin.Occupy(mustQty(t, 7)); err != nil {
		t.Fatalf("expected stow to succeed within capacity, got %v", err)
	}
	if bin.Occupied().Int() != 7 {
		t.Fatalf("expected occupied=7, got %d", bin.Occupied().Int())
	}
}

// Named invariant: "bin-capacity rejection" — a full bin rejects stow.
func TestBin_Occupy_ExceedsCapacity_Rejected(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))

	if err := bin.Occupy(mustQty(t, 10)); err != nil {
		t.Fatalf("expected initial fill to succeed, got %v", err)
	}

	err := bin.Occupy(mustQty(t, 1))
	if err != ErrBinFull {
		t.Fatalf("expected ErrBinFull when stowing into a full bin, got %v", err)
	}
	if bin.Occupied().Int() != 10 {
		t.Fatalf("occupied should be unchanged after rejected stow, got %d", bin.Occupied().Int())
	}
}

func TestBin_Occupy_PartialOverflow_Rejected(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))
	_ = bin.Occupy(mustQty(t, 8))

	if err := bin.Occupy(mustQty(t, 3)); err != ErrBinFull {
		t.Fatalf("expected ErrBinFull, got %v", err)
	}
}

func TestBin_Release_ReturnsCapacity(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))
	_ = bin.Occupy(mustQty(t, 6))

	if err := bin.Release(mustQty(t, 4)); err != nil {
		t.Fatalf("unexpected error releasing: %v", err)
	}
	if bin.Occupied().Int() != 2 {
		t.Fatalf("expected occupied=2, got %d", bin.Occupied().Int())
	}
}

func TestBin_Release_ExceedsOccupancy_Rejected(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))
	_ = bin.Occupy(mustQty(t, 2))

	if err := bin.Release(mustQty(t, 5)); err != ErrReleaseExceedsOccupancy {
		t.Fatalf("expected ErrReleaseExceedsOccupancy, got %v", err)
	}
}

func TestNewBin_RejectsEmptyID(t *testing.T) {
	if _, err := NewBin("", mustQty(t, 10)); err != shared.ErrEmptyBinID {
		t.Fatalf("expected ErrEmptyBinID, got %v", err)
	}
}

func TestBin_Occupy_RejectsZeroQuantity(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))

	if err := bin.Occupy(mustQty(t, 0)); err != shared.ErrZeroQuantity {
		t.Fatalf("expected ErrZeroQuantity, got %v", err)
	}
}

func TestBin_Release_RejectsZeroQuantity(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))

	if err := bin.Release(mustQty(t, 0)); err != shared.ErrZeroQuantity {
		t.Fatalf("expected ErrZeroQuantity, got %v", err)
	}
}

func TestBin_Accessors(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := NewBin(binID, mustQty(t, 10))
	_ = bin.Occupy(mustQty(t, 4))

	if bin.ID() != binID {
		t.Fatalf("expected ID()=%v, got %v", binID, bin.ID())
	}
	if bin.Capacity().Int() != 10 {
		t.Fatalf("expected Capacity()=10, got %d", bin.Capacity().Int())
	}
	if bin.Available().Int() != 6 {
		t.Fatalf("expected Available()=6, got %d", bin.Available().Int())
	}
	if bin.IsFull() {
		t.Fatalf("expected IsFull()=false at partial occupancy")
	}
	_ = bin.Occupy(mustQty(t, 6))
	if !bin.IsFull() {
		t.Fatalf("expected IsFull()=true at full occupancy")
	}
}

func TestRehydrateBin_ReconstructsWithoutValidation(t *testing.T) {
	binID, _ := shared.NewBinId("A-1-1")
	bin := RehydrateBin(binID, mustQty(t, 10), mustQty(t, 4), 3)

	if bin.ID() != binID || bin.Capacity().Int() != 10 || bin.Occupied().Int() != 4 {
		t.Fatalf("expected rehydrated fields to round-trip, got id=%v capacity=%d occupied=%d",
			bin.ID(), bin.Capacity().Int(), bin.Occupied().Int())
	}
	if bin.Version() != 3 {
		t.Fatalf("expected rehydrated version to round-trip, got %d", bin.Version())
	}
}
