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

func TestBin_Resize(t *testing.T) {
	tests := []struct {
		name         string
		capacity     int
		occupied     int
		newCapacity  int
		wantErr      error
		wantCapacity int
	}{
		{name: "grow an empty bin", capacity: 10, occupied: 0, newCapacity: 20, wantErr: nil, wantCapacity: 20},
		{name: "shrink an empty bin", capacity: 10, occupied: 0, newCapacity: 1, wantErr: nil, wantCapacity: 1},
		{name: "same capacity is a no-op", capacity: 10, occupied: 4, newCapacity: 10, wantErr: nil, wantCapacity: 10},
		{name: "shrink above occupancy", capacity: 10, occupied: 4, newCapacity: 5, wantErr: nil, wantCapacity: 5},
		{name: "shrink exactly to occupancy fills the bin", capacity: 10, occupied: 4, newCapacity: 4, wantErr: nil, wantCapacity: 4},
		{name: "shrink below occupancy is rejected", capacity: 10, occupied: 4, newCapacity: 3, wantErr: ErrCapacityBelowOccupancy, wantCapacity: 10},
		{name: "zero capacity is rejected", capacity: 10, occupied: 0, newCapacity: 0, wantErr: ErrInvalidCapacity, wantCapacity: 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			binID, _ := shared.NewBinId("A-1-1")
			bin, err := NewBin(binID, mustQty(t, tc.capacity))
			if err != nil {
				t.Fatalf("unexpected error building bin: %v", err)
			}
			if tc.occupied > 0 {
				if err := bin.Occupy(mustQty(t, tc.occupied)); err != nil {
					t.Fatalf("unexpected error occupying bin: %v", err)
				}
			}

			err = bin.Resize(mustQty(t, tc.newCapacity))
			if err != tc.wantErr {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if bin.Capacity().Int() != tc.wantCapacity {
				t.Fatalf("expected capacity=%d, got %d", tc.wantCapacity, bin.Capacity().Int())
			}
			if bin.Occupied().Int() != tc.occupied {
				t.Fatalf("resize must never change occupancy: expected %d, got %d", tc.occupied, bin.Occupied().Int())
			}
		})
	}
}
