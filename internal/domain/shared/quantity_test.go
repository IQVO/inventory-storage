package shared

import (
	"errors"
	"testing"
)

func TestNewQuantity_RejectsNegative(t *testing.T) {
	if _, err := NewQuantity(-1); !errors.Is(err, ErrNegativeQuantity) {
		t.Fatalf("expected ErrNegativeQuantity, got %v", err)
	}
}

func TestNewQuantity_AllowsZero(t *testing.T) {
	q, err := NewQuantity(0)
	if err != nil || q.Int() != 0 {
		t.Fatalf("expected zero quantity, got %v err=%v", q, err)
	}
}

func TestNewPositiveQuantity_RejectsZero(t *testing.T) {
	if _, err := NewPositiveQuantity(0); !errors.Is(err, ErrZeroQuantity) {
		t.Fatalf("expected ErrZeroQuantity, got %v", err)
	}
}

func TestQuantity_Sub_RejectsNegativeResult(t *testing.T) {
	q, _ := NewQuantity(2)
	other, _ := NewQuantity(5)
	if _, err := q.Sub(other); !errors.Is(err, ErrNegativeQuantity) {
		t.Fatalf("expected ErrNegativeQuantity, got %v", err)
	}
}

func TestSKU_RejectsEmpty(t *testing.T) {
	if _, err := NewSKU(""); !errors.Is(err, ErrEmptySKU) {
		t.Fatalf("expected ErrEmptySKU, got %v", err)
	}
}

func TestBinId_RejectsEmpty(t *testing.T) {
	if _, err := NewBinId(""); !errors.Is(err, ErrEmptyBinID) {
		t.Fatalf("expected ErrEmptyBinID, got %v", err)
	}
}

func TestNewQuantity_AllowsPositive(t *testing.T) {
	q, err := NewQuantity(3)
	if err != nil || q.Int() != 3 {
		t.Fatalf("expected quantity=3, got %v err=%v", q, err)
	}
}

func TestNewPositiveQuantity_AllowsPositive(t *testing.T) {
	q, err := NewPositiveQuantity(3)
	if err != nil || q.Int() != 3 {
		t.Fatalf("expected quantity=3, got %v err=%v", q, err)
	}
}

func TestQuantity_Sub_Succeeds(t *testing.T) {
	q, _ := NewQuantity(5)
	other, _ := NewQuantity(2)
	result, err := q.Sub(other)
	if err != nil || result.Int() != 3 {
		t.Fatalf("expected result=3, got %v err=%v", result, err)
	}
}

// Boundary: an exactly-zero result is a valid Sub, not a negative-quantity
// error — e.g. a full release of a reservation.
func TestQuantity_Sub_ResultExactlyZero_Succeeds(t *testing.T) {
	q, _ := NewQuantity(5)
	other, _ := NewQuantity(5)
	result, err := q.Sub(other)
	if err != nil || result.Int() != 0 {
		t.Fatalf("expected result=0 with no error, got %v err=%v", result, err)
	}
}

func TestQuantity_Add(t *testing.T) {
	q, _ := NewQuantity(2)
	other, _ := NewQuantity(3)
	if got := q.Add(other).Int(); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestQuantity_GreaterThan(t *testing.T) {
	small, _ := NewQuantity(2)
	large, _ := NewQuantity(5)
	if !large.GreaterThan(small) {
		t.Fatalf("expected 5 > 2")
	}
	if small.GreaterThan(large) {
		t.Fatalf("expected 2 not > 5")
	}
	if small.GreaterThan(small) {
		t.Fatalf("expected equal quantities not greater-than each other")
	}
}

func TestSKU_String(t *testing.T) {
	sku, _ := NewSKU("SKU-1")
	if sku.String() != "SKU-1" {
		t.Fatalf("expected SKU-1, got %s", sku.String())
	}
}

func TestBinId_String(t *testing.T) {
	binID, _ := NewBinId("A-1-1")
	if binID.String() != "A-1-1" {
		t.Fatalf("expected A-1-1, got %s", binID.String())
	}
}
