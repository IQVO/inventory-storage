package reservation

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func lineAllocs(t *testing.T) []Allocation {
	t.Helper()
	return []Allocation{{StockUnitID: "su-1", Quantity: mustQty(t, 5)}}
}

func intPtr(v int) *int { return &v }

func TestNewForLine_StoresTheLineNo(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	r, err := NewForLine("r-1", sku, mustQty(t, 5), "order-1", intPtr(3), lineAllocs(t), time.Unix(0, 0), time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.LineNo() == nil || *r.LineNo() != 3 {
		t.Fatalf("LineNo = %v, want 3", r.LineNo())
	}
}

// New (the pre-decision-18 constructor) leaves the line unknown.
func TestNew_LeavesTheLineNoUnknown(t *testing.T) {
	if got := newActive(t).LineNo(); got != nil {
		t.Fatalf("LineNo = %v, want nil", *got)
	}
}

func TestNewForLine_NilLineNoIsUnknown(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	r, err := NewForLine("r-1", sku, mustQty(t, 5), "order-1", nil, lineAllocs(t), time.Unix(0, 0), time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.LineNo() != nil {
		t.Fatalf("LineNo = %v, want nil", *r.LineNo())
	}
}

func TestValidLineNo(t *testing.T) {
	cases := map[int]bool{0: false, -1: false, 1: true, 2: true, MaxLineNo: true, MaxLineNo + 1: false}
	for n, want := range cases {
		if got := ValidLineNo(n); got != want {
			t.Errorf("ValidLineNo(%d) = %v, want %v", n, got, want)
		}
	}
}

// The upper bound is accepted: it is the largest int32, what line_no INTEGER stores.
func TestNewForLine_AcceptsMaxLineNo(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	r, err := NewForLine("r-1", sku, mustQty(t, 5), "order-1", intPtr(MaxLineNo), lineAllocs(t), time.Unix(0, 0), time.Hour)
	if err != nil || r.LineNo() == nil || *r.LineNo() != MaxLineNo {
		t.Fatalf("NewForLine(MaxLineNo) = %v, %v", r, err)
	}
}

func TestNewForLine_RejectsANonPositiveLineNo(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	for _, n := range []int{0, -1, -42, MaxLineNo + 1} {
		if _, err := NewForLine("r-1", sku, mustQty(t, 5), "order-1", intPtr(n), lineAllocs(t), time.Unix(0, 0), time.Hour); !errors.Is(err, ErrInvalidLineNo) {
			t.Errorf("lineNo %d: err = %v, want ErrInvalidLineNo", n, err)
		}
	}
}

// A line number is valid iff 1 <= n <= 2147483647 (the 32-bit line_no column).
func TestNewForLine_LineNoBoundaryTable(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	cases := []struct {
		n    int
		want bool
	}{
		{0, false},
		{1, true},
		{2147483647, true},
		{2147483648, false},
		{9223372036854775807, false},
	}
	for _, c := range cases {
		r, err := NewForLine("r-1", sku, mustQty(t, 5), "order-1", intPtr(c.n), lineAllocs(t), time.Unix(0, 0), time.Hour)
		if c.want {
			if err != nil || r == nil || r.LineNo() == nil || *r.LineNo() != c.n {
				t.Errorf("lineNo %d: got (%v, %v), want accepted", c.n, r, err)
			}
			continue
		}
		if !errors.Is(err, ErrInvalidLineNo) {
			t.Errorf("lineNo %d: err = %v, want ErrInvalidLineNo", c.n, err)
		}
	}
}

// The aggregate must not alias the caller's pointer: mutating the argument
// afterwards cannot change a stored reservation.
func TestNewForLine_CopiesTheLineNo(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	n := 2
	r, err := NewForLine("r-1", sku, mustQty(t, 5), "order-1", &n, lineAllocs(t), time.Unix(0, 0), time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	n = 99
	if *r.LineNo() != 2 {
		t.Fatalf("LineNo = %d after the caller changed its variable, want 2", *r.LineNo())
	}
}

func TestRehydrateForLine_RoundTripsTheLineNo(t *testing.T) {
	sku, _ := shared.NewSKU("SKU-1")
	created := time.Unix(0, 0)
	r := RehydrateForLine("r-1", sku, mustQty(t, 5), "order-1", intPtr(4), lineAllocs(t), StatusActive, created, created.Add(time.Hour), 1)
	if r.LineNo() == nil || *r.LineNo() != 4 {
		t.Fatalf("LineNo = %v, want 4", r.LineNo())
	}
	legacy := Rehydrate("r-2", sku, mustQty(t, 5), "order-1", lineAllocs(t), StatusActive, created, created.Add(time.Hour), 1)
	if legacy.LineNo() != nil {
		t.Fatalf("a legacy reservation has LineNo %v, want nil", *legacy.LineNo())
	}
}
