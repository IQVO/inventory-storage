package packaging_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/packaging"
)

var from = time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC)

func TestNewPackLevelNormalisesGTINs(t *testing.T) {
	p, err := packaging.NewPackLevel("SKU-1", "CASE", "24006381333935", "4006381333931", 12, from, 3)
	if err != nil {
		t.Fatal(err)
	}
	if p.GTIN.String() != "24006381333935" || p.ChildGTIN != "04006381333931" || p.Level != packaging.LevelCase || p.UnitsPerPack != 12 || p.Version != 3 {
		t.Fatalf("unexpected level %+v", p)
	}
}

func TestNewPackLevelRejectsBadInput(t *testing.T) {
	bad := []struct {
		name                    string
		sku, level, gtin, child string
		units                   int
		version                 int64
	}{
		{"empty sku", "", "CASE", "24006381333935", "", 12, 1},
		{"unknown level", "SKU-1", "BOX", "24006381333935", "", 12, 1},
		{"bad gtin", "SKU-1", "CASE", "24006381333936", "", 12, 1},
		{"bad child gtin", "SKU-1", "CASE", "24006381333935", "12", 12, 1},
		{"zero units", "SKU-1", "CASE", "24006381333935", "", 0, 1},
		{"zero version", "SKU-1", "CASE", "24006381333935", "", 12, 0},
	}
	for _, tc := range bad {
		if _, err := packaging.NewPackLevel(tc.sku, tc.level, tc.gtin, tc.child, tc.units, from, tc.version); !errors.Is(err, packaging.ErrInvalidPackLevel) {
			t.Errorf("%s: err = %v, want ErrInvalidPackLevel", tc.name, err)
		}
	}
}

func TestActiveWindow(t *testing.T) {
	p, _ := packaging.NewPackLevel("SKU-1", "CASE", "24006381333935", "", 12, from, 1)
	if p.Active(from.Add(-time.Second)) {
		t.Error("not active before effective_from")
	}
	if !p.Active(from) || !p.Active(from.Add(time.Hour)) {
		t.Error("active from effective_from while open ended")
	}
	end := from.Add(time.Hour)
	p.EffectiveTo = &end
	if !p.Active(end.Add(-time.Second)) || p.Active(end) || p.Active(end.Add(time.Hour)) {
		t.Error("active until effective_to, exclusive")
	}
}
