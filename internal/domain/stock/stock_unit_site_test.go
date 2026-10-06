package stock

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func mustSite(t *testing.T, raw string) shared.SiteID {
	t.Helper()
	site, err := shared.NewSiteID(raw)
	if err != nil {
		t.Fatalf("NewSiteID(%q): %v", raw, err)
	}
	return site
}

func TestNewStockUnitAtSite_RecordsCustody(t *testing.T) {
	site := mustSite(t, "SITE-A")
	u, err := NewStockUnitAtSite("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), site)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.SiteID() != site {
		t.Fatalf("site id = %q, want %q", u.SiteID(), site)
	}
}

func TestNewStockUnitAtSite_RequiresValidSite(t *testing.T) {
	if _, err := NewStockUnitAtSite("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), ""); err == nil {
		t.Fatal("empty site id must be rejected at construction")
	}
}

func TestStockUnit_IsAtSite(t *testing.T) {
	site := mustSite(t, "SITE-A")
	other := mustSite(t, "SITE-B")
	u, _ := NewStockUnitAtSite("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), site)

	if !u.IsAtSite(site) {
		t.Fatal("unit must report itself at its own site")
	}
	if u.IsAtSite(other) {
		t.Fatal("unit must not report itself at a different site")
	}
	if u.IsAtSite("") {
		t.Fatal("no unit is at the empty (unrecorded) site")
	}
}

func TestStockUnit_LegacyUnitHasNoSiteCustody(t *testing.T) {
	// A legacy site-less row (persisted before site custody existed)
	// rehydrates with an empty SiteID and is never transfer-allocatable.
	u := RehydrateStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), mustQty(t, 0), StateAvailable, 1)
	if u.SiteID() != "" {
		t.Fatalf("legacy unit site = %q, want empty", u.SiteID())
	}
	if u.IsAtSite(mustSite(t, "SITE-A")) {
		t.Fatal("legacy site-less unit must never match a transfer origin site")
	}
}

func TestRehydrateStockUnitAtSite(t *testing.T) {
	u := RehydrateStockUnitAtSite("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), mustQty(t, 2), StateReserved, mustSite(t, "SITE-A"), 4)
	if u.SiteID() != mustSite(t, "SITE-A") {
		t.Fatalf("site = %q, want SITE-A", u.SiteID())
	}
	if u.Version() != 4 {
		t.Fatalf("version = %d, want 4", u.Version())
	}
}
