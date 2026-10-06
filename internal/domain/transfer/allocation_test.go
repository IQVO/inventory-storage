package transfer

import (
	"testing"
	"time"

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

func mustSKU(t *testing.T) shared.SKU {
	t.Helper()
	sku, _ := shared.NewSKU("SKU-T")
	return sku
}

func mustQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("NewQuantity(%d): %v", v, err)
	}
	return q
}

var decidedAt = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func TestNewAllocated_RequiresReservation(t *testing.T) {
	if _, err := NewAllocated("tr-1", "tl-1", mustSite(t, "SITE-A"), mustSKU(t), mustQty(t, 5), "", decidedAt); err != ErrNoReservationOnAllocated {
		t.Fatalf("err = %v, want ErrNoReservationOnAllocated", err)
	}
}

func TestNewRejected_RequiresReason(t *testing.T) {
	if _, err := NewRejected("tr-1", "tl-1", mustSite(t, "SITE-A"), mustSKU(t), mustQty(t, 5), "", decidedAt); err != ErrMissingReasonOnRejected {
		t.Fatalf("err = %v, want ErrMissingReasonOnRejected", err)
	}
}

func TestAllocation_FieldsRoundTrip(t *testing.T) {
	a, err := NewAllocated("tr-1", "tl-1", mustSite(t, "SITE-A"), mustSKU(t), mustQty(t, 5), "res-1", decidedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.Outcome() != OutcomeAllocated || a.ReservationID() != "res-1" || a.RejectionReason() != "" {
		t.Fatalf("allocated allocation shape wrong: %+v", a)
	}

	r, err := NewRejected("tr-2", "tl-2", mustSite(t, "SITE-B"), mustSKU(t), mustQty(t, 3), ReasonInsufficientUsable, decidedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Outcome() != OutcomeRejected || r.ReservationID() != "" || r.RejectionReason() != ReasonInsufficientUsable {
		t.Fatalf("rejected allocation shape wrong: %+v", r)
	}
}

func TestAllocation_IsReplayOf(t *testing.T) {
	a, _ := NewAllocated("tr-1", "tl-1", mustSite(t, "SITE-A"), mustSKU(t), mustQty(t, 5), "res-1", decidedAt)

	if !a.IsReplayOf("tr-1", "tl-1", mustSite(t, "SITE-A"), mustSKU(t), mustQty(t, 5)) {
		t.Fatal("identical command must be recognized as the replay")
	}
	for name, mutate := range map[string]func() (string, string, shared.SiteID, shared.SKU, shared.Quantity){
		"different transfer": func() (string, string, shared.SiteID, shared.SKU, shared.Quantity) {
			return "tr-9", "tl-1", mustSite(t, "SITE-A"), mustSKU(t), mustQty(t, 5)
		},
		"different site": func() (string, string, shared.SiteID, shared.SKU, shared.Quantity) {
			return "tr-1", "tl-1", mustSite(t, "SITE-Z"), mustSKU(t), mustQty(t, 5)
		},
		"different quantity": func() (string, string, shared.SiteID, shared.SKU, shared.Quantity) {
			return "tr-1", "tl-1", mustSite(t, "SITE-A"), mustSKU(t), mustQty(t, 6)
		},
	} {
		tr, tl, site, sku, qty := mutate()
		if a.IsReplayOf(tr, tl, site, sku, qty) {
			t.Fatalf("%s: mutated command must NOT be a replay", name)
		}
	}
}
