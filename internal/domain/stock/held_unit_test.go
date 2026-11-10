package stock

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var heldAt = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)

func heldSource(t *testing.T, qty int) HeldSource {
	t.Helper()
	return HeldSource{
		ReceiptID: "rcpt-1", ASNNumber: "ASN-9", LineNo: 3, SKU: mustSKU(t), Quantity: mustQty(t, qty),
		OwnerRef: "seller-a", SSCC: "00012345678901234567",
	}
}

func newHeld(t *testing.T, qty int) *HeldUnit {
	t.Helper()
	h, err := NewHeldUnit("hu-1", heldSource(t, qty), ReasonDamaged, heldAt)
	if err != nil {
		t.Fatalf("NewHeldUnit: %v", err)
	}
	return h
}

func TestNewHeldUnit_IsHeldWithSourceAndReason(t *testing.T) {
	h := newHeld(t, 4)
	if h.State() != StateHeld || h.Reason() != ReasonDamaged || h.Quantity().Int() != 4 {
		t.Fatalf("state %s reason %s qty %d", h.State(), h.Reason(), h.Quantity().Int())
	}
	if h.ReceiptID() != "rcpt-1" || h.ASNNumber() != "ASN-9" || h.LineNo() != 3 || h.SKU() != mustSKU(t) {
		t.Fatalf("source lost: %s %s %d %s", h.ReceiptID(), h.ASNNumber(), h.LineNo(), h.SKU())
	}
	if h.OwnerRef() != "seller-a" || h.SSCC() != "00012345678901234567" || !h.HeldAt().Equal(heldAt) {
		t.Fatalf("provenance lost: %s %s %v", h.OwnerRef(), h.SSCC(), h.HeldAt())
	}
	if h.Version() != 1 || h.Disposition() != "" || h.ReworkCount() != 0 {
		t.Fatalf("fresh unit: version %d disposition %q rework %d", h.Version(), h.Disposition(), h.ReworkCount())
	}
}

func TestNewHeldUnit_RejectsInvalidInput(t *testing.T) {
	cases := map[string]func(s *HeldSource) HoldReason{
		"no sku":      func(s *HeldSource) HoldReason { s.SKU = ""; return ReasonDamaged },
		"zero qty":    func(s *HeldSource) HoldReason { s.Quantity = 0; return ReasonDamaged },
		"no receipt":  func(s *HeldSource) HoldReason { s.ReceiptID = ""; return ReasonDamaged },
		"line zero":   func(s *HeldSource) HoldReason { s.LineNo = 0; return ReasonDamaged },
		"bad reason":  func(s *HeldSource) HoldReason { return HoldReason("BROKEN") },
		"empty reson": func(s *HeldSource) HoldReason { return "" },
	}
	for name, mutate := range cases {
		src := heldSource(t, 2)
		reason := mutate(&src)
		if _, err := NewHeldUnit("hu-x", src, reason, heldAt); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, err := NewHeldUnit("", heldSource(t, 2), ReasonDamaged, heldAt); err == nil {
		t.Error("empty id must be rejected")
	}
}

func TestHeldUnit_IsNeverUsable(t *testing.T) {
	h := newHeld(t, 9)
	if h.Usable() != 0 {
		t.Fatalf("a held unit must have 0 usable, got %d", h.Usable())
	}
}

func TestHeldUnit_CannotBeReserved(t *testing.T) {
	h := newHeld(t, 9)
	if err := h.Reserve(mustQty(t, 1)); !errors.Is(err, ErrUnitHeld) {
		t.Fatalf("Reserve on a held unit = %v, want ErrUnitHeld", err)
	}
	if h.Usable() != 0 || h.State() != StateHeld {
		t.Fatalf("a rejected reserve changed the unit: usable %d state %s", h.Usable(), h.State())
	}
}

func TestHeldUnit_Decide_Release_ClosesAndKeepsQuantity(t *testing.T) {
	h := newHeld(t, 6)
	at := heldAt.Add(time.Hour)
	if err := h.Decide(DispositionRelease, "", at); err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if h.State() != StateRemoved || h.Disposition() != DispositionRelease || !h.DecidedAt().Equal(at) {
		t.Fatalf("state %s disposition %s decidedAt %v", h.State(), h.Disposition(), h.DecidedAt())
	}
	if h.Quantity().Int() != 6 || h.Usable() != 0 {
		t.Fatalf("quantity %d usable %d: a disposition conserves the quantity and never makes it usable", h.Quantity().Int(), h.Usable())
	}
}

func TestHeldUnit_Decide_RemovingDispositions(t *testing.T) {
	for _, d := range []Disposition{DispositionReturnToVendor, DispositionScrap} {
		h := newHeld(t, 5)
		if err := h.Decide(d, "", heldAt.Add(time.Minute)); err != nil {
			t.Fatalf("%s: %v", d, err)
		}
		if h.State() != StateRemoved || h.Disposition() != d || h.Quantity().Int() != 5 {
			t.Fatalf("%s: state %s disposition %s qty %d", d, h.State(), h.Disposition(), h.Quantity().Int())
		}
	}
}

func TestHeldUnit_Decide_Rework_StaysHeldWithReasonUpdated(t *testing.T) {
	h := newHeld(t, 5)
	if err := h.Decide(DispositionRework, ReasonSuspect, heldAt.Add(time.Minute)); err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if h.State() != StateHeld || h.Reason() != ReasonSuspect || h.ReworkCount() != 1 || h.Disposition() != DispositionRework {
		t.Fatalf("state %s reason %s rework %d disposition %s", h.State(), h.Reason(), h.ReworkCount(), h.Disposition())
	}
	if h.Usable() != 0 || h.Quantity().Int() != 5 {
		t.Fatalf("a reworked unit is still held: usable %d qty %d", h.Usable(), h.Quantity().Int())
	}
}

func TestHeldUnit_Decide_Rework_DefaultsReasonToOther(t *testing.T) {
	h := newHeld(t, 5)
	if err := h.Decide(DispositionRework, "", heldAt); err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if h.Reason() != ReasonOther {
		t.Fatalf("reason = %s, want OTHER", h.Reason())
	}
}

func TestHeldUnit_Decide_ReworkThenRelease_Allowed(t *testing.T) {
	h := newHeld(t, 5)
	if err := h.Decide(DispositionRework, ReasonOther, heldAt); err != nil {
		t.Fatalf("rework: %v", err)
	}
	if err := h.Decide(DispositionRelease, "", heldAt.Add(time.Hour)); err != nil {
		t.Fatalf("release after rework: %v", err)
	}
	if h.State() != StateRemoved || h.ReworkCount() != 1 {
		t.Fatalf("state %s rework %d", h.State(), h.ReworkCount())
	}
}

func TestHeldUnit_Decide_OnClosedUnit_Forbidden(t *testing.T) {
	for _, first := range []Disposition{DispositionRelease, DispositionReturnToVendor, DispositionScrap} {
		for _, second := range []Disposition{DispositionRelease, DispositionReturnToVendor, DispositionScrap, DispositionRework} {
			h := newHeld(t, 5)
			if err := h.Decide(first, "", heldAt); err != nil {
				t.Fatalf("%s: %v", first, err)
			}
			if err := h.Decide(second, "", heldAt.Add(time.Hour)); !errors.Is(err, ErrHeldUnitClosed) {
				t.Fatalf("%s after %s = %v, want ErrHeldUnitClosed", second, first, err)
			}
			if h.Disposition() != first {
				t.Fatalf("a refused decision must not overwrite the audit: %s", h.Disposition())
			}
		}
	}
}

func TestHeldUnit_Decide_InvalidInput(t *testing.T) {
	h := newHeld(t, 5)
	if err := h.Decide(Disposition("DONATE"), "", heldAt); !errors.Is(err, ErrInvalidDisposition) {
		t.Fatalf("unknown disposition = %v", err)
	}
	if err := h.Decide(DispositionRework, HoldReason("NOPE"), heldAt); !errors.Is(err, ErrInvalidHoldReason) {
		t.Fatalf("unknown rework reason = %v", err)
	}
	if h.State() != StateHeld || h.Reason() != ReasonDamaged || h.ReworkCount() != 0 {
		t.Fatalf("a refused decision changed the unit: %s %s %d", h.State(), h.Reason(), h.ReworkCount())
	}
}

func TestParseHoldReason_ClosedSet(t *testing.T) {
	for _, v := range []string{"DAMAGED", "QC_PENDING", "SUSPECT", "OTHER"} {
		r, err := ParseHoldReason(v)
		if err != nil || string(r) != v {
			t.Errorf("%s: %v %v", v, r, err)
		}
	}
	for _, v := range []string{"", "damaged", "BROKEN"} {
		if _, err := ParseHoldReason(v); !errors.Is(err, ErrInvalidHoldReason) {
			t.Errorf("%q: %v", v, err)
		}
	}
}

func TestParseDisposition_ClosedSet(t *testing.T) {
	for _, v := range []string{"RELEASE", "RETURN_TO_VENDOR", "SCRAP", "REWORK"} {
		d, err := ParseDisposition(v)
		if err != nil || string(d) != v {
			t.Errorf("%s: %v %v", v, d, err)
		}
	}
	for _, v := range []string{"", "release", "RESTOCK"} {
		if _, err := ParseDisposition(v); !errors.Is(err, ErrInvalidDisposition) {
			t.Errorf("%q: %v", v, err)
		}
	}
}

func TestRehydrateHeldUnit_RoundTrips(t *testing.T) {
	decided := heldAt.Add(time.Hour)
	h := RehydrateHeldUnit(HeldRecord{
		ID: "hu-7", Source: heldSource(t, 8), Reason: ReasonOther, State: StateRemoved, Disposition: DispositionScrap,
		DecidedAt: decided, HeldAt: heldAt, ReworkCount: 2, Version: 5, LPNs: []string{"LPN-1", "LPN-2"},
	})
	if h.ID() != "hu-7" || h.State() != StateRemoved || h.Disposition() != DispositionScrap || h.ReworkCount() != 2 || h.Version() != 5 {
		t.Fatalf("rehydrate lost fields: %+v", h)
	}
	if len(h.LPNs()) != 2 || h.LPNs()[0] != "LPN-1" {
		t.Fatalf("lpns = %v", h.LPNs())
	}
}

func TestStockUnit_Held_IsNeverUsableOrReservable(t *testing.T) {
	u := RehydrateStockUnit("su-1", mustSKU(t), mustBin(t), mustQty(t, 5), 0, StateHeld, 1)
	if u.Usable() != 0 {
		t.Fatalf("a HELD stock unit has usable %d, want 0", u.Usable())
	}
	if err := u.Reserve(mustQty(t, 1)); !errors.Is(err, ErrUnitHeld) {
		t.Fatalf("Reserve = %v, want ErrUnitHeld", err)
	}
	if err := u.AddQuantity(mustQty(t, 1)); !errors.Is(err, ErrUnitHeld) {
		t.Fatalf("AddQuantity = %v, want ErrUnitHeld", err)
	}
	if err := u.RelocateOut(mustQty(t, 1)); !errors.Is(err, ErrUnitHeld) {
		t.Fatalf("RelocateOut = %v, want ErrUnitHeld", err)
	}
	if err := u.MoveTo(mustBin(t), shared.SiteID("S1")); !errors.Is(err, ErrUnitHeld) {
		t.Fatalf("MoveTo = %v, want ErrUnitHeld", err)
	}
	if u.CanReserveWhole("") {
		t.Fatal("a held unit can never be reserved whole")
	}
	if u.Reserved() != 0 || u.State() != StateHeld {
		t.Fatalf("rejected operations changed the unit: reserved %d state %s", u.Reserved(), u.State())
	}
}
