package transfer_test

import (
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

var receiptAt = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func rSite(t *testing.T, raw string) shared.SiteID {
	t.Helper()
	site, err := shared.NewSiteID(raw)
	if err != nil {
		t.Fatalf("NewSiteID(%q): %v", raw, err)
	}
	return site
}

func rSKU(t *testing.T, raw string) shared.SKU {
	t.Helper()
	sku, err := shared.NewSKU(raw)
	if err != nil {
		t.Fatalf("NewSKU(%q): %v", raw, err)
	}
	return sku
}

func rQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	qty, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("NewQuantity(%d): %v", v, err)
	}
	return qty
}

func TestNewStagedReceipt_ComputesSignedVariance(t *testing.T) {
	cases := []struct {
		name                string
		expected, recvd     int
		wantVariance        int
		wantOver, wantShort bool
	}{
		{"exact", 6, 6, 0, false, false},
		{"over", 6, 8, 2, true, false},
		{"short", 6, 5, -1, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := transfer.NewStagedReceipt("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), "res-1", rQty(t, tc.expected), rQty(t, tc.recvd), receiptAt)
			if err != nil {
				t.Fatalf("NewStagedReceipt: %v", err)
			}
			if r.Variance() != tc.wantVariance {
				t.Fatalf("variance = %d, want %d", r.Variance(), tc.wantVariance)
			}
			if r.IsOver() != tc.wantOver || r.IsShort() != tc.wantShort {
				t.Fatalf("over/short = %v/%v, want %v/%v", r.IsOver(), r.IsShort(), tc.wantOver, tc.wantShort)
			}
			if r.State() != transfer.ReceiptStaged {
				t.Fatalf("state = %s, want STAGED", r.State())
			}
		})
	}
}

func TestNewStagedReceipt_RejectsInvalidInput(t *testing.T) {
	site := rSite(t, "SITE-D")
	sku := rSKU(t, "SKU-1")
	cases := []struct {
		name               string
		transferID, lineID string
		reservation        string
		expected, received int
	}{
		{"no transfer id", "", "tl", "res", 5, 5},
		{"no line id", "tr", "", "res", 5, 5},
		{"no reservation", "tr", "tl", "", 5, 5},
		{"zero expected", "tr", "tl", "res", 0, 5},
		{"zero received", "tr", "tl", "res", 5, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := transfer.NewStagedReceipt(tc.transferID, tc.lineID, site, sku, tc.reservation, rQty(t, tc.expected), rQty(t, tc.received), receiptAt); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	// An invalid site and an invalid SKU are rejected by their value
	// objects, surfaced unchanged.
	if _, err := transfer.NewStagedReceipt("tr", "tl", "", sku, "res", rQty(t, 5), rQty(t, 5), receiptAt); err == nil {
		t.Fatal("empty site must be rejected")
	}
	if _, err := transfer.NewStagedReceipt("tr", "tl", site, "", "res", rQty(t, 5), rQty(t, 5), receiptAt); err == nil {
		t.Fatal("empty sku must be rejected")
	}
}

func TestReceipt_MarkStowed_RecordsLegsAndGuardsReplay(t *testing.T) {
	r, err := transfer.NewStagedReceipt("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), "res-1", rQty(t, 5), rQty(t, 5), receiptAt)
	if err != nil {
		t.Fatalf("NewStagedReceipt: %v", err)
	}

	stowedAt := receiptAt.Add(5 * time.Minute)
	binA, _ := shared.NewBinId("BIN-A")
	binB, _ := shared.NewBinId("BIN-B")
	legs := []transfer.StowLeg{
		{StockUnitID: "su-1", BinID: binA, Quantity: rQty(t, 3)},
		{StockUnitID: "su-2", BinID: binB, Quantity: rQty(t, 2)},
	}
	if err := r.MarkStowed(legs, stowedAt); err != nil {
		t.Fatalf("MarkStowed: %v", err)
	}
	if r.State() != transfer.ReceiptStowed || !r.StowedAt().Equal(stowedAt) {
		t.Fatalf("state/stowedAt = %s/%v", r.State(), r.StowedAt())
	}
	if len(r.StowLegs()) != 2 || r.StowLegs()[0].StockUnitID != "su-1" {
		t.Fatalf("stow legs = %+v", r.StowLegs())
	}

	// A second MarkStowed is the replay guard.
	if err := r.MarkStowed(legs, stowedAt); err != transfer.ErrReceiptAlreadyStowed {
		t.Fatalf("second MarkStowed err = %v, want ErrReceiptAlreadyStowed", err)
	}
	// Empty legs are refused.
	fresh, _ := transfer.NewStagedReceipt("tr", "tl2", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), "res-1", rQty(t, 5), rQty(t, 5), receiptAt)
	if err := fresh.MarkStowed(nil, stowedAt); err != transfer.ErrStowLegsRequired {
		t.Fatalf("empty-legs err = %v, want ErrStowLegsRequired", err)
	}
}

func TestReceipt_IsReplayOf_MatchesDefiningFields(t *testing.T) {
	r, err := transfer.NewStagedReceipt("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), "res-1", rQty(t, 6), rQty(t, 5), receiptAt)
	if err != nil {
		t.Fatalf("NewStagedReceipt: %v", err)
	}
	if !r.IsReplayOf("tr", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 5)) {
		t.Fatal("an identical scan must be a replay")
	}
	// The EXPECTED quantity is not part of the scan: only the fields the
	// caller supplied define the replay check.
	for _, mut := range []struct {
		name string
		fn   func() bool
	}{
		{"different transfer", func() bool { return r.IsReplayOf("tr-2", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 5)) }},
		{"different site", func() bool { return r.IsReplayOf("tr", rSite(t, "SITE-X"), rSKU(t, "SKU-1"), rQty(t, 5)) }},
		{"different sku", func() bool { return r.IsReplayOf("tr", rSite(t, "SITE-D"), rSKU(t, "SKU-2"), rQty(t, 5)) }},
		{"different received", func() bool { return r.IsReplayOf("tr", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 4)) }},
	} {
		if mut.fn() {
			t.Fatalf("%s must NOT be a replay", mut.name)
		}
	}
}

func TestReceipt_Accessors(t *testing.T) {
	r, err := transfer.NewStagedReceipt("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), "res-9", rQty(t, 6), rQty(t, 5), receiptAt)
	if err != nil {
		t.Fatalf("NewStagedReceipt: %v", err)
	}
	if r.TransferID() != "tr" || r.TransferLineID() != "tl" || r.ReservationID() != "res-9" {
		t.Fatalf("ids = %s/%s/%s", r.TransferID(), r.TransferLineID(), r.ReservationID())
	}
	if r.DestinationSiteID() != rSite(t, "SITE-D") || r.SKU() != rSKU(t, "SKU-1") {
		t.Fatal("site/sku accessors")
	}
	if r.ExpectedQuantity().Int() != 6 || r.ReceivedQuantity().Int() != 5 {
		t.Fatal("quantity accessors")
	}
	if !r.StagedAt().Equal(receiptAt) || r.StowLegsRaw() != nil {
		t.Fatal("stagedAt/stowLegs accessors")
	}
}

func TestReceipt_Rehydrate(t *testing.T) {
	bin, _ := shared.NewBinId("BIN-A")
	stowedAt := receiptAt.Add(time.Minute)
	rehydrated, err := transfer.RehydrateReceipt("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), "res-9", rQty(t, 6), rQty(t, 5), -1, transfer.ReceiptStowed,
		[]transfer.StowLeg{{StockUnitID: "su-1", BinID: bin, Quantity: rQty(t, 5)}}, receiptAt, stowedAt)
	if err != nil {
		t.Fatalf("RehydrateReceipt: %v", err)
	}
	if rehydrated.State() != transfer.ReceiptStowed || rehydrated.Variance() != -1 || len(rehydrated.StowLegs()) != 1 {
		t.Fatalf("rehydrated = %+v", rehydrated)
	}
	if !rehydrated.StowedAt().Equal(stowedAt) {
		t.Fatal("rehydrated stowedAt")
	}
}

func TestNewException_MatchesIdenticalScan(t *testing.T) {
	exc, err := transfer.NewException("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 5), transfer.ExceptionUnknownTransfer, "detail", receiptAt)
	if err != nil {
		t.Fatalf("NewException: %v", err)
	}
	if exc.Kind() != transfer.ExceptionUnknownTransfer || exc.Detail() != "detail" || exc.TransferID() != "tr" {
		t.Fatalf("exception = %+v", exc)
	}
	if !exc.Matches("tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 5)) {
		t.Fatal("Matches must accept the identical scan")
	}
	if exc.Matches("tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 4)) {
		t.Fatal("Matches must reject a different quantity")
	}
}

func TestNewException_Validation(t *testing.T) {
	// Validation: empty kind, empty line, negative quantity.
	if _, err := transfer.NewException("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 5), "", "d", receiptAt); err != transfer.ErrExceptionKindRequired {
		t.Fatalf("empty kind err = %v", err)
	}
	if _, err := transfer.NewException("tr", "", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 5), transfer.ExceptionUnknownTransfer, "d", receiptAt); err == nil {
		t.Fatal("empty line id must be rejected")
	}
	if _, err := transfer.NewException("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), shared.Quantity(-1), transfer.ExceptionUnknownTransfer, "d", receiptAt); err != shared.ErrNegativeQuantity {
		t.Fatalf("negative quantity err = %v", err)
	}
	if _, err := transfer.NewException("tr", "tl", "", rSKU(t, "SKU-1"), rQty(t, 5), transfer.ExceptionUnknownTransfer, "d", receiptAt); err == nil {
		t.Fatal("empty site must be rejected")
	}

}

func TestRehydrateException_RoundTrips(t *testing.T) {
	rh := transfer.RehydrateException("tr", "tl", rSite(t, "SITE-D"), rSKU(t, "SKU-1"), rQty(t, 5), transfer.ExceptionUnrecognizedTransfer, "d2", receiptAt)
	if rh.Kind() != transfer.ExceptionUnrecognizedTransfer || rh.Detail() != "d2" || !rh.CreatedAt().Equal(receiptAt) {
		t.Fatalf("rehydrated = %+v", rh)
	}
	if rh.DestinationSiteID() != rSite(t, "SITE-D") || rh.SKU() != rSKU(t, "SKU-1") || rh.ReceivedQuantity().Int() != 5 || rh.TransferLineID() != "tl" {
		t.Fatal("rehydrated accessors")
	}
}
