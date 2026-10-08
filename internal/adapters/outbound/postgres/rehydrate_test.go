package postgres

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// A corrupt row must come back as a wrapped error naming the offending value,
// never as a zero-value VO inside an aggregate (audit 2026-10-05, F5).

// fakeRow satisfies rowScanner by assigning fixed column values, so
// scanStockUnit can be exercised without a database.
type fakeRow struct {
	id, sku, bin string
	quantity     int
	reserved     int
	state        string
	site         *string
	version      int
}

func (f fakeRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = f.id
	*(dest[1].(*string)) = f.sku
	*(dest[2].(*string)) = f.bin
	*(dest[3].(*int)) = f.quantity
	*(dest[4].(*int)) = f.reserved
	*(dest[5].(*string)) = f.state
	*(dest[6].(**string)) = f.site
	*(dest[7].(*int)) = f.version
	return nil
}

func TestScanStockUnit_ValidRowRehydrates(t *testing.T) {
	unit, err := scanStockUnit(fakeRow{id: "su-1", sku: "SKU-1", bin: "BIN-1", quantity: 5, reserved: 2, state: "RESERVED", version: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unit.SKU().String() != "SKU-1" || unit.BinID().String() != "BIN-1" || unit.Quantity().Int() != 5 || unit.Reserved().Int() != 2 || unit.Version() != 3 {
		t.Fatalf("rehydrated unit does not match the row: %+v", unit)
	}
	if unit.SiteID() != "" {
		t.Fatalf("row without a site must rehydrate site-less, got %q", unit.SiteID())
	}
}

func TestScanStockUnit_SiteRowRehydratesWithCustody(t *testing.T) {
	unit, err := scanStockUnit(fakeRow{id: "su-1", sku: "SKU-1", bin: "BIN-1", quantity: 5, reserved: 2, state: "RESERVED", site: &[]string{"SITE-A"}[0], version: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unit.SiteID() != shared.SiteID("SITE-A") {
		t.Fatalf("site = %q, want SITE-A", unit.SiteID())
	}
	if !unit.IsAtSite(shared.SiteID("SITE-A")) {
		t.Fatal("unit must report itself at SITE-A")
	}
}

func TestScanStockUnit_CorruptSiteReturnsError(t *testing.T) {
	empty := ""
	unit, err := scanStockUnit(fakeRow{id: "su-1", sku: "SKU-1", bin: "BIN-1", quantity: 5, state: "AVAILABLE", site: &empty, version: 1})
	if err == nil {
		t.Fatalf("expected an error for an empty site_id, got unit %+v", unit)
	}
	if !errors.Is(err, shared.ErrEmptySiteID) {
		t.Fatalf("error %q does not wrap ErrEmptySiteID", err)
	}
}

func TestScanStockUnit_CorruptRowReturnsError(t *testing.T) {
	good := fakeRow{id: "su-1", sku: "SKU-1", bin: "BIN-1", quantity: 5, reserved: 2, state: "AVAILABLE", version: 1}
	cases := []struct {
		name    string
		mutate  func(*fakeRow)
		wantErr error
		wantMsg string
	}{
		{"empty sku", func(r *fakeRow) { r.sku = "" }, shared.ErrEmptySKU, `rehydrate sku ""`},
		{"empty bin id", func(r *fakeRow) { r.bin = "" }, shared.ErrEmptyBinID, `rehydrate bin id ""`},
		{"negative quantity", func(r *fakeRow) { r.quantity = -1 }, shared.ErrNegativeQuantity, "rehydrate quantity -1"},
		{"negative reserved", func(r *fakeRow) { r.reserved = -4 }, shared.ErrNegativeQuantity, "rehydrate reserved -4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := good
			tc.mutate(&row)
			unit, err := scanStockUnit(row)
			if err == nil {
				t.Fatalf("expected an error for a row with %s, got unit %+v", tc.name, unit)
			}
			if unit != nil {
				t.Fatalf("expected no aggregate alongside the error, got %+v", unit)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error %q does not wrap %v", err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) || !strings.Contains(err.Error(), `stock unit "su-1"`) {
				t.Fatalf("error %q should mention %q and the stock unit id", err, tc.wantMsg)
			}
		})
	}
}

func TestRehydrateBin(t *testing.T) {
	bin, err := rehydrateBin("BIN-1", 10, 4, 2)
	if err != nil {
		t.Fatalf("unexpected error for a valid row: %v", err)
	}
	if bin.Capacity().Int() != 10 || bin.Occupied().Int() != 4 || bin.Version() != 2 {
		t.Fatalf("rehydrated bin does not match the row: %+v", bin)
	}

	for _, tc := range []struct {
		name               string
		capacity, occupied int
		wantMsg            string
	}{
		{"negative capacity", -1, 0, "rehydrate bin capacity -1"},
		{"negative occupied", 10, -3, "rehydrate bin occupied -3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin, err := rehydrateBin("BIN-1", tc.capacity, tc.occupied, 1)
			if err == nil || bin != nil {
				t.Fatalf("expected (nil, error), got (%+v, %v)", bin, err)
			}
			if !errors.Is(err, shared.ErrNegativeQuantity) {
				t.Fatalf("error %q does not wrap ErrNegativeQuantity", err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) || !strings.Contains(err.Error(), `bin "BIN-1"`) {
				t.Fatalf("error %q should mention %q and the bin id", err, tc.wantMsg)
			}
		})
	}
}

func TestRehydrateReservation(t *testing.T) {
	now := time.Now().UTC()
	three := 3
	res, err := rehydrateReservation("res-1", "SKU-1", 3, "order-1", &three, nil, "ACTIVE", now, now.Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("unexpected error for a valid row: %v", err)
	}
	if res.SKU().String() != "SKU-1" || res.Quantity().Int() != 3 {
		t.Fatalf("rehydrated reservation does not match the row: %+v", res)
	}
	if got := res.LineNo(); got == nil || *got != 3 {
		t.Fatalf("rehydrated LineNo = %v, want 3", got)
	}
	legacy, err := rehydrateReservation("res-2", "SKU-1", 3, "order-1", nil, nil, "ACTIVE", now, now.Add(time.Hour), 1)
	if err != nil || legacy.LineNo() != nil {
		t.Fatalf("a NULL line_no must rehydrate as unknown, got %v, %v", legacy.LineNo(), err)
	}

	for _, tc := range []struct {
		name     string
		sku      string
		quantity int
		wantErr  error
		wantMsg  string
	}{
		{"empty sku", "", 3, shared.ErrEmptySKU, `rehydrate sku ""`},
		{"negative quantity", "SKU-1", -2, shared.ErrNegativeQuantity, "rehydrate quantity -2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := rehydrateReservation("res-1", tc.sku, tc.quantity, "order-1", nil, nil, "ACTIVE", now, now, 1)
			if err == nil || res != nil {
				t.Fatalf("expected (nil, error), got (%+v, %v)", res, err)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error %q does not wrap %v", err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) || !strings.Contains(err.Error(), `reservation "res-1"`) {
				t.Fatalf("error %q should mention %q and the reservation id", err, tc.wantMsg)
			}
		})
	}
}

func TestRehydrateAllocation(t *testing.T) {
	alloc, err := rehydrateAllocation("res-1", "su-1", 2, "")
	if err != nil {
		t.Fatalf("a legacy allocation with no bin id must still hydrate: %v", err)
	}
	if alloc != (reservation.Allocation{StockUnitID: "su-1", BinID: "", Quantity: 2}) {
		t.Fatalf("unexpected allocation: %+v", alloc)
	}

	_, err = rehydrateAllocation("res-1", "su-1", -5, "BIN-1")
	if err == nil {
		t.Fatal("expected an error for a negative allocation quantity")
	}
	if !errors.Is(err, shared.ErrNegativeQuantity) {
		t.Fatalf("error %q does not wrap ErrNegativeQuantity", err)
	}
	if !strings.Contains(err.Error(), "rehydrate allocation quantity -5") || !strings.Contains(err.Error(), `stock unit "su-1"`) {
		t.Fatalf("error %q should name the quantity and the stock unit", err)
	}
}
