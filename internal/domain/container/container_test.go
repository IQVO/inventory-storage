package container_test

import (
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func spec() container.RegisterSpec {
	return container.RegisterSpec{
		LPN: "003400000000000017", SKU: "SKU-1", GTIN: "24006381333935", UnitsPerPack: 12,
		Quantity: 12, OwnerRef: "seller-a", Lot: "L1", Expiry: "2027-03-01",
		ReceiptID: "R-1", LineNo: 1,
	}
}

func registered(t *testing.T) *container.Container {
	t.Helper()
	c, err := container.Register(spec())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return c
}

func stowed(t *testing.T) *container.Container {
	t.Helper()
	c := registered(t)
	if err := c.Stow("BIN-C", "S1", "su-1"); err != nil {
		t.Fatalf("Stow: %v", err)
	}
	return c
}

func TestRegisterBuildsASealedContainerWithoutALocation(t *testing.T) {
	c := registered(t)
	if c.State() != container.StateSealed || c.BinID() != "" || c.SiteID() != "" || c.StockUnitID() != "" {
		t.Fatalf("new container must be SEALED and unlocated: %+v", c)
	}
	got := map[string]any{
		"lpn": c.LPN(), "sku": c.SKU(), "gtin": c.GTIN(), "units": c.UnitsPerPack(), "qty": c.Quantity(),
		"owner": c.OwnerRef(), "lot": c.Lot(), "expiry": c.Expiry(), "receipt": c.ReceiptID(), "line": c.LineNo(), "version": c.Version(),
	}
	want := map[string]any{
		"lpn": "003400000000000017", "sku": shared.SKU("SKU-1"), "gtin": "24006381333935", "units": 12, "qty": shared.Quantity(12),
		"owner": shared.OwnerRef("seller-a"), "lot": "L1", "expiry": "2027-03-01", "receipt": "R-1", "line": 1, "version": 1,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
}

func TestRegisterEnforcesQuantityEqualsUnitsPerPackTimesOneCase(t *testing.T) {
	s := spec()
	s.Quantity = 11
	if _, err := container.Register(s); !errors.Is(err, container.ErrContainerQuantityMismatch) {
		t.Fatalf("11 eaches in a case of 12 = %v, want ErrContainerQuantityMismatch", err)
	}
	s.Quantity = 24
	if _, err := container.Register(s); !errors.Is(err, container.ErrContainerQuantityMismatch) {
		t.Fatalf("24 eaches in a case of 12 = %v, want ErrContainerQuantityMismatch", err)
	}
}

func TestRegisterRejectsInvalidIdentity(t *testing.T) {
	bad := map[string]func(*container.RegisterSpec){
		"empty lpn":       func(s *container.RegisterSpec) { s.LPN = "" },
		"lpn with space":  func(s *container.RegisterSpec) { s.LPN = "A B" },
		"lpn too long":    func(s *container.RegisterSpec) { s.LPN = string(make([]byte, 65)) },
		"empty sku":       func(s *container.RegisterSpec) { s.SKU = "" },
		"bad gtin":        func(s *container.RegisterSpec) { s.GTIN = "123" },
		"zero units":      func(s *container.RegisterSpec) { s.UnitsPerPack = 0 },
		"bad owner":       func(s *container.RegisterSpec) { s.OwnerRef = "no good" },
		"empty receipt":   func(s *container.RegisterSpec) { s.ReceiptID = "" },
		"line below one":  func(s *container.RegisterSpec) { s.LineNo = 0 },
		"lot too long":    func(s *container.RegisterSpec) { s.Lot = "123456789012345678901" },
		"expiry too long": func(s *container.RegisterSpec) { s.Expiry = string(make([]byte, 33)) },
	}
	for name, mutate := range bad {
		s := spec()
		mutate(&s)
		if _, err := container.Register(s); err == nil {
			t.Errorf("%s: Register accepted an invalid spec", name)
		}
	}
}

func TestStowRecordsBinSiteAndUnitAndStaysSealed(t *testing.T) {
	c := stowed(t)
	if c.State() != container.StateSealed || c.BinID() != "BIN-C" || c.SiteID() != "S1" || c.StockUnitID() != "su-1" {
		t.Fatalf("after stow: %+v", c)
	}
}

func TestRestowToAnotherSiteIsRejected(t *testing.T) {
	c := stowed(t)
	if err := c.Stow("BIN-Z", "S2", "su-1"); !errors.Is(err, container.ErrContainerNotAtSite) {
		t.Fatalf("restow at another site = %v, want ErrContainerNotAtSite", err)
	}
	if c.BinID() != "BIN-C" {
		t.Fatalf("a rejected stow must not move the container, bin = %s", c.BinID())
	}
	if err := c.Stow("BIN-D", "S1", "su-1"); err != nil || c.BinID() != "BIN-D" {
		t.Fatalf("restow within the site = %v bin %s", err, c.BinID())
	}
}

func TestStowRequiresASealedContainer(t *testing.T) {
	c := stowed(t)
	if err := c.Break(12); err != nil {
		t.Fatal(err)
	}
	if err := c.Stow("BIN-D", "S1", "su-2"); !errors.Is(err, container.ErrContainerNotSealed) {
		t.Fatalf("stow of an OPEN container = %v, want ErrContainerNotSealed", err)
	}
}

func TestBreakConservesQuantity(t *testing.T) {
	c := stowed(t)
	if err := c.Break(11); !errors.Is(err, container.ErrBreakNotConserving) {
		t.Fatalf("Break(11) = %v, want ErrBreakNotConserving", err)
	}
	if err := c.Break(13); !errors.Is(err, container.ErrBreakNotConserving) {
		t.Fatalf("Break(13) = %v, want ErrBreakNotConserving", err)
	}
	if c.State() != container.StateSealed {
		t.Fatalf("a rejected break must leave the container SEALED, got %s", c.State())
	}
	if err := c.Break(12); err != nil || c.State() != container.StateOpen {
		t.Fatalf("Break(12) = %v state %s, want OPEN", err, c.State())
	}
}

func TestBreakNeedsSealedAndStowed(t *testing.T) {
	if err := registered(t).Break(12); !errors.Is(err, container.ErrContainerNotStowed) {
		t.Fatalf("break of an unstowed container = %v, want ErrContainerNotStowed", err)
	}
	c := stowed(t)
	if err := c.Break(12); err != nil {
		t.Fatal(err)
	}
	if err := c.Break(12); !errors.Is(err, container.ErrContainerNotSealed) {
		t.Fatalf("second break (OPEN) = %v, want ErrContainerNotSealed", err)
	}
	if err := c.Decant(); err != nil || c.State() != container.StateEmpty || c.BinID() != "" {
		t.Fatalf("Decant = %v state %s bin %q", err, c.State(), c.BinID())
	}
	if err := c.Break(12); !errors.Is(err, container.ErrContainerNotSealed) {
		t.Fatalf("break of an EMPTY container = %v, want ErrContainerNotSealed", err)
	}
}

func TestDecantNeedsAnOpenContainer(t *testing.T) {
	if err := stowed(t).Decant(); !errors.Is(err, container.ErrContainerNotOpen) {
		t.Fatalf("Decant of SEALED = %v, want ErrContainerNotOpen", err)
	}
}

func TestEmptyWhenTheWholeCaseLeaves(t *testing.T) {
	c := stowed(t)
	if err := c.EmptyWhole(); err != nil || c.State() != container.StateEmpty || c.BinID() != "" {
		t.Fatalf("EmptyWhole = %v state %s bin %q", err, c.State(), c.BinID())
	}
	if err := c.EmptyWhole(); !errors.Is(err, container.ErrContainerNotSealed) {
		t.Fatalf("EmptyWhole twice = %v, want ErrContainerNotSealed", err)
	}
}

func TestRehydrateRestoresEveryField(t *testing.T) {
	c := container.Rehydrate(container.Snapshot{
		LPN: "L1", SKU: "SKU-1", GTIN: "24006381333935", UnitsPerPack: 12, Quantity: 12, OwnerRef: "seller-a",
		SiteID: "S1", BinID: "BIN-C", State: container.StateOpen, Lot: "L", Expiry: "2027-03-01",
		StockUnitID: "su-1", ReceiptID: "R-1", LineNo: 2, Version: 7,
	})
	if c.State() != container.StateOpen || c.Version() != 7 || c.LineNo() != 2 || c.SiteID() != shared.SiteID("S1") {
		t.Fatalf("rehydrated: %+v", c)
	}
}

func TestHandlingUnitAcceptance(t *testing.T) {
	cases := []struct {
		hu            container.HandlingUnit
		sealed, loose bool
	}{
		{container.HandlingEach, false, true},
		{container.HandlingCase, true, false},
		{container.HandlingPallet, true, false},
		{container.HandlingUnknown, false, true},
	}
	for _, tc := range cases {
		if got := tc.hu.AcceptsSealedCase(); got != tc.sealed {
			t.Errorf("%q AcceptsSealedCase = %v, want %v", tc.hu, got, tc.sealed)
		}
		if got := tc.hu.AcceptsLoose(); got != tc.loose {
			t.Errorf("%q AcceptsLoose = %v, want %v", tc.hu, got, tc.loose)
		}
	}
	if container.ParseHandlingUnit("case") != container.HandlingCase || container.ParseHandlingUnit("CASE") != container.HandlingCase ||
		container.ParseHandlingUnit("PALLET") != container.HandlingPallet || container.ParseHandlingUnit("each") != container.HandlingEach ||
		container.ParseHandlingUnit("junk") != container.HandlingUnknown || container.ParseHandlingUnit("") != container.HandlingUnknown {
		t.Error("ParseHandlingUnit mapping is wrong")
	}
}
