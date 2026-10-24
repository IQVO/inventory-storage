package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

var ctx = context.Background()

// stowed registers LPN-1 and stows it into CASE-1 at site S1.
func stowed(t *testing.T) *cw {
	t.Helper()
	w := newCW()
	w.bin(t, "S1-A-Z-1-1-1-1", "S1", 100, container.HandlingCase)
	w.bin(t, "S1-A-Z-1-1-1-2", "S1", 100, container.HandlingEach)
	w.registered(t, "LPN-1")
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-1"); err != nil {
		t.Fatalf("stow: %v", err)
	}
	return w
}

func TestStowContainerCreatesTheSealedCaseUnitAndSetsTheSiteFromTheBin(t *testing.T) {
	w := stowed(t)
	c := w.containerOf(t, "LPN-1")
	unit, _ := w.Stock.FindByID(ctx, c.StockUnitID())
	bin, _ := w.Locations.FindByID(ctx, "S1-A-Z-1-1-1-1")
	owned, _ := w.Usable.ExecuteFor(ctx, "SKU-1", "seller-a")
	fleet, _ := w.Usable.Execute(ctx, "SKU-1")
	assertFacts(t, map[string]any{
		"bin": c.BinID().String(), "site": c.SiteID().String(), "state": string(c.State()), "hasUnit": c.StockUnitID() != "",
		"unitPack": string(unit.PackState()), "unitQty": unit.Quantity().Int(), "unitOwner": unit.OwnerRef().String(), "unitSite": unit.SiteID().String(),
		"occupied": bin.Occupied().Int(), "containerStowed": countEvents(w.env, "ContainerStowed"), "itemStowed": countEvents(w.env, "ItemStowed"),
		"usable": owned.Usable.Int(), "looseUsable": owned.LooseUsable.Int(), "cases": owned.ReservableCases, "fleetUsable": fleet.Usable.Int(),
	}, map[string]any{
		"bin": "S1-A-Z-1-1-1-1", "site": "S1", "state": string(container.StateSealed), "hasUnit": true,
		"unitPack": string(stock.PackSealedCase), "unitQty": 12, "unitOwner": "seller-a", "unitSite": "S1",
		"occupied": 12, "containerStowed": 1, "itemStowed": 1,
		"usable": 12, "looseUsable": 0, "cases": 1, "fleetUsable": 0,
	})
}

func TestStowContainerIsIdempotentForTheSameBin(t *testing.T) {
	w := stowed(t)
	before := len(w.Events.Events())
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-1"); err != nil {
		t.Fatal(err)
	}
	if len(w.Events.Events()) != before {
		t.Fatal("re-stowing into the same bin must change and publish nothing")
	}
}

func TestStowContainerRejections(t *testing.T) {
	w := newCW()
	w.bin(t, "S1-A-Z-1-1-1-1", "S1", 100, container.HandlingCase)
	w.bin(t, "S1-A-Z-1-1-1-2", "S1", 100, container.HandlingEach)
	w.bin(t, "S1-A-Z-1-1-1-3", "S1", 100, container.HandlingUnknown)
	w.bin(t, "S1-A-Z-1-1-1-4", "S1", 6, container.HandlingPallet)
	w.bin(t, "S2-A-Z-1-1-1-1", "S2", 100, container.HandlingCase)
	w.registered(t, "LPN-1")
	cases := []struct {
		name string
		lpn  string
		bin  shared.BinId
		want error
	}{
		{"unknown container", "NOPE", "S1-A-Z-1-1-1-1", usecases.ErrContainerNotFound},
		{"unknown bin", "LPN-1", "S1-NO-BIN", usecases.ErrBinNotFound},
		{"each-only bin", "LPN-1", "S1-A-Z-1-1-1-2", container.ErrBinHandlingUnitMismatch},
		{"bin without handling unit fails closed", "LPN-1", "S1-A-Z-1-1-1-3", container.ErrBinHandlingUnitMismatch},
		{"bin too small", "LPN-1", "S1-A-Z-1-1-1-4", usecasesBinFull()},
	}
	for _, tc := range cases {
		if _, err := w.Stow.Execute(ctx, tc.lpn, tc.bin); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if c := w.containerOf(t, "LPN-1"); c.BinID() != "" {
		t.Fatalf("a rejected stow placed the container in %s", c.BinID())
	}
	if len(unitLister{w.Stock}.mustUnits(t)) != 0 {
		t.Fatal("a rejected stow left a stock unit behind")
	}
}

func TestStowContainerNeedsABinWithASite(t *testing.T) {
	w := newCW()
	w.bin(t, "NOSITE", "", 100, container.HandlingCase)
	w.registered(t, "LPN-1")
	if _, err := w.Stow.Execute(ctx, "LPN-1", "NOSITE"); !errors.Is(err, container.ErrContainerNotAtSite) {
		t.Fatalf("err = %v, want ErrContainerNotAtSite", err)
	}
}

func TestRestowMovesTheUnitWithinTheSiteAndRefusesAnotherSite(t *testing.T) {
	w := stowed(t)
	w.bin(t, "S1-A-Z-1-1-2-1", "S1", 100, container.HandlingPallet)
	w.bin(t, "S2-A-Z-1-1-1-1", "S2", 100, container.HandlingCase)
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S2-A-Z-1-1-1-1"); !errors.Is(err, container.ErrContainerNotAtSite) {
		t.Fatalf("restow at another site = %v, want ErrContainerNotAtSite", err)
	}
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S1-A-Z-1-1-2-1"); err != nil {
		t.Fatalf("restow within the site: %v", err)
	}
	c := w.containerOf(t, "LPN-1")
	unit, _ := w.Stock.FindByID(ctx, c.StockUnitID())
	from, _ := w.Locations.FindByID(ctx, "S1-A-Z-1-1-1-1")
	to, _ := w.Locations.FindByID(ctx, "S1-A-Z-1-1-2-1")
	if c.BinID() != "S1-A-Z-1-1-2-1" || unit.BinID() != "S1-A-Z-1-1-2-1" || from.Occupied() != 0 || to.Occupied() != 12 {
		t.Fatalf("restow: container %s unit %s occupancy %d -> %d", c.BinID(), unit.BinID(), from.Occupied(), to.Occupied())
	}
	if countEvents(w.env, "ContainerStowed") != 2 {
		t.Fatalf("ContainerStowed = %d, want 2", countEvents(w.env, "ContainerStowed"))
	}
}

func TestReservedContainerCannotBeRestowed(t *testing.T) {
	w := stowed(t)
	w.bin(t, "S1-A-Z-1-1-2-1", "S1", 100, container.HandlingCase)
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D-1", OwnerRef: "seller-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S1-A-Z-1-1-2-1"); !errors.Is(err, container.ErrContainerReserved) {
		t.Fatalf("restow of a reserved case = %v, want ErrContainerReserved", err)
	}
}

func TestStowRequiresASealedContainer(t *testing.T) {
	w := stowed(t)
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-1"); !errors.Is(err, container.ErrContainerNotSealed) {
		t.Fatalf("stow of an EMPTY container = %v, want ErrContainerNotSealed", err)
	}
}

func TestBreakCaseMovesExactlyUnitsPerPackEachesAndEmptiesTheContainer(t *testing.T) {
	w := stowed(t)
	caseUnitID := w.containerOf(t, "LPN-1").StockUnitID()
	c, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2")
	if err != nil {
		t.Fatalf("break: %v", err)
	}
	caseUnit, _ := w.Stock.FindByID(ctx, caseUnitID)
	loose, _ := w.Stock.FindByBin(ctx, "S1-A-Z-1-1-1-2")
	from, _ := w.Locations.FindByID(ctx, "S1-A-Z-1-1-1-1")
	to, _ := w.Locations.FindByID(ctx, "S1-A-Z-1-1-1-2")
	owned, _ := w.Usable.ExecuteFor(ctx, "SKU-1", "seller-a")
	facts := map[string]any{
		"state": string(c.State()), "bin": c.BinID().String(), "caseUnit": string(caseUnit.State()), "caseUnitQty": caseUnit.Quantity().Int(),
		"looseUnits": len(loose), "fromOccupied": from.Occupied().Int(), "toOccupied": to.Occupied().Int(),
		"usable": owned.Usable.Int(), "looseUsable": owned.LooseUsable.Int(), "cases": owned.ReservableCases,
		"caseBroken": countEvents(w.env, "CaseBroken"), "emptied": countEvents(w.env, "ContainerEmptied"),
	}
	assertFacts(t, facts, map[string]any{
		"state": string(container.StateEmpty), "bin": "", "caseUnit": string(stock.StateRemoved), "caseUnitQty": 0,
		"looseUnits": 1, "fromOccupied": 0, "toOccupied": 12, "usable": 12, "looseUsable": 12, "cases": 0, "caseBroken": 1, "emptied": 1,
	})
	assertFacts(t, map[string]any{"qty": loose[0].Quantity().Int(), "loose": loose[0].IsLoose(), "owner": loose[0].OwnerRef().String(), "sku": loose[0].SKU().String()},
		map[string]any{"qty": 12, "loose": true, "owner": "seller-a", "sku": "SKU-1"})
}

func TestBreakCaseAddsToAnExistingLooseUnit(t *testing.T) {
	w := stowed(t)
	other, _ := stock.NewLooseUnit("su-loose", "SKU-1", "S1-A-Z-1-1-1-2", 5, "S1", "seller-a")
	if err := w.Stock.Save(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2"); err != nil {
		t.Fatal(err)
	}
	units, _ := w.Stock.FindByBin(ctx, "S1-A-Z-1-1-1-2")
	if len(units) != 1 || units[0].Quantity() != 17 {
		t.Fatalf("loose units = %+v, want one holding 17", units)
	}
}

func TestBreakCaseKeepsOwnersApart(t *testing.T) {
	w := stowed(t)
	fleet, _ := stock.NewLooseUnit("su-fleet", "SKU-1", "S1-A-Z-1-1-1-2", 5, "S1", "")
	if err := w.Stock.Save(ctx, fleet); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2"); err != nil {
		t.Fatal(err)
	}
	units, _ := w.Stock.FindByBin(ctx, "S1-A-Z-1-1-1-2")
	if len(units) != 2 {
		t.Fatalf("units = %d, want fleet and seller-a kept as two rows", len(units))
	}
	if fleet.Quantity() != 5 {
		t.Fatalf("fleet unit was changed to %d", fleet.Quantity())
	}
}

func TestBreakCaseInPlaceNetsOccupancyToZero(t *testing.T) {
	w := newCW()
	w.bin(t, "S1-A-Z-1-1-1-9", "S1", 100, container.HandlingUnknown)
	w.bin(t, "S1-A-Z-1-1-1-1", "S1", 100, container.HandlingCase)
	w.registered(t, "LPN-1")
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-1"); err != nil {
		t.Fatal(err)
	}
	// A case bin does not take loose eaches, so an in-place break is refused.
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-1"); !errors.Is(err, container.ErrBinHandlingUnitMismatch) {
		t.Fatalf("break into the case bin = %v", err)
	}
	w.Handling.Set("S1-A-Z-1-1-1-1", container.HandlingEach)
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-1"); err != nil {
		t.Fatalf("in-place break: %v", err)
	}
	bin, _ := w.Locations.FindByID(ctx, "S1-A-Z-1-1-1-1")
	if bin.Occupied() != 12 {
		t.Fatalf("occupancy = %d, want 12 (moved within the same bin)", bin.Occupied())
	}
}

func TestBreakCaseRejections(t *testing.T) {
	w := stowed(t)
	w.bin(t, "S2-A-Z-1-1-1-1", "S2", 100, container.HandlingEach)
	w.bin(t, "S1-A-Z-1-1-1-5", "S1", 100, container.HandlingPallet)
	w.bin(t, "S1-A-Z-1-1-1-6", "S1", 5, container.HandlingEach)
	cases := []struct {
		name string
		bin  shared.BinId
		want error
	}{
		{"unknown loose bin", "S1-NO-BIN", usecases.ErrBinNotFound},
		{"loose bin at another site", "S2-A-Z-1-1-1-1", container.ErrContainerNotAtSite},
		{"case-only loose bin", "S1-A-Z-1-1-1-5", container.ErrBinHandlingUnitMismatch},
		{"loose bin without room", "S1-A-Z-1-1-1-6", usecasesBinFull()},
	}
	for _, tc := range cases {
		if _, err := w.Break.Execute(ctx, "LPN-1", tc.bin); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if c := w.containerOf(t, "LPN-1"); c.State() != container.StateSealed || c.BinID() != "S1-A-Z-1-1-1-1" {
		t.Fatalf("rejected breaks changed the container: %s in %s", c.State(), c.BinID())
	}
	if loose, _ := w.Stock.FindByBin(ctx, "S1-A-Z-1-1-1-6"); len(loose) != 0 {
		t.Fatal("a rejected break left loose stock behind")
	}
	if _, err := w.Break.Execute(ctx, "NOPE", "S1-A-Z-1-1-1-2"); !errors.Is(err, usecases.ErrContainerNotFound) {
		t.Fatalf("unknown lpn = %v", err)
	}
}

func TestBreakOfAnOpenOrEmptyOrUnstowedContainerIsRejected(t *testing.T) {
	w := stowed(t)
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2"); !errors.Is(err, container.ErrContainerNotSealed) {
		t.Fatalf("second break (EMPTY) = %v, want ErrContainerNotSealed", err)
	}
	w2 := newCW()
	w2.bin(t, "S1-A-Z-1-1-1-2", "S1", 100, container.HandlingEach)
	w2.registered(t, "LPN-2")
	if _, err := w2.Break.Execute(ctx, "LPN-2", "S1-A-Z-1-1-1-2"); !errors.Is(err, container.ErrContainerNotStowed) {
		t.Fatalf("break of an unstowed container = %v, want ErrContainerNotStowed", err)
	}
}

func TestBreakOfAReservedCaseIsRejected(t *testing.T) {
	w := stowed(t)
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D-1", OwnerRef: "seller-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2"); !errors.Is(err, container.ErrContainerReserved) {
		t.Fatalf("break of a reserved case = %v, want ErrContainerReserved", err)
	}
}

func TestBreakRefusesABrokenContainerUnitLink(t *testing.T) {
	w := stowed(t)
	c := w.containerOf(t, "LPN-1")
	unit, _ := w.Stock.FindByID(ctx, c.StockUnitID())
	if err := unit.RelocateOut(2); err != nil { // the unit no longer holds the case
		t.Fatal(err)
	}
	if _, err := w.Break.Execute(ctx, "LPN-1", "S1-A-Z-1-1-1-2"); !errors.Is(err, container.ErrBreakNotConserving) {
		t.Fatalf("break with a short unit = %v, want ErrBreakNotConserving", err)
	}
}
