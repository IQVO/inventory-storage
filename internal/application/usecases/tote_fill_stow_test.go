package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

const (
	decant = "WH1-RCV-DEC-D01-01-01-A"
	asrs   = "WH1-AUTO-ASRS-ASRS1-00-00-S"
)

// decanted registers TOTE-1, places it at the decant station and fills it
// with 6 x SKU-1 and 4 x SKU-2 (flow 2: "decant fills container C").
func (w *tw) decanted(t *testing.T) {
	t.Helper()
	w.bin(t, decant, "WH1", 100, container.HandlingUnknown)
	w.bin(t, asrs, "WH1", 100000, container.HandlingUnknown)
	w.Types.Set(asrs, location.TypeAutomatedStorage)
	if _, _, err := w.Register.Execute(context.Background(), "TOTE-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Stow.Execute(context.Background(), "TOTE-1", decant); err != nil {
		t.Fatalf("place at decant: %v", err)
	}
	for _, f := range []usecases.FillToteCommand{{LPN: "TOTE-1", SKU: "SKU-1", Quantity: 6}, {LPN: "TOTE-1", SKU: "SKU-2", Quantity: 4}} {
		if _, err := w.Fill.Execute(context.Background(), f); err != nil {
			t.Fatalf("fill %s: %v", f.SKU, err)
		}
	}
}

func (w *tw) occupied(t *testing.T, id string) int {
	t.Helper()
	b, err := w.Locations.FindByID(context.Background(), shared.BinId(id))
	if err != nil || b == nil {
		t.Fatalf("bin %s: %v", id, err)
	}
	return b.Occupied().Int()
}

func TestFillToteRecordsTheContentsAtTheTotesBin(t *testing.T) {
	w := newTW()
	w.decanted(t)
	view, err := w.Get.Execute(context.Background(), "TOTE-1")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{"bin": string(view.Tote.BinID()), "site": string(view.Tote.SiteID()), "lines": len(view.Contents),
		"sku0": string(view.Contents[0].SKU()), "qty0": view.Contents[0].Quantity().Int(), "lpn0": view.Contents[0].ContainerLPN(),
		"bin0": string(view.Contents[0].BinID()), "occupied": w.occupied(t, decant),
		"filled": countEvents(w.env, "ToteFilled"), "stowed": countEvents(w.env, "ItemStowed")}
	assertFacts(t, got, map[string]any{"bin": decant, "site": "WH1", "lines": 2, "sku0": "SKU-1", "qty0": 6, "lpn0": "TOTE-1",
		"bin0": decant, "occupied": 10, "filled": 2, "stowed": 2})
}

func TestFillToteRefusesATotesInNoBin(t *testing.T) {
	w := newTW()
	_, _, _ = w.Register.Execute(context.Background(), "TOTE-1")
	if _, err := w.Fill.Execute(context.Background(), usecases.FillToteCommand{LPN: "TOTE-1", SKU: "SKU-1", Quantity: 1}); !errors.Is(err, container.ErrToteNotPlaced) {
		t.Fatalf("err = %v", err)
	}
}

func TestFillToteRespectsBinCapacityAndWritesNothingOnRefusal(t *testing.T) {
	w := newTW()
	w.decanted(t)
	before := countEvents(w.env, "ToteFilled")
	if _, err := w.Fill.Execute(context.Background(), usecases.FillToteCommand{LPN: "TOTE-1", SKU: "SKU-3", Quantity: 91}); !errors.Is(err, location.ErrBinFull) {
		t.Fatalf("err = %v", err)
	}
	view, _ := w.Get.Execute(context.Background(), "TOTE-1")
	if len(view.Contents) != 2 || w.occupied(t, decant) != 10 || countEvents(w.env, "ToteFilled") != before {
		t.Fatal("a refused fill changed the ledger")
	}
}

func TestFillToteUnknownToteIsNotFound(t *testing.T) {
	w := newTW()
	if _, err := w.Fill.Execute(context.Background(), usecases.FillToteCommand{LPN: "NOPE", SKU: "SKU-1", Quantity: 1}); !errors.Is(err, usecases.ErrToteNotFound) {
		t.Fatalf("err = %v", err)
	}
}

// Acceptance "Putaway" (implementation-handoff): inventory-storage shows C's
// contents in the AS/RS location.
func TestStowToteMovesTheContentsIntoTheASRSLocation(t *testing.T) {
	w := newTW()
	w.decanted(t)
	tote, err := w.Stow.Execute(context.Background(), "TOTE-1", asrs)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := w.Get.Execute(context.Background(), "TOTE-1")
	stowed := lastToteStowed(t, w)
	got := map[string]any{"tote": string(tote.BinID()), "unit0": string(view.Contents[0].BinID()), "unit1": string(view.Contents[1].BinID()),
		"decant": w.occupied(t, decant), "asrs": w.occupied(t, asrs), "from": string(stowed.FromBin), "to": string(stowed.ToBin), "lines": len(stowed.Contents)}
	assertFacts(t, got, map[string]any{"tote": asrs, "unit0": asrs, "unit1": asrs, "decant": 0, "asrs": 10, "from": decant, "to": asrs, "lines": 2})
}

func TestStowToteIntoItsOwnBinIsANoOp(t *testing.T) {
	w := newTW()
	w.decanted(t)
	before := countEvents(w.env, "ToteStowed")
	if _, err := w.Stow.Execute(context.Background(), "TOTE-1", decant); err != nil {
		t.Fatal(err)
	}
	if countEvents(w.env, "ToteStowed") != before || w.occupied(t, decant) != 10 {
		t.Fatal("a same-bin stow changed something")
	}
}

func TestStowToteRefusesAnotherSiteAndMovesNothing(t *testing.T) {
	w := newTW()
	w.decanted(t)
	w.bin(t, "WH2-AUTO-ASRS-ASRS1-00-00-S", "WH2", 1000, container.HandlingUnknown)
	if _, err := w.Stow.Execute(context.Background(), "TOTE-1", "WH2-AUTO-ASRS-ASRS1-00-00-S"); !errors.Is(err, container.ErrContainerNotAtSite) {
		t.Fatalf("err = %v", err)
	}
	if w.occupied(t, decant) != 10 {
		t.Fatal("a refused stow moved stock")
	}
}

func TestStowToteRefusesAToteHoldingReservedStock(t *testing.T) {
	w := newTW()
	w.decanted(t)
	if _, err := w.Reserve.ExecuteFor(context.Background(), usecases.ReserveRequest{SKU: "SKU-1", Quantity: 2, DemandRef: "order-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Stow.Execute(context.Background(), "TOTE-1", asrs); !errors.Is(err, container.ErrToteReserved) {
		t.Fatalf("err = %v", err)
	}
	if w.occupied(t, asrs) != 0 {
		t.Fatal("a refused stow moved stock")
	}
}

func TestStowToteUnknownBinOrTote(t *testing.T) {
	w := newTW()
	w.decanted(t)
	if _, err := w.Stow.Execute(context.Background(), "TOTE-1", "NOPE"); !errors.Is(err, usecases.ErrBinNotFound) {
		t.Fatalf("bin err = %v", err)
	}
	if _, err := w.Stow.Execute(context.Background(), "NOPE", asrs); !errors.Is(err, usecases.ErrToteNotFound) {
		t.Fatalf("tote err = %v", err)
	}
}

// Carry from the Task 3 review (progress.md): a refused PlaceIn on an
// ALREADY-placed tote changes nothing, neither its state, location and
// version nor the pending events.
func TestRefusedPlaceInOnAPlacedToteChangesNothing(t *testing.T) {
	placed := container.RehydrateTote(container.ToteSnapshot{LPN: "TOTE-1", SiteID: "WH1", BinID: decant, Version: 3})
	before := placed.Snapshot()
	for _, site := range []shared.SiteID{"WH2", ""} {
		if err := placed.PlaceIn(asrs, site); !errors.Is(err, container.ErrContainerNotAtSite) {
			t.Fatalf("site %q: err = %v", site, err)
		}
	}
	if err := placed.PlaceIn("", "WH1"); !errors.Is(err, shared.ErrEmptyBinID) {
		t.Fatalf("empty bin: err = %v", err)
	}
	if placed.Snapshot() != before {
		t.Fatalf("a refused PlaceIn changed the tote: %+v, want %+v", placed.Snapshot(), before)
	}
}

// Carry from the Task 3 review, through StowTote: a placed tote refused by
// PlaceIn (another site, or a destination whose site is unknown) keeps its
// persisted state, location and version, its contents stay where they were,
// and no event is pending.
func TestStowToteRefusedPlaceInOfAPlacedToteChangesNothing(t *testing.T) {
	w := newTW()
	w.decanted(t)
	w.bin(t, "WH2-AUTO-ASRS-ASRS1-00-00-S", "WH2", 1000, container.HandlingUnknown)
	w.bin(t, "NOSITE", "", 1000, container.HandlingUnknown)
	before, _ := w.Totes.FindByLPN(context.Background(), "TOTE-1")
	events := len(w.Events.Events())
	for _, bin := range []shared.BinId{"WH2-AUTO-ASRS-ASRS1-00-00-S", "NOSITE"} {
		if _, err := w.Stow.Execute(context.Background(), "TOTE-1", bin); !errors.Is(err, container.ErrContainerNotAtSite) {
			t.Fatalf("%s: err = %v", bin, err)
		}
	}
	after, _ := w.Totes.FindByLPN(context.Background(), "TOTE-1")
	view, _ := w.Get.Execute(context.Background(), "TOTE-1")
	got := map[string]any{"tote": after.Snapshot(), "events": len(w.Events.Events()), "decant": w.occupied(t, decant),
		"unit0": string(view.Contents[0].BinID()), "unit1": string(view.Contents[1].BinID())}
	assertFacts(t, got, map[string]any{"tote": before.Snapshot(), "events": events, "decant": 10, "unit0": decant, "unit1": decant})
}

func lastToteStowed(t *testing.T, w *tw) shared.ToteStowed {
	t.Helper()
	evs := w.Events.Events()
	for i := len(evs) - 1; i >= 0; i-- {
		if e, ok := evs[i].(shared.ToteStowed); ok {
			return e
		}
	}
	t.Fatal("no ToteStowed")
	return shared.ToteStowed{}
}
