package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

func (w *tw) guardedBins(t *testing.T) {
	t.Helper()
	w.bin(t, asrs, "WH1", 100000, container.HandlingCase)
	w.bin(t, "WH1-STOR-RSV-A01-01-01-A", "WH1", 100, container.HandlingEach)
	w.Types.Set(asrs, location.TypeAutomatedStorage)
	w.Types.Set("WH1-STOR-RSV-A01-01-01-A", "PalletRack")
}

func TestLooseStowIntoAutomatedStorageIsRefused(t *testing.T) {
	w := newTW()
	w.guardedBins(t)
	stow := &usecases.StowStock{Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock, LocationTypes: w.Types}
	if _, err := stow.Execute(context.Background(), "SKU-1", 5, asrs); !errors.Is(err, usecases.ErrAutomatedStorageRequiresContainer) {
		t.Fatalf("err = %v", err)
	}
	if w.occupied(t, asrs) != 0 || len(w.Events.Events()) != 0 {
		t.Fatal("a refused stow wrote something")
	}
	if _, err := stow.Execute(context.Background(), "SKU-1", 5, "WH1-STOR-RSV-A01-01-01-A"); err != nil {
		t.Fatalf("a PalletRack bin must still take loose stock: %v", err)
	}
}

func TestLooseStowIntoABinOfUnknownTypeIsAllowed(t *testing.T) {
	w := newTW()
	w.bin(t, "WH1-STOR-RSV-A02-01-01-A", "WH1", 100, container.HandlingEach)
	stow := &usecases.StowStock{Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock, LocationTypes: w.Types}
	if _, err := stow.Execute(context.Background(), "SKU-1", 5, "WH1-STOR-RSV-A02-01-01-A"); err != nil {
		t.Fatalf("unknown type must fail open (ADR 0013): %v", err)
	}
	unguarded := &usecases.StowStock{Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock}
	w.guardedBins(t)
	if _, err := unguarded.Execute(context.Background(), "SKU-1", 5, asrs); err != nil {
		t.Fatalf("a StowStock without LocationTypes keeps today's behaviour: %v", err)
	}
}

func TestRelocationIntoAutomatedStorageIsRefused(t *testing.T) {
	w := newTW()
	w.guardedBins(t)
	stow := &usecases.StowStock{Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock}
	if _, err := stow.Execute(context.Background(), "SKU-1", 5, "WH1-STOR-RSV-A01-01-01-A"); err != nil {
		t.Fatal(err)
	}
	move := &usecases.RelocateStock{Stock: w.Stock, Locations: w.Locations, Relocations: memoryRelocations(), Events: w.Events, Clock: w.Clock, LocationTypes: w.Types}
	_, err := move.Execute(context.Background(), usecases.RelocateStockCommand{SKU: "SKU-1", FromBin: "WH1-STOR-RSV-A01-01-01-A", ToBin: asrs, Quantity: 2, Reason: relocation.ReasonManual})
	if !errors.Is(err, usecases.ErrAutomatedStorageRequiresContainer) {
		t.Fatalf("err = %v", err)
	}
}

func TestCaseBreakIntoAutomatedStorageIsRefused(t *testing.T) {
	w := newTW()
	w.guardedBins(t)
	w.bin(t, "S1-A-Z-1-1-1-1", "WH1", 100, container.HandlingCase)
	w.registered(t, "003400000000000017")
	if _, err := w.cw.Stow.Execute(context.Background(), "003400000000000017", "S1-A-Z-1-1-1-1"); err != nil {
		t.Fatal(err)
	}
	w.Break.LocationTypes = w.Types
	if _, err := w.Break.Execute(context.Background(), "003400000000000017", asrs); !errors.Is(err, usecases.ErrAutomatedStorageRequiresContainer) {
		t.Fatalf("err = %v", err)
	}
}

func TestASealedCaseMayBeStowedIntoAutomatedStorage(t *testing.T) {
	w := newTW()
	w.guardedBins(t)
	w.registered(t, "003400000000000017")
	if _, err := w.cw.Stow.Execute(context.Background(), "003400000000000017", asrs); err != nil {
		t.Fatalf("a sealed case is a container (ADR 0054 decision 8): %v", err)
	}
}

func memoryRelocations() *memory.RelocationRepo { return memory.NewRelocationRepo() }

// carry (Task 7 review): a transfer stow into an AutomatedStorage bin is
// refused before any write; the receipt stays STAGED and no event is raised.
func TestTransferStowIntoAutomatedStorageIsRefused(t *testing.T) {
	e := newReceiptEnv()
	types := memory.NewBinLocationTypes()
	types.Set(asrs, location.TypeAutomatedStorage)
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, asrs, "WH1", 100000)
	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "WH1", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}
	before := len(e.Events.Events())
	uc := e.stowUseCase()
	uc.LocationTypes = types
	_, err := uc.Execute(context.Background(), "tl-1", []usecases.StowBin{{BinID: asrs, Quantity: 5}})
	if !errors.Is(err, usecases.ErrAutomatedStorageRequiresContainer) {
		t.Fatalf("err = %v, want ErrAutomatedStorageRequiresContainer", err)
	}
	r, _ := e.Receipts.FindByTransferLineID(context.Background(), "tl-1")
	units, _ := e.Stock.FindBySKU(context.Background(), "SKU-T")
	if r == nil || r.State() != transfer.ReceiptStaged || len(units) != 0 || len(e.Events.Events()) != before {
		t.Fatalf("a refused transfer stow wrote something: receipt %+v, units %d, events %d -> %d", r, len(units), before, len(e.Events.Events()))
	}
}

// failingTypes is a location-type lookup whose read fails.
type failingTypes struct{ err error }

func (f failingTypes) LocationType(context.Context, shared.BinId) (string, error) { return "", f.err }

// carry (Task 7 review): a failing location-type lookup is not "unknown type,
// fail open": the guard returns its error and the stow writes nothing.
func TestLocationTypeLookupErrorPropagatesFromTheGuard(t *testing.T) {
	w := newTW()
	w.guardedBins(t)
	boom := errors.New("x")
	stow := &usecases.StowStock{Stock: w.Stock, Locations: w.Locations, Events: w.Events, Clock: w.Clock, LocationTypes: failingTypes{err: boom}}
	if _, err := stow.Execute(context.Background(), "SKU-1", 5, "WH1-STOR-RSV-A01-01-01-A"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the lookup's error", err)
	}
	units, _ := w.Stock.FindBySKU(context.Background(), "SKU-1")
	if w.occupied(t, "WH1-STOR-RSV-A01-01-01-A") != 0 || len(units) != 0 || len(w.Events.Events()) != 0 {
		t.Fatal("a stow whose type lookup failed wrote something")
	}
}
