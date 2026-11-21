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
