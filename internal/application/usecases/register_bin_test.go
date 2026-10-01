package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

type registerBinCase struct {
	name         string
	seedCapacity int // 0 = bin absent
	stow         int
	capacity     int
	wantErr      error
	wantOutcome  usecases.RegisterBinOutcome
	wantCapacity int // persisted capacity afterwards (0 = bin must stay absent)
}

func TestRegisterBin(t *testing.T) {
	tests := []registerBinCase{
		{name: "absent bin is created", capacity: 10, wantOutcome: usecases.BinCreated, wantCapacity: 10},
		{name: "same capacity is a no-op", seedCapacity: 10, stow: 3, capacity: 10, wantOutcome: usecases.BinUnchanged, wantCapacity: 10},
		{name: "grow resizes", seedCapacity: 10, stow: 3, capacity: 25, wantOutcome: usecases.BinResized, wantCapacity: 25},
		{name: "shrink to occupancy resizes", seedCapacity: 10, stow: 3, capacity: 3, wantOutcome: usecases.BinResized, wantCapacity: 3},
		{name: "shrink below occupancy is rejected", seedCapacity: 10, stow: 3, capacity: 2, wantErr: location.ErrCapacityBelowOccupancy, wantCapacity: 10},
		{name: "zero capacity is rejected before any read", capacity: 0, wantErr: location.ErrInvalidCapacity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runRegisterBinCase(t, tc) })
	}
}

// runRegisterBinCase seeds the case's starting state, runs RegisterBin once,
// and checks both the returned values and what ended up persisted.
func runRegisterBinCase(t *testing.T, tc registerBinCase) {
	t.Helper()
	e := newEnv()
	binID := mustBinID(t, "R-1-1")
	if tc.seedCapacity > 0 {
		seedBin(t, e, binID, tc.seedCapacity)
	}
	if tc.stow > 0 {
		stow := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}
		if _, err := stow.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, tc.stow), binID); err != nil {
			t.Fatalf("unexpected error stowing: %v", err)
		}
	}
	uc := &usecases.RegisterBin{Locations: e.Locations}

	bin, outcome, err := uc.Execute(context.Background(), binID, shared.Quantity(tc.capacity))
	if !errors.Is(err, tc.wantErr) {
		t.Fatalf("expected error %v, got %v", tc.wantErr, err)
	}
	assertRegisterResult(t, tc, binID, bin, outcome)
	assertPersistedBin(t, e, binID, tc.wantCapacity, tc.stow)
}

func assertRegisterResult(t *testing.T, tc registerBinCase, binID shared.BinId, bin *location.Bin, outcome usecases.RegisterBinOutcome) {
	t.Helper()
	if tc.wantErr != nil {
		if bin != nil || outcome != 0 {
			t.Fatalf("expected nil bin and zero outcome on error, got %v / %v", bin, outcome)
		}
		return
	}
	if outcome != tc.wantOutcome {
		t.Fatalf("expected outcome %v, got %v", tc.wantOutcome, outcome)
	}
	if bin.ID() != binID || bin.Capacity().Int() != tc.wantCapacity || bin.Occupied().Int() != tc.stow {
		t.Fatalf("unexpected returned bin: id=%v capacity=%d occupied=%d", bin.ID(), bin.Capacity().Int(), bin.Occupied().Int())
	}
}

func assertPersistedBin(t *testing.T, e env, binID shared.BinId, wantCapacity, wantOccupied int) {
	t.Helper()
	stored, _ := e.Locations.FindByID(context.Background(), binID)
	if wantCapacity == 0 {
		if stored != nil {
			t.Fatalf("expected no bin to be persisted, got %+v", stored)
		}
		return
	}
	if stored == nil || stored.Capacity().Int() != wantCapacity || stored.Occupied().Int() != wantOccupied {
		t.Fatalf("unexpected persisted bin: %+v", stored)
	}
}

func TestRegisterBin_RejectsEmptyID(t *testing.T) {
	e := newEnv()
	uc := &usecases.RegisterBin{Locations: e.Locations}
	if _, _, err := uc.Execute(context.Background(), "", mustQty(t, 5)); err != shared.ErrEmptyBinID {
		t.Fatalf("expected ErrEmptyBinID, got %v", err)
	}
}

func TestRegisterBin_RepoFailures_Propagate(t *testing.T) {
	tests := []struct {
		name     string
		seed     bool
		capacity int
		repo     func(e env) *failingLocationRepo
	}{
		{name: "find fails", capacity: 5, repo: func(e env) *failingLocationRepo {
			return &failingLocationRepo{delegate: e.Locations, failFindByID: true}
		}},
		{name: "save fails on create", capacity: 5, repo: func(e env) *failingLocationRepo {
			return &failingLocationRepo{delegate: e.Locations, failSave: true}
		}},
		{name: "save fails on resize", seed: true, capacity: 7, repo: func(e env) *failingLocationRepo {
			return &failingLocationRepo{delegate: e.Locations, failSave: true}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv()
			if tc.seed {
				seedBin(t, e, mustBinID(t, "R-1-1"), 5)
			}
			uc := &usecases.RegisterBin{Locations: tc.repo(e)}
			if _, _, err := uc.Execute(context.Background(), mustBinID(t, "R-1-1"), mustQty(t, tc.capacity)); err != errFake {
				t.Fatalf("expected errFake, got %v", err)
			}
		})
	}
}

// The no-op path must not write: a same-capacity PUT never bumps the
// optimistic-concurrency version or races a concurrent stow.
func TestRegisterBin_SameCapacity_DoesNotSave(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "R-1-1"), 5)
	uc := &usecases.RegisterBin{Locations: &failingLocationRepo{delegate: e.Locations, failSave: true}}
	if _, outcome, err := uc.Execute(context.Background(), mustBinID(t, "R-1-1"), mustQty(t, 5)); err != nil || outcome != usecases.BinUnchanged {
		t.Fatalf("expected BinUnchanged with no save, got outcome=%v err=%v", outcome, err)
	}
}

func TestGetBin(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "G-1-1"), 8)
	uc := &usecases.GetBin{Locations: e.Locations}

	bin, err := uc.Execute(context.Background(), mustBinID(t, "G-1-1"))
	if err != nil || bin.Capacity().Int() != 8 {
		t.Fatalf("expected bin with capacity 8, got %v / %v", bin, err)
	}
	if _, err := uc.Execute(context.Background(), mustBinID(t, "NOPE")); err != usecases.ErrBinNotFound {
		t.Fatalf("expected ErrBinNotFound, got %v", err)
	}
	if _, err := uc.Execute(context.Background(), ""); err != shared.ErrEmptyBinID {
		t.Fatalf("expected ErrEmptyBinID, got %v", err)
	}
	failing := &usecases.GetBin{Locations: &failingLocationRepo{delegate: e.Locations, failFindByID: true}}
	if _, err := failing.Execute(context.Background(), mustBinID(t, "G-1-1")); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}
