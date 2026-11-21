package main

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
)

// recordingUnitOfWork is a distinct, non-nil UnitOfWork: comparing a use
// case's UnitOfWork with it proves the wiring handed over the adapter set's
// unit of work (a nil one would compare equal to a missing field).
type recordingUnitOfWork struct{ calls int }

func (u *recordingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	u.calls++
	return fn(ctx)
}

// carry (Task 5 review): the tote use cases that write commit in the adapter
// set's unit of work, so the license-plate claim, the tote, the stock, the
// bins and the outbox rows commit or roll back together (ADR 0054, D1).
func TestBuildServer_WiresTheToteUseCasesInTheUnitOfWork(t *testing.T) {
	set := memoryAdapters(quietLogger())
	uow := &recordingUnitOfWork{}
	set.uow = uow
	containers := memory.NewContainerRepo()
	set.containers, set.totes, set.containerStock = containers, memory.NewToteRepo(containers), memory.NewStockRepo()
	s := buildServer(set, system.Clock{}, nil, noHandlingUnits{}, noLocationTypes{}, nil, nil)

	if s.RegisterTote.UnitOfWork != uow || s.FillTote.UnitOfWork != uow || s.StowTote.UnitOfWork != uow {
		t.Fatal("RegisterTote, FillTote and StowTote must run in the adapter set's UnitOfWork")
	}
	if s.RegisterTote.Totes != set.totes || s.FillTote.Totes != set.totes || s.StowTote.Totes != set.totes ||
		s.GetTote.Totes != set.totes || s.ListBinTotes.Totes != set.totes {
		t.Fatal("every tote use case must use the adapter set's tote repo")
	}
	if s.StowTote.Contents != set.containerStock || s.GetTote.Stock != set.containerStock {
		t.Fatal("StowTote and GetTote must read a tote's units through the adapter set's container stock")
	}
}

// carry (Task 7 review): every loose-stock write into a bin gets the same
// LocationTypes lookup (ADR 0054, D9).
func TestBuildServer_WiresTheLocationTypesIntoEveryLooseStow(t *testing.T) {
	types := memory.NewBinLocationTypes()
	s := buildServer(memoryAdapters(quietLogger()), system.Clock{}, nil, noHandlingUnits{}, types, nil, nil)
	if s.StowStock.LocationTypes != types || s.BreakCase.LocationTypes != types ||
		s.StowTransferStock.LocationTypes != types || s.RelocateStock.LocationTypes != types {
		t.Fatal("StowStock, BreakCase, StowTransferStock and RelocateStock must share the location-type lookup")
	}
}

// carry (Task 11 review): a fill and a stow apply the placement rules of a
// plain stow and raise BinStockLevelChanged (ADR 0054, D12). A nil Placement
// or Levels is the documented pass-through, so a field dropped from the
// composition root would switch both off silently in production.
func TestBuildServer_WiresThePlacementAndLevelsIntoTheToteFillAndStow(t *testing.T) {
	set := memoryAdapters(quietLogger())
	set.binLevels = memory.NewBinLevelLedger(memory.NewStockRepo())
	containers := memory.NewContainerRepo()
	set.containers, set.totes, set.containerStock = containers, memory.NewToteRepo(containers), memory.NewStockRepo()
	s := buildServer(set, memory.SystemClock{}, nil, noHandlingUnits{}, noLocationTypes{}, nil, nil)

	if s.FillTote.Placement == nil || s.StowTote.Placement != s.FillTote.Placement {
		t.Fatal("FillTote and StowTote must share the plain stow's placement rules")
	}
	if s.FillTote.Levels == nil || s.StowTote.Levels == nil {
		t.Fatal("FillTote and StowTote must raise BinStockLevelChanged through the level emitter")
	}
}
