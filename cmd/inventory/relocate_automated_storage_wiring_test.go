package main

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// carry (Task 7 review): with LocationTypes wired into the RelocateStock the
// TaskCompleted RELOCATE branch drives, a RELOCATE task into an
// AutomatedStorage bin is a deterministic rejection: it dead-letters
// (ErrMalformedRelocationTask) and names the rule.
func TestNewApplyRelocationTask_ARelocateIntoAutomatedStorageDeadLetters(t *testing.T) {
	ctx := context.Background()
	set := memoryAdapters(quietLogger())
	from, to := shared.BinId("WH1-STOR-AMB-A01-01-01-A"), shared.BinId("WH1-AUTO-ASRS-ASRS1-00-00-S")
	for id, occupied := range map[shared.BinId]shared.Quantity{from: 5, to: 0} {
		if err := set.locations.Save(ctx, location.RehydrateBinAtSite(id, 100, occupied, "WH1", 1)); err != nil {
			t.Fatal(err)
		}
	}
	unit, err := stock.NewLooseUnit("su-1", "SKU-1", from, 5, "WH1", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := set.stock.Save(ctx, unit); err != nil {
		t.Fatal(err)
	}
	types := memory.NewBinLocationTypes()
	types.Set(to, location.TypeAutomatedStorage)

	uc := newApplyRelocationTask(set, system.Clock{}, memory.NewProcessedEventRepo(), types)
	qty := 5
	res, err := uc.Execute(ctx, usecases.RelocationTaskCompletion{
		EventID: "evt-1", TaskID: "task-1", TaskType: usecases.RelocateTaskType,
		SKU: "SKU-1", FromBin: from.String(), ToBin: to.String(), Quantity: &qty,
	})
	if !errors.Is(err, usecases.ErrMalformedRelocationTask) || !errors.Is(err, usecases.ErrAutomatedStorageRequiresContainer) {
		t.Fatalf("outcome %q err %v: want a dead-letter naming automated-storage-requires-container", res.Outcome, err)
	}
}

// carry (Task 7 review): the REPLENISH branch drives the same guarded
// RelocateStock.
func TestNewApplyReplenishTask_WiresTheLocationTypes(t *testing.T) {
	types := memory.NewBinLocationTypes()
	uc := newApplyReplenishTask(memoryAdapters(quietLogger()), system.Clock{}, memory.NewProcessedEventRepo(), types)
	rs, ok := uc.Relocate.(*usecases.RelocateStock)
	if !ok || rs.LocationTypes != types {
		t.Fatalf("Relocate = %T, LocationTypes must be the lookup handed to the consumer", uc.Relocate)
	}
}
