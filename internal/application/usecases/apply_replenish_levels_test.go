package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// A completed REPLENISH task raises StockRelocated (REPLENISHMENT, source_ref =
// request_ref) and then the two BinStockLevelChanged events with reason
// REPLENISHMENT, source bin first, then destination (ADR 0040, ADR 0041).
func TestBinLevels_ReplenishTask_RaisesReplenishmentLevelsSourceThenDestination(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	uc := &usecases.ApplyReplenishTask{
		Relocate: &usecases.RelocateStock{
			Stock: e.Stock, Locations: e.Locations, Relocations: memory.NewRelocationRepo(),
			Events: e.Events, Clock: e.Clock, Levels: e.Levels,
		},
		ProcessedEvents: memory.NewProcessedEventRepo(),
	}
	in := usecases.ReplenishTaskCompletion{
		EventID: "evt-r1", TaskID: "task-r1", TaskType: usecases.ReplenishTaskType,
		SKU: "SKU-1", FromBin: string(lvBinA), ToBin: string(lvBinB), Quantity: intPtr(4), RequestRef: "repl-1",
	}

	res, err := uc.Execute(context.Background(), in)
	if err != nil || res.Outcome != usecases.ReplenishApplied {
		t.Fatalf("replenish: outcome %q err %v", res.Outcome, err)
	}

	evs := e.levelEvents()
	if len(evs) != 3 {
		t.Fatalf("level events = %d, want stow + source + destination", len(evs))
	}
	assertLevel(t, evs[1], wantLevel{lvBinA, "SKU-1", 6, 0, 2, shared.BinLevelReplenishment})
	assertLevel(t, evs[2], wantLevel{lvBinB, "SKU-1", 4, 0, 1, shared.BinLevelReplenishment})
	assertStockRelocatedReplenishment(t, e, "repl-1")

	// A redelivery changes nothing and raises nothing.
	if again, err := uc.Execute(context.Background(), in); err != nil || again.Outcome != usecases.ReplenishDuplicate {
		t.Fatalf("redelivery: outcome %q err %v", again.Outcome, err)
	}
	if got := len(e.levelEvents()); got != 3 {
		t.Fatalf("level events = %d after a redelivery, want 3", got)
	}
}

func assertStockRelocatedReplenishment(t *testing.T, e levelEnv, wantRef string) {
	t.Helper()
	var found int
	for _, ev := range e.Events.Events() {
		moved, ok := ev.(shared.StockRelocated)
		if !ok {
			continue
		}
		found++
		if moved.Reason != string(relocation.ReasonReplenishment) || moved.SourceRef != wantRef || moved.Quantity != 4 {
			t.Fatalf("StockRelocated reason %q source_ref %q quantity %d, want REPLENISHMENT / %s / 4",
				moved.Reason, moved.SourceRef, moved.Quantity, wantRef)
		}
	}
	if found != 1 {
		t.Fatalf("StockRelocated events = %d, want exactly 1", found)
	}
}
