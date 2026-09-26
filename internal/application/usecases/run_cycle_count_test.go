package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func TestRunCycleCount_MatchesSystem_NoDiscrepancy(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock}

	result, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Discrepancy {
		t.Fatalf("expected no discrepancy")
	}
}

func TestRunCycleCount_Shortfall_FlagsUnlocated(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock}

	result, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 4))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Discrepancy {
		t.Fatalf("expected discrepancy on shortfall")
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 0 {
		t.Fatalf("expected usable=0 once the unit is flagged unlocated, got %d", usable.Usable.Int())
	}

	foundUnlocated := false
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ItemUnlocated" {
			foundUnlocated = true
		}
	}
	if !foundUnlocated {
		t.Fatalf("expected an ItemUnlocated event to be published")
	}
}

func TestRunCycleCount_Overage_FlagsDiscrepancyWithoutUnlocating(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 20, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock}

	result, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 15))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Discrepancy {
		t.Fatalf("expected discrepancy on overage")
	}

	usableUC := &usecases.GetUsable{Stock: e.Stock}
	usable, _ := usableUC.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if usable.Usable.Int() != 10 {
		t.Fatalf("expected usable unchanged by overage (no auto-reconcile), got %d", usable.Usable.Int())
	}
}

func TestRunCycleCount_EmptyBin_NoDiscrepancy(t *testing.T) {
	e := newEnv()
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock}

	result, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Discrepancy {
		t.Fatalf("expected no discrepancy for an empty bin counted as zero")
	}
}

func TestRunCycleCount_SkipsAlreadyUnlocatedUnits(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock}

	// First count flags a full shortfall, marking the unit Unlocated.
	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), 0); err != nil {
		t.Fatalf("unexpected error on first count: %v", err)
	}

	// A second count should skip the already-Unlocated unit when computing
	// systemQty, so an empty count now reconciles cleanly.
	result, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), 0)
	if err != nil {
		t.Fatalf("unexpected error on second count: %v", err)
	}
	if result.Discrepancy {
		t.Fatalf("expected no discrepancy once the unlocated unit is excluded from systemQty")
	}
}

func TestRunCycleCount_StockFindByBinFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stockRepo := &failingStockRepo{delegate: e.Stock, failFindByBin: true}
	uc := &usecases.RunCycleCount{Stock: stockRepo, Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), 0); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestRunCycleCount_EventPublishFails_PropagatesError_NoDiscrepancy(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: failingEvents{}, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 10)); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestRunCycleCount_EventPublishFails_Overage(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 20, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: failingEvents{}, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 15)); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestRunCycleCount_StockSaveFails_Shortfall(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	stockRepo := &failingStockRepo{delegate: e.Stock, failSave: true}
	uc := &usecases.RunCycleCount{Stock: stockRepo, Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 4)); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

// A shortfall spread across two units in the same bin: the first unit
// covers the shortfall (partially — it is marked fully Unlocated by
// design), and once the shortfall reaches zero the loop must break and
// leave the second unit untouched.
func TestRunCycleCount_ShortfallAcrossUnits_StopsWhenShortfallCovered(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 5)
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 5)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock}

	result, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 7))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Discrepancy {
		t.Fatalf("expected discrepancy on shortfall")
	}

	unlocated := 0
	remaining := 0
	for _, unit := range mustUnitsForBin(t, e, "A-1-1") {
		switch unit.State() {
		case stock.StateUnlocated:
			unlocated++
		default:
			remaining += unit.Quantity().Int()
		}
	}
	if unlocated != 1 {
		t.Fatalf("expected exactly 1 unlocated unit (shortfall covered by the first), got %d", unlocated)
	}
	if remaining != 5 {
		t.Fatalf("expected the untouched unit to still hold 5, got %d", remaining)
	}

	itemUnlocated := 0
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "ItemUnlocated" {
			itemUnlocated++
		}
	}
	if itemUnlocated != 1 {
		t.Fatalf("expected exactly 1 ItemUnlocated event, got %d", itemUnlocated)
	}
}

// The ItemUnlocated publish inside the shortfall loop failing must abort
// the count — earlier publishes (DiscrepancyDetected) already succeeded,
// which is exactly what selectiveFailingEvents exists to arrange.
func TestRunCycleCount_EventPublishFails_ItemUnlocated(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: selectiveFailingEvents{delegate: e.Events, failName: "ItemUnlocated"}, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 4)); err != errFake {
		t.Fatalf("expected errFake from ItemUnlocated publish, got %v", err)
	}
}

// The closing CycleCountCompleted(discrepancy=true) publish of the OVERAGE
// branch failing must abort the count after DiscrepancyDetected already
// went out — failingEvents cannot reach this branch (it dies on the first
// publish).
func TestRunCycleCount_EventPublishFails_CycleCountCompleted_Overage(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 20, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: selectiveFailingEvents{delegate: e.Events, failName: "CycleCountCompleted"}, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 15)); err != errFake {
		t.Fatalf("expected errFake from closing CycleCountCompleted publish, got %v", err)
	}
}

// mustUnitsForBin returns every stocked unit for binID via the repo, for
// post-count state assertions.
func mustUnitsForBin(t *testing.T, e env, binID string) []*stock.StockUnit {
	t.Helper()
	units, err := e.Stock.FindByBin(context.Background(), mustBinID(t, binID))
	if err != nil {
		t.Fatalf("unexpected error finding units: %v", err)
	}
	return units
}

// Same selective failure as the overage variant, but on the SHORTFALL
// path: DiscrepancyDetected and ItemUnlocated both publish, then the
// closing CycleCountCompleted fails and aborts the count.
func TestRunCycleCount_EventPublishFails_CycleCountCompleted_Shortfall(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: selectiveFailingEvents{delegate: e.Events, failName: "CycleCountCompleted"}, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustBinID(t, "A-1-1"), mustQty(t, 4)); err != errFake {
		t.Fatalf("expected errFake from closing CycleCountCompleted publish, got %v", err)
	}
}
