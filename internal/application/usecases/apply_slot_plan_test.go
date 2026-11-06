package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/allocation"
)

func slotUpdate(eventID, plan string, at time.Time, assignments ...allocation.RawAssignment) usecases.SlotPlanApprovedUpdate {
	return usecases.SlotPlanApprovedUpdate{EventID: eventID, PlanID: plan, SiteID: "WH1", ApprovedAt: at, Assignments: assignments}
}

func TestApplySlotPlan_ReplacesTheSiteMapWholesaleAndIgnoresStaleAndDuplicates(t *testing.T) {
	faces := memory.NewForwardFaceRepo()
	uc := &usecases.ApplySlotPlan{Faces: faces, ProcessedEvents: memory.NewProcessedEventRepo()}
	ctx := context.Background()
	t1 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

	run := func(u usecases.SlotPlanApprovedUpdate) usecases.SlotPlanOutcome {
		t.Helper()
		out, err := uc.Execute(ctx, u)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	roleOf := func(sku, bin string) allocation.BinRole {
		t.Helper()
		f, _ := faces.ForwardFacesFor(ctx, mustSKU(t, sku))
		return f.RoleOf(mustBinID(t, bin))
	}

	if got := run(slotUpdate("e1", "p1", t1, allocation.RawAssignment{SKU: "SKU-1", Slot: "F-1"})); got != usecases.SlotPlanApplied {
		t.Fatalf("outcome = %s", got)
	}
	if roleOf("SKU-1", "F-1") != allocation.RoleForward {
		t.Fatal("F-1 should be SKU-1's forward face")
	}
	// Same event id again: duplicate.
	if got := run(slotUpdate("e1", "p1", t1, allocation.RawAssignment{SKU: "SKU-1", Slot: "F-1"})); got != usecases.SlotPlanDuplicate {
		t.Fatalf("outcome = %s, want DUPLICATE", got)
	}
	// An older plan delivered late: stale, the map is untouched.
	if got := run(slotUpdate("e0", "p0", t1.Add(-time.Hour), allocation.RawAssignment{SKU: "SKU-1", Slot: "F-9"})); got != usecases.SlotPlanStale {
		t.Fatalf("outcome = %s, want STALE", got)
	}
	if roleOf("SKU-1", "F-9") != allocation.RoleReserve {
		t.Fatal("a stale plan changed the map")
	}
	// A newer plan replaces the map: F-1 is no longer SKU-1's face, SKU-2 gained one.
	if got := run(slotUpdate("e2", "p2", t1.Add(time.Hour),
		allocation.RawAssignment{SKU: "SKU-1", Slot: "F-2"}, allocation.RawAssignment{SKU: "SKU-2", Slot: "F-3"})); got != usecases.SlotPlanApplied {
		t.Fatalf("outcome = %s", got)
	}
	if roleOf("SKU-1", "F-1") != allocation.RoleReserve || roleOf("SKU-1", "F-2") != allocation.RoleForward || roleOf("SKU-2", "F-3") != allocation.RoleForward {
		t.Fatal("the newer plan did not replace the map wholesale")
	}
	// Equal approval instant: the higher plan id wins, whatever the delivery order.
	if got := run(slotUpdate("e3", "p9", t1.Add(time.Hour), allocation.RawAssignment{SKU: "SKU-1", Slot: "F-7"})); got != usecases.SlotPlanApplied {
		t.Fatalf("tie outcome = %s", got)
	}
	if got := run(slotUpdate("e4", "p5", t1.Add(time.Hour), allocation.RawAssignment{SKU: "SKU-1", Slot: "F-8"})); got != usecases.SlotPlanStale {
		t.Fatalf("lower plan id on a tie = %s, want STALE", got)
	}
}

func TestApplySlotPlan_MalformedUpdatesAreDeterministicErrors(t *testing.T) {
	uc := &usecases.ApplySlotPlan{Faces: memory.NewForwardFaceRepo(), ProcessedEvents: memory.NewProcessedEventRepo()}
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	bad := []usecases.SlotPlanApprovedUpdate{
		slotUpdate("", "p1", at),
		slotUpdate("e", "", at),
		slotUpdate("e", "p1", time.Time{}),
		{EventID: "e", PlanID: "p1", SiteID: "", ApprovedAt: at},
		slotUpdate("e", "p1", at, allocation.RawAssignment{SKU: "", Slot: "F-1"}),
		slotUpdate("e", "p1", at, allocation.RawAssignment{SKU: "SKU-1", Slot: ""}),
	}
	for i, u := range bad {
		if _, err := uc.Execute(context.Background(), u); !errors.Is(err, usecases.ErrMalformedSlotPlan) {
			t.Fatalf("case %d: err = %v, want ErrMalformedSlotPlan", i, err)
		}
	}
}
