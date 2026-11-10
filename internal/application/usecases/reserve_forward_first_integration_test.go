//go:build integration

// Forward-first allocation through the REAL Postgres adapters (ADR 0050): the
// forward pick face comes from the forward_pick_faces read model fed by
// ApplySlotPlan, the stock rows come from the real stock repo, and the order
// is the domain policy (allocation.Order) - never SQL - so the adapter and the
// policy cannot disagree. Boots its own Postgres via ocDB, never t.Skip.
package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func TestIntegration_ReserveStock_ForwardFirstOnRealRows(t *testing.T) {
	pool := ocDB(t)
	ctx := context.Background()
	locations := postgres.NewLocationRepo(pool)
	stockRepo := postgres.NewStockRepo(pool)
	reservations := postgres.NewReservationRepo(pool)
	faces := postgres.NewForwardFaceRepo(pool)

	sku, _ := shared.NewSKU("IT-FWD-FIRST-SKU")
	// The reserve bin is saved FIRST and sorts BEFORE the forward bin by id:
	// first-fit and a bin-id ORDER BY would both drain it.
	seed := func(bin, unitID string, qty int) {
		binID, _ := shared.NewBinId(bin)
		b, err := location.NewBin(binID, mustQty(t, 100))
		if err != nil {
			t.Fatal(err)
		}
		if err := locations.Save(ctx, b); err != nil {
			t.Fatal(err)
		}
		u, err := stock.NewStockUnit(unitID, sku, binID, mustQty(t, qty))
		if err != nil {
			t.Fatal(err)
		}
		if err := stockRepo.Save(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	seed("IT-FWD-A-RESERVE", "it-fwd-su-reserve", 20)
	seed("IT-FWD-Z-FACE", "it-fwd-su-face", 5)

	apply := &usecases.ApplySlotPlan{Faces: faces, ProcessedEvents: postgres.NewProcessedEventRepo(pool)}
	approve := func(eventID, planID string, at time.Time, slot string) usecases.SlotPlanOutcome {
		t.Helper()
		out, err := apply.Execute(ctx, usecases.SlotPlanApprovedUpdate{
			EventID: eventID, PlanID: planID, SiteID: "IT-WH1", ApprovedAt: at,
			Assignments: []allocation.RawAssignment{{SKU: sku.String(), Slot: slot}},
		})
		if err != nil {
			t.Fatalf("apply %s: %v", planID, err)
		}
		return out
	}
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	if got := approve("it-fwd-ev-1", "it-plan-1", t0, "IT-FWD-Z-FACE"); got != usecases.SlotPlanApplied {
		t.Fatalf("first plan outcome = %s", got)
	}
	if got := approve("it-fwd-ev-1", "it-plan-1", t0, "IT-FWD-Z-FACE"); got != usecases.SlotPlanDuplicate {
		t.Fatalf("redelivered event outcome = %s, want DUPLICATE", got)
	}
	// A plan approved EARLIER that arrives late must not overwrite the map.
	if got := approve("it-fwd-ev-0", "it-plan-0", t0.Add(-time.Hour), "IT-FWD-A-RESERVE"); got != usecases.SlotPlanStale {
		t.Fatalf("late older plan outcome = %s, want STALE", got)
	}

	uc := &usecases.ReserveStock{
		Stock: stockRepo, Reservations: reservations, Events: events.NewBufferedPublisher(),
		Clock: memory.NewFixedClock(time.Now().UTC()), Forward: faces,
	}
	bins := func(demand string, qty int) map[string]int {
		t.Helper()
		res, err := uc.Execute(ctx, sku, mustQty(t, qty), demand)
		if err != nil {
			t.Fatalf("reserve %d: %v", qty, err)
		}
		return drawn(res)
	}

	// Full forward + full reserve: the forward face is consumed first.
	if got := bins("it-fwd-order-1", 3); got["IT-FWD-Z-FACE"] != 3 || len(got) != 1 {
		t.Fatalf("drawn %v, want 3 from the forward face only", got)
	}
	// Partial forward (2 left): the remainder first, then spill to reserve.
	if got := bins("it-fwd-order-2", 6); got["IT-FWD-Z-FACE"] != 2 || got["IT-FWD-A-RESERVE"] != 4 || len(got) != 2 {
		t.Fatalf("drawn %v, want 2 forward + 4 reserve", got)
	}
	// Forward empty: the reserve bin serves the pick.
	if got := bins("it-fwd-order-3", 5); got["IT-FWD-A-RESERVE"] != 5 || len(got) != 1 {
		t.Fatalf("drawn %v, want 5 from reserve", got)
	}

	// A newer plan moves the pick face: the map is replaced wholesale, so the
	// old face is plain reserve and the new one is drawn first.
	if got := approve("it-fwd-ev-2", "it-plan-2", t0.Add(time.Hour), "IT-FWD-A-RESERVE"); got != usecases.SlotPlanApplied {
		t.Fatalf("newer plan outcome = %s", got)
	}
	got, err := faces.ForwardFacesFor(ctx, sku)
	if err != nil {
		t.Fatal(err)
	}
	if got.RoleOf("IT-FWD-A-RESERVE") != allocation.RoleForward || got.RoleOf("IT-FWD-Z-FACE") != allocation.RoleReserve {
		t.Fatalf("roles after supersession: A=%s Z=%s", got.RoleOf("IT-FWD-A-RESERVE"), got.RoleOf("IT-FWD-Z-FACE"))
	}
}
