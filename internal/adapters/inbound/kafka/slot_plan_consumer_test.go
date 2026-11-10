package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

const typeSlotPlanApprovedForTest = "com.warehouse.wms.slotting-optimization.slotplan.SlotPlanApproved"

type slotPlanRecorder struct {
	got []usecases.SlotPlanApprovedUpdate
	err error
}

func (r *slotPlanRecorder) Execute(_ context.Context, u usecases.SlotPlanApprovedUpdate) (usecases.SlotPlanOutcome, error) {
	r.got = append(r.got, u)
	return usecases.SlotPlanApplied, r.err
}

func TestSlotPlanConsumer_ApprovedIsApplied(t *testing.T) {
	rec := &slotPlanRecorder{}
	c := &SlotPlanConsumer{Apply: rec, Logger: quietLogger()}
	payload := map[string]any{
		"plan_id": "plan-1", "site_id": "WH1", "approved_at": "2026-11-01T10:00:00Z",
		"assignments": []map[string]any{
			{"sku": "SKU-1", "slot": "WH1-FWD-A01-01-01-A"},
			{"sku": "SKU-2", "slot": "WH1-FWD-A01-01-02-A", "pick_unit": "CASE", "cases_capacity": 4},
		},
		"moves": []map[string]any{{"sku": "SKU-1", "to_slot": "WH1-FWD-A01-01-01-A", "kind": "Assign"}},
	}
	if err := c.Handle(context.Background(), kafkaMsg(productMasterCE(t, "ev-1", typeSlotPlanApprovedForTest, payload))); err != nil {
		t.Fatal(err)
	}
	if len(rec.got) != 1 {
		t.Fatalf("applied %d updates, want 1", len(rec.got))
	}
	u := rec.got[0]
	if u.EventID != "ev-1" || u.PlanID != "plan-1" || u.SiteID != "WH1" ||
		!u.ApprovedAt.Equal(time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)) ||
		len(u.Assignments) != 2 || u.Assignments[0].SKU != "SKU-1" || u.Assignments[0].Slot != "WH1-FWD-A01-01-01-A" {
		t.Fatalf("update = %+v", u)
	}
}

func TestSlotPlanConsumer_OtherTypesAreCommittedPast(t *testing.T) {
	rec := &slotPlanRecorder{}
	c := &SlotPlanConsumer{Apply: rec, Logger: quietLogger()}
	other := "com.warehouse.wms.slotting-optimization.slotplan.SlotPlanRejected"
	if err := c.Handle(context.Background(), kafkaMsg(productMasterCE(t, "ev-2", other, map[string]any{"plan_id": "p"}))); err != nil {
		t.Fatal(err)
	}
	if len(rec.got) != 0 {
		t.Fatalf("a non-approved type was applied: %+v", rec.got)
	}
}

func TestSlotPlanConsumer_MalformedPlanIsSkippedNotRetried(t *testing.T) {
	rec := &slotPlanRecorder{err: usecases.ErrMalformedSlotPlan}
	c := &SlotPlanConsumer{Apply: rec, Logger: quietLogger()}
	payload := map[string]any{"plan_id": "", "site_id": "WH1", "approved_at": "2026-11-01T10:00:00Z", "assignments": []map[string]any{}}
	if err := c.Handle(context.Background(), kafkaMsg(productMasterCE(t, "ev-3", typeSlotPlanApprovedForTest, payload))); err != nil {
		t.Fatalf("a deterministic poison must be committed past, got %v", err)
	}
}

func TestSlotPlanConsumer_TransientFailureIsReturnedForRetry(t *testing.T) {
	boom := errors.New("db down")
	rec := &slotPlanRecorder{err: boom}
	c := &SlotPlanConsumer{Apply: rec, Logger: quietLogger()}
	payload := map[string]any{"plan_id": "p", "site_id": "WH1", "approved_at": "2026-11-01T10:00:00Z", "assignments": []map[string]any{}}
	if err := c.Handle(context.Background(), kafkaMsg(productMasterCE(t, "ev-4", typeSlotPlanApprovedForTest, payload))); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the transient error", err)
	}
}
