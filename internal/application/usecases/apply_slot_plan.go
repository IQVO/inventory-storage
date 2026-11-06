package usecases

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/allocation"
)

// SlotPlanConsumer is the processed_events consumer name under which
// ApplySlotPlan claims CloudEvents ids.
const SlotPlanConsumer = "slotting-slot-plan"

// ErrMalformedSlotPlan marks a SlotPlanApproved that can never be applied. It
// is deterministic: the consumer logs it and commits past the message.
var ErrMalformedSlotPlan = allocation.ErrMalformedSiteMap

// SlotPlanApprovedUpdate is one SlotPlanApproved occurrence as decoded by the
// inbound adapter (wire strings, not yet validated).
type SlotPlanApprovedUpdate struct {
	// EventID is the CloudEvents id, the dedupe key.
	EventID     string
	PlanID      string
	SiteID      string
	ApprovedAt  time.Time
	Assignments []allocation.RawAssignment
}

// SlotPlanOutcome says what ApplySlotPlan did with one update.
type SlotPlanOutcome string

const (
	// SlotPlanApplied: the site's forward map is now this plan's.
	SlotPlanApplied SlotPlanOutcome = "APPLIED"
	// SlotPlanStale: the held plan is the same or later; no write.
	SlotPlanStale SlotPlanOutcome = "STALE"
	// SlotPlanDuplicate: this CloudEvents id was already applied.
	SlotPlanDuplicate SlotPlanOutcome = "DUPLICATE"
)

// ApplySlotPlan keeps this service's local forward-face read model in step
// with slotting-optimization, the owner of the slot plan (ADR 0050). The
// CloudEvents id claim and the replacement of the site's map run in ONE
// UnitOfWork. It raises no domain event: slotting -> inventory-storage is
// one-way and nothing reaches the outbox.
type ApplySlotPlan struct {
	Faces           ports.ForwardFaceStore
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and the replacement. Optional: nil means
	// no transactional backing (unit tests).
	UnitOfWork ports.UnitOfWork
}

// Execute validates u and applies it. It returns an error wrapping
// ErrMalformedSlotPlan for a deterministic problem and the repository error
// unchanged for a transient one.
func (uc *ApplySlotPlan) Execute(ctx context.Context, u SlotPlanApprovedUpdate) (SlotPlanOutcome, error) {
	if u.EventID == "" {
		return "", fmt.Errorf("%w: empty event id", ErrMalformedSlotPlan)
	}
	m, err := allocation.NewSiteMap(u.SiteID, u.PlanID, u.ApprovedAt, u.Assignments)
	if err != nil {
		return "", err
	}
	outcome := SlotPlanDuplicate
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, SlotPlanConsumer, u.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			outcome = SlotPlanDuplicate
			return nil
		}
		applied, err := uc.Faces.ReplaceSiteMap(ctx, m)
		if err != nil {
			return err
		}
		outcome = SlotPlanStale
		if applied {
			outcome = SlotPlanApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}
