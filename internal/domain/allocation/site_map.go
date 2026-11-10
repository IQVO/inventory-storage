package allocation

import (
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ErrMalformedSiteMap marks a forward-slot map that can never be applied
// (missing plan id or site, invalid SKU or bin). It is deterministic: the
// consumer logs it and commits past the message.
var ErrMalformedSiteMap = errors.New("malformed forward-slot map")

// Assignment is one SKU -> forward pick face pair of an approved slot plan.
type Assignment struct {
	SKU shared.SKU
	Bin shared.BinId
}

// SiteMap is the FULL forward-slot map of one site as of one approved slot
// plan (slotting-optimization's SlotPlanApproved). It replaces the site's
// previous map wholesale; a SKU absent from it has no forward pick face.
type SiteMap struct {
	SiteID      string
	PlanID      string
	ApprovedAt  time.Time
	Assignments []Assignment
}

// NewSiteMap validates the wire strings of one SlotPlanApproved into a SiteMap.
func NewSiteMap(siteID, planID string, approvedAt time.Time, assignments []RawAssignment) (SiteMap, error) {
	if siteID == "" {
		return SiteMap{}, fmt.Errorf("%w: empty site id", ErrMalformedSiteMap)
	}
	if planID == "" {
		return SiteMap{}, fmt.Errorf("%w: empty plan id", ErrMalformedSiteMap)
	}
	if approvedAt.IsZero() {
		return SiteMap{}, fmt.Errorf("%w: no approved_at", ErrMalformedSiteMap)
	}
	out := make([]Assignment, 0, len(assignments))
	for _, a := range assignments {
		sku, err := shared.NewSKU(a.SKU)
		if err != nil {
			return SiteMap{}, fmt.Errorf("%w: sku %q: %w", ErrMalformedSiteMap, a.SKU, err)
		}
		if a.Slot == "" {
			return SiteMap{}, fmt.Errorf("%w: sku %q has an empty slot", ErrMalformedSiteMap, a.SKU)
		}
		out = append(out, Assignment{SKU: sku, Bin: shared.BinId(a.Slot)})
	}
	return SiteMap{SiteID: siteID, PlanID: planID, ApprovedAt: approvedAt.UTC().Truncate(time.Microsecond), Assignments: out}, nil
}

// RawAssignment is an assignment as decoded from the wire, not yet validated.
type RawAssignment struct {
	SKU  string
	Slot string
}

// Supersedes reports whether m should replace the map currently held for its
// site, which was approved at heldAt by heldPlan: strictly later approval
// wins; on an equal instant the higher plan id wins, so two consumers (or a
// redelivery storm) converge on the same map whatever the delivery order.
func (m SiteMap) Supersedes(heldPlan string, heldAt time.Time) bool {
	if m.ApprovedAt.After(heldAt) {
		return true
	}
	return m.ApprovedAt.Equal(heldAt) && m.PlanID > heldPlan
}
