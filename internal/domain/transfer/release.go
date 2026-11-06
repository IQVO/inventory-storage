package transfer

import (
	"errors"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ReleaseOutcome is the closed set of answers a transfer allocation
// release command can settle on (wire-stable; the planning saga settles
// on both as success — either way the hold is gone).
type ReleaseOutcome string

const (
	// ReleaseOutcomeReleased: the reservation still held the stock and
	// this command revoked it — the quantity is back in usable.
	ReleaseOutcomeReleased ReleaseOutcome = "RELEASED"
	// ReleaseOutcomeAlreadyResolved: the reservation had already been
	// resolved (expired — lazily — or confirmed/revoked) before this
	// command arrived, so there was nothing left to release. Also the
	// answer for a line whose allocation was REJECTED (no hold ever
	// existed). The hold is gone either way.
	ReleaseOutcomeAlreadyResolved ReleaseOutcome = "ALREADY_RESOLVED"
)

// ErrInvalidReleaseOutcome marks a release decision recorded with an
// outcome outside the closed set.
var ErrInvalidReleaseOutcome = errors.New("transfer allocation release outcome must be RELEASED or ALREADY_RESOLVED")

// Release is ONE decided transfer allocation release: what was asked,
// and what this service answered. It is a decision record keyed by
// transfer_line_id — the release ledger row, the mirror of Allocation —
// not a second stock-holding aggregate. The Reservation aggregate still
// holds (or held) the stock; this record makes the release command/reply
// exchange replay-safe at the database level.
type Release struct {
	transferID     string
	transferLineID string
	reservationID  string
	originSiteID   shared.SiteID
	sku            shared.SKU
	quantity       shared.Quantity
	outcome        ReleaseOutcome
	reason         string
	decidedAt      time.Time
}

// NewRelease records the decision for one transfer allocation release
// command. outcome must be in the closed set; reason is the command's
// cancellation reason carried through (or IDEMPOTENCY_CONFLICT on a
// conflicting replay's answer).
func NewRelease(transferID, transferLineID, reservationID string, originSiteID shared.SiteID, sku shared.SKU, qty shared.Quantity, outcome ReleaseOutcome, reason string, decidedAt time.Time) (*Release, error) {
	if outcome != ReleaseOutcomeReleased && outcome != ReleaseOutcomeAlreadyResolved {
		return nil, ErrInvalidReleaseOutcome
	}
	return &Release{
		transferID:     transferID,
		transferLineID: transferLineID,
		reservationID:  reservationID,
		originSiteID:   originSiteID,
		sku:            sku,
		quantity:       qty,
		outcome:        outcome,
		reason:         reason,
		decidedAt:      decidedAt,
	}, nil
}

// RehydrateRelease rebuilds a Release from its persisted ledger row. The
// outcome constraint is enforced by the transfer_allocation_releases
// table's CHECK; this rehydrate trusts it the way the other aggregates
// trust their rows.
func RehydrateRelease(transferID, transferLineID, reservationID string, originSiteID shared.SiteID, sku shared.SKU, qty shared.Quantity, outcome ReleaseOutcome, reason string, decidedAt time.Time) *Release {
	return &Release{
		transferID:     transferID,
		transferLineID: transferLineID,
		reservationID:  reservationID,
		originSiteID:   originSiteID,
		sku:            sku,
		quantity:       qty,
		outcome:        outcome,
		reason:         reason,
		decidedAt:      decidedAt,
	}
}

func (r *Release) TransferID() string          { return r.transferID }
func (r *Release) TransferLineID() string      { return r.transferLineID }
func (r *Release) ReservationID() string       { return r.reservationID }
func (r *Release) OriginSiteID() shared.SiteID { return r.originSiteID }
func (r *Release) SKU() shared.SKU             { return r.sku }
func (r *Release) Quantity() shared.Quantity   { return r.quantity }
func (r *Release) Outcome() ReleaseOutcome     { return r.outcome }
func (r *Release) Reason() string              { return r.reason }
func (r *Release) DecidedAt() time.Time        { return r.decidedAt }

// IsReplayOf reports whether command is byte-identical (in the fields
// that define the decision) to the release command this ledger row
// already decided. A replay matching everything is the retry case —
// return the original outcome untouched. Any field differing is an
// idempotency conflict: the same line id is being reused for a different
// release request.
func (r *Release) IsReplayOf(transferID, transferLineID, reservationID string, originSiteID shared.SiteID, sku shared.SKU, qty shared.Quantity, reason string) bool {
	return r.transferID == transferID &&
		r.transferLineID == transferLineID &&
		r.reservationID == reservationID &&
		r.originSiteID == originSiteID &&
		r.sku == sku &&
		r.quantity == qty &&
		r.reason == reason
}
