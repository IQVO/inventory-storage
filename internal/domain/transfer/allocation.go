// Package transfer holds the site-scoped transfer allocation decision
// record: the ledger fact that ties one planning transfer line to the
// reservation (or rejection) inventory-storage answered it with. The
// Reservation aggregate still holds the stock; this record makes the
// command/reply exchange replay-safe at the database level.
package transfer

import (
	"errors"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Outcome is the closed set of answers a transfer allocation command can
// settle on.
type Outcome string

const (
	OutcomeAllocated Outcome = "ALLOCATED"
	OutcomeRejected  Outcome = "REJECTED"
)

// Rejection reasons form a closed set (wire-stable; consumers dispatch on
// them). ORIGIN_SITE_UNKNOWN: no usable stock was found in the requested
// origin site's custody (including the case where the site holds none of
// the SKU and the case where the site itself is unknown to this service —
// both mean "this origin cannot donate"). INSUFFICIENT_USABLE: the origin
// site's custody holds the SKU but not enough usable quantity.
// IDEMPOTENCY_CONFLICT: the same transfer_line_id was replayed with a
// DIFFERENT command payload than the one already decided.
type RejectionReason string

const (
	ReasonOriginSiteUnknown   RejectionReason = "ORIGIN_SITE_UNKNOWN"
	ReasonInsufficientUsable  RejectionReason = "INSUFFICIENT_USABLE"
	ReasonIdempotencyConflict RejectionReason = "IDEMPOTENCY_CONFLICT"
)

var (
	ErrNoReservationOnAllocated = errors.New("an allocated transfer allocation must reference the reservation that holds its stock")
	ErrReasonOnAllocated        = errors.New("an allocated transfer allocation cannot carry a rejection reason")
	ErrMissingReasonOnRejected  = errors.New("a rejected transfer allocation must carry a rejection reason")
	ErrReservationOnRejected    = errors.New("a rejected transfer allocation cannot reference a reservation")
)

// Allocation is ONE decided transfer line: what was asked, and what this
// service answered. It is a decision record keyed by transfer_line_id —
// the ledger row — not a second stock-holding aggregate.
type Allocation struct {
	transferID        string
	transferLineID    string
	originSiteID      shared.SiteID
	sku               shared.SKU
	requestedQuantity shared.Quantity
	reservationID     string
	outcome           Outcome
	rejectionReason   RejectionReason
	decidedAt         time.Time
}

// NewAllocated records the decision "origin site's stock was reserved for
// this transfer line" and names the Reservation holding it.
func NewAllocated(transferID, transferLineID string, originSiteID shared.SiteID, sku shared.SKU, requestedQty shared.Quantity, reservationID string, decidedAt time.Time) (*Allocation, error) {
	if reservationID == "" {
		return nil, ErrNoReservationOnAllocated
	}
	return &Allocation{
		transferID:        transferID,
		transferLineID:    transferLineID,
		originSiteID:      originSiteID,
		sku:               sku,
		requestedQuantity: requestedQty,
		reservationID:     reservationID,
		outcome:           OutcomeAllocated,
		decidedAt:         decidedAt,
	}, nil
}

// NewRejected records the decision "no stock was held for this transfer
// line" and the closed reason why.
func NewRejected(transferID, transferLineID string, originSiteID shared.SiteID, sku shared.SKU, requestedQty shared.Quantity, reason RejectionReason, decidedAt time.Time) (*Allocation, error) {
	if reason == "" {
		return nil, ErrMissingReasonOnRejected
	}
	return &Allocation{
		transferID:        transferID,
		transferLineID:    transferLineID,
		originSiteID:      originSiteID,
		sku:               sku,
		requestedQuantity: requestedQty,
		outcome:           OutcomeRejected,
		rejectionReason:   reason,
		decidedAt:         decidedAt,
	}, nil
}

// Rehydrate rebuilds an Allocation from its persisted ledger row. The
// shape invariants (reservation iff allocated, reason iff rejected) are
// enforced by the transfer_allocations table's CHECK constraints; this
// rehydrate trusts them the way the other aggregates trust their rows.
func Rehydrate(transferID, transferLineID string, originSiteID shared.SiteID, sku shared.SKU, requestedQty shared.Quantity, reservationID string, outcome Outcome, reason RejectionReason, decidedAt time.Time) *Allocation {
	return &Allocation{
		transferID:        transferID,
		transferLineID:    transferLineID,
		originSiteID:      originSiteID,
		sku:               sku,
		requestedQuantity: requestedQty,
		reservationID:     reservationID,
		outcome:           outcome,
		rejectionReason:   reason,
		decidedAt:         decidedAt,
	}
}

func (a *Allocation) TransferID() string                 { return a.transferID }
func (a *Allocation) TransferLineID() string             { return a.transferLineID }
func (a *Allocation) OriginSiteID() shared.SiteID        { return a.originSiteID }
func (a *Allocation) SKU() shared.SKU                    { return a.sku }
func (a *Allocation) RequestedQuantity() shared.Quantity { return a.requestedQuantity }
func (a *Allocation) ReservationID() string              { return a.reservationID }
func (a *Allocation) Outcome() Outcome                   { return a.outcome }
func (a *Allocation) RejectionReason() RejectionReason   { return a.rejectionReason }
func (a *Allocation) DecidedAt() time.Time               { return a.decidedAt }

// IsReplayOf reports whether command is byte-identical (in the fields
// that define the decision) to the command this allocation already
// decided. A replay matching everything is the retry case — return the
// original outcome untouched. Any field differing is an idempotency
// conflict: the same line id is being reused for a different request.
func (a *Allocation) IsReplayOf(transferID, transferLineID string, originSiteID shared.SiteID, sku shared.SKU, qty shared.Quantity) bool {
	return a.transferID == transferID &&
		a.originSiteID == originSiteID &&
		a.sku == sku &&
		a.requestedQuantity == qty &&
		a.transferLineID == transferLineID
}
