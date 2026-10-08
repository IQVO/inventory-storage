// Package reservation holds the Reservation aggregate: a revocable binding
// of a quantity to demand. Physical delivery can fail after allocation, so a
// reservation must be releasable and re-allocatable against a different
// holding — it is never bound permanently to one StockUnit.
package reservation

import (
	"errors"
	"math"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var (
	ErrAlreadyResolved = errors.New("reservation is already resolved (confirmed, revoked, or expired)")
	ErrExpired         = errors.New("reservation has expired")
	ErrNoAllocations   = errors.New("reservation requires at least one allocation")
	ErrInvalidLineNo   = errors.New("reservation line number must be between 1 and 2147483647")
)

// MaxLineNo is the largest valid line number: the line_no columns are 32-bit
// INTEGERs (migration 0035), so anything above would fail at the database.
const MaxLineNo = math.MaxInt32

// ValidLineNo reports whether n is a valid order line number: 1..MaxLineNo.
func ValidLineNo(n int) bool { return n >= 1 && n <= MaxLineNo }

// Status is the lifecycle stage of a Reservation.
type Status string

const (
	StatusActive    Status = "ACTIVE"
	StatusConfirmed Status = "CONFIRMED"
	StatusRevoked   Status = "REVOKED"
	StatusExpired   Status = "EXPIRED"
)

// Allocation records how much of a reservation's quantity was drawn from a
// specific StockUnit, so it can be returned to that same unit on revoke, or
// consumed from it on confirm-pick.
//
// BinID is the pick location: the bin the StockUnit sat in when the
// allocation was made (a StockUnit never changes bin, so this is stable for
// the allocation's lifetime). It is what lets a picker's RF gun show WHERE
// to go. It is empty only for allocations persisted before the field
// existed and that could not be backfilled (see ADR 0025).
type Allocation struct {
	StockUnitID string
	BinID       shared.BinId
	Quantity    shared.Quantity
}

// Reservation is the aggregate root for a revocable claim against usable
// inventory.
type Reservation struct {
	id        string
	sku       shared.SKU
	quantity  shared.Quantity
	demandRef string
	// lineNo is the order line this reservation is for (decision 18, ADR
	// 0036); nil when unknown (a reservation made before the field existed,
	// or by a client that does not send it).
	lineNo      *int
	allocations []Allocation
	status      Status
	createdAt   time.Time
	expiresAt   time.Time
	// version is optimistic-concurrency infrastructure metadata (ADR
	// 0019) — inert, never reasoned about by business logic.
	version int
}

// New creates an Active reservation. Allocations must sum to quantity and
// the caller (the ReserveStock use case) is responsible for having already
// verified quantity <= usable at reserve time. A freshly created
// aggregate always starts at version 1 (see ADR 0019).
func New(id string, sku shared.SKU, quantity shared.Quantity, demandRef string, allocations []Allocation, createdAt time.Time, timeout time.Duration) (*Reservation, error) {
	return NewForLine(id, sku, quantity, demandRef, nil, allocations, createdAt, timeout)
}

// NewForLine is New for a reservation that knows which order line it serves
// (decision 18, ADR 0036). lineNo is optional: nil means unknown; a non-nil
// value must be between 1 and MaxLineNo (ErrInvalidLineNo). The value is copied.
func NewForLine(id string, sku shared.SKU, quantity shared.Quantity, demandRef string, lineNo *int, allocations []Allocation, createdAt time.Time, timeout time.Duration) (*Reservation, error) {
	if lineNo != nil && !ValidLineNo(*lineNo) {
		return nil, ErrInvalidLineNo
	}
	if len(allocations) == 0 {
		return nil, ErrNoAllocations
	}
	return &Reservation{
		id:          id,
		sku:         sku,
		quantity:    quantity,
		demandRef:   demandRef,
		lineNo:      copyLineNo(lineNo),
		allocations: allocations,
		status:      StatusActive,
		createdAt:   createdAt,
		expiresAt:   createdAt.Add(timeout),
		version:     1,
	}, nil
}

// Rehydrate reconstructs a Reservation from persisted state, including the
// row's current optimistic-concurrency version (ADR 0019). The line number
// is unknown; see RehydrateForLine.
func Rehydrate(id string, sku shared.SKU, quantity shared.Quantity, demandRef string, allocations []Allocation, status Status, createdAt, expiresAt time.Time, version int) *Reservation {
	return RehydrateForLine(id, sku, quantity, demandRef, nil, allocations, status, createdAt, expiresAt, version)
}

// RehydrateForLine is Rehydrate for a row that may carry a line number
// (nil for rows written before ADR 0036).
func RehydrateForLine(id string, sku shared.SKU, quantity shared.Quantity, demandRef string, lineNo *int, allocations []Allocation, status Status, createdAt, expiresAt time.Time, version int) *Reservation {
	return &Reservation{
		id: id, sku: sku, quantity: quantity, demandRef: demandRef, lineNo: copyLineNo(lineNo),
		allocations: allocations, status: status, createdAt: createdAt, expiresAt: expiresAt,
		version: version,
	}
}

func copyLineNo(n *int) *int {
	if n == nil {
		return nil
	}
	v := *n
	return &v
}

func (r *Reservation) ID() string                { return r.id }
func (r *Reservation) SKU() shared.SKU           { return r.sku }
func (r *Reservation) Quantity() shared.Quantity { return r.quantity }
func (r *Reservation) DemandRef() string         { return r.demandRef }
func (r *Reservation) Allocations() []Allocation { return r.allocations }

// LineNo is the order line this reservation is for, or nil when unknown.
// The returned pointer is a copy.
func (r *Reservation) LineNo() *int         { return copyLineNo(r.lineNo) }
func (r *Reservation) Status() Status       { return r.status }
func (r *Reservation) CreatedAt() time.Time { return r.createdAt }
func (r *Reservation) ExpiresAt() time.Time { return r.expiresAt }

// Version reports the optimistic-concurrency version this aggregate was
// loaded at (or 1 for a freshly constructed one). Infrastructure-only —
// no business-logic method reads or mutates this (ADR 0019).
func (r *Reservation) Version() int { return r.version }

// IsExpired reports whether now is past this reservation's timeout.
func (r *Reservation) IsExpired(now time.Time) bool {
	return now.After(r.expiresAt)
}

// Revoke cancels the reservation, returning its quantity to usable. It
// cannot be revoked twice, and cannot be revoked once confirmed or expired
// (no double-consume of the resolution).
func (r *Reservation) Revoke() error {
	if r.status != StatusActive {
		return ErrAlreadyResolved
	}
	r.status = StatusRevoked
	return nil
}

// Confirm resolves the reservation into a completed pick. It cannot be
// confirmed twice, nor after it has been revoked or has expired.
func (r *Reservation) Confirm(now time.Time) error {
	if r.status != StatusActive {
		return ErrAlreadyResolved
	}
	if r.IsExpired(now) {
		return ErrExpired
	}
	r.status = StatusConfirmed
	return nil
}

// Expire transitions an unresolved, timed-out reservation to Expired so its
// quantity can be returned to usable.
func (r *Reservation) Expire() error {
	if r.status != StatusActive {
		return ErrAlreadyResolved
	}
	r.status = StatusExpired
	return nil
}
