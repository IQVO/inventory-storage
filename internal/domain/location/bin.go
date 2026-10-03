// Package location holds the Bin aggregate: a coded slot in chaotic storage.
// Any SKU may occupy any free bin, but the sum of stock held in a bin must
// never exceed its capacity.
package location

import (
	"errors"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var (
	ErrBinFull                 = errors.New("bin is full: capacity exceeded")
	ErrInvalidCapacity         = errors.New("bin capacity must be greater than zero")
	ErrReleaseExceedsOccupancy = errors.New("cannot release more than is occupied")
	// ErrCapacityBelowOccupancy is returned by Resize when the requested
	// capacity is smaller than what the bin already holds: shrinking a bin
	// below its occupancy would break the sum(stock in bin) <= capacity
	// invariant for stock that is physically already there.
	ErrCapacityBelowOccupancy = errors.New("bin capacity cannot be set below current occupancy")
)

// Bin is the aggregate root for a coded storage slot.
type Bin struct {
	id       shared.BinId
	capacity shared.Quantity
	occupied shared.Quantity
	// version is optimistic-concurrency infrastructure metadata (ADR
	// 0018) — inert, never reasoned about by business logic.
	version int
}

// NewBin constructs an empty Bin with the given capacity. A freshly
// created aggregate always starts at version 1 (see ADR 0018).
func NewBin(id shared.BinId, capacity shared.Quantity) (*Bin, error) {
	if id == "" {
		return nil, shared.ErrEmptyBinID
	}
	if capacity.Int() <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Bin{id: id, capacity: capacity, occupied: 0, version: 1}, nil
}

// RehydrateBin reconstructs a Bin from persisted state without re-running
// creation invariants (used by repositories). version is the row's
// current optimistic-concurrency version (ADR 0018).
func RehydrateBin(id shared.BinId, capacity, occupied shared.Quantity, version int) *Bin {
	return &Bin{id: id, capacity: capacity, occupied: occupied, version: version}
}

func (b *Bin) ID() shared.BinId          { return b.id }
func (b *Bin) Capacity() shared.Quantity { return b.capacity }
func (b *Bin) Occupied() shared.Quantity { return b.occupied }

// Version reports the optimistic-concurrency version this aggregate was
// loaded at (or 1 for a freshly constructed one). Infrastructure-only —
// no business-logic method reads or mutates this (ADR 0018).
func (b *Bin) Version() int { return b.version }
func (b *Bin) Available() shared.Quantity {
	avail, _ := b.capacity.Sub(b.occupied)
	return avail
}
func (b *Bin) IsFull() bool { return b.occupied.GreaterThan(b.capacity) || b.occupied == b.capacity }

// Occupy reserves physical space in the bin for a stow. A full (or
// over-capacity) bin rejects the stow.
func (b *Bin) Occupy(qty shared.Quantity) error {
	if qty.Int() <= 0 {
		return shared.ErrZeroQuantity
	}
	if b.occupied.Add(qty).GreaterThan(b.capacity) {
		return ErrBinFull
	}
	b.occupied = b.occupied.Add(qty)
	return nil
}

// Release frees physical space in the bin, e.g. when stock is picked out of it.
func (b *Bin) Release(qty shared.Quantity) error {
	if qty.Int() <= 0 {
		return shared.ErrZeroQuantity
	}
	remaining, err := b.occupied.Sub(qty)
	if err != nil {
		return ErrReleaseExceedsOccupancy
	}
	b.occupied = remaining
	return nil
}

// Resize changes the bin's capacity (e.g. inventory control re-registering
// a slot after a physical re-rack). The new capacity must be greater than
// zero and must not drop below what the bin already holds — a resize never
// strands stock that is physically in the slot. Resizing exactly to the
// current occupancy is allowed (the bin simply becomes full).
func (b *Bin) Resize(capacity shared.Quantity) error {
	if capacity.Int() <= 0 {
		return ErrInvalidCapacity
	}
	if b.occupied.GreaterThan(capacity) {
		return ErrCapacityBelowOccupancy
	}
	b.capacity = capacity
	return nil
}
