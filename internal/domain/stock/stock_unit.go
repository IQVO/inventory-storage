// Package stock holds the StockUnit aggregate: a quantity of a SKU at a
// specific bin. Every physical item has exactly one known bin, or is
// flagged Unlocated — this is the core rule of the bounded context.
package stock

import (
	"errors"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var (
	// ErrStowRequiresItemAndLocation: a stow is invalid without both an
	// item-scan (SKU) and a location-scan (BinId). Skipping either is
	// precisely how inventory gets lost.
	ErrStowRequiresItemAndLocation = errors.New("stow requires both an item scan (sku) and a location scan (bin)")
	ErrInsufficientUsable          = errors.New("reserve quantity exceeds usable quantity")
	ErrInsufficientReserved        = errors.New("pick quantity exceeds reserved quantity")
	ErrUnitUnlocated               = errors.New("stock unit is unlocated")
)

// StockUnit is the aggregate root binding a SKU to a Bin with a quantity and
// a lifecycle state.
type StockUnit struct {
	id       string
	sku      shared.SKU
	binID    shared.BinId
	quantity shared.Quantity
	reserved shared.Quantity
	state    State
	// version is optimistic-concurrency infrastructure metadata (ADR
	// 0018): inert, unexported, carried by the aggregate but never read
	// or reasoned about by business logic. It exists solely so the repo
	// can round-trip the DB-read version through Rehydrate -> mutate ->
	// Save and detect a lost-update race on Save.
	version int
}

// NewStockUnit stows a quantity of a SKU into a bin. Both the SKU and the
// BinId must be present (item-scan + location-scan) or the stow is rejected.
// A freshly created aggregate always starts at version 1 (see ADR 0019).
func NewStockUnit(id string, sku shared.SKU, binID shared.BinId, qty shared.Quantity) (*StockUnit, error) {
	if sku == "" || binID == "" {
		return nil, ErrStowRequiresItemAndLocation
	}
	if qty.Int() <= 0 {
		return nil, shared.ErrZeroQuantity
	}
	return &StockUnit{
		id:       id,
		sku:      sku,
		binID:    binID,
		quantity: qty,
		reserved: 0,
		state:    StateAvailable,
		version:  1,
	}, nil
}

// RehydrateStockUnit reconstructs a StockUnit from persisted state,
// including the row's current version (ADR 0019) so a later Save can be
// version-guarded against a concurrent modification.
func RehydrateStockUnit(id string, sku shared.SKU, binID shared.BinId, qty, reserved shared.Quantity, state State, version int) *StockUnit {
	return &StockUnit{id: id, sku: sku, binID: binID, quantity: qty, reserved: reserved, state: state, version: version}
}

func (u *StockUnit) ID() string                { return u.id }
func (u *StockUnit) SKU() shared.SKU           { return u.sku }
func (u *StockUnit) BinID() shared.BinId       { return u.binID }
func (u *StockUnit) Quantity() shared.Quantity { return u.quantity }
func (u *StockUnit) Reserved() shared.Quantity { return u.reserved }
func (u *StockUnit) State() State              { return u.state }

// Version reports the optimistic-concurrency version this aggregate was
// loaded at (or 1 for a freshly constructed one). Infrastructure-only —
// no business-logic method reads or mutates this (ADR 0019).
func (u *StockUnit) Version() int { return u.version }

// Usable is the portion of this unit's on-hand quantity not already reserved.
// Unlocated or fully removed units contribute no usable quantity.
func (u *StockUnit) Usable() shared.Quantity {
	if u.state == StateUnlocated || u.state == StateRemoved {
		return 0
	}
	usable, err := u.quantity.Sub(u.reserved)
	if err != nil {
		return 0
	}
	return usable
}

// Reserve binds qty of this unit's usable quantity to demand. It never
// allows reserved quantity to exceed on-hand quantity (no negative usable,
// no double-consume).
func (u *StockUnit) Reserve(qty shared.Quantity) error {
	if u.state == StateUnlocated || u.state == StateRemoved {
		return ErrUnitUnlocated
	}
	if qty.GreaterThan(u.Usable()) {
		return ErrInsufficientUsable
	}
	u.reserved = u.reserved.Add(qty)
	u.state = StateReserved
	return nil
}

// ReleaseReservation returns qty from reserved back to usable, e.g. on
// revoke or expiry.
func (u *StockUnit) ReleaseReservation(qty shared.Quantity) error {
	remaining, err := u.reserved.Sub(qty)
	if err != nil {
		return ErrInsufficientReserved
	}
	u.reserved = remaining
	if u.reserved.Int() == 0 && u.state == StateReserved {
		u.state = StateAvailable
	}
	return nil
}

// Pick physically removes qty from this unit's reserved (and on-hand)
// quantity. The unit transitions to Removed once its quantity reaches zero,
// or Picked if quantity remains.
func (u *StockUnit) Pick(qty shared.Quantity) error {
	remainingReserved, err := u.reserved.Sub(qty)
	if err != nil {
		return ErrInsufficientReserved
	}
	remainingQty, err := u.quantity.Sub(qty)
	if err != nil {
		return ErrInsufficientReserved
	}
	u.reserved = remainingReserved
	u.quantity = remainingQty
	if u.quantity.Int() == 0 {
		u.state = StateRemoved
	} else {
		u.state = StatePicked
	}
	return nil
}

// MarkUnlocated flags this unit as lost: the physical item exists but the
// system no longer knows where.
func (u *StockUnit) MarkUnlocated() {
	u.state = StateUnlocated
}
