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
	// ErrSiteCustodyRequired: a site-scoped operation (a transfer
	// allocation) was attempted against a unit with no recorded site
	// custody. Site custody is a validated fact, never a guess.
	ErrSiteCustodyRequired  = errors.New("stock unit has no recorded site custody")
	ErrInsufficientUsable   = errors.New("reserve quantity exceeds usable quantity")
	ErrInsufficientReserved = errors.New("pick quantity exceeds reserved quantity")
	ErrUnitUnlocated        = errors.New("stock unit is unlocated")
	// ErrNotSealedCase: a whole-case operation was attempted on loose stock.
	ErrNotSealedCase = errors.New("stock unit is not a sealed case")
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
	// siteID is the custody fact: the warehouse site this unit physically
	// sits at. It is empty only for rows persisted before site custody
	// existed (rehydrated legacy units); such units remain allocatable for
	// ordinary demand but are NEVER allocatable for site-scoped transfers.
	siteID shared.SiteID
	// ownerRef segregates stock by owner (ADR 0045). Empty = fleet-owned,
	// which is every row that predates owners.
	ownerRef shared.OwnerRef
	// packState is LOOSE or SEALED_CASE; the zero value reads as LOOSE.
	packState PackState
	// version is optimistic-concurrency infrastructure metadata (ADR
	// 0019): inert, unexported, carried by the aggregate but never read
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

// NewStockUnitAtSite stows a quantity of a SKU into a bin AT a named
// warehouse site, recording the custody fact. The site must itself be a
// valid, non-empty SiteID — site custody is a validated fact, never a
// defaulted one, so the wrong-facility-donates-stock failure mode is
// unrepresentable on the creation path.
func NewStockUnitAtSite(id string, sku shared.SKU, binID shared.BinId, qty shared.Quantity, siteID shared.SiteID) (*StockUnit, error) {
	if _, err := shared.NewSiteID(siteID.String()); err != nil {
		return nil, err
	}
	unit, err := NewStockUnit(id, sku, binID, qty)
	if err != nil {
		return nil, err
	}
	unit.siteID = siteID
	return unit, nil
}

// RehydrateStockUnit reconstructs a StockUnit from persisted state,
// including the row's current version (ADR 0019) so a later Save can be
// version-guarded against a concurrent modification. The rehydrated unit
// carries NO site custody (empty SiteID): rows persisted before the site
// column existed hydrate exactly like this.
func RehydrateStockUnit(id string, sku shared.SKU, binID shared.BinId, qty, reserved shared.Quantity, state State, version int) *StockUnit {
	return &StockUnit{id: id, sku: sku, binID: binID, quantity: qty, reserved: reserved, state: state, version: version}
}

// RehydrateStockUnitAtSite is RehydrateStockUnit with the site custody
// fact read back from the row's site_id column (NULL for legacy rows,
// which keep using RehydrateStockUnit).
func RehydrateStockUnitAtSite(id string, sku shared.SKU, binID shared.BinId, qty, reserved shared.Quantity, state State, siteID shared.SiteID, version int) *StockUnit {
	return &StockUnit{id: id, sku: sku, binID: binID, quantity: qty, reserved: reserved, state: state, siteID: siteID, version: version}
}

// NewLooseUnit stows loose eaches of sku for owner into binID at siteID.
// owner may be empty (fleet-owned).
func NewLooseUnit(id string, sku shared.SKU, binID shared.BinId, qty shared.Quantity, siteID shared.SiteID, owner shared.OwnerRef) (*StockUnit, error) {
	unit, err := NewStockUnitAtSite(id, sku, binID, qty, siteID)
	if err != nil {
		return nil, err
	}
	unit.ownerRef = owner
	return unit, nil
}

// NewSealedCaseUnit creates the unit that stands for one sealed case stowed
// in binID (ADR 0045): its quantity is the case's eaches and it is never
// split by a quantity reservation or a relocation.
func NewSealedCaseUnit(id string, sku shared.SKU, binID shared.BinId, qty shared.Quantity, siteID shared.SiteID, owner shared.OwnerRef) (*StockUnit, error) {
	unit, err := NewLooseUnit(id, sku, binID, qty, siteID, owner)
	if err != nil {
		return nil, err
	}
	unit.packState = PackSealedCase
	return unit, nil
}

// WithOwnership returns u carrying the persisted owner and pack state. It is
// used by repositories rehydrating a row; the empty owner is fleet-owned and
// the empty pack state is LOOSE.
func (u *StockUnit) WithOwnership(owner shared.OwnerRef, pack PackState) *StockUnit {
	u.ownerRef = owner
	u.packState = pack
	return u
}

func (u *StockUnit) ID() string                { return u.id }
func (u *StockUnit) SKU() shared.SKU           { return u.sku }
func (u *StockUnit) BinID() shared.BinId       { return u.binID }
func (u *StockUnit) Quantity() shared.Quantity { return u.quantity }
func (u *StockUnit) Reserved() shared.Quantity { return u.reserved }
func (u *StockUnit) State() State              { return u.state }

// OwnerRef is the owner this unit's stock belongs to; empty = fleet-owned.
func (u *StockUnit) OwnerRef() shared.OwnerRef { return u.ownerRef }

// PackState is LOOSE or SEALED_CASE.
func (u *StockUnit) PackState() PackState {
	if u.packState == PackSealedCase {
		return PackSealedCase
	}
	return PackLoose
}

// IsLoose reports whether the unit holds loose eaches.
func (u *StockUnit) IsLoose() bool { return u.PackState() == PackLoose }

// ReservableBy reports whether a quantity reservation for owner may draw on
// this unit: same owner (an absent owner matches fleet stock only, never any
// owner) and loose eaches (a sealed case is only reserved whole).
func (u *StockUnit) ReservableBy(owner shared.OwnerRef) bool {
	return u.ownerRef == owner && u.IsLoose()
}

// SiteID reports this unit's site custody. Empty means "not recorded"
// (a legacy row persisted before site custody existed).
func (u *StockUnit) SiteID() shared.SiteID { return u.siteID }

// IsAtSite reports whether this unit is in site's custody. A unit with
// no recorded site (legacy) is at NO site — the wrong-facility failure
// mode fails closed.
func (u *StockUnit) IsAtSite(site shared.SiteID) bool {
	if site == "" {
		return false
	}
	return u.siteID == site
}

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

// ReserveFor is Reserve for a reservation bound to owner. A unit of another
// owner is rejected with ErrOwnerMismatch: a reservation never spans owners.
func (u *StockUnit) ReserveFor(owner shared.OwnerRef, qty shared.Quantity) error {
	if u.ownerRef != owner {
		return shared.ErrOwnerMismatch
	}
	return u.Reserve(qty)
}

// ReserveWhole binds the entire quantity of a sealed-case unit to demand. It
// fails with ErrInsufficientUsable when any part is already reserved, so a
// case is never partially allocated.
func (u *StockUnit) ReserveWhole(owner shared.OwnerRef) error {
	if u.PackState() != PackSealedCase {
		return ErrNotSealedCase
	}
	if u.Usable() != u.quantity || u.quantity.Int() == 0 {
		return ErrInsufficientUsable
	}
	return u.ReserveFor(owner, u.quantity)
}

// AddQuantity puts qty more eaches into a present loose unit (the loose side
// of a case break). A removed or unlocated unit cannot take stock.
func (u *StockUnit) AddQuantity(qty shared.Quantity) error {
	if qty.Int() <= 0 {
		return shared.ErrZeroQuantity
	}
	if u.state == StateUnlocated || u.state == StateRemoved {
		return ErrUnitUnlocated
	}
	u.quantity = u.quantity.Add(qty)
	return nil
}

// MoveTo re-locates a whole sealed-case unit into binID at siteID (a
// re-stow of its container). Reserved quantity never moves: the reservation
// names this unit's current pick location.
func (u *StockUnit) MoveTo(binID shared.BinId, siteID shared.SiteID) error {
	if u.reserved.Int() > 0 {
		return ErrInsufficientUsable
	}
	if u.state == StateUnlocated || u.state == StateRemoved {
		return ErrUnitUnlocated
	}
	u.binID = binID
	u.siteID = siteID
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

// RelocateOut takes qty of this unit's USABLE quantity out of its bin for
// an intra-site relocation (ADR 0039). Reserved quantity never moves: a
// reservation's allocation names this unit and its bin as the pick
// location, so moving it would send the picker to the wrong slot. The
// unit transitions to Removed once its quantity reaches zero (only
// possible when nothing is reserved); otherwise its state is unchanged.
func (u *StockUnit) RelocateOut(qty shared.Quantity) error {
	if qty.Int() <= 0 {
		return shared.ErrZeroQuantity
	}
	if u.state == StateUnlocated || u.state == StateRemoved {
		return ErrUnitUnlocated
	}
	if qty.GreaterThan(u.Usable()) {
		return ErrInsufficientUsable
	}
	// qty <= Usable() <= quantity, so this can never go negative.
	u.quantity -= qty
	if u.quantity.Int() == 0 {
		u.state = StateRemoved
	}
	return nil
}

// MarkUnlocated flags this unit as lost: the physical item exists but the
// system no longer knows where.
func (u *StockUnit) MarkUnlocated() {
	u.state = StateUnlocated
}
