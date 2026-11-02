package shared

import "time"

// DomainEvent is a past-tense fact published by an aggregate. Adapters
// (outbound/events) serialize and publish these; the domain never depends on
// the publishing mechanism.
type DomainEvent interface {
	EventName() string
	OccurredAt() time.Time
}

type base struct {
	Name string
	At   time.Time
}

func (b base) EventName() string     { return b.Name }
func (b base) OccurredAt() time.Time { return b.At }

func newBase(name string, occurredAt time.Time) base {
	return base{Name: name, At: occurredAt}
}

// StockReceived: goods were received against a SKU and staged, awaiting stow.
type StockReceived struct {
	base
	SKU      SKU
	Quantity Quantity
}

func NewStockReceived(occurredAt time.Time, sku SKU, qty Quantity) StockReceived {
	return StockReceived{base: newBase("StockReceived", occurredAt), SKU: sku, Quantity: qty}
}

// ItemStowed: a quantity of a SKU was placed into a bin (item-scan +
// location-scan both present).
type ItemStowed struct {
	base
	SKU      SKU
	BinID    BinId
	Quantity Quantity
}

func NewItemStowed(occurredAt time.Time, sku SKU, binID BinId, qty Quantity) ItemStowed {
	return ItemStowed{base: newBase("ItemStowed", occurredAt), SKU: sku, BinID: binID, Quantity: qty}
}

// LocationRecorded: the bin now authoritatively holds this StockUnit.
type LocationRecorded struct {
	base
	StockUnitID string
	BinID       BinId
}

func NewLocationRecorded(occurredAt time.Time, stockUnitID string, binID BinId) LocationRecorded {
	return LocationRecorded{base: newBase("LocationRecorded", occurredAt), StockUnitID: stockUnitID, BinID: binID}
}

// StockReserved: a quantity was revocably bound to demand.
type StockReserved struct {
	base
	ReservationID string
	SKU           SKU
	Quantity      Quantity
	DemandRef     string
}

func NewStockReserved(occurredAt time.Time, reservationID string, sku SKU, qty Quantity, demandRef string) StockReserved {
	return StockReserved{base: newBase("StockReserved", occurredAt), ReservationID: reservationID, SKU: sku, Quantity: qty, DemandRef: demandRef}
}

// ReservationExpired: a reservation's timeout elapsed before pick confirmation.
type ReservationExpired struct {
	base
	ReservationID string
}

func NewReservationExpired(occurredAt time.Time, reservationID string) ReservationExpired {
	return ReservationExpired{base: newBase("ReservationExpired", occurredAt), ReservationID: reservationID}
}

// ReservationRevoked: a reservation was cancelled and its quantity returned to usable.
type ReservationRevoked struct {
	base
	ReservationID string
}

func NewReservationRevoked(occurredAt time.Time, reservationID string) ReservationRevoked {
	return ReservationRevoked{base: newBase("ReservationRevoked", occurredAt), ReservationID: reservationID}
}

// StockPicked: reserved quantity was physically removed from its bin(s).
type StockPicked struct {
	base
	ReservationID string
	SKU           SKU
	Quantity      Quantity
}

func NewStockPicked(occurredAt time.Time, reservationID string, sku SKU, qty Quantity) StockPicked {
	return StockPicked{base: newBase("StockPicked", occurredAt), ReservationID: reservationID, SKU: sku, Quantity: qty}
}

// ItemUnlocated: a physical item's bin is no longer known (lost).
type ItemUnlocated struct {
	base
	StockUnitID string
	SKU         SKU
	BinID       BinId
	Quantity    Quantity
}

func NewItemUnlocated(occurredAt time.Time, stockUnitID string, sku SKU, binID BinId, qty Quantity) ItemUnlocated {
	return ItemUnlocated{base: newBase("ItemUnlocated", occurredAt), StockUnitID: stockUnitID, SKU: sku, BinID: binID, Quantity: qty}
}

// CycleCountCompleted: a bin's contents were verified against system records.
type CycleCountCompleted struct {
	base
	BinID       BinId
	CountedQty  Quantity
	SystemQty   Quantity
	Discrepancy bool
}

func NewCycleCountCompleted(occurredAt time.Time, binID BinId, counted, system Quantity, discrepancy bool) CycleCountCompleted {
	return CycleCountCompleted{base: newBase("CycleCountCompleted", occurredAt), BinID: binID, CountedQty: counted, SystemQty: system, Discrepancy: discrepancy}
}

// DiscrepancyDetected: a cycle count found system records did not match reality.
type DiscrepancyDetected struct {
	base
	BinID      BinId
	CountedQty Quantity
	SystemQty  Quantity
}

func NewDiscrepancyDetected(occurredAt time.Time, binID BinId, counted, system Quantity) DiscrepancyDetected {
	return DiscrepancyDetected{base: newBase("DiscrepancyDetected", occurredAt), BinID: binID, CountedQty: counted, SystemQty: system}
}

// StockRelocated: a quantity of one SKU was moved from one bin to another
// bin of the same site (ADR 0039). Reason is the closed relocation reason
// (SLOTTING, CONSOLIDATION, MANUAL); SourceRef is the caller's correlation
// (a slot-plan move, a fulfillment-execution relocation_ref), possibly empty.
type StockRelocated struct {
	base
	RelocationID string
	SiteID       SiteID
	SKU          SKU
	FromBin      BinId
	ToBin        BinId
	Quantity     Quantity
	Reason       string
	SourceRef    string
}

func NewStockRelocated(occurredAt time.Time, relocationID string, siteID SiteID, sku SKU, fromBin, toBin BinId, qty Quantity, reason, sourceRef string) StockRelocated {
	return StockRelocated{
		base:         newBase("StockRelocated", occurredAt),
		RelocationID: relocationID,
		SiteID:       siteID,
		SKU:          sku,
		FromBin:      fromBin,
		ToBin:        toBin,
		Quantity:     qty,
		Reason:       reason,
		SourceRef:    sourceRef,
	}
}

// BinLevelReason is the closed set of causes carried by BinStockLevelChanged
// (ADR 0040). The values are wire-stable; consumers ignore values they do not
// know.
type BinLevelReason string

const (
	// BinLevelStow: stock was placed into the bin (stow, transfer stow).
	BinLevelStow BinLevelReason = "STOW"
	// BinLevelPick: a confirmed pick physically removed stock from the bin.
	BinLevelPick BinLevelReason = "PICK"
	// BinLevelRelocate: an intra-site relocation took stock out of, or put
	// stock into, the bin.
	BinLevelRelocate BinLevelReason = "RELOCATE"
	// BinLevelReplenishment: a relocation whose own reason is REPLENISHMENT
	// (raised by the later pick-face replenishment change; never emitted
	// while no relocation can carry that reason).
	BinLevelReplenishment BinLevelReason = "REPLENISHMENT"
	// BinLevelCount: a cycle count marked part of the bin's stock unlocated.
	BinLevelCount BinLevelReason = "COUNT"
	// BinLevelReserve: stock in the bin was bound to a reservation.
	BinLevelReserve BinLevelReason = "RESERVE"
	// BinLevelRevoke: a revoked or expired reservation released stock in the bin.
	BinLevelRevoke BinLevelReason = "REVOKE"
	// BinLevelUnlocated: reserved on the wire for an unlocated receive; this
	// service has no receive path that names a bin, so it is never emitted.
	BinLevelUnlocated BinLevelReason = "UNLOCATED"
)

// BinStockLevelChanged: the absolute stock level of one SKU in one bin after a
// ledger change (ADR 0040). OnHand is the quantity physically in the bin as
// the ledger sees it (located, still-present units); Reserved is the part of it
// bound to active reservations. Both are levels, never deltas. Version is the
// persisted per-(bin, sku) counter that strictly increases with every event of
// that pair, so a consumer keeps the highest and ignores lower ones.
type BinStockLevelChanged struct {
	base
	SiteID   SiteID
	BinID    BinId
	SKU      SKU
	OnHand   Quantity
	Reserved Quantity
	Version  int64
	Reason   BinLevelReason
	// OwnerRef is empty for fleet-owned stock (the event is then byte-identical
	// to one from before owners existed); otherwise the levels are that
	// owner's share of the (bin, sku) (ADR 0045).
	OwnerRef OwnerRef
}

// NewBinStockLevelChanged builds the event; the caller resolved the levels and
// the version inside the ledger change's transaction.
func NewBinStockLevelChanged(occurredAt time.Time, siteID SiteID, binID BinId, sku SKU, onHand, reserved Quantity, version int64, reason BinLevelReason) BinStockLevelChanged {
	return BinStockLevelChanged{
		base:     newBase("BinStockLevelChanged", occurredAt),
		SiteID:   siteID,
		BinID:    binID,
		SKU:      sku,
		OnHand:   onHand,
		Reserved: reserved,
		Version:  version,
		Reason:   reason,
	}
}

// ForOwner returns the event scoped to owner's stock in the (bin, sku).
func (e BinStockLevelChanged) ForOwner(owner OwnerRef) BinStockLevelChanged {
	e.OwnerRef = owner
	return e
}
