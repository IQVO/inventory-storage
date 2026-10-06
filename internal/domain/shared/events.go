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

// TransferStockAllocated: origin-site stock was reserved for a network
// transfer line (command/reply leg of the transfer saga — the Reservation
// aggregate holds the stock; this event carries the transfer correlation).
type TransferStockAllocated struct {
	base
	TransferID     string
	TransferLineID string
	OriginSiteID   SiteID
	ReservationID  string
	SKU            SKU
	Quantity       Quantity
	Allocations    []TransferAllocationLeg
	ExpiresAt      time.Time
}

// TransferAllocationLeg is one stock unit's contribution to a transfer
// allocation, with its pick location.
type TransferAllocationLeg struct {
	StockUnitID string
	BinID       BinId
	Quantity    Quantity
}

func NewTransferStockAllocated(occurredAt time.Time, transferID, transferLineID string, originSiteID SiteID, reservationID string, sku SKU, qty Quantity, legs []TransferAllocationLeg, expiresAt time.Time) TransferStockAllocated {
	return TransferStockAllocated{
		base:           newBase("TransferStockAllocated", occurredAt),
		TransferID:     transferID,
		TransferLineID: transferLineID,
		OriginSiteID:   originSiteID,
		ReservationID:  reservationID,
		SKU:            sku,
		Quantity:       qty,
		Allocations:    legs,
		ExpiresAt:      expiresAt,
	}
}

// TransferStockAllocationRejected: no stock was held for a network
// transfer line, and the closed reason says why.
type TransferStockAllocationRejected struct {
	base
	TransferID        string
	TransferLineID    string
	OriginSiteID      SiteID
	SKU               SKU
	RequestedQuantity Quantity
	Reason            string
}

func NewTransferStockAllocationRejected(occurredAt time.Time, transferID, transferLineID string, originSiteID SiteID, sku SKU, requestedQty Quantity, reason string) TransferStockAllocationRejected {
	return TransferStockAllocationRejected{
		base:              newBase("TransferStockAllocationRejected", occurredAt),
		TransferID:        transferID,
		TransferLineID:    transferLineID,
		OriginSiteID:      originSiteID,
		SKU:               sku,
		RequestedQuantity: requestedQty,
		Reason:            reason,
	}
}
