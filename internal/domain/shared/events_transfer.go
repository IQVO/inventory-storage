package shared

import "time"

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

// TransferStockAllocationReleased: the compensation leg of the transfer
// saga (ADR 0050) — a transfer cancelled after allocation had its origin
// hold resolved. `Outcome` is the closed set RELEASED (the reservation
// still held the stock; this revoked it, quantity back in usable) |
// ALREADY_RESOLVED (the reservation had already expired or been
// confirmed — either way the hold is gone). Both settle the saga as
// success. `Reason` is the command's cancellation reason carried
// through, or IDEMPOTENCY_CONFLICT on a conflicting replay's answer.
type TransferStockAllocationReleased struct {
	base
	TransferID     string
	TransferLineID string
	ReservationID  string
	OriginSiteID   SiteID
	SKU            SKU
	Quantity       Quantity
	Outcome        string
	Reason         string
}

func NewTransferStockAllocationReleased(occurredAt time.Time, transferID, transferLineID, reservationID string, originSiteID SiteID, sku SKU, qty Quantity, outcome, reason string) TransferStockAllocationReleased {
	return TransferStockAllocationReleased{
		base:           newBase("TransferStockAllocationReleased", occurredAt),
		TransferID:     transferID,
		TransferLineID: transferLineID,
		ReservationID:  reservationID,
		OriginSiteID:   originSiteID,
		SKU:            sku,
		Quantity:       qty,
		Outcome:        outcome,
		Reason:         reason,
	}
}

// TransferReceiptStaged: a destination site counted goods against a
// recognized (ALLOCATED) transfer line (ADR 0031, Phase 3 — destination
// receipt custody). No usable stock moves: the destination's usable
// rises only when the stow completes (TransferStockStowed).
type TransferReceiptStaged struct {
	base
	TransferID        string
	TransferLineID    string
	DestinationSiteID SiteID
	SKU               SKU
	ExpectedQuantity  Quantity
	ReceivedQuantity  Quantity
	// Variance is SIGNED (received - expected): positive = over-receipt,
	// negative = short-receipt, 0 = exact. Over/short is explicit on the
	// wire, never silently absorbed.
	Variance int
}

func NewTransferReceiptStaged(occurredAt time.Time, transferID, transferLineID string, destinationSiteID SiteID, sku SKU, expectedQty, receivedQty Quantity, variance int) TransferReceiptStaged {
	return TransferReceiptStaged{
		base:              newBase("TransferReceiptStaged", occurredAt),
		TransferID:        transferID,
		TransferLineID:    transferLineID,
		DestinationSiteID: destinationSiteID,
		SKU:               sku,
		ExpectedQuantity:  expectedQty,
		ReceivedQuantity:  receivedQty,
		Variance:          variance,
	}
}

// TransferStockStowed: the destination site placed a staged transfer
// receipt's goods into destination bins — the ONLY path that raises the
// destination site's usable stock for a transfer (exactly once per
// transfer_line_id, ADR 0031).
type TransferStockStowed struct {
	base
	TransferID        string
	TransferLineID    string
	DestinationSiteID SiteID
	SKU               SKU
	ReceivedQuantity  Quantity
	StowedQuantity    Quantity
	// Allocations are the destination StockUnits the stow created, with
	// the bin each was placed into.
	Allocations []TransferAllocationLeg
}

func NewTransferStockStowed(occurredAt time.Time, transferID, transferLineID string, destinationSiteID SiteID, sku SKU, receivedQty, stowedQty Quantity, allocations []TransferAllocationLeg) TransferStockStowed {
	return TransferStockStowed{
		base:              newBase("TransferStockStowed", occurredAt),
		TransferID:        transferID,
		TransferLineID:    transferLineID,
		DestinationSiteID: destinationSiteID,
		SKU:               sku,
		ReceivedQuantity:  receivedQty,
		StowedQuantity:    stowedQty,
		Allocations:       allocations,
	}
}
