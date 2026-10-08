// Inventory exceptions: the quarantine record (ADR 0031).
//
// An Exception is what a destination scan becomes when it CANNOT be
// matched to a recognized, ALLOCATED transfer line: the goods are
// physically in the building but the system has no lawful custody fact
// to attach them to. The scan is quarantined — recorded here, exactly
// once per (transfer_line_id, destination) attempt — and NO stock is
// created, NO usable quantity rises, and NO event that could raise
// availability is published. A human resolves the exception later.
//
// This is the fail-closed half of ADR 0031: an unrecognized arrival
// must never silently raise destination availability.
package transfer

import (
	"errors"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ExceptionKind is the closed set of quarantine reasons.
type ExceptionKind string

const (
	// ExceptionUnknownTransfer: no transfer_allocations row exists for
	// the scanned transfer_line_id — this service never allocated
	// anything for that line.
	ExceptionUnknownTransfer ExceptionKind = "UNKNOWN_TRANSFER"
	// ExceptionUnrecognizedTransfer: a ledger row exists but its outcome
	// is not ALLOCATED (it was REJECTED at allocation time), or the
	// scanned transfer/destination does not match the allocated row.
	ExceptionUnrecognizedTransfer ExceptionKind = "UNRECOGNIZED_TRANSFER"
)

var (
	ErrExceptionKindRequired = errors.New("inventory exception requires a kind")
	// ErrExceptionReceiptAfterQuarantine is returned by Quarantine when
	// the same (transfer_line_id, destination_site_id) scan was already
	// quarantined with a DIFFERENT payload: the original quarantine
	// stands, and the caller must refuse rather than overwrite it.
	ErrExceptionReceiptAfterQuarantine = errors.New("this scan was already quarantined with a different payload")
)

// Exception is ONE quarantined destination scan. It is deliberately NOT
// a stock-holding aggregate: creating an exception changes no stock.
type Exception struct {
	transferID        string
	transferLineID    string
	destinationSiteID shared.SiteID
	sku               shared.SKU
	receivedQuantity  shared.Quantity
	kind              ExceptionKind
	detail            string
	createdAt         time.Time
}

// NewException records one quarantined scan with its closed kind.
func NewException(transferID, transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, receivedQty shared.Quantity, kind ExceptionKind, detail string, createdAt time.Time) (*Exception, error) {
	if kind == "" {
		return nil, ErrExceptionKindRequired
	}
	if _, err := shared.NewSiteID(destinationSiteID.String()); err != nil {
		return nil, err
	}
	if transferLineID == "" {
		return nil, errors.New("inventory exception requires a transfer line id")
	}
	if receivedQty.Int() < 0 {
		return nil, shared.ErrNegativeQuantity
	}
	return &Exception{
		transferID:        transferID,
		transferLineID:    transferLineID,
		destinationSiteID: destinationSiteID,
		sku:               sku,
		receivedQuantity:  receivedQty,
		kind:              kind,
		detail:            detail,
		createdAt:         createdAt,
	}, nil
}

// Matches reports whether this quarantine record was written for the
// exact same scan (same line, site, sku and quantity): a repeated
// identical scan is a benign replay of the SAME quarantine, not a new
// exception.
func (e *Exception) Matches(transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, receivedQty shared.Quantity) bool {
	return e.transferLineID == transferLineID &&
		e.destinationSiteID == destinationSiteID &&
		e.sku == sku &&
		e.receivedQuantity == receivedQty
}

// RehydrateException rebuilds an Exception from its persisted row,
// trusting the table's CHECK constraints (closed kind, non-negative
// quantity) the way the other aggregates trust their rows.
func RehydrateException(transferID, transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, receivedQty shared.Quantity, kind ExceptionKind, detail string, createdAt time.Time) *Exception {
	return &Exception{
		transferID:        transferID,
		transferLineID:    transferLineID,
		destinationSiteID: destinationSiteID,
		sku:               sku,
		receivedQuantity:  receivedQty,
		kind:              kind,
		detail:            detail,
		createdAt:         createdAt,
	}
}

func (e *Exception) TransferID() string                { return e.transferID }
func (e *Exception) TransferLineID() string            { return e.transferLineID }
func (e *Exception) DestinationSiteID() shared.SiteID  { return e.destinationSiteID }
func (e *Exception) SKU() shared.SKU                   { return e.sku }
func (e *Exception) ReceivedQuantity() shared.Quantity { return e.receivedQuantity }
func (e *Exception) Kind() ExceptionKind               { return e.kind }
func (e *Exception) Detail() string                    { return e.detail }
func (e *Exception) CreatedAt() time.Time              { return e.createdAt }
