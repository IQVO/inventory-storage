// Destination-side transfer receipt custody (ADR 0031, Phase 3).
//
// A Receipt is the destination warehouse's answer to one transfer line's
// arrival: what the ledger said was coming (expected), what was actually
// counted at the dock (received), and the signed variance between them.
// It is keyed by transfer_line_id — exactly like the origin-side
// transfer_allocations ledger — so a replayed stage command can never
// create a second receipt, and a stow can never run twice.
//
// The Receipt deliberately does NOT raise usable stock. Stock at the
// destination site is created only by the stow step (StowTransferStock),
// which records the StockUnits it created back onto the receipt so an
// idempotent replay can return the ORIGINAL outcome without creating a
// second unit or republishing the stow event.
package transfer

import (
	"errors"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ReceiptState is the closed lifecycle of a destination receipt.
type ReceiptState string

const (
	// ReceiptStaged: the arrival was counted against a recognized
	// (ALLOCATED) transfer line, awaiting stow. No usable stock yet.
	ReceiptStaged ReceiptState = "STAGED"
	// ReceiptStowed: the received quantity was placed into destination
	// bins; the destination site's usable stock has risen exactly once.
	ReceiptStowed ReceiptState = "STOWED"
)

var (
	// ErrReceiptAlreadyStowed is returned by MarkStowed when the receipt
	// has already completed its stow. The caller routes this to the
	// replay path (return the original outcome), never to a second stow.
	ErrReceiptAlreadyStowed = errors.New("transfer receipt is already stowed")
	// ErrStowLegsRequired: a stow with no legs would silently mark the
	// receipt stowed while placing nothing — a custody hole.
	ErrStowLegsRequired = errors.New("a stowed receipt must record at least one stow leg")
	// ErrVarianceOnStow: the recorded variance belongs to the stage step
	// and is immutable; a stow must not rewrite history.
	ErrVarianceOnStow = errors.New("cannot change a receipt's variance at stow time")
)

// StowLeg is one destination StockUnit created by the stow, with the bin
// it was placed into. It mirrors shared.TransferAllocationLeg on the
// origin side (stock_unit_id, bin_id, quantity) so both halves of the
// transfer correlation read the same way.
type StowLeg struct {
	StockUnitID string
	BinID       shared.BinId
	Quantity    shared.Quantity
}

// Receipt is ONE transfer line's destination-side custody record.
type Receipt struct {
	transferID        string
	transferLineID    string
	destinationSiteID shared.SiteID
	sku               shared.SKU
	reservationID     string
	expectedQuantity  shared.Quantity
	receivedQuantity  shared.Quantity
	variance          int
	state             ReceiptState
	stowLegs          []StowLeg
	stagedAt          time.Time
	stowedAt          time.Time
}

// NewStagedReceipt records "this transfer line's goods physically arrived
// at destinationSiteID and receivedQty of them were counted", against a
// ledger row that said expectedQty were coming. The variance is computed
// here, signed (received - expected): positive is over, negative is
// short, zero is exact. An over/short is EXPLICIT — never silently
// absorbed into the stowed quantity.
func NewStagedReceipt(transferID, transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, reservationID string, expectedQty, receivedQty shared.Quantity, stagedAt time.Time) (*Receipt, error) {
	if transferID == "" {
		return nil, errors.New("staged receipt requires a transfer id")
	}
	if transferLineID == "" {
		return nil, errors.New("staged receipt requires a transfer line id")
	}
	if _, err := shared.NewSiteID(destinationSiteID.String()); err != nil {
		return nil, err
	}
	if _, err := shared.NewSKU(sku.String()); err != nil {
		return nil, err
	}
	if reservationID == "" {
		return nil, errors.New("staged receipt requires the allocation's reservation id (the origin-side hold)")
	}
	if expectedQty.Int() <= 0 {
		return nil, shared.ErrZeroQuantity
	}
	if receivedQty.Int() <= 0 {
		return nil, shared.ErrZeroQuantity
	}
	return &Receipt{
		transferID:        transferID,
		transferLineID:    transferLineID,
		destinationSiteID: destinationSiteID,
		sku:               sku,
		reservationID:     reservationID,
		expectedQuantity:  expectedQty,
		receivedQuantity:  receivedQty,
		variance:          receivedQty.Int() - expectedQty.Int(),
		state:             ReceiptStaged,
		stagedAt:          stagedAt,
	}, nil
}

// RehydrateReceipt rebuilds a Receipt from its persisted row. stowLegs
// is the decoded JSONB column payload (nil for a STAGED receipt); the
// shape invariants (stowed iff stowedAt set, variance == received-
// expected) are enforced by the transfer_receipts table's CHECK
// constraints, which this rehydrate trusts the way the other aggregates
// trust their rows.
func RehydrateReceipt(transferID, transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, reservationID string, expectedQty, receivedQty shared.Quantity, variance int, state ReceiptState, stowLegs []StowLeg, stagedAt, stowedAt time.Time) (*Receipt, error) {
	r := &Receipt{
		transferID:        transferID,
		transferLineID:    transferLineID,
		destinationSiteID: destinationSiteID,
		sku:               sku,
		reservationID:     reservationID,
		expectedQuantity:  expectedQty,
		receivedQuantity:  receivedQty,
		variance:          variance,
		state:             state,
		stagedAt:          stagedAt,
		stowedAt:          stowedAt,
		stowLegs:          stowLegs,
	}
	return r, nil
}

// MarkStowed completes the receipt: legs are the destination StockUnits
// the stow created (recorded so a replay can return the original
// outcome), and the receipt's state moves STAGED -> STOWED. The variance
// set at stage time is immutable.
func (r *Receipt) MarkStowed(legs []StowLeg, stowedAt time.Time) error {
	if r.state == ReceiptStowed {
		return ErrReceiptAlreadyStowed
	}
	if len(legs) == 0 {
		return ErrStowLegsRequired
	}
	r.stowLegs = legs
	r.stowedAt = stowedAt
	r.state = ReceiptStowed
	return nil
}

// StowLegsJSON is the persisted form of the stow legs, encoded by the
// REPOSITORY (not the domain — no wire tags live here; see the
// architecture fitness test). snake_case matches the wire shape of the
// TransferStockStowed event's allocations.
func (r *Receipt) StowLegsRaw() []StowLeg { return r.stowLegs }

// IsReplayOf reports whether a stage command is byte-identical (in the
// fields that define the receipt) to the one this receipt already
// answered. A matching replay returns the original receipt untouched;
// any field differing on the SAME transfer_line_id is an idempotency
// conflict the caller must refuse, not a second receipt.
func (r *Receipt) IsReplayOf(transferID string, destinationSiteID shared.SiteID, sku shared.SKU, receivedQty shared.Quantity) bool {
	return r.transferID == transferID &&
		r.destinationSiteID == destinationSiteID &&
		r.sku == sku &&
		r.receivedQuantity == receivedQty
}

func (r *Receipt) TransferID() string                { return r.transferID }
func (r *Receipt) TransferLineID() string            { return r.transferLineID }
func (r *Receipt) DestinationSiteID() shared.SiteID  { return r.destinationSiteID }
func (r *Receipt) SKU() shared.SKU                   { return r.sku }
func (r *Receipt) ReservationID() string             { return r.reservationID }
func (r *Receipt) ExpectedQuantity() shared.Quantity { return r.expectedQuantity }
func (r *Receipt) ReceivedQuantity() shared.Quantity { return r.receivedQuantity }

// Variance is the SIGNED difference received - expected from the stage
// step: positive = over-receipt, negative = short-receipt, 0 = exact.
// It is computed once at stage time and never rewritten.
func (r *Receipt) Variance() int       { return r.variance }
func (r *Receipt) State() ReceiptState { return r.state }
func (r *Receipt) StowLegs() []StowLeg { return r.stowLegs }
func (r *Receipt) StagedAt() time.Time { return r.stagedAt }
func (r *Receipt) StowedAt() time.Time { return r.stowedAt }

// IsOver reports whether the receipt counted MORE than the ledger
// promised (a positive variance).
func (r *Receipt) IsOver() bool { return r.variance > 0 }

// IsShort reports whether the receipt counted FEWER than the ledger
// promised (a negative variance).
func (r *Receipt) IsShort() bool { return r.variance < 0 }
