package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// InboundReceiptConsumerName is the processed_events consumer name under
// which BookInboundReceiptLine claims CloudEvents ids.
const InboundReceiptConsumerName = "inbound-receipt-line"

// Wire values of inbound-receiving's ReceiptLineReceived `condition`
// (its apis/asyncapi.yaml enum).
const (
	InboundConditionGood    = "Good"
	InboundConditionDamaged = "Damaged"
)

// ErrMalformedInboundReceiptLine marks a ReceiptLineReceived that can never be
// booked (no CloudEvents id, empty SKU, quantity <= 0, unknown condition). It
// is deterministic: the consumer logs it and commits past the message, never
// retries it.
var ErrMalformedInboundReceiptLine = errors.New("malformed inbound receipt line")

// errHoldNotWired: a Damaged line arrived but no HoldStock is wired. It is a
// composition error, never a deterministic problem of the message, so it is
// returned (the consumer retries) instead of dropping the units.
var errHoldNotWired = errors.New("held stock is not wired: a Damaged line cannot be dropped")

// InboundReceiptLine is one inbound-receiving ReceiptLineReceived occurrence,
// as decoded by the inbound adapter (wire values, not yet validated).
type InboundReceiptLine struct {
	// EventID is the CloudEvents id, the dedupe key.
	EventID   string
	ReceiptID string
	ASNNumber string
	LineNo    int
	SKU       string
	Quantity  int
	// Condition is "Good" or "Damaged".
	Condition string
	// OwnerRef, PackGTIN, Cases and SSCC are the optional pack provenance of
	// the line (ADR 0045). OwnerRef is recorded on the receipt booking;
	// PackGTIN, Cases and SSCC are provenance only and are NEVER added to the
	// booked quantity: Quantity, in eaches, is the sole authority.
	OwnerRef string
	PackGTIN string
	Cases    int
	SSCC     string
}

// BookOutcome says what BookInboundReceiptLine did with one line.
type BookOutcome string

const (
	// LineBooked: a Good line went through ReceiveStock (StockReceived raised,
	// quantity staged, not yet usable).
	LineBooked BookOutcome = "BOOKED"
	// LineHeld: a Damaged line was booked as HELD stock (ADR 0052): StockHeld
	// raised, never usable, waiting for a disposition.
	LineHeld BookOutcome = "HELD"
	// LineDuplicate: this CloudEvents id was already handled.
	LineDuplicate BookOutcome = "DUPLICATE"
)

// stockReceiver is the existing ReceiveStock use case; satisfied by
// *ReceiveStock, so its rules are not duplicated.
type stockReceiver interface {
	Execute(ctx context.Context, sku shared.SKU, qty shared.Quantity) (StagedReceipt, error)
}

// stockHolder is the HoldStock use case.
type stockHolder interface {
	Execute(ctx context.Context, source stock.HeldSource, reason stock.HoldReason) (*stock.HeldUnit, error)
}

// BookInboundReceiptLine books a received inbound line (ADR 0037, ADR 0052).
// The Good units become staged stock: the same effect as POST /stock-receipts,
// because it runs the very same ReceiveStock use case. The Damaged units
// become HELD stock through HoldStock: owned, visible, never usable.
//
// The CloudEvents id claim and ReceiveStock's (or HoldStock's) save and outbox
// write run in ONE UnitOfWork (their own brackets join it), so a failure rolls
// both back and the redelivery is applied in full.
type BookInboundReceiptLine struct {
	Receive stockReceiver
	// Hold books the Damaged lines as held stock. Required for a Damaged line.
	Hold            stockHolder
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and the receive. Optional: nil means "no
	// transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
	// Metrics counts booked and held units after commit. Optional.
	Metrics ports.InboundReceiptMetrics
	// HeldMetrics adds the held units to inventory.held_units after commit.
	// Optional.
	HeldMetrics ports.HeldStockMetrics
	// Bookings stores the eaches booked per (receipt id, line no), the bound
	// on containers (ADR 0045). Optional with Linker: nil records nothing.
	Bookings ports.ReceiptBookingRepo
	// Linker settles the cartons waiting for this line's booking, in the same
	// transaction. Optional.
	Linker ContainerLinkSettler
}

// Execute validates l and books it. It returns an error wrapping
// ErrMalformedInboundReceiptLine for a deterministic problem, and the
// repository error unchanged for a transient one.
func (uc *BookInboundReceiptLine) Execute(ctx context.Context, l InboundReceiptLine) (BookOutcome, error) {
	sku, qty, owner, err := l.validate()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedInboundReceiptLine, err)
	}

	outcome := LineDuplicate
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, InboundReceiptConsumerName, l.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			outcome = LineDuplicate
			return nil
		}
		if l.Condition == InboundConditionDamaged {
			outcome = LineHeld
			return uc.hold(ctx, l, sku, owner, qty)
		}
		if _, err := uc.Receive.Execute(ctx, sku, qty); err != nil {
			return err
		}
		outcome = LineBooked
		return uc.recordBooking(ctx, l, sku, owner, qty)
	})
	if err != nil {
		return "", err
	}

	uc.record(ctx, outcome, qty)
	return outcome, nil
}

// hold books the Damaged units of l as held stock with reason DAMAGED. The
// units are not staged and not counted as a booking: a held line is not stock
// a container may be linked to.
func (uc *BookInboundReceiptLine) hold(ctx context.Context, l InboundReceiptLine, sku shared.SKU, owner shared.OwnerRef, qty shared.Quantity) error {
	if uc.Hold == nil {
		return errHoldNotWired
	}
	_, err := uc.Hold.Execute(ctx, stock.HeldSource{
		ReceiptID: l.ReceiptID, ASNNumber: l.ASNNumber, LineNo: l.LineNo, SKU: sku, Quantity: qty, OwnerRef: owner, SSCC: l.SSCC,
	}, stock.ReasonDamaged)
	return err
}

// recordBooking stores the booked eaches of a line and settles the cartons
// that were waiting for it. A line without a receipt id or line number (an
// event from before they were mandatory) records nothing.
func (uc *BookInboundReceiptLine) recordBooking(ctx context.Context, l InboundReceiptLine, sku shared.SKU, owner shared.OwnerRef, qty shared.Quantity) error {
	if uc.Bookings == nil || l.ReceiptID == "" || l.LineNo < 1 {
		return nil
	}
	if err := uc.Bookings.AddBooking(ctx, l.ReceiptID, l.LineNo, sku, owner, qty); err != nil {
		return err
	}
	if uc.Linker == nil {
		return nil
	}
	_, err := uc.Linker.Settle(ctx, l.ReceiptID, l.LineNo)
	return err
}

// record counts the units after the transaction committed, so a rolled-back
// attempt never inflates the counter.
func (uc *BookInboundReceiptLine) record(ctx context.Context, outcome BookOutcome, qty shared.Quantity) {
	switch outcome {
	case LineBooked:
		if uc.Metrics != nil {
			uc.Metrics.InboundReceiptUnits(ctx, ports.InboundReceiptOutcomeBooked, qty.Int())
		}
	case LineHeld:
		if uc.Metrics != nil {
			uc.Metrics.InboundReceiptUnits(ctx, ports.InboundReceiptOutcomeHeld, qty.Int())
		}
		if uc.HeldMetrics != nil {
			uc.HeldMetrics.HeldUnits(ctx, string(stock.ReasonDamaged), qty.Int())
		}
	case LineDuplicate:
		// A redelivery: already counted when first handled.
	}
}

// validate checks the wire fields through the domain constructors, so only a
// valid (sku, quantity, condition) ever reaches ReceiveStock or HoldStock.
func (l InboundReceiptLine) validate() (shared.SKU, shared.Quantity, shared.OwnerRef, error) {
	if l.EventID == "" {
		return "", 0, "", errors.New("empty event id")
	}
	sku, err := shared.NewSKU(l.SKU)
	if err != nil {
		return "", 0, "", err
	}
	qty, err := shared.NewPositiveQuantity(l.Quantity)
	if err != nil {
		return "", 0, "", err
	}
	if l.Condition != InboundConditionGood && l.Condition != InboundConditionDamaged {
		return "", 0, "", fmt.Errorf("unknown condition %q", l.Condition)
	}
	owner, err := shared.NewOwnerRef(l.OwnerRef)
	if err != nil {
		return "", 0, "", err
	}
	if l.Condition == InboundConditionDamaged && (l.ReceiptID == "" || l.LineNo < 1) {
		return "", 0, "", errors.New("a Damaged line needs its receipt id and line number: a hold names its source")
	}
	return sku, qty, owner, nil
}
