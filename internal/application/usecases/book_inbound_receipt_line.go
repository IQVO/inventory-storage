package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
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
}

// BookOutcome says what BookInboundReceiptLine did with one line.
type BookOutcome string

const (
	// LineBooked: a Good line went through ReceiveStock (StockReceived raised,
	// quantity staged, not yet usable).
	LineBooked BookOutcome = "BOOKED"
	// LineDamagedNotBooked: a Damaged line; v1 does not book it.
	LineDamagedNotBooked BookOutcome = "DAMAGED_NOT_BOOKED"
	// LineDuplicate: this CloudEvents id was already handled.
	LineDuplicate BookOutcome = "DUPLICATE"
)

// stockReceiver is the existing ReceiveStock use case; satisfied by
// *ReceiveStock, so its rules are not duplicated.
type stockReceiver interface {
	Execute(ctx context.Context, sku shared.SKU, qty shared.Quantity) (StagedReceipt, error)
}

// BookInboundReceiptLine books the Good units of a received inbound line as
// staged stock (ADR 0037): the same effect as POST /stock/receive, because it
// runs the very same ReceiveStock use case. Damaged units are acknowledged but
// not booked.
//
// The CloudEvents id claim and ReceiveStock's outbox write run in ONE
// UnitOfWork (ReceiveStock's own bracket joins it), so a failure rolls both
// back and the redelivery is applied in full.
type BookInboundReceiptLine struct {
	Receive         stockReceiver
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and the receive. Optional: nil means "no
	// transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
	// Metrics counts booked and damaged units after commit. Optional.
	Metrics ports.InboundReceiptMetrics
}

// Execute validates l and books it. It returns an error wrapping
// ErrMalformedInboundReceiptLine for a deterministic problem, and the
// repository error unchanged for a transient one.
func (uc *BookInboundReceiptLine) Execute(ctx context.Context, l InboundReceiptLine) (BookOutcome, error) {
	sku, qty, err := l.validate()
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
			outcome = LineDamagedNotBooked
			return nil
		}
		if _, err := uc.Receive.Execute(ctx, sku, qty); err != nil {
			return err
		}
		outcome = LineBooked
		return nil
	})
	if err != nil {
		return "", err
	}

	uc.record(ctx, outcome, qty)
	return outcome, nil
}

// record counts the units after the transaction committed, so a rolled-back
// attempt never inflates the counter.
func (uc *BookInboundReceiptLine) record(ctx context.Context, outcome BookOutcome, qty shared.Quantity) {
	if uc.Metrics == nil {
		return
	}
	switch outcome {
	case LineBooked:
		uc.Metrics.InboundReceiptUnits(ctx, ports.InboundReceiptOutcomeBooked, qty.Int())
	case LineDamagedNotBooked:
		uc.Metrics.InboundReceiptUnits(ctx, ports.InboundReceiptOutcomeDamaged, qty.Int())
	case LineDuplicate:
		// A redelivery: already counted when first handled.
	}
}

// validate checks the wire fields through the domain constructors, so only a
// valid (sku, quantity, condition) ever reaches ReceiveStock.
func (l InboundReceiptLine) validate() (shared.SKU, shared.Quantity, error) {
	if l.EventID == "" {
		return "", 0, errors.New("empty event id")
	}
	sku, err := shared.NewSKU(l.SKU)
	if err != nil {
		return "", 0, err
	}
	qty, err := shared.NewPositiveQuantity(l.Quantity)
	if err != nil {
		return "", 0, err
	}
	if l.Condition != InboundConditionGood && l.Condition != InboundConditionDamaged {
		return "", 0, fmt.Errorf("unknown condition %q", l.Condition)
	}
	return sku, qty, nil
}
