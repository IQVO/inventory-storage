package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ReturnLineConsumerName is the processed_events consumer name under
// which BookReturnLine claims CloudEvents ids. It is the provenance
// record for the return flow, exactly as `inbound-receipt-line` is for
// inbound receipts (ADR 0037): the stock event itself carries only
// {sku, quantity}, so a staged return line is told apart from a dock
// receipt line by WHICH consumer claimed its CloudEvents id, never by a
// field on StockReceived.
const ReturnLineConsumerName = "return-line-staged"

// ReturnLineConditionGood is the only `condition` a staged return line
// carries: returns-processing pins it as the constant "Good" (only a
// RESTOCK disposition stages a line; a staged line is by definition
// restockable). Anything else on the wire is a producer contract break.
const ReturnLineConditionGood = InboundConditionGood

// ErrMalformedReturnLine marks a ReturnLineStaged that can never be
// booked (no CloudEvents id, empty SKU, quantity <= 0, an empty
// disposition_id, a condition other than Good). It is deterministic:
// the consumer logs it and commits past the message, never retries it.
var ErrMalformedReturnLine = errors.New("malformed return line")

// ReturnLine is one returns-processing ReturnLineStaged occurrence, as
// decoded by the inbound adapter (wire values, not yet validated).
type ReturnLine struct {
	// EventID is the CloudEvents id, the dedupe key.
	EventID string
	// DispositionID is the RESTOCK disposition this staging belongs to
	// (the producer's idempotency key; also the Kafka key and subject).
	DispositionID string
	ReceiptID     string
	RmaID         string
	SiteID        string
	LineNo        int
	SKU           string
	Quantity      int
	// Condition is always "Good" on this event; anything else is
	// malformed.
	Condition string
	// ReceivedAt is the producer's wire timestamp, logged only: the
	// resulting StockReceived is stamped by this service's clock.
	ReceivedAt string
}

// ReturnBookOutcome says what BookReturnLine did with one line.
type ReturnBookOutcome string

const (
	// ReturnLineBooked: the line went through ReceiveStock (StockReceived
	// raised, quantity staged, not yet usable).
	ReturnLineBooked ReturnBookOutcome = "BOOKED"
	// ReturnLineDuplicate: this CloudEvents id was already handled.
	ReturnLineDuplicate ReturnBookOutcome = "DUPLICATE"
)

// BookReturnLine books a staged return line as staged stock (ADR 0044):
// the same ReceiveStock use case an inbound receipt line and
// POST /stock/receive drive, so the effect is identical and stow stays
// the RF action. It is the returns-processing counterpart of
// BookInboundReceiptLine (ADR 0037).
//
// The CloudEvents id claim and ReceiveStock's outbox write run in ONE
// UnitOfWork (ReceiveStock's own bracket joins it), so a failure rolls
// both back and the redelivery is applied in full.
type BookReturnLine struct {
	Receive         stockReceiver
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and the receive. Optional: nil means
	// "no transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
}

// Execute validates l and books it. It returns an error wrapping
// ErrMalformedReturnLine for a deterministic problem, and the repository
// error unchanged for a transient one.
func (uc *BookReturnLine) Execute(ctx context.Context, l ReturnLine) (ReturnBookOutcome, error) {
	sku, qty, err := l.validate()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedReturnLine, err)
	}

	outcome := ReturnLineDuplicate
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, ReturnLineConsumerName, l.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			outcome = ReturnLineDuplicate
			return nil
		}
		if _, err := uc.Receive.Execute(ctx, sku, qty); err != nil {
			return err
		}
		outcome = ReturnLineBooked
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// validate checks the wire fields through the domain constructors, so
// only a valid (sku, quantity) ever reaches ReceiveStock. The return
// context (receipt, RMA, disposition, site) is provenance, not stock
// data: it is logged by the consumer and claimed under
// ReturnLineConsumerName, never stored on the stock event.
func (l ReturnLine) validate() (shared.SKU, shared.Quantity, error) {
	if l.EventID == "" {
		return "", 0, errors.New("empty event id")
	}
	if l.DispositionID == "" {
		return "", 0, errors.New("empty disposition id")
	}
	sku, err := shared.NewSKU(l.SKU)
	if err != nil {
		return "", 0, err
	}
	qty, err := shared.NewPositiveQuantity(l.Quantity)
	if err != nil {
		return "", 0, err
	}
	if l.Condition != ReturnLineConditionGood {
		return "", 0, fmt.Errorf("condition %q: a staged return line is always Good", l.Condition)
	}
	return sku, qty, nil
}
