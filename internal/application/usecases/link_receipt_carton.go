package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/shared/gs1"
)

// InboundCartonConsumerName is the processed_events consumer name under which
// LinkReceiptCarton claims CloudEvents ids (ADR 0045 section 6).
const InboundCartonConsumerName = "inbound-carton"

// ErrMalformedReceiptCarton marks a ReceiptCartonReceived that can never be
// linked (no event id, no receipt id, no LPN, an invalid content). It is
// deterministic: the consumer logs it and commits past the message.
var ErrMalformedReceiptCarton = errors.New("malformed receipt carton")

// ReceiptCartonContent is one content line of a received carton, as decoded
// by the inbound adapter (wire values, not yet validated).
type ReceiptCartonContent struct {
	LineNo    int
	SKU       string
	GTIN      string
	Quantity  int
	Lot       string
	Expiry    string
	Condition string
}

// ReceiptCarton is one inbound-receiving ReceiptCartonReceived occurrence.
// It carries identity only: it never books a quantity.
type ReceiptCarton struct {
	// EventID is the CloudEvents id, the dedupe key.
	EventID    string
	ReceiptID  string
	ASNNumber  string
	SSCC       string
	ParentSSCC string
	// LPN is the license plate: the SSCC, or an internal LPN-<site>-<ulid>
	// for an unlabelled carton. Empty falls back to the SSCC.
	LPN      string
	Sealed   bool
	OwnerRef string
	Contents []ReceiptCartonContent
}

// CartonOutcome says what LinkReceiptCarton did with one carton.
type CartonOutcome string

const (
	// CartonDuplicate: this CloudEvents id was already handled.
	CartonDuplicate CartonOutcome = "DUPLICATE"
	// CartonNoContainer: a mixed, unsealed or damaged carton; its eaches stay
	// ordinary staged stock and no container is created.
	CartonNoContainer CartonOutcome = "NO_CONTAINER"
	// CartonRecorded: the carton became a pending link and was settled as far
	// as the booking and the packaging view allow.
	CartonRecorded CartonOutcome = "RECORDED"
)

// ContainerLinkSettler re-evaluates the pending links of one receipt line;
// satisfied by *ContainerLinker.
type ContainerLinkSettler interface {
	Settle(ctx context.Context, receiptID string, lineNo int) (SettleResult, error)
}

// LinkReceiptCarton records a received carton as a pending link and settles
// it (ADR 0045 section 6). The event-id claim, the link and the settle run in
// ONE UnitOfWork, so a redelivery is applied in full or not at all, and a
// redelivery under a different event id is a no-op because the link key
// (receipt id, line no, lpn) and the unique LPN make it so.
type LinkReceiptCarton struct {
	ProcessedEvents ports.ProcessedEventRepo
	Pending         ports.PendingLinkRepo
	Linker          ContainerLinkSettler
	// Held records the LPN of a Damaged content on the held stock of its line
	// (ADR 0052). Optional: nil records nothing.
	Held ports.HeldStockRepo
	// UnitOfWork brackets the claim, the link and the settle. Optional: nil
	// means "no transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
}

// Execute validates c and links it. It wraps ErrMalformedReceiptCarton for a
// deterministic problem and returns repository errors unchanged.
func (uc *LinkReceiptCarton) Execute(ctx context.Context, c ReceiptCarton) (CartonOutcome, SettleResult, error) {
	link, eligible, err := c.link()
	if err != nil {
		return "", SettleResult{}, fmt.Errorf("%w: %w", ErrMalformedReceiptCarton, err)
	}
	outcome := CartonDuplicate
	var settled SettleResult
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, InboundCartonConsumerName, c.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			outcome = CartonDuplicate
			return nil
		}
		if !eligible {
			outcome = CartonNoContainer
			return uc.markDamaged(ctx, c)
		}
		outcome = CartonRecorded
		if err := uc.Pending.Add(ctx, link); err != nil {
			return err
		}
		settled, err = uc.Linker.Settle(ctx, link.ReceiptID, link.LineNo)
		return err
	})
	if err != nil {
		return "", SettleResult{}, err
	}
	return outcome, settled, nil
}

// markDamaged records the LPN of every Damaged content of c on the held line
// it names. It books NO quantity: ReceiptLineReceived is the sole quantity
// authority (ADR 0045), so opening a hold here would count the same units
// twice. The mark is keyed (receipt, line, lpn), so it is idempotent and does
// not depend on whether the line event arrived first.
func (uc *LinkReceiptCarton) markDamaged(ctx context.Context, c ReceiptCarton) error {
	if uc.Held == nil {
		return nil
	}
	lpn := c.LPN
	if lpn == "" {
		lpn = c.SSCC
	}
	if lpn == "" {
		return nil
	}
	for _, content := range c.Contents {
		if content.Condition != InboundConditionDamaged || content.LineNo < 1 {
			continue
		}
		if err := uc.Held.MarkCarton(ctx, c.ReceiptID, content.LineNo, lpn, c.SSCC); err != nil {
			return err
		}
	}
	return nil
}

// link validates the event and builds the pending link. eligible is false for
// a carton that never gets a container: not sealed, mixed (not exactly one
// content line) or damaged.
func (c ReceiptCarton) link() (container.PendingLink, bool, error) {
	if c.EventID == "" {
		return container.PendingLink{}, false, errors.New("empty event id")
	}
	if c.ReceiptID == "" {
		return container.PendingLink{}, false, errors.New("empty receipt id")
	}
	if !c.Sealed || len(c.Contents) != 1 || c.Contents[0].Condition == InboundConditionDamaged {
		return container.PendingLink{}, false, nil
	}
	content := c.Contents[0]
	if content.Condition != InboundConditionGood {
		return container.PendingLink{}, false, fmt.Errorf("unknown condition %q", content.Condition)
	}
	return c.pendingLink(content)
}

func (c ReceiptCarton) pendingLink(content ReceiptCartonContent) (container.PendingLink, bool, error) {
	lpn := c.LPN
	if lpn == "" {
		lpn = c.SSCC
	}
	if lpn == "" {
		return container.PendingLink{}, false, errors.New("empty lpn")
	}
	sku, err := shared.NewSKU(content.SKU)
	if err != nil {
		return container.PendingLink{}, false, err
	}
	qty, err := shared.NewPositiveQuantity(content.Quantity)
	if err != nil {
		return container.PendingLink{}, false, err
	}
	gtin, err := gs1.ParseGTIN(content.GTIN)
	if err != nil {
		return container.PendingLink{}, false, err
	}
	owner, err := shared.NewOwnerRef(c.OwnerRef)
	if err != nil {
		return container.PendingLink{}, false, err
	}
	if content.LineNo < 1 {
		return container.PendingLink{}, false, fmt.Errorf("line no %d", content.LineNo)
	}
	return container.PendingLink{
		ReceiptID: c.ReceiptID, LineNo: content.LineNo, LPN: lpn, SSCC: c.SSCC, ParentSSCC: c.ParentSSCC,
		SKU: sku, GTIN: gtin.String(), Quantity: qty, OwnerRef: owner, Lot: content.Lot, Expiry: content.Expiry,
		Status: container.LinkPending,
	}, true, nil
}
