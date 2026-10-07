package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// StageTransferReceipt is the destination-receiving half of the transfer
// saga (ADR 0031, Phase 3): a destination site scanned an arrival and
// counted received_quantity of sku against a transfer line.
//
// The lookup key is the transfer_allocations ledger row for
// transfer_line_id — the same anchor the origin side decided against:
//
//   - NO ledger row for the line: this service never allocated stock
//     for it. The scan is QUARANTINED — an inventory_exceptions row is
//     written (kind UNKNOWN_TRANSFER), nothing that could raise usable
//     stock is published, and the caller gets an explicit problem
//     (ErrTransferLineUnrecognized).
//
//   - A ledger row whose outcome is NOT ALLOCATED (it was REJECTED), or
//     whose transfer_id does not match the scan: the goods cannot be
//     lawfully received against it. QUARANTINED (kind
//     UNRECOGNIZED_TRANSFER), caller gets ErrTransferNotAllocated.
//
//   - A destination site equal to the allocation's ORIGIN site is
//     physically impossible (a transfer never ships back to its donor):
//     QUARANTINED (UNRECOGNIZED_TRANSFER), caller gets
//     ErrTransferWrongDestination.
//
//   - A recognized ALLOCATED row: a transfer_receipts row is created
//     (state STAGED, expected_quantity from the ledger's
//     requested_quantity, received_quantity as counted, variance =
//     received - expected SIGNED) and TransferReceiptStaged is
//     published through the same transactional outbox. NO usable stock
//     moves yet — the destination's usable rises only at
//     StowTransferStock.
//
// A quarantine is a COMMITTED outcome, not a rollback: the exception row
// is durable (written inside the same ONE UnitOfWork as every other
// write) and Execute returns BOTH the exception (in the Result) and the
// explicit problem (as the error).
//
// Idempotency is DATABASE-level, exactly like the origin side: the
// transfer_receipts row is unique on transfer_line_id. A replayed
// identical stage returns the ORIGINAL receipt (Replay=true) touching
// nothing; the same line id with a DIFFERENT payload is refused
// (ErrTransferReceiptConflict) with the original receipt immutable. A
// repeated identical QUARANTINED scan is also a benign replay: the
// existing exception row is returned and no second row is written (the
// unique constraint on the scan tuple backs this).
type StageTransferReceipt struct {
	Transfers  ports.TransferAllocationRepo
	Receipts   ports.TransferReceiptRepo
	Exceptions ports.InventoryExceptionRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// ErrMalformedReceiptCommand marks a stage command that failed
// validation (empty ids, non-positive quantity). Deterministic.
var ErrMalformedReceiptCommand = errors.New("transfer receipt command failed validation")

// ReceiptCommand is one validated destination scan as this service
// needs it: what line the operator claims the goods belong to, which
// site received them, and how many were counted.
type ReceiptCommand struct {
	TransferID        string
	TransferLineID    string
	DestinationSiteID shared.SiteID
	SKU               shared.SKU
	ReceivedQuantity  shared.Quantity
}

// ValidateReceiptCommand checks the command's own shape. It returns a
// wrapped ErrMalformedReceiptCommand naming the first missing field.
func ValidateReceiptCommand(cmd ReceiptCommand) error {
	if cmd.TransferID == "" {
		return fmt.Errorf("%w: transfer_id is empty", ErrMalformedReceiptCommand)
	}
	if cmd.TransferLineID == "" {
		return fmt.Errorf("%w: transfer_line_id is empty", ErrMalformedReceiptCommand)
	}
	if _, err := shared.NewSiteID(cmd.DestinationSiteID.String()); err != nil {
		return fmt.Errorf("%w: destination_site_id is empty", ErrMalformedReceiptCommand)
	}
	if _, err := shared.NewSKU(cmd.SKU.String()); err != nil {
		return fmt.Errorf("%w: sku is empty", ErrMalformedReceiptCommand)
	}
	if cmd.ReceivedQuantity.Int() <= 0 {
		return fmt.Errorf("%w: received_quantity must be positive", ErrMalformedReceiptCommand)
	}
	return nil
}

// ErrTransferReceiptConflict: the transfer_line_id already has a
// receipt staged with a DIFFERENT payload. The original receipt stands
// immutable; this command is refused.
var ErrTransferReceiptConflict = errors.New("transfer line already has a receipt for a different scan")

// StageReceiptResult carries the outcome of one stage command: either
// the staged (or originally-staged-on-replay) receipt, or — for a
// quarantined scan — the exception row. Problem is non-nil exactly for
// a quarantine: the explicit problem the caller must see, returned as
// Execute's error as well so no caller can miss it.
type StageReceiptResult struct {
	Receipt   *transfer.Receipt
	Exception *transfer.Exception
	Replay    bool
	Problem   error
}

// Quarantined reports whether this result is a quarantine (the scan
// could not be matched to a recognized ALLOCATED transfer line).
func (r *StageReceiptResult) Quarantined() bool { return r.Exception != nil }

// Execute decides one destination scan. See the type doc for the full
// contract. The UnitOfWork closure returns an error ONLY for real
// failures (which roll the transaction back); a quarantine COMMITS by
// setting decided (which carries its own Problem) and returning nil.
func (uc *StageTransferReceipt) Execute(ctx context.Context, cmd ReceiptCommand) (*StageReceiptResult, error) {
	if err := ValidateReceiptCommand(cmd); err != nil {
		return nil, err
	}

	// Fast path: this line already has a receipt — return the original
	// (identical payload) or refuse (conflicting payload) without
	// touching anything.
	if existing, err := uc.Receipts.FindByTransferLineID(ctx, cmd.TransferLineID); err != nil {
		return nil, err
	} else if existing != nil {
		return replayOrRefuseReceipt(existing, cmd)
	}

	// decided carries a committed non-happy-path outcome (an in-tx
	// replay or a quarantine) out through the closure. It stays nil on
	// the happy path, which falls through to the receipt re-read below.
	var decided *StageReceiptResult

	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		// Re-check inside the transaction: a concurrent duplicate may
		// have staged between the outer read and here.
		existing, err := uc.Receipts.FindByTransferLineID(ctx, cmd.TransferLineID)
		if err != nil {
			return err
		}
		if existing != nil {
			res, replayErr := replayOrRefuseReceipt(existing, cmd)
			if replayErr == nil {
				decided = res
				return nil
			}
			// A conflict refuses THIS command; nothing has been written
			// in this scope, so propagating the error rolls back an
			// empty transaction. The original receipt stands.
			return replayErr
		}
		return uc.decide(ctx, cmd, &decided)
	})
	if err != nil {
		// Lost the unique-constraint race against a concurrent stage:
		// the original receipt stands; answer this caller with it.
		if errors.Is(err, ErrTransferReceiptAlreadyStaged) {
			existing, ferr := uc.Receipts.FindByTransferLineID(ctx, cmd.TransferLineID)
			if ferr == nil && existing != nil {
				return replayOrRefuseReceipt(existing, cmd)
			}
		}
		return nil, err
	}
	if decided != nil {
		// A committed quarantine (returns its Problem as the error) or
		// an in-tx replay (Problem nil).
		return decided, decided.Problem
	}

	// Happy path committed. Re-read so the returned Result carries the
	// persisted receipt (single source of truth).
	receipt, err := uc.Receipts.FindByTransferLineID(ctx, cmd.TransferLineID)
	if err != nil {
		return nil, err
	}
	if receipt == nil {
		return nil, fmt.Errorf("receipt for line %q committed but the row is missing", cmd.TransferLineID)
	}
	return &StageReceiptResult{Receipt: receipt, Replay: false}, nil
}

// decide runs inside the caller's UnitOfWork: it looks the line up in
// the ledger and either quarantines (committing the exception row by
// setting *decided and returning nil) or stages the receipt.
func (uc *StageTransferReceipt) decide(ctx context.Context, cmd ReceiptCommand, decided **StageReceiptResult) error {
	allocation, err := uc.Transfers.FindByTransferLineID(ctx, cmd.TransferLineID)
	if err != nil {
		return err
	}

	switch {
	case allocation == nil:
		return uc.quarantine(ctx, cmd, transfer.ExceptionUnknownTransfer, ErrTransferLineUnrecognized, decided)
	case allocation.Outcome() != transfer.OutcomeAllocated:
		return uc.quarantine(ctx, cmd, transfer.ExceptionUnrecognizedTransfer,
			fmt.Errorf("%w: line %q was decided %s (%s), not ALLOCATED", ErrTransferNotAllocated, cmd.TransferLineID, allocation.Outcome(), allocation.RejectionReason()),
			decided)
	case allocation.TransferID() != cmd.TransferID:
		return uc.quarantine(ctx, cmd, transfer.ExceptionUnrecognizedTransfer,
			fmt.Errorf("%w: scan says transfer %q but line %q was allocated under transfer %q", ErrTransferNotAllocated, cmd.TransferID, cmd.TransferLineID, allocation.TransferID()),
			decided)
	case allocation.OriginSiteID() == cmd.DestinationSiteID:
		return uc.quarantine(ctx, cmd, transfer.ExceptionUnrecognizedTransfer,
			fmt.Errorf("%w: destination %q is the transfer's own origin site", ErrTransferWrongDestination, cmd.DestinationSiteID),
			decided)
	}

	return uc.stage(ctx, cmd, allocation)
}

// stage writes the STAGED receipt and queues TransferReceiptStaged in
// the outbox — one transaction with the caller's. The variance is
// computed by the aggregate (received - expected, signed) and is
// EXPLICIT in the event: over/short is never silently absorbed.
func (uc *StageTransferReceipt) stage(ctx context.Context, cmd ReceiptCommand, allocation *transfer.Allocation) error {
	now := uc.Clock.Now()
	receipt, err := transfer.NewStagedReceipt(
		cmd.TransferID, cmd.TransferLineID, cmd.DestinationSiteID, cmd.SKU,
		allocation.ReservationID(), allocation.RequestedQuantity(), cmd.ReceivedQuantity, now,
	)
	if err != nil {
		return err
	}
	if err := uc.Receipts.Save(ctx, receipt); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, shared.NewTransferReceiptStaged(
		now, cmd.TransferID, cmd.TransferLineID, cmd.DestinationSiteID, cmd.SKU,
		receipt.ExpectedQuantity(), receipt.ReceivedQuantity(), receipt.Variance(),
	))
}

// quarantine writes the inventory_exceptions row for an unrecognized
// scan — a COMMITTED outcome — by setting *decided and returning nil,
// so the UnitOfWork transaction commits the exception row. Nothing that
// raises usable stock is published. A repeated IDENTICAL scan is a
// benign replay: the existing row is returned, no second row is written
// (the DB unique constraint on the scan tuple backs this).
func (uc *StageTransferReceipt) quarantine(ctx context.Context, cmd ReceiptCommand, kind transfer.ExceptionKind, problem error, decided **StageReceiptResult) error {
	if existing, err := uc.Exceptions.FindByScan(ctx, cmd.TransferLineID, cmd.DestinationSiteID, cmd.SKU, cmd.ReceivedQuantity); err != nil {
		return err
	} else if existing != nil {
		*decided = &StageReceiptResult{Exception: existing, Replay: true, Problem: problem}
		return nil
	}

	exc, err := transfer.NewException(cmd.TransferID, cmd.TransferLineID, cmd.DestinationSiteID, cmd.SKU, cmd.ReceivedQuantity, kind, problem.Error(), uc.Clock.Now())
	if err != nil {
		return err
	}
	if err := uc.Exceptions.Save(ctx, exc); err != nil {
		// A concurrent identical scan won the race: return its row.
		if existing, ferr := uc.Exceptions.FindByScan(ctx, cmd.TransferLineID, cmd.DestinationSiteID, cmd.SKU, cmd.ReceivedQuantity); ferr == nil && existing != nil {
			*decided = &StageReceiptResult{Exception: existing, Replay: true, Problem: problem}
			return nil
		}
		return err
	}
	*decided = &StageReceiptResult{Exception: exc, Problem: problem}
	return nil
}

// replayOrRefuseReceipt answers a duplicate stage command found against
// an existing receipt. The identical-replay case returns the original
// receipt with Replay=true and no write; a differing payload on the
// SAME transfer_line_id is refused — the original receipt is immutable.
func replayOrRefuseReceipt(existing *transfer.Receipt, cmd ReceiptCommand) (*StageReceiptResult, error) {
	if existing.IsReplayOf(cmd.TransferID, cmd.DestinationSiteID, cmd.SKU, cmd.ReceivedQuantity) {
		return &StageReceiptResult{Receipt: existing, Replay: true}, nil
	}
	return nil, ErrTransferReceiptConflict
}
