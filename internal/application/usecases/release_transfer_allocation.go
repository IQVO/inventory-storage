package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// ReleaseTransferAllocation is the compensation leg of the network
// transfer saga (ADR 0050): a transfer cancelled AFTER allocation gets
// its origin-site hold resolved here, so a stuck or abandoned ALLOCATED
// transfer never strands its reservation until lazy expiry.
//
// Idempotency is DATABASE-level, keyed by transfer_line_id exactly like
// the allocation leg: the transfer_allocation_releases ledger row is
// unique on transfer_line_id, and the entire decision — the reservation
// resolution (stock returned to usable), the release ledger row, and the
// outbox row for the TransferStockAllocationReleased fact — commits in
// ONE UnitOfWork. A replayed command returns the original outcome
// without touching stock or republishing; the same line id with a
// DIFFERENT payload is answered with the original outcome and reason
// IDEMPOTENCY_CONFLICT (the allocation leg's conflict discipline) while
// the original decision stands.
//
// The outcome is a closed set, both settling the saga as success:
// RELEASED (the reservation still held the stock; this revoked it) or
// ALREADY_RESOLVED (the reservation had already expired — lazily — or
// been confirmed; or the line's allocation was REJECTED, so no hold ever
// existed — either way the hold is gone).
//
// Refusals are deterministic and publish NOTHING: a command naming a
// transfer_line_id this service never decided (ErrUnknownTransferLine),
// or one disagreeing with the allocation ledger row
// (ErrTransferReleaseMismatch), is logged by the consumer and committed
// past — never retried, never blocking the partition.
//
// Reservation resolution reuses RevokeReservation's logic — lazy expiry
// first (expireIfDue), then Revoke + releaseAllocations — but does NOT
// publish ReservationRevoked: transfer reservations never emitted
// StockReserved on the integration topic (AllocateTransferStock
// publishes TransferStockAllocated instead), so a ReservationRevoked
// here would make wes-work-planning's observed-usable projection
// increment a count it never decremented. The saga's fact reply is the
// only event this leg publishes.
type ReleaseTransferAllocation struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Transfers    ports.TransferAllocationRepo
	Releases     ports.TransferReleaseRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	UnitOfWork   ports.UnitOfWork
	// Levels raises BinStockLevelChanged (REVOKE) for the released bins
	// in the decision's transaction (ADR 0040). Optional: nil means "not
	// emitted".
	Levels *BinLevelEmitter
}

// TransferReleaseCommand is one validated
// TransferAllocationReleaseRequested occurrence as this service needs it.
type TransferReleaseCommand struct {
	TransferID     string
	TransferLineID string
	ReservationID  string
	OriginSiteID   shared.SiteID
	SKU            shared.SKU
	Quantity       shared.Quantity
	Reason         string
}

// ValidateReleaseCommand checks the command's own shape. It returns a
// wrapped ErrMalformedTransferCommand naming the first missing field, so
// the Kafka consumer can classify it as deterministic poison.
func ValidateReleaseCommand(cmd TransferReleaseCommand) error {
	if cmd.TransferID == "" {
		return fmt.Errorf("%w: transfer_id is empty", ErrMalformedTransferCommand)
	}
	if cmd.TransferLineID == "" {
		return fmt.Errorf("%w: transfer_line_id is empty", ErrMalformedTransferCommand)
	}
	if cmd.ReservationID == "" {
		return fmt.Errorf("%w: reservation_id is empty", ErrMalformedTransferCommand)
	}
	if _, err := shared.NewSiteID(cmd.OriginSiteID.String()); err != nil {
		return fmt.Errorf("%w: origin_site_id is empty", ErrMalformedTransferCommand)
	}
	if _, err := shared.NewSKU(cmd.SKU.String()); err != nil {
		return fmt.Errorf("%w: sku is empty", ErrMalformedTransferCommand)
	}
	if cmd.Quantity.Int() <= 0 {
		return fmt.Errorf("%w: quantity must be positive", ErrMalformedTransferCommand)
	}
	return nil
}

// ReleaseResult is the reply side of the release exchange: the decided
// release-ledger outcome. A replayed command returns the ORIGINAL
// outcome with Replay=true.
type ReleaseResult struct {
	Release *transfer.Release
	Replay  bool
}

// Execute decides one transfer allocation release command. See the type
// doc for the idempotency, atomicity and refusal contract.
func (uc *ReleaseTransferAllocation) Execute(ctx context.Context, cmd TransferReleaseCommand) (*ReleaseResult, error) {
	if err := ValidateReleaseCommand(cmd); err != nil {
		return nil, err
	}

	// Fast path: the release ledger already decided this exact line.
	// Return the original outcome WITHOUT touching stock, reservations
	// or events — the original fact reply is already durable in the
	// outbox.
	if existing, err := uc.Releases.FindByTransferLineID(ctx, cmd.TransferLineID); err != nil {
		return nil, err
	} else if existing != nil {
		return uc.replayResult(ctx, existing, cmd)
	}

	var decided *ReleaseResult
	err := uc.Levels.Atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		// Re-check inside the transaction: a concurrent duplicate may
		// have inserted the ledger row between the outer read and here.
		existing, err := uc.Releases.FindByTransferLineID(ctx, cmd.TransferLineID)
		if err != nil {
			return err
		}
		if existing != nil {
			decided, err = uc.decideReplay(ctx, existing, cmd)
			return err
		}
		return uc.decideNew(ctx, cmd)
	})
	if err != nil {
		// Lost the unique-constraint race: a concurrent duplicate
		// inserted the release row first and our whole transaction
		// rolled back. Return the ORIGINAL outcome untouched.
		if errors.Is(err, ErrTransferReleaseAlreadyDecided) {
			existing, ferr := uc.Releases.FindByTransferLineID(ctx, cmd.TransferLineID)
			if ferr == nil && existing != nil {
				return uc.replayResult(ctx, existing, cmd)
			}
		}
		return nil, err
	}
	if decided != nil {
		return decided, nil
	}

	// Happy path committed. Re-read the ledger row so the returned
	// ReleaseResult carries the persisted decision (single source of
	// truth).
	ledger, err := uc.Releases.FindByTransferLineID(ctx, cmd.TransferLineID)
	if err != nil {
		return nil, err
	}
	if ledger == nil {
		return nil, fmt.Errorf("transfer allocation release for line %q committed but the ledger row is missing", cmd.TransferLineID)
	}
	return &ReleaseResult{Release: ledger, Replay: false}, nil
}

// decideNew decides a release for a transfer line that was never
// released, inside the caller's UnitOfWork transaction. The allocation
// ledger is authoritative for WHICH reservation holds the line's stock;
// the command is validated against it before anything moves.
func (uc *ReleaseTransferAllocation) decideNew(ctx context.Context, cmd TransferReleaseCommand) error {
	allocation, err := uc.Transfers.FindByTransferLineID(ctx, cmd.TransferLineID)
	if err != nil {
		return err
	}
	if allocation == nil {
		// This service never decided an allocation for the line: no
		// hold exists to release, and no answer can be correlated.
		// Deterministic refusal — logged and committed past, no event.
		return fmt.Errorf("%w: %s", ErrUnknownTransferLine, cmd.TransferLineID)
	}

	now := uc.Clock.Now()

	// A REJECTED allocation never held stock: the hold is gone (it never
	// existed), so the release settles as ALREADY_RESOLVED without
	// touching the reservation side. The mismatch check below does not
	// apply — a rejection ledger row carries no reservation.
	if allocation.Outcome() == transfer.OutcomeRejected {
		return uc.settle(ctx, cmd, transfer.ReleaseOutcomeAlreadyResolved, now)
	}

	if err := matchAllocation(cmd, allocation); err != nil {
		return err
	}

	res, err := uc.Reservations.FindByID(ctx, allocation.ReservationID())
	if err != nil {
		return err
	}
	if res == nil {
		return fmt.Errorf("allocation ledger for line %q names reservation %q, which is missing", cmd.TransferLineID, allocation.ReservationID())
	}

	// Lazy expiry first (RevokeReservation's own discipline): a timed-out
	// ACTIVE reservation is resolved as Expired here — its quantity was
	// already returned to usable by the expiry — so the release answers
	// ALREADY_RESOLVED rather than releasing a second time. The joined
	// UnitOfWork keeps any expiry writes in THIS transaction.
	res, err = expireIfDue(ctx, uc.UnitOfWork, uc.Levels, uc.Stock, uc.Reservations, uc.Events, uc.Clock, res)
	if err != nil {
		return err
	}

	if err := res.Revoke(); err != nil {
		if !errors.Is(err, reservation.ErrAlreadyResolved) {
			return err
		}
		// Confirmed, revoked or expired before we got here: the hold is
		// gone, nothing moves, the saga still settles as success.
		return uc.settle(ctx, cmd, transfer.ReleaseOutcomeAlreadyResolved, now)
	}

	// RELEASED: return the held quantity to usable (RevokeReservation's
	// release loop), persist the reservation, write the release ledger
	// row and queue the fact reply — one transaction.
	if err := releaseAllocations(ctx, uc.Stock, uc.Levels, res); err != nil {
		return err
	}
	if err := uc.Reservations.Save(ctx, res); err != nil {
		return err
	}
	return uc.settle(ctx, cmd, transfer.ReleaseOutcomeReleased, now)
}

// settle writes the release ledger row and queues the
// TransferStockAllocationReleased fact in the caller's transaction.
func (uc *ReleaseTransferAllocation) settle(ctx context.Context, cmd TransferReleaseCommand, outcome transfer.ReleaseOutcome, now time.Time) error {
	ledger, err := transfer.NewRelease(cmd.TransferID, cmd.TransferLineID, cmd.ReservationID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, outcome, cmd.Reason, now)
	if err != nil {
		return err
	}
	if err := uc.Releases.Save(ctx, ledger); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, shared.NewTransferStockAllocationReleased(now, cmd.TransferID, cmd.TransferLineID, cmd.ReservationID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, string(outcome), cmd.Reason))
}

// matchAllocation refuses a release command that disagrees with the
// allocation ledger row it names — the ledger, not the command, is the
// record of what was decided. Deterministic (ErrTransferReleaseMismatch):
// the consumer commits past it and no event is published.
func matchAllocation(cmd TransferReleaseCommand, allocation *transfer.Allocation) error {
	if cmd.TransferID != allocation.TransferID() ||
		cmd.ReservationID != allocation.ReservationID() ||
		cmd.OriginSiteID != allocation.OriginSiteID() ||
		cmd.SKU != allocation.SKU() ||
		cmd.Quantity != allocation.RequestedQuantity() {
		return fmt.Errorf("%w: line %s decided as transfer=%s reservation=%s site=%s sku=%s qty=%d",
			ErrTransferReleaseMismatch, cmd.TransferLineID,
			allocation.TransferID(), allocation.ReservationID(), allocation.OriginSiteID(), allocation.SKU(), allocation.RequestedQuantity().Int())
	}
	return nil
}

// decideReplay answers a duplicate release command found INSIDE the
// transaction. A byte-identical replay returns the original outcome (the
// durable outbox already holds its fact, so nothing is republished). A
// same-line-id/different-payload command is an idempotency conflict: the
// original ledger decision stands, and THIS command is answered with the
// original outcome and reason IDEMPOTENCY_CONFLICT, published in the
// same transaction.
func (uc *ReleaseTransferAllocation) decideReplay(ctx context.Context, existing *transfer.Release, cmd TransferReleaseCommand) (*ReleaseResult, error) {
	if existing.IsReplayOf(cmd.TransferID, cmd.TransferLineID, cmd.ReservationID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, cmd.Reason) {
		return &ReleaseResult{Release: existing, Replay: true}, nil
	}
	return uc.replyConflict(ctx, existing, cmd)
}

// replayResult is the read-path twin of decideReplay for a duplicate
// found OUTSIDE any transaction. The identical-replay case needs no
// write at all; the conflict case needs its answer published atomically,
// so it re-enters a UnitOfWork.
func (uc *ReleaseTransferAllocation) replayResult(ctx context.Context, existing *transfer.Release, cmd TransferReleaseCommand) (*ReleaseResult, error) {
	if existing.IsReplayOf(cmd.TransferID, cmd.TransferLineID, cmd.ReservationID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, cmd.Reason) {
		return &ReleaseResult{Release: existing, Replay: true}, nil
	}

	var out *ReleaseResult
	err := uc.Levels.Atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		r, err := uc.replyConflict(ctx, existing, cmd)
		if err != nil {
			return err
		}
		out = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// replyConflict publishes the conflict answer for a release command whose
// line id was already decided differently, WITHOUT writing a ledger row
// (the ledger is one-row-per-line; the original decision is immutable).
// The answer carries the ORIGINAL outcome — the hold is gone either way,
// so the saga settles — and reason IDEMPOTENCY_CONFLICT so the planner
// can tell it apart from a clean settle. Must run inside the caller's
// UnitOfWork so the reply is durable. The ReleaseResult's Release is an
// unsaved view of the conflict answer, exactly like the allocation leg's
// replyConflict.
func (uc *ReleaseTransferAllocation) replyConflict(ctx context.Context, existing *transfer.Release, cmd TransferReleaseCommand) (*ReleaseResult, error) {
	now := uc.Clock.Now()
	if err := uc.Events.Publish(ctx, shared.NewTransferStockAllocationReleased(now, cmd.TransferID, cmd.TransferLineID, cmd.ReservationID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, string(existing.Outcome()), RejectionIdempotencyConflict)); err != nil {
		return nil, err
	}
	conflict, err := transfer.NewRelease(cmd.TransferID, cmd.TransferLineID, cmd.ReservationID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, existing.Outcome(), RejectionIdempotencyConflict, now)
	if err != nil {
		return nil, err
	}
	return &ReleaseResult{Release: conflict, Replay: false}, nil
}
