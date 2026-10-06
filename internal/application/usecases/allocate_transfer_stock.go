package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// AllocateTransferStock is the command/reply leg of the network transfer
// saga: it holds origin-site stock for a planning transfer line and
// answers with a ledgered outcome, ALLOCATED or REJECTED, exactly once
// per transfer_line_id.
//
// Idempotency is DATABASE-level, not use-case-level: the
// transfer_allocations ledger row is unique on transfer_line_id, and the
// entire decision — stock decrements, the Reservation holding the stock,
// the ledger row, and the outbox rows for the reply event — commits in
// ONE UnitOfWork. A replayed command either finds the ledger row on the
// read path (return the original outcome, touch nothing — the original
// reply event is already durable in the outbox) or races the insert and
// fails on the unique constraint (re-read the original and return it),
// so the same line can never be decided — or double-decrement stock —
// twice.
//
// Rejection reasons form a closed set (wire-stable): ORIGIN_SITE_UNKNOWN,
// INSUFFICIENT_USABLE, IDEMPOTENCY_CONFLICT.
type AllocateTransferStock struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Transfers    ports.TransferAllocationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	Timeout      time.Duration
	UnitOfWork   ports.UnitOfWork
}

// ErrMalformedTransferCommand marks a command that failed validation
// (empty ids, non-positive quantity). Deterministic: the consumer logs
// it, commits past, never retries.
var ErrMalformedTransferCommand = errors.New("transfer allocation command failed validation")

// TransferCommand is one validated TransferAllocationRequested
// occurrence as this service needs it.
type TransferCommand struct {
	TransferID     string
	TransferLineID string
	OriginSiteID   shared.SiteID
	SKU            shared.SKU
	Quantity       shared.Quantity
}

// ValidateCommand checks the command's own shape (not the stock). It
// returns a wrapped ErrMalformedTransferCommand naming the first missing
// field, so the Kafka consumer can classify it as deterministic poison.
func ValidateCommand(cmd TransferCommand) error {
	if cmd.TransferID == "" {
		return fmt.Errorf("%w: transfer_id is empty", ErrMalformedTransferCommand)
	}
	if cmd.TransferLineID == "" {
		return fmt.Errorf("%w: transfer_line_id is empty", ErrMalformedTransferCommand)
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

// Result is the reply side of the exchange: the decided ledger outcome
// plus the reservation correlation when allocated. A replayed command
// returns the ORIGINAL outcome with Replay=true; a command whose line id
// was already decided DIFFERENTLY returns an IDEMPOTENCY_CONFLICT
// rejection with Replay=false.
type Result struct {
	Allocation  *transfer.Allocation
	Reservation *reservation.Reservation
	Replay      bool
}

// Execute decides one transfer allocation command. See the type doc for
// the idempotency and atomicity contract.
func (uc *AllocateTransferStock) Execute(ctx context.Context, cmd TransferCommand) (*Result, error) {
	if err := ValidateCommand(cmd); err != nil {
		return nil, err
	}

	// Fast path: the ledger already decided this exact line. Return the
	// original outcome WITHOUT touching stock, reservations or events —
	// the original reply event is already durable in the outbox.
	if existing, err := uc.Transfers.FindByTransferLineID(ctx, cmd.TransferLineID); err != nil {
		return nil, err
	} else if existing != nil {
		return uc.replayResult(ctx, existing, cmd)
	}

	timeout := uc.Timeout
	if timeout <= 0 {
		timeout = DefaultReservationTimeout
	}

	// decided carries the closure's outcome out through atomically (a
	// closure cannot return a second value). It stays nil on the happy
	// path, which falls through to the ledger re-read below.
	var decided *Result

	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		// Re-check inside the transaction: a concurrent duplicate may
		// have inserted the ledger row between the outer read and here.
		existing, err := uc.Transfers.FindByTransferLineID(ctx, cmd.TransferLineID)
		if err != nil {
			return err
		}
		if existing != nil {
			decided, err = uc.decideReplay(ctx, existing, cmd)
			return err
		}
		return uc.decideNew(ctx, cmd, timeout)
	})
	if err != nil {
		// Lost the unique-constraint race: a concurrent duplicate
		// inserted the ledger row first and our whole transaction rolled
		// back. Return the ORIGINAL outcome untouched.
		if errors.Is(err, ErrTransferLineAlreadyDecided) {
			existing, ferr := uc.Transfers.FindByTransferLineID(ctx, cmd.TransferLineID)
			if ferr == nil && existing != nil {
				return uc.replayResult(ctx, existing, cmd)
			}
		}
		return nil, err
	}
	if decided != nil {
		return decided, nil
	}

	// Happy path committed. Re-read the ledger row so the returned Result
	// carries the persisted decision (single source of truth).
	ledger, err := uc.Transfers.FindByTransferLineID(ctx, cmd.TransferLineID)
	if err != nil {
		return nil, err
	}
	if ledger == nil {
		return nil, fmt.Errorf("transfer allocation for line %q committed but the ledger row is missing", cmd.TransferLineID)
	}
	res, err := uc.Reservations.FindByID(ctx, ledger.ReservationID())
	if err != nil {
		return nil, err
	}
	return &Result{Allocation: ledger, Reservation: res, Replay: false}, nil
}

// decideNew decides a transfer line that has never been decided, inside
// the caller's UnitOfWork transaction: it draws stock, creates the
// Reservation holding it, writes the ledger row, and queues the reply
// event in the outbox — all part of that ONE transaction. It returns no
// Result on purpose: Execute's post-commit ledger re-read builds the
// returned outcome from the persisted row (single source of truth).
func (uc *AllocateTransferStock) decideNew(ctx context.Context, cmd TransferCommand, timeout time.Duration) error {
	units, err := uc.Stock.FindBySKUAtSite(ctx, cmd.SKU, cmd.OriginSiteID)
	if err != nil {
		return err
	}

	allocations, touched, reason := drawFromUnits(units, cmd.Quantity)
	if reason != "" {
		return uc.reject(ctx, cmd, reason)
	}

	resID, err := uc.Reservations.NextID(ctx)
	if err != nil {
		return err
	}
	now := uc.Clock.Now()
	res, err := reservation.New(resID, cmd.SKU, cmd.Quantity, TransferDemandRef(cmd.TransferID, cmd.TransferLineID), allocations, now, timeout)
	if err != nil {
		return err
	}

	// Stock decrements + the Reservation holding the stock + the
	// ledger row + the reply event's outbox row: one transaction.
	for _, unit := range touched {
		if err := uc.Stock.Save(ctx, unit); err != nil {
			return err
		}
	}
	if err := uc.Reservations.Save(ctx, res); err != nil {
		return err
	}

	ledger, err := transfer.NewAllocated(cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, resID, now)
	if err != nil {
		return err
	}
	if err := uc.Transfers.Save(ctx, ledger); err != nil {
		return err
	}

	legs := make([]shared.TransferAllocationLeg, 0, len(allocations))
	for _, alloc := range allocations {
		legs = append(legs, shared.TransferAllocationLeg{StockUnitID: alloc.StockUnitID, BinID: alloc.BinID, Quantity: alloc.Quantity})
	}
	return uc.Events.Publish(ctx, shared.NewTransferStockAllocated(now, cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, resID, cmd.SKU, cmd.Quantity, legs, res.ExpiresAt()))
}

// decideReplay answers a duplicate command found INSIDE the transaction.
// A byte-identical replay returns the original outcome (the durable
// outbox already holds its reply event, so nothing is republished). A
// same-line-id/different-payload command is an idempotency conflict: the
// original ledger decision stands, and THIS command is answered with an
// IDEMPOTENCY_CONFLICT rejection published in the same transaction.
func (uc *AllocateTransferStock) decideReplay(ctx context.Context, existing *transfer.Allocation, cmd TransferCommand) (*Result, error) {
	if existing.IsReplayOf(cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity) {
		res, err := uc.Reservations.FindByID(ctx, existing.ReservationID())
		if err != nil {
			return nil, err
		}
		return &Result{Allocation: existing, Reservation: res, Replay: true}, nil
	}

	return uc.replyConflict(ctx, cmd)
}

// replayResult is the read-path twin of decideReplay for a duplicate
// found OUTSIDE any transaction. The identical-replay case needs no
// write at all; the conflict case needs its rejection published
// atomically, so it re-enters a UnitOfWork via reject.
func (uc *AllocateTransferStock) replayResult(ctx context.Context, existing *transfer.Allocation, cmd TransferCommand) (*Result, error) {
	if existing.IsReplayOf(cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity) {
		res, err := uc.Reservations.FindByID(ctx, existing.ReservationID())
		if err != nil {
			return nil, err
		}
		return &Result{Allocation: existing, Reservation: res, Replay: true}, nil
	}

	var out *Result
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		r, err := uc.replyConflict(ctx, cmd)
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

// replyConflict publishes the IDEMPOTENCY_CONFLICT rejection event for a
// command whose line id was already decided differently, WITHOUT writing
// a ledger row (the ledger is one-row-per-line; the original decision is
// immutable). Must run inside the caller's UnitOfWork so the reply is
// durable. The Result's Allocation is an unsaved view of the conflict —
// its rejection_reason says IDEMPOTENCY_CONFLICT even though no row
// carries it, because the ROW that exists for that line still carries
// the ORIGINAL outcome.
func (uc *AllocateTransferStock) replyConflict(ctx context.Context, cmd TransferCommand) (*Result, error) {
	now := uc.Clock.Now()
	if err := uc.Events.Publish(ctx, shared.NewTransferStockAllocationRejected(now, cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, RejectionIdempotencyConflict)); err != nil {
		return nil, err
	}
	rejected, err := transfer.NewRejected(cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, transfer.ReasonIdempotencyConflict, now)
	if err != nil {
		return nil, err
	}
	return &Result{Allocation: rejected, Replay: false}, nil
}

// reject records a FIRST-decision rejection (ORIGIN_SITE_UNKNOWN,
// INSUFFICIENT_USABLE): the REJECTED ledger row and the rejection reply
// event, inside the caller's UnitOfWork. The IDEMPOTENCY_CONFLICT reason
// never takes this path — see replyConflict.
// exists to keep the event publish + optional ledger write in one place.
func (uc *AllocateTransferStock) reject(ctx context.Context, cmd TransferCommand, reason string) error {
	now := uc.Clock.Now()
	ledger, err := transfer.NewRejected(cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, transfer.RejectionReason(reason), now)
	if err != nil {
		return err
	}
	if err := uc.Transfers.Save(ctx, ledger); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, shared.NewTransferStockAllocationRejected(now, cmd.TransferID, cmd.TransferLineID, cmd.OriginSiteID, cmd.SKU, cmd.Quantity, reason))
}

// drawFromUnits greedily draws qty from the units' usable quantity
// (already site-scoped by the caller's FindBySKUAtSite). It returns the
// drawn allocations and the mutated (not persisted) units, or a non-empty
// rejection reason when the units cannot cover qty:
// ORIGIN_SITE_UNKNOWN when there were no units at all in that site's
// custody (a site this service has no recorded custody for is
// indistinguishable from one holding none of the SKU — both mean "this
// origin cannot donate"), INSUFFICIENT_USABLE when the units exist but
// their combined usable quantity falls short.
func drawFromUnits(units []*stock.StockUnit, qty shared.Quantity) ([]reservation.Allocation, []*stock.StockUnit, string) {
	if len(units) == 0 {
		return nil, nil, RejectionOriginSiteUnknown
	}

	totalUsable := shared.Quantity(0)
	for _, unit := range units {
		totalUsable = totalUsable.Add(unit.Usable())
	}
	if qty.GreaterThan(totalUsable) {
		return nil, nil, RejectionInsufficientUsable
	}

	remaining := qty
	var allocations []reservation.Allocation
	var touched []*stock.StockUnit
	for _, unit := range units {
		if remaining.Int() == 0 {
			break
		}
		usable := unit.Usable()
		if usable.Int() == 0 {
			continue
		}
		take := usable
		if !remaining.GreaterThan(usable) {
			take = remaining
		}
		if err := unit.Reserve(take); err != nil {
			// A unit cannot cover what its own usable promised — treat
			// as insufficient rather than double-drawing.
			return nil, nil, RejectionInsufficientUsable
		}
		allocations = append(allocations, reservation.Allocation{StockUnitID: unit.ID(), BinID: unit.BinID(), Quantity: take})
		touched = append(touched, unit)
		remaining, _ = remaining.Sub(take)
	}
	if remaining.Int() > 0 {
		return nil, nil, RejectionInsufficientUsable
	}
	return allocations, touched, ""
}

// TransferDemandRef namespaces a transfer line's reservation so it can
// never collide with an order-management demandRef, and reads naturally
// in GET /reservations?demandRef=... output.
func TransferDemandRef(transferID, lineID string) string {
	return "transfer:" + transferID + ":" + lineID
}
