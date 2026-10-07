package usecases

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// PickTaskType is the exact task_type string fulfillment-execution publishes
// on TaskCompleted for a pick task (its AsyncAPI enum value).
const PickTaskType = "PICK"

// ConfirmPicksConsumer is the processed_events consumer name under which
// ConfirmPicksForOrder claims CloudEvents ids.
const ConfirmPicksConsumer = "task-completed-confirm-pick"

// ErrMalformedPickCompletion marks a TaskCompleted that can never be handled
// (no CloudEvents id to dedupe on). It is deterministic: the consumer
// dead-letters the message immediately and never retries it.
var ErrMalformedPickCompletion = errors.New("malformed pick completion")

// PickCompletion is one fulfillment-execution TaskCompleted occurrence, as
// decoded by the inbound adapter (wire strings, not yet interpreted).
type PickCompletion struct {
	// EventID is the CloudEvents id, the dedupe key.
	EventID string
	TaskID  string
	// TaskType is the task's own type (PICK, PACK, ...); empty when the
	// producer could not resolve it.
	TaskType string
	// OrderRef is the completed task's order reference, which is the OrderId
	// order-management reserved against (demand_ref). Empty when the
	// producer predates the field or the task could not be found.
	OrderRef string
}

// ConfirmPicksOutcome says what ConfirmPicksForOrder did with one event.
type ConfirmPicksOutcome string

const (
	// PicksIgnored: not a PICK task, or no order_ref. Nothing was claimed.
	PicksIgnored ConfirmPicksOutcome = "IGNORED"
	// PicksDuplicate: this CloudEvents id was already handled.
	PicksDuplicate ConfirmPicksOutcome = "DUPLICATE"
	// PicksNoReservations: no reservation carries this demand_ref (a
	// transfer or non-inventory order). A successful no-op.
	PicksNoReservations ConfirmPicksOutcome = "NO_RESERVATIONS"
	// PicksProcessed: the order's reservations were walked; see the counts.
	PicksProcessed ConfirmPicksOutcome = "PROCESSED"
)

// ConfirmPicksResult is the per-event summary: how many of the order's
// reservations ended in each state.
type ConfirmPicksResult struct {
	Outcome ConfirmPicksOutcome
	// Confirmed reservations were ACTIVE and are now CONFIRMED (picked).
	Confirmed int
	// AlreadyPicked reservations were already CONFIRMED.
	AlreadyPicked int
	// Revoked reservations were REVOKED before the pick completed.
	Revoked int
	// Expired reservations had timed out (ADR 0003): skipped, never an
	// error, because blocking the partition cannot bring the stock back.
	Expired int
}

// reservationConfirmer is the single-reservation confirm-pick logic this use
// case reuses; satisfied by *ConfirmPick, so its rules are not duplicated.
type reservationConfirmer interface {
	Execute(ctx context.Context, reservationID string) error
}

// ConfirmPicksForOrder turns "a PICK task for order X completed" into the
// physical decrement of every ACTIVE reservation whose demand_ref is X
// (ADR 0035, superseding ADR 0032). Granularity is the ORDER: a Task carries
// no SKU or quantity, so the order's reservations are the finest unit the
// event can name. Short picks are therefore not modelled.
//
// The CloudEvents id claim and every ConfirmPick write (stock, bin,
// reservation, outbox rows) run in ONE UnitOfWork, so a failure rolls all of
// them back and the redelivery is applied in full. A redelivered event
// confirms nothing new: the claim short-circuits it, and even a NEW id for
// the same order only finds reservations that are no longer ACTIVE.
type ConfirmPicksForOrder struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	// Confirm is the existing confirm-pick use case (*ConfirmPick).
	Confirm         reservationConfirmer
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and all confirmations. Optional: nil
	// means "no transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
	// Metrics counts per-reservation outcomes after commit. Optional.
	Metrics ports.PickConfirmationMetrics
}

// Execute handles one completion. It returns an error wrapping
// ErrMalformedPickCompletion for a deterministic problem, and the underlying
// error unchanged for a transient one (the consumer retries those).
func (uc *ConfirmPicksForOrder) Execute(ctx context.Context, in PickCompletion) (ConfirmPicksResult, error) {
	if in.TaskType != PickTaskType || strings.TrimSpace(in.OrderRef) == "" {
		return ConfirmPicksResult{Outcome: PicksIgnored}, nil
	}
	if in.EventID == "" {
		return ConfirmPicksResult{}, fmt.Errorf("%w: empty event id", ErrMalformedPickCompletion)
	}

	var result ConfirmPicksResult
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		// The closure may run again on a retried transaction: start clean.
		result = ConfirmPicksResult{}

		claimed, err := uc.ProcessedEvents.Claim(ctx, ConfirmPicksConsumer, in.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			result.Outcome = PicksDuplicate
			return nil
		}

		reservations, err := uc.Reservations.FindByDemandRef(ctx, in.OrderRef)
		if err != nil {
			return err
		}
		if len(reservations) == 0 {
			result.Outcome = PicksNoReservations
			return nil
		}
		// Deterministic order, so concurrent handlers lock rows alike.
		sort.Slice(reservations, func(i, j int) bool { return reservations[i].ID() < reservations[j].ID() })

		result.Outcome = PicksProcessed
		for _, res := range reservations {
			if err := uc.settle(ctx, res, &result); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return ConfirmPicksResult{}, err
	}
	uc.record(ctx, result)
	return result, nil
}

// settle confirms one reservation when it is ACTIVE and counts it in result
// otherwise.
func (uc *ConfirmPicksForOrder) settle(ctx context.Context, res *reservation.Reservation, result *ConfirmPicksResult) error {
	switch res.Status() {
	case reservation.StatusConfirmed:
		result.AlreadyPicked++
		return nil
	case reservation.StatusRevoked:
		result.Revoked++
		return nil
	case reservation.StatusExpired:
		result.Expired++
		return nil
	}

	// ACTIVE in storage. Lazy expiry (ADR 0003): one past its timeout is
	// resolved here exactly as any other read would (stock returned to usable,
	// ReservationExpired raised) and then skipped, not confirmed.
	if res.IsExpired(uc.Clock.Now()) {
		if _, err := expireIfDue(ctx, uc.UnitOfWork, uc.Stock, uc.Reservations, uc.Events, uc.Clock, res); err != nil {
			return err
		}
		result.Expired++
		return nil
	}

	if err := uc.Confirm.Execute(ctx, res.ID()); err != nil {
		// The clock crossing the timeout between the check above and
		// ConfirmPick's own lazy expiry is the only way to land here with a
		// domain error: the reservation was expired, not broken.
		if errors.Is(err, reservation.ErrExpired) || errors.Is(err, reservation.ErrAlreadyResolved) {
			result.Expired++
			return nil
		}
		return err
	}
	result.Confirmed++
	return nil
}

// record publishes the per-reservation counters once the transaction has
// committed, so a rolled-back attempt never inflates them.
func (uc *ConfirmPicksForOrder) record(ctx context.Context, r ConfirmPicksResult) {
	if uc.Metrics == nil {
		return
	}
	for outcome, n := range map[string]int{
		ports.PickOutcomeConfirmed:     r.Confirmed,
		ports.PickOutcomeAlreadyPicked: r.AlreadyPicked,
		ports.PickOutcomeRevoked:       r.Revoked,
		ports.PickOutcomeExpired:       r.Expired,
	} {
		if n > 0 {
			uc.Metrics.PickConfirmation(ctx, outcome, n)
		}
	}
}
