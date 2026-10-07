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
	// PicksAwaiting: the pick was counted but it is not the order's last one
	// (PicksSeen < PicksNeeded); nothing was confirmed.
	PicksAwaiting ConfirmPicksOutcome = "AWAITING_LAST_PICK"
	// PicksNothingToConfirm: no ACTIVE reservation is left to confirm (all are
	// already CONFIRMED, REVOKED or EXPIRED). A successful no-op.
	PicksNothingToConfirm ConfirmPicksOutcome = "NOTHING_TO_CONFIRM"
	// PicksProcessed: this was the last pick; the order's reservations were
	// walked, see the counts.
	PicksProcessed ConfirmPicksOutcome = "PROCESSED"
)

// ConfirmPicksResult is the per-event summary: where the order's pick count
// stands and how many of its reservations ended in each state.
type ConfirmPicksResult struct {
	Outcome ConfirmPicksOutcome
	// PicksSeen is the order's completed-PICK count including this event, and
	// PicksNeeded the count at which the order is confirmed (ACTIVE +
	// CONFIRMED reservations). Zero when the event was not counted.
	PicksSeen   int
	PicksNeeded int
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

// ConfirmPicksForOrder turns "the LAST PICK task for order X completed" into
// the physical decrement of every ACTIVE reservation whose demand_ref is X
// (ADR 0035, superseding ADR 0032).
//
// A PICK task is per order LINE and every one publishes the same order_ref,
// while a Reservation has no line identity (only sku, quantity, demand_ref),
// so a task cannot be mapped to one reservation. The order's picks are
// counted instead (order_pick_progress) and the reservations are confirmed
// when the count reaches needed = ACTIVE + CONFIRMED reservations (REVOKED and
// EXPIRED never get picked). Earlier picks only record progress: confirming
// then would mark unpicked lines as picked with no undo, whereas confirming
// late is safe (the reservation keeps the stock unavailable meanwhile).
// Short picks are not modelled.
//
// The CloudEvents id claim, the counter increment and every ConfirmPick write
// (stock, bin, reservation, outbox rows) run in ONE UnitOfWork, so a failure
// rolls all of them back and the redelivery is applied in full. A redelivered
// event is short-circuited by the claim before it can touch the counter, and
// even a NEW id for the same order only finds reservations that are no longer
// ACTIVE.
type ConfirmPicksForOrder struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	// Confirm is the existing confirm-pick use case (*ConfirmPick).
	Confirm         reservationConfirmer
	ProcessedEvents ports.ProcessedEventRepo
	// PickProgress counts the order's completed PICK tasks, in the same
	// UnitOfWork as the claim.
	PickProgress ports.OrderPickProgressRepo
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
		return uc.countAndConfirm(ctx, in.OrderRef, &result)
	})
	if err != nil {
		return ConfirmPicksResult{}, err
	}
	uc.record(ctx, result)
	return result, nil
}

// countAndConfirm runs inside the claim's transaction: it counts this pick for
// orderRef and, when it is the order's last, confirms the ACTIVE reservations.
func (uc *ConfirmPicksForOrder) countAndConfirm(ctx context.Context, orderRef string, result *ConfirmPicksResult) error {
	reservations, err := uc.Reservations.FindByDemandRef(ctx, orderRef)
	if err != nil {
		return err
	}
	if len(reservations) == 0 {
		result.Outcome = PicksNoReservations
		return nil
	}
	// Deterministic order, so concurrent handlers lock rows alike.
	sort.Slice(reservations, func(i, j int) bool { return reservations[i].ID() < reservations[j].ID() })

	needed, active := confirmable(reservations)
	if needed == 0 {
		// Every reservation is REVOKED or EXPIRED: nothing can ever be
		// confirmed, so no progress row is left behind either.
		result.Outcome = PicksNothingToConfirm
		return nil
	}

	// Count this pick in the SAME transaction as the claim: a rollback
	// un-counts it, and a redelivery never reaches this line.
	picked, err := uc.PickProgress.RecordPick(ctx, orderRef, uc.Clock.Now())
	if err != nil {
		return err
	}
	result.PicksSeen, result.PicksNeeded = picked, needed

	switch {
	case picked < needed:
		// Not the last pick: confirming now would mark lines nobody has
		// picked yet as picked, with no undo (ADR 0035).
		result.Outcome = PicksAwaiting
		return nil
	case active == 0:
		// Everything confirmable is already CONFIRMED (an extra or late
		// event, or the REST route got there first).
		result.Outcome = PicksNothingToConfirm
		return nil
	}

	result.Outcome = PicksProcessed
	for _, res := range reservations {
		if err := uc.settle(ctx, res, result); err != nil {
			return err
		}
	}
	return nil
}

// confirmable returns how many of the order's reservations still await (or
// already got) a pick, ACTIVE + CONFIRMED, and how many of those are ACTIVE.
// REVOKED and EXPIRED reservations will never be picked for, so they do not
// count. An ACTIVE reservation already past its timeout still counts until a
// read resolves it to EXPIRED (ADR 0003, lazy expiry).
func confirmable(reservations []*reservation.Reservation) (needed, active int) {
	for _, res := range reservations {
		switch res.Status() {
		case reservation.StatusActive:
			needed++
			active++
		case reservation.StatusConfirmed:
			needed++
		}
	}
	return needed, active
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
