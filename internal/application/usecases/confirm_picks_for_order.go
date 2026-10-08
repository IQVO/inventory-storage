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
	// LineNo is the order line this PICK task was for, when the producer
	// sends it (decision 18, ADR 0036); nil when unknown. With it the pick
	// confirms exactly that line's reservation; without it, or when no
	// reservation of the order carries a line, the ADR 0035 last-pick
	// counting applies.
	LineNo *int
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
	// PicksLineSettled: the event named a line and the reservations of that
	// line were walked, see the counts (per-line path, ADR 0036). Nothing
	// was counted.
	PicksLineSettled ConfirmPicksOutcome = "LINE_SETTLED"
	// PicksLineNotFound: the event named a line but no reservation of the
	// order carries it, and the order has no line-less reservation to fall
	// back to. A successful no-op.
	PicksLineNotFound ConfirmPicksOutcome = "LINE_NOT_FOUND"
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

// ConfirmPicksForOrder turns a completed PICK task for order X into the
// physical decrement of the right ACTIVE reservations of X.
//
// Per-line path (decision 18, ADR 0036). When the event carries the task's
// line_no, exactly the ACTIVE reservation(s) with (demand_ref = X, line_no =
// that line) are confirmed through ConfirmPick; CONFIRMED, REVOKED and EXPIRED
// ones are skipped (expired counted). Nothing is counted: a line that was not
// picked is never touched, so the "one pick early" edge of ADR 0035 does not
// exist for line-aware events.
//
// Counting path (ADR 0035, kept as the backward-compatible fallback). When
// the event has no line_no, or the event names a line that none of the
// order's reservations carries but the order has reservations made before
// line_no existed (NULL), the pick is counted against THOSE reservations
// (order_pick_progress) and they are confirmed when the count reaches
// needed = ACTIVE + CONFIRMED (REVOKED and EXPIRED never get picked). A PICK
// task is per order LINE and every one publishes the same order_ref, so
// without a line a task cannot be mapped to one reservation; earlier picks
// only record progress, because confirming then would mark unpicked lines as
// picked with no undo, whereas confirming late is safe (the reservation keeps
// the stock unavailable meanwhile). Short picks are not modelled.
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
	if in.LineNo != nil && *in.LineNo < 1 {
		return ConfirmPicksResult{}, fmt.Errorf("%w: line_no %d is not a line number", ErrMalformedPickCompletion, *in.LineNo)
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
		return uc.confirm(ctx, in, &result)
	})
	if err != nil {
		return ConfirmPicksResult{}, err
	}
	uc.record(ctx, result)
	return result, nil
}

// confirm runs inside the claim's transaction. It picks the per-line path when
// the event names a line some reservation of the order carries, and otherwise
// the ADR 0035 counting path (over the line-less reservations only when the
// event named a line).
func (uc *ConfirmPicksForOrder) confirm(ctx context.Context, in PickCompletion, result *ConfirmPicksResult) error {
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

	if in.LineNo != nil {
		if line := forLine(reservations, *in.LineNo); len(line) > 0 {
			return uc.settleLine(ctx, line, result)
		}
		// No reservation carries this line: only reservations made before
		// line_no existed can still be served, by counting (ADR 0035).
		reservations = withoutLine(reservations)
		if len(reservations) == 0 {
			result.Outcome = PicksLineNotFound
			return nil
		}
	}
	return uc.countAndConfirm(ctx, in.OrderRef, reservations, result)
}

// forLine returns the reservations stored for order line lineNo.
func forLine(reservations []*reservation.Reservation, lineNo int) []*reservation.Reservation {
	var out []*reservation.Reservation
	for _, res := range reservations {
		if n := res.LineNo(); n != nil && *n == lineNo {
			out = append(out, res)
		}
	}
	return out
}

// withoutLine returns the reservations that carry no line number.
func withoutLine(reservations []*reservation.Reservation) []*reservation.Reservation {
	var out []*reservation.Reservation
	for _, res := range reservations {
		if res.LineNo() == nil {
			out = append(out, res)
		}
	}
	return out
}

// settleLine is the per-line path: it walks the line's reservations, confirming
// the ACTIVE ones and counting the rest, and writes no order_pick_progress row.
func (uc *ConfirmPicksForOrder) settleLine(ctx context.Context, line []*reservation.Reservation, result *ConfirmPicksResult) error {
	result.Outcome = PicksLineSettled
	for _, res := range line {
		if err := uc.settle(ctx, res, result); err != nil {
			return err
		}
	}
	return nil
}

// countAndConfirm runs inside the claim's transaction: it counts this pick for
// orderRef against reservations (the whole order, or only its line-less
// reservations) and, when it is the last, confirms the ACTIVE ones.
func (uc *ConfirmPicksForOrder) countAndConfirm(ctx context.Context, orderRef string, reservations []*reservation.Reservation, result *ConfirmPicksResult) error {
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
