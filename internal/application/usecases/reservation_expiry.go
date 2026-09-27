package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// expireIfDue implements LAZY reservation expiry: it is called by every use
// case that looks up a Reservation (GetReservationsByDemandRef,
// ReserveStock's own idempotency lookup, RevokeReservation), so a timed-out
// reservation is discovered and resolved at the moment it is next read,
// rather than by a background sweep job (a decision made explicitly over a
// sweeper — see ADR on lazy reservation expiry).
//
// If res is nil, not Active, or not yet past its timeout, it is returned
// unchanged — this makes expireIfDue safe to call unconditionally on every
// lookup without a caller-side guard, and guarantees an already-resolved
// reservation (Confirmed, Revoked, or already Expired) is never
// re-processed.
//
// When a reservation genuinely has timed out, this:
//  1. returns each of its allocated quantities to the owning StockUnit's
//     usable pool (mirrors RevokeReservation's own release loop — Expire()
//     itself only flips the Reservation's own status, exactly like Revoke()
//     does, so the StockUnit side effect is this caller's responsibility,
//     not the aggregate's);
//  2. transitions the Reservation to Expired and persists it;
//  3. publishes ReservationExpired through the same ports.EventPublisher
//     (and therefore the same outbox/Kafka wiring) every other domain event
//     in this service already uses — no new transport.
//
// Every write above (the released StockUnit(s), the Reservation, and the
// published event) runs inside one atomic scope bracketed by uow (ADR
// 0017, transactional outbox): a nil uow runs them back to back, matching
// every other use case's atomically convention.
func expireIfDue(ctx context.Context, uow ports.UnitOfWork, stock ports.StockRepo, reservations ports.ReservationRepo, pub ports.EventPublisher, clock ports.Clock, res *reservation.Reservation) (*reservation.Reservation, error) {
	if res == nil || res.Status() != reservation.StatusActive {
		return res, nil
	}

	now := clock.Now()
	if !res.IsExpired(now) {
		return res, nil
	}

	err := atomically(ctx, uow, func(ctx context.Context) error {
		for _, alloc := range res.Allocations() {
			unit, err := stock.FindByID(ctx, alloc.StockUnitID)
			if err != nil {
				return err
			}
			if unit == nil {
				return ErrStockUnitNotFound
			}
			if err := unit.ReleaseReservation(alloc.Quantity); err != nil {
				return err
			}
			if err := stock.Save(ctx, unit); err != nil {
				return err
			}
		}

		if err := res.Expire(); err != nil {
			return err
		}
		if err := reservations.Save(ctx, res); err != nil {
			return err
		}
		return pub.Publish(ctx, shared.NewReservationExpired(now, res.ID()))
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// expireAllIfDue applies expireIfDue to every reservation in results,
// in place order, so a caller that looks up several reservations at once
// (GetReservationsByDemandRef) gets every one of them lazily resolved
// rather than just the first. Each resolved reservation's writes are
// bracketed in their OWN atomic scope (one outbox commit per expired
// reservation), rather than one scope for the whole batch — a later
// reservation's expiry failing must not roll back an earlier one that
// already committed.
func expireAllIfDue(ctx context.Context, uow ports.UnitOfWork, stock ports.StockRepo, reservations ports.ReservationRepo, pub ports.EventPublisher, clock ports.Clock, results []*reservation.Reservation) ([]*reservation.Reservation, error) {
	for i, res := range results {
		updated, err := expireIfDue(ctx, uow, stock, reservations, pub, clock, res)
		if err != nil {
			return nil, err
		}
		results[i] = updated
	}
	return results, nil
}
