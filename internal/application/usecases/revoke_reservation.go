package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// RevokeReservation cancels a reservation and returns its quantity to
// usable, so a failed physical delivery never strands an order.
type RevokeReservation struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	Metrics      ports.ReservationMetrics
	// UnitOfWork brackets every Save/Publish this use case makes
	// atomically (ADR 0017). Optional: nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

func (uc *RevokeReservation) Execute(ctx context.Context, reservationID string) error {
	res, err := uc.Reservations.FindByID(ctx, reservationID)
	if err != nil {
		return err
	}
	if res == nil {
		return ErrReservationNotFound
	}

	// Lazy expiry: resolve a timed-out ACTIVE reservation here too, so a
	// revoke attempt against one that has already timed out sees it as
	// genuinely Expired (ErrAlreadyResolved) rather than silently
	// succeeding a second, redundant release of its allocated quantity
	// (expireIfDue already returned it to usable and raised
	// ReservationExpired). An already-resolved reservation (Confirmed,
	// Revoked, or Expired) passes through unchanged.
	res, err = expireIfDue(ctx, uc.UnitOfWork, uc.Stock, uc.Reservations, uc.Events, uc.Clock, res)
	if err != nil {
		return err
	}

	if err := res.Revoke(); err != nil {
		return err
	}

	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		for _, alloc := range res.Allocations() {
			unit, err := uc.Stock.FindByID(ctx, alloc.StockUnitID)
			if err != nil {
				return err
			}
			if unit == nil {
				return ErrStockUnitNotFound
			}
			if err := unit.ReleaseReservation(alloc.Quantity); err != nil {
				return err
			}
			if err := uc.Stock.Save(ctx, unit); err != nil {
				return err
			}
		}

		if err := uc.Reservations.Save(ctx, res); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewReservationRevoked(uc.Clock.Now(), res.ID()))
	})
	if err != nil {
		return err
	}

	if uc.Metrics != nil {
		uc.Metrics.ReservationRevoked(ctx)
	}

	return nil
}
