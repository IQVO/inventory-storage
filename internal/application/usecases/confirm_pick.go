package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ConfirmPick consumes a reservation: reserved quantity is physically
// removed from its bin(s) and the corresponding bin capacity is released.
type ConfirmPick struct {
	Stock        ports.StockRepo
	Locations    ports.LocationRepo
	Reservations ports.ReservationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	// UnitOfWork brackets every Save/Publish this use case makes
	// atomically (ADR 0017). Optional: nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

func (uc *ConfirmPick) Execute(ctx context.Context, reservationID string) error {
	res, err := uc.Reservations.FindByID(ctx, reservationID)
	if err != nil {
		return err
	}
	if res == nil {
		return ErrReservationNotFound
	}

	// Lazy expiry: a pick confirmation is also a reservation lookup, so a
	// reservation that has silently timed out (still ACTIVE in storage)
	// is transitioned to Expired here, its quantity returned to usable,
	// and ReservationExpired raised — Confirm(now) below then correctly
	// rejects it via ErrAlreadyResolved rather than never discovering the
	// timeout at all.
	res, err = expireIfDue(ctx, uc.UnitOfWork, uc.Stock, uc.Reservations, uc.Events, uc.Clock, res)
	if err != nil {
		return err
	}

	now := uc.Clock.Now()
	if err := res.Confirm(now); err != nil {
		return err
	}

	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		for _, alloc := range res.Allocations() {
			unit, err := uc.Stock.FindByID(ctx, alloc.StockUnitID)
			if err != nil {
				return err
			}
			if unit == nil {
				return ErrStockUnitNotFound
			}

			binID := unit.BinID()
			if err := unit.Pick(alloc.Quantity); err != nil {
				return err
			}
			if err := uc.Stock.Save(ctx, unit); err != nil {
				return err
			}

			bin, err := uc.Locations.FindByID(ctx, binID)
			if err != nil {
				return err
			}
			if bin == nil {
				return ErrBinNotFound
			}
			if err := bin.Release(alloc.Quantity); err != nil {
				return err
			}
			if err := uc.Locations.Save(ctx, bin); err != nil {
				return err
			}
		}

		if err := uc.Reservations.Save(ctx, res); err != nil {
			return err
		}

		return uc.Events.Publish(ctx, shared.NewStockPicked(now, res.ID(), res.SKU(), res.Quantity()))
	})
}
