package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// GetReservationsByDemandRef looks up every Reservation aggregate ever
// created against a given demand reference (an external system's order+line
// reference, e.g. from order-management). A demandRef can have multiple
// reservations across its lifetime — one revoked, a retry that succeeded —
// so the result is a slice of domain aggregates, not a single value or a DTO
// (DTOs are the HTTP adapter's concern, not this layer's).
//
// This is the service's read path for a reservation by demand reference, so
// it is where LAZY expiry is enforced: any ACTIVE result whose timeout has
// elapsed is transitioned to Expired, has its allocated quantity returned to
// usable, and raises ReservationExpired — all before the (now-current)
// results are returned to the caller. Stock/Events/Clock are required for
// this; a nil Clock would panic, matching this codebase's convention of not
// defending against a mis-wired composition root.
type GetReservationsByDemandRef struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	// UnitOfWork brackets the writes expireAllIfDue makes when resolving
	// a timed-out reservation atomically (ADR 0017). Optional: nil means
	// "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

func (uc *GetReservationsByDemandRef) Execute(ctx context.Context, demandRef string) ([]*reservation.Reservation, error) {
	results, err := uc.Reservations.FindByDemandRef(ctx, demandRef)
	if err != nil {
		return nil, err
	}
	return expireAllIfDue(ctx, uc.UnitOfWork, uc.Stock, uc.Reservations, uc.Events, uc.Clock, results)
}
