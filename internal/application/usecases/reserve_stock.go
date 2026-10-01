package usecases

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// DefaultReservationTimeout is used when a use case is constructed with a
// zero Timeout.
const DefaultReservationTimeout = 30 * time.Minute

// ReserveStock revocably binds a quantity of a SKU's usable inventory to
// demand. It does not bind to one specific StockUnit permanently: the
// allocation it draws from is recorded on the Reservation so a later revoke
// returns exactly that quantity, but a future reserve is free to draw from a
// different holding.
type ReserveStock struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	Metrics      ports.ReservationMetrics
	Timeout      time.Duration
	// UnitOfWork brackets every Save/Publish this use case makes
	// atomically (ADR 0017), including the ones expireAllIfDue/expireIfDue
	// make internally. Optional: nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

func (uc *ReserveStock) Execute(ctx context.Context, sku shared.SKU, qty shared.Quantity, demandRef string) (*reservation.Reservation, error) {
	if qty.Int() <= 0 {
		return nil, shared.ErrZeroQuantity
	}

	res, err := uc.activeReservationFor(ctx, demandRef)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return res, nil
	}

	units, err := uc.Stock.FindBySKU(ctx, sku)
	if err != nil {
		return nil, err
	}

	allocations, touched, err := uc.allocate(units, qty)
	if err != nil {
		return nil, err
	}

	for _, unit := range touched {
		if err := uc.Stock.Save(ctx, unit); err != nil {
			return nil, err
		}
	}

	id, err := uc.Reservations.NextID(ctx)
	if err != nil {
		return nil, err
	}

	timeout := uc.Timeout
	if timeout <= 0 {
		timeout = DefaultReservationTimeout
	}

	now := uc.Clock.Now()
	res, err = reservation.New(id, sku, qty, demandRef, allocations, now, timeout)
	if err != nil {
		return nil, err
	}

	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Reservations.Save(ctx, res); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewStockReserved(now, res.ID(), sku, qty, demandRef))
	})
	if err != nil {
		return nil, err
	}

	if uc.Metrics != nil {
		uc.Metrics.ReservationCreated(ctx)
	}

	return res, nil
}

// activeReservationFor implements the idempotency guard for Execute:
// a client-side retry after a dropped response (e.g. the first call's
// Reservation was created and stock allocated, but the response never
// reached the caller) must not create a second Reservation and
// double-reserve stock for the same demandRef. If an ACTIVE reservation
// already exists for this demandRef, treat this call as the retry and hand
// back that same reservation instead of allocating again. A demandRef
// legitimately has multiple reservations across its lifetime (revoke +
// retry), so this only short-circuits when an unresolved one is still open —
// once it's revoked/confirmed/expired, a new demandRef call is a genuine new
// reservation attempt, not a retry.
//
// This is a best-effort, not a hard uniqueness guarantee: two concurrent
// first-attempts for the same demandRef racing this check could still both
// pass it before either has saved. Closing that window would need a DB-level
// constraint; see REST_AUDIT.md's idempotency notes for the accepted scope
// here. It returns nil when no ACTIVE reservation remains for demandRef.
func (uc *ReserveStock) activeReservationFor(ctx context.Context, demandRef string) (*reservation.Reservation, error) {
	existing, err := uc.Reservations.FindByDemandRef(ctx, demandRef)
	if err != nil {
		return nil, err
	}
	// Lazy expiry: this is a read of every reservation ever created
	// against demandRef, so it is a point where a timed-out ACTIVE
	// reservation must be resolved before being reasoned about — a
	// caller retrying against a demandRef whose only "active" match has
	// actually timed out must fall through to a genuine new reservation
	// attempt, not be handed back an expired one as if it were live.
	existing, err = expireAllIfDue(ctx, uc.UnitOfWork, uc.Stock, uc.Reservations, uc.Events, uc.Clock, existing)
	if err != nil {
		return nil, err
	}
	for _, res := range existing {
		if res.Status() == reservation.StatusActive {
			return res, nil
		}
	}
	return nil, nil
}

// allocate reserves qty across the given stock units, greedily drawing from
// each unit's usable quantity until the full amount is covered. It returns
// the allocations recorded and the units touched (mutated but not yet
// persisted; persisting is Execute's job, so a later failure never leaves a
// partially-drawn reservation behind). It fails with ErrInsufficientUsable —
// before any caller-visible state has been persisted — when the units'
// combined usable quantity cannot cover qty.
func (uc *ReserveStock) allocate(units []*stock.StockUnit, qty shared.Quantity) ([]reservation.Allocation, []*stock.StockUnit, error) {
	totalUsable := shared.Quantity(0)
	for _, unit := range units {
		totalUsable = totalUsable.Add(unit.Usable())
	}
	if qty.GreaterThan(totalUsable) {
		return nil, nil, ErrInsufficientUsable
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
			return nil, nil, err
		}
		allocations = append(allocations, reservation.Allocation{StockUnitID: unit.ID(), BinID: unit.BinID(), Quantity: take})
		touched = append(touched, unit)
		remaining, _ = remaining.Sub(take)
	}

	if remaining.Int() > 0 {
		return nil, nil, ErrInsufficientUsable
	}

	return allocations, touched, nil
}
