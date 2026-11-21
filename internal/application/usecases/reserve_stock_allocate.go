package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// allocate reserves qty across the given stock units, drawing in
// allocation.OrderFor (ADR 0050's forward pick face before reserve storage,
// then the deterministic tie-breakers, plus the container rule below) until
// the full amount is covered. It
// returns the allocations recorded and the units touched (mutated but not yet
// persisted; persisting is Execute's job, inside its UnitOfWork, so a later failure never leaves a
// partially-drawn reservation behind). It fails with ErrInsufficientUsable —
// before any caller-visible state has been persisted — when the units'
// combined usable quantity cannot cover qty. The order is allocation.OrderFor:
// ADR 0050's, with one covering container drawn first after the forward faces
// (ADR 0054); every allocation names the container its unit is in.
// carry (Task 7 review): the first sentence names OrderFor, as the code does.
func (uc *ReserveStock) allocate(ctx context.Context, sku shared.SKU, all []*stock.StockUnit, qty shared.Quantity, owner shared.OwnerRef) ([]reservation.Allocation, []*stock.StockUnit, error) {
	faces := allocation.ForwardFaces{}
	if uc.Forward != nil {
		var err error
		if faces, err = uc.Forward.ForwardFacesFor(ctx, sku); err != nil {
			return nil, nil, err
		}
	}
	units := allocation.OrderFor(reservableUnits(all, owner), faces, qty)
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
		if err := unit.ReserveFor(owner, take); err != nil {
			return nil, nil, err
		}
		allocations = append(allocations, reservation.Allocation{StockUnitID: unit.ID(), BinID: unit.BinID(), Quantity: take, ContainerLPN: unit.ContainerLPN()})
		touched = append(touched, unit)
		remaining, _ = remaining.Sub(take)
	}

	if remaining.Int() > 0 {
		return nil, nil, ErrInsufficientUsable
	}

	return allocations, touched, nil
}

// reservableUnits keeps the loose units of owner: stock of another owner never
// satisfies a reservation, and a sealed case is never split by a quantity
// reservation (ADR 0045). An empty owner keeps fleet-owned stock only.
func reservableUnits(units []*stock.StockUnit, owner shared.OwnerRef) []*stock.StockUnit {
	out := make([]*stock.StockUnit, 0, len(units))
	for _, unit := range units {
		if unit.ReservableBy(owner) {
			out = append(out, unit)
		}
	}
	return out
}
