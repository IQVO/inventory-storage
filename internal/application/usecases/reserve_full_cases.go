package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// ReserveFullCasesCommand asks for whole sealed cases of one SKU.
type ReserveFullCasesCommand struct {
	SKU       shared.SKU
	Cases     int
	DemandRef string
	LineNo    *int
	// OwnerRef selects whose cases may be reserved; empty = fleet-owned only.
	OwnerRef shared.OwnerRef
}

// ReserveFullCases reserves `Cases` whole SEALED, stowed containers of a SKU
// for one owner (ADR 0045 section 2). It is a revocable reservation like any
// other (ADR 0003): each allocation binds a container's SEALED_CASE unit in
// full and records its bin as the pick location. It never allocates part of a
// case, never allocates from LOOSE stock, and never allocates another owner's
// case. Confirming the pick removes the unit and empties the container.
type ReserveFullCases struct {
	// Reserve supplies the stock and reservation repos, clock, outbox and
	// unit of work of the quantity reservation, so both commit the same way.
	Reserve    *ReserveStock
	Containers ports.ContainerRepo
}

// Execute reserves the cases and returns the reservation. A replay (an ACTIVE
// reservation of the same demand, line, SKU, owner and case count made of
// sealed cases) returns that reservation instead of allocating again.
func (uc *ReserveFullCases) Execute(ctx context.Context, cmd ReserveFullCasesCommand) (*reservation.Reservation, error) {
	if cmd.Cases < 1 {
		return nil, shared.ErrZeroQuantity
	}
	if cmd.LineNo != nil && !reservation.ValidLineNo(*cmd.LineNo) {
		return nil, reservation.ErrInvalidLineNo
	}
	if res, err := uc.replay(ctx, cmd); err != nil || res != nil {
		return res, err
	}
	allocations, touched, err := uc.pickCases(ctx, cmd)
	if err != nil {
		return nil, err
	}
	id, err := uc.Reserve.Reservations.NextID(ctx)
	if err != nil {
		return nil, err
	}
	now := uc.Reserve.Clock.Now()
	total := shared.Quantity(0)
	for _, a := range allocations {
		total = total.Add(a.Quantity)
	}
	res, err := reservation.NewForLine(id, cmd.SKU, total, cmd.DemandRef, cmd.LineNo, allocations, now, uc.Reserve.timeout())
	if err != nil {
		return nil, err
	}
	res.ForOwner(cmd.OwnerRef)
	if err := uc.Reserve.commit(ctx, res, touched, now); err != nil {
		return nil, err
	}
	return res, nil
}

// pickCases chooses cmd.Cases whole cases, oldest container first, without
// changing anything persistent.
func (uc *ReserveFullCases) pickCases(ctx context.Context, cmd ReserveFullCasesCommand) ([]reservation.Allocation, []*stock.StockUnit, error) {
	containers, err := uc.Containers.FindStowedBySKU(ctx, cmd.SKU, cmd.OwnerRef)
	if err != nil {
		return nil, nil, err
	}
	var touched []*stock.StockUnit
	for _, c := range containers {
		if len(touched) == cmd.Cases {
			break
		}
		unit, err := uc.Reserve.Stock.FindByID(ctx, c.StockUnitID())
		if err != nil {
			return nil, nil, err
		}
		if unit != nil && unit.CanReserveWhole(cmd.OwnerRef) {
			touched = append(touched, unit)
		}
	}
	if len(touched) < cmd.Cases {
		return nil, nil, ErrInsufficientUsable
	}
	// Only now, with every case secured, change the units.
	allocations := make([]reservation.Allocation, 0, len(touched))
	for _, unit := range touched {
		if err := unit.ReserveWhole(cmd.OwnerRef); err != nil {
			return nil, nil, err
		}
		allocations = append(allocations, reservation.Allocation{StockUnitID: unit.ID(), BinID: unit.BinID(), Quantity: unit.Reserved()})
	}
	return allocations, touched, nil
}

// replay finds an ACTIVE reservation that is what this command would create.
func (uc *ReserveFullCases) replay(ctx context.Context, cmd ReserveFullCasesCommand) (*reservation.Reservation, error) {
	existing, err := uc.Reserve.Reservations.FindByDemandRef(ctx, cmd.DemandRef)
	if err != nil {
		return nil, err
	}
	existing, err = expireAllIfDue(ctx, uc.Reserve.UnitOfWork, uc.Reserve.Levels, uc.Reserve.Stock, uc.Reserve.Reservations, uc.Reserve.Events, uc.Reserve.Clock, existing)
	if err != nil {
		return nil, err
	}
	for _, res := range existing {
		same := res.Status() == reservation.StatusActive && res.SKU() == cmd.SKU && res.OwnerRef() == cmd.OwnerRef &&
			len(res.Allocations()) == cmd.Cases && lineMatches(res.LineNo(), cmd.LineNo)
		if !same {
			continue
		}
		whole, err := uc.allSealedCases(ctx, res)
		if err != nil {
			return nil, err
		}
		if whole {
			return res, nil
		}
	}
	return nil, nil
}

func lineMatches(have, want *int) bool {
	if have == nil || want == nil {
		return true
	}
	return *have == *want
}

func (uc *ReserveFullCases) allSealedCases(ctx context.Context, res *reservation.Reservation) (bool, error) {
	for _, a := range res.Allocations() {
		unit, err := uc.Reserve.Stock.FindByID(ctx, a.StockUnitID)
		if err != nil {
			return false, err
		}
		if unit == nil || unit.PackState() != stock.PackSealedCase {
			return false, nil
		}
	}
	return true, nil
}
