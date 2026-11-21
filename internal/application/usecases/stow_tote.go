package usecases

import (
	"context"
	"sort"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// StowTote gives a tote and all its contents a new logical location (ADR
// 0054, POST /totes/{lpn}/stowages): scan the tote, scan the location. In
// ONE unit of work the destination is occupied by the contents' eaches, the
// source released, every unit moved, the tote saved; BinStockLevelChanged is
// raised for both bins and ToteStowed published. Into the bin it is already
// in, it changes nothing. A tote holding reserved stock cannot move: each
// allocation names its pick bin (ADR 0025). A goods-to-person trip is not a
// stow: the tote keeps its logical location (WCS seam decision 2).
type StowTote struct {
	Totes     ports.ToteRepo
	Contents  ports.ContainerStock
	Stock     ports.StockRepo
	Locations ports.LocationRepo
	Events    ports.EventPublisher
	Clock     ports.Clock
	// Placement applies a plain stow's placement rules to every SKU of the
	// tote at the destination. Optional: nil skips them.
	Placement *StowStock
	// UnitOfWork brackets every write atomically (ADR 0017). Optional.
	UnitOfWork ports.UnitOfWork
	// Levels raises BinStockLevelChanged (RELOCATE) for both bins. Optional.
	Levels *BinLevelEmitter
}

// Execute stows the tote lpn into binID.
func (uc *StowTote) Execute(ctx context.Context, lpn string, binID shared.BinId) (*container.Tote, error) {
	var stowed *container.Tote
	err := uc.Levels.Atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		t, err := uc.stow(ctx, lpn, binID)
		stowed = t
		return err
	})
	if err != nil {
		return nil, err
	}
	return stowed, nil
}

func (uc *StowTote) stow(ctx context.Context, lpn string, binID shared.BinId) (*container.Tote, error) {
	t, err := loadTote(ctx, uc.Totes, lpn)
	if err != nil {
		return nil, err
	}
	to, err := uc.Locations.FindByID(ctx, binID)
	if err != nil {
		return nil, err
	}
	if to == nil {
		return nil, ErrBinNotFound
	}
	units, err := uc.Contents.FindByContainer(ctx, lpn)
	if err != nil {
		return nil, err
	}
	units = present(units)
	if err := uc.movable(ctx, units, to.ID()); err != nil {
		return nil, err
	}
	fromID := t.BinID()
	if err := t.PlaceIn(to.ID(), to.EffectiveSite()); err != nil {
		return nil, err
	}
	if fromID == to.ID() {
		return t, nil
	}
	if err := uc.move(ctx, units, fromID, to); err != nil {
		return nil, err
	}
	if err := uc.Totes.Save(ctx, t); err != nil {
		return nil, err
	}
	return t, uc.announce(ctx, t, units, fromID)
}

// movable refuses a tote holding reserved stock and applies the placement
// rules of every SKU at the destination.
func (uc *StowTote) movable(ctx context.Context, units []*stock.StockUnit, to shared.BinId) error {
	for _, u := range units {
		if u.Reserved().Int() > 0 {
			return container.ErrToteReserved
		}
		if err := uc.Placement.placeable(ctx, u.SKU(), to); err != nil {
			return err
		}
	}
	return nil
}

// move transfers the contents' occupancy and moves every unit to to.
func (uc *StowTote) move(ctx context.Context, units []*stock.StockUnit, fromID shared.BinId, to *location.Bin) error {
	total := shared.Quantity(0)
	for _, u := range units {
		total = total.Add(u.Quantity())
	}
	if total.Int() == 0 {
		return nil
	}
	from, err := uc.Locations.FindByID(ctx, fromID)
	if err != nil {
		return err
	}
	if from == nil {
		return ErrBinNotFound
	}
	if err := moveBinOccupancy(from, to, total); err != nil {
		return err
	}
	if err := saveBinsOrdered(ctx, uc.Locations, from, to); err != nil {
		return err
	}
	for _, u := range units {
		if err := u.MoveTo(to.ID(), to.EffectiveSite()); err != nil {
			return err
		}
		if err := uc.Stock.Save(ctx, u); err != nil {
			return err
		}
		for _, bin := range []shared.BinId{fromID, to.ID()} {
			if err := uc.Levels.TrackFor(ctx, shared.BinLevelRelocate, bin, u.SKU(), u.OwnerRef()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (uc *StowTote) announce(ctx context.Context, t *container.Tote, units []*stock.StockUnit, fromID shared.BinId) error {
	now := uc.Clock.Now()
	for _, u := range units {
		if err := uc.Events.Publish(ctx, shared.NewLocationRecorded(now, u.ID(), u.BinID())); err != nil {
			return err
		}
	}
	return uc.Events.Publish(ctx, shared.NewToteStowed(now, t.LPN(), t.SiteID(), fromID, t.BinID(), toteLines(units)))
}

// toteLines sums the contents per (sku, owner), ordered by SKU then owner.
func toteLines(units []*stock.StockUnit) []shared.ToteLine {
	type key struct {
		sku   shared.SKU
		owner shared.OwnerRef
	}
	sums := map[key]shared.Quantity{}
	for _, u := range units {
		k := key{u.SKU(), u.OwnerRef()}
		sums[k] = sums[k].Add(u.Quantity())
	}
	lines := make([]shared.ToteLine, 0, len(sums))
	for k, q := range sums {
		lines = append(lines, shared.ToteLine{SKU: k.sku, Quantity: q, OwnerRef: k.owner})
	}
	sort.Slice(lines, func(i, j int) bool {
		if lines[i].SKU != lines[j].SKU {
			return lines[i].SKU < lines[j].SKU
		}
		return lines[i].OwnerRef < lines[j].OwnerRef
	})
	return lines
}
