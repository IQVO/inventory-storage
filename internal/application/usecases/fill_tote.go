package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// FillToteCommand is one decant into a tote: scan the item, scan the tote.
type FillToteCommand struct {
	LPN      string
	SKU      shared.SKU
	Quantity shared.Quantity
	OwnerRef shared.OwnerRef
}

// FillTote stows loose eaches into a tote at the tote's current bin (ADR
// 0054, WCS flow 2 "decant fills container C"). It is a stow: the same
// placement and capacity checks and the same events as StowStock, plus
// ToteFilled. The tote is saved (version-guarded) so a concurrent StowTote
// of the same tote loses with ErrConcurrentModification instead of leaving a
// unit behind in the old bin.
type FillTote struct {
	Totes     ports.ToteRepo
	Stock     ports.StockRepo
	Locations ports.LocationRepo
	Events    ports.EventPublisher
	Clock     ports.Clock
	// Placement applies the hazmat, temperature and segregation rules of a
	// plain stow. Optional: nil skips them.
	Placement *StowStock
	// UnitOfWork brackets every write atomically (ADR 0017). Optional.
	UnitOfWork ports.UnitOfWork
	// Levels raises BinStockLevelChanged (STOW) for the bin. Optional.
	Levels *BinLevelEmitter
}

// Execute fills the tote and returns the new stock unit.
func (uc *FillTote) Execute(ctx context.Context, cmd FillToteCommand) (*stock.StockUnit, error) {
	var unit *stock.StockUnit
	err := uc.Levels.Atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		u, err := uc.fill(ctx, cmd)
		unit = u
		return err
	})
	if err != nil {
		return nil, err
	}
	return unit, nil
}

func (uc *FillTote) fill(ctx context.Context, cmd FillToteCommand) (*stock.StockUnit, error) {
	t, err := loadTote(ctx, uc.Totes, cmd.LPN)
	if err != nil {
		return nil, err
	}
	if !t.IsPlaced() {
		return nil, container.ErrToteNotPlaced
	}
	bin, err := uc.Locations.FindByID(ctx, t.BinID())
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}
	if err := uc.Placement.placeable(ctx, cmd.SKU, bin.ID()); err != nil {
		return nil, err
	}
	id, err := uc.Stock.NextID(ctx)
	if err != nil {
		return nil, err
	}
	unit, err := stock.NewToteUnit(id, cmd.SKU, bin.ID(), cmd.Quantity, t.SiteID(), cmd.OwnerRef, t.LPN())
	if err != nil {
		return nil, err
	}
	if err := bin.Occupy(cmd.Quantity); err != nil {
		return nil, err
	}
	if err := uc.save(ctx, t, unit, bin.ID()); err != nil {
		return nil, err
	}
	if err := uc.Locations.Save(ctx, bin); err != nil {
		return nil, err
	}
	return unit, uc.announce(ctx, t, unit)
}

func (uc *FillTote) save(ctx context.Context, t *container.Tote, unit *stock.StockUnit, bin shared.BinId) error {
	if err := uc.Stock.Save(ctx, unit); err != nil {
		return err
	}
	if err := uc.Totes.Save(ctx, t); err != nil {
		return err
	}
	return uc.Levels.TrackFor(ctx, shared.BinLevelStow, bin, unit.SKU(), unit.OwnerRef())
}

func (uc *FillTote) announce(ctx context.Context, t *container.Tote, unit *stock.StockUnit) error {
	now := uc.Clock.Now()
	if err := uc.Events.Publish(ctx, shared.NewItemStowed(now, unit.SKU(), unit.BinID(), unit.Quantity())); err != nil {
		return err
	}
	if err := uc.Events.Publish(ctx, shared.NewLocationRecorded(now, unit.ID(), unit.BinID())); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, shared.NewToteFilled(now, t.LPN(), unit.SKU(), unit.Quantity(), unit.OwnerRef(), t.SiteID(), unit.BinID(), unit.ID()))
}

// loadTote reads a tote or returns ErrToteNotFound.
func loadTote(ctx context.Context, totes ports.ToteRepo, lpn string) (*container.Tote, error) {
	t, err := totes.FindByLPN(ctx, lpn)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrToteNotFound
	}
	return t, nil
}

// placeable runs StowStock's placement and segregation rules for sku in bin.
// A nil StowStock applies none (the optional Placement of the tote use cases).
func (uc *StowStock) placeable(ctx context.Context, sku shared.SKU, bin shared.BinId) error {
	if uc == nil {
		return nil
	}
	if err := uc.checkPlacement(ctx, sku, bin); err != nil {
		return err
	}
	return uc.checkSegregation(ctx, sku, bin)
}
