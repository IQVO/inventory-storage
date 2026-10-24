package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// StowContainer places a sealed container in a bin: scan the LPN, scan the
// location (ADR 0045 section 2). It requires the container to be SEALED, the
// bin to exist, to be at a known site and to accept a sealed case (a CASE or
// PALLET bin; a bin with no recorded handling unit fails closed). The first
// stow sets the container's site from the bin (the receipt names none) and
// creates the SEALED_CASE StockUnit that makes the case's eaches usable; the
// bin's capacity check is in eaches. Stowing an already stowed container into
// another bin of the same site moves its unit; into the bin it is already in,
// it changes nothing.
type StowContainer struct {
	Containers    ports.ContainerRepo
	Stock         ports.StockRepo
	Locations     ports.LocationRepo
	HandlingUnits ports.BinHandlingUnitLookup
	Events        ports.EventPublisher
	Clock         ports.Clock
	// Placement applies the hazmat, temperature and segregation rules a plain
	// stow applies (a StowStock built with only Stock, Classifications and
	// LocationLookup). Optional: nil skips them.
	Placement *StowStock
	// UnitOfWork brackets every write atomically (ADR 0017). Optional.
	UnitOfWork ports.UnitOfWork
	// Levels raises BinStockLevelChanged for the owner's share (ADR 0040). Optional.
	Levels *BinLevelEmitter
}

// Execute stows the container with this LPN into binID.
func (uc *StowContainer) Execute(ctx context.Context, lpn string, binID shared.BinId) (*container.Container, error) {
	var stowed *container.Container
	err := uc.Levels.Atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		c, err := uc.stow(ctx, lpn, binID)
		stowed = c
		return err
	})
	if err != nil {
		return nil, err
	}
	return stowed, nil
}

func (uc *StowContainer) stow(ctx context.Context, lpn string, binID shared.BinId) (*container.Container, error) {
	c, err := loadContainer(ctx, uc.Containers, lpn)
	if err != nil {
		return nil, err
	}
	if c.State() != container.StateSealed {
		return nil, container.ErrContainerNotSealed
	}
	bin, err := uc.destination(ctx, c, binID)
	if err != nil {
		return nil, err
	}
	switch {
	case c.BinID() == binID:
		return c, nil
	case c.BinID() == "":
		return c, uc.firstStow(ctx, c, bin)
	}
	return c, uc.restow(ctx, c, bin)
}

// destination validates the bin for a sealed case and returns it.
func (uc *StowContainer) destination(ctx context.Context, c *container.Container, binID shared.BinId) (*location.Bin, error) {
	bin, err := uc.Locations.FindByID(ctx, binID)
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}
	site := bin.EffectiveSite()
	if site == "" || (c.SiteID() != "" && c.SiteID() != site) {
		return nil, container.ErrContainerNotAtSite
	}
	hu, err := uc.HandlingUnits.HandlingUnit(ctx, binID)
	if err != nil {
		return nil, err
	}
	if !hu.AcceptsSealedCase() {
		return nil, container.ErrBinHandlingUnitMismatch
	}
	if uc.Placement != nil {
		if err := uc.Placement.checkPlacement(ctx, c.SKU(), binID); err != nil {
			return nil, err
		}
		if err := uc.Placement.checkSegregation(ctx, c.SKU(), binID); err != nil {
			return nil, err
		}
	}
	return bin, nil
}

func (uc *StowContainer) firstStow(ctx context.Context, c *container.Container, bin *location.Bin) error {
	unitID, err := uc.Stock.NextID(ctx)
	if err != nil {
		return err
	}
	site := bin.EffectiveSite()
	unit, err := stock.NewSealedCaseUnit(unitID, c.SKU(), bin.ID(), c.Quantity(), site, c.OwnerRef())
	if err != nil {
		return err
	}
	if err := bin.Occupy(c.Quantity()); err != nil {
		return err
	}
	if err := c.Stow(bin.ID(), site, unitID); err != nil {
		return err
	}
	if err := uc.save(ctx, c, bin, unit); err != nil {
		return err
	}
	if err := uc.Levels.TrackFor(ctx, shared.BinLevelStow, bin.ID(), c.SKU(), c.OwnerRef()); err != nil {
		return err
	}
	now := uc.Clock.Now()
	if err := uc.Events.Publish(ctx, shared.NewItemStowed(now, c.SKU(), bin.ID(), c.Quantity())); err != nil {
		return err
	}
	if err := uc.Events.Publish(ctx, shared.NewLocationRecorded(now, unitID, bin.ID())); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, shared.NewContainerStowed(now, c.LPN(), c.SKU(), c.Quantity(), c.OwnerRef(), site, bin.ID()))
}

func (uc *StowContainer) save(ctx context.Context, c *container.Container, bin *location.Bin, unit *stock.StockUnit) error {
	if err := uc.Locations.Save(ctx, bin); err != nil {
		return err
	}
	if err := uc.Stock.Save(ctx, unit); err != nil {
		return err
	}
	return uc.Containers.Save(ctx, c)
}

// restow moves the unit of an already stowed container to another bin of the
// same site. A reserved case cannot move: the reservation names its pick bin.
func (uc *StowContainer) restow(ctx context.Context, c *container.Container, to *location.Bin) error {
	unit, err := uc.Stock.FindByID(ctx, c.StockUnitID())
	if err != nil {
		return err
	}
	if unit == nil {
		return ErrStockUnitNotFound
	}
	if unit.Reserved().Int() > 0 {
		return container.ErrContainerReserved
	}
	from, err := uc.Locations.FindByID(ctx, c.BinID())
	if err != nil {
		return err
	}
	if from == nil {
		return ErrBinNotFound
	}
	fromID := from.ID()
	if err := moveBinOccupancy(from, to, c.Quantity()); err != nil {
		return err
	}
	if err := unit.MoveTo(to.ID(), to.EffectiveSite()); err != nil {
		return err
	}
	if err := c.Stow(to.ID(), to.EffectiveSite(), unit.ID()); err != nil {
		return err
	}
	if err := saveBinsOrdered(ctx, uc.Locations, from, to); err != nil {
		return err
	}
	if err := uc.Stock.Save(ctx, unit); err != nil {
		return err
	}
	if err := uc.Containers.Save(ctx, c); err != nil {
		return err
	}
	for _, id := range []shared.BinId{fromID, to.ID()} {
		if err := uc.Levels.TrackFor(ctx, shared.BinLevelRelocate, id, c.SKU(), c.OwnerRef()); err != nil {
			return err
		}
	}
	return uc.Events.Publish(ctx, shared.NewContainerStowed(uc.Clock.Now(), c.LPN(), c.SKU(), c.Quantity(), c.OwnerRef(), to.EffectiveSite(), to.ID()))
}

// loadContainer reads a container or returns ErrContainerNotFound.
func loadContainer(ctx context.Context, containers ports.ContainerRepo, lpn string) (*container.Container, error) {
	c, err := containers.FindByLPN(ctx, lpn)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, ErrContainerNotFound
	}
	return c, nil
}

// saveBinsOrdered saves two bins in ascending id order, so two opposite
// moves lock the bin rows in the same order.
func saveBinsOrdered(ctx context.Context, locations ports.LocationRepo, a, b *location.Bin) error {
	if b.ID() < a.ID() {
		a, b = b, a
	}
	if err := locations.Save(ctx, a); err != nil {
		return err
	}
	return locations.Save(ctx, b)
}

// moveBinOccupancy occupies the destination and releases the source; when the
// release fails the destination is restored, so neither bin changes.
func moveBinOccupancy(from, to *location.Bin, qty shared.Quantity) error {
	if err := to.Occupy(qty); err != nil {
		return err
	}
	if err := from.Release(qty); err != nil {
		_ = to.Release(qty)
		return err
	}
	return nil
}
