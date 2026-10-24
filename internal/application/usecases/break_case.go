package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// BreakCase opens a stowed, sealed case and moves its eaches to loose stock in
// looseBin (ADR 0045 section 3). It is the only way a sealed case becomes
// loose eaches and it is explicit, never inferred from a pick.
//
// It conserves quantity: in ONE UnitOfWork the SEALED_CASE unit is emptied by
// the container's quantity, the same number of eaches is added to a LOOSE unit
// of the same (sku, owner) in looseBin, and the break is rejected unless the
// eaches produced equal the container's quantity. The container goes SEALED ->
// OPEN -> EMPTY in that transaction (OPEN is transient in v1). Breaking a case
// that is reserved is rejected: the reservation names the case, not its eaches.
type BreakCase struct {
	Containers    ports.ContainerRepo
	Stock         ports.StockRepo
	Locations     ports.LocationRepo
	HandlingUnits ports.BinHandlingUnitLookup
	Events        ports.EventPublisher
	Clock         ports.Clock
	// Placement applies the hazmat, temperature and segregation rules to the
	// loose eaches entering looseBin. Optional: nil skips them.
	Placement  *StowStock
	UnitOfWork ports.UnitOfWork
	Levels     *BinLevelEmitter
}

// Execute breaks the container with this LPN into looseBin.
func (uc *BreakCase) Execute(ctx context.Context, lpn string, looseBinID shared.BinId) (*container.Container, error) {
	var broken *container.Container
	err := uc.Levels.Atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		c, err := uc.brk(ctx, lpn, looseBinID)
		broken = c
		return err
	})
	if err != nil {
		return nil, err
	}
	return broken, nil
}

// breakPlan is everything loaded and validated before any change is made.
type breakPlan struct {
	c     *container.Container
	unit  *stock.StockUnit
	from  *location.Bin
	loose *location.Bin
}

func (uc *BreakCase) brk(ctx context.Context, lpn string, looseBinID shared.BinId) (*container.Container, error) {
	plan, err := uc.plan(ctx, lpn, looseBinID)
	if err != nil {
		return nil, err
	}
	c, qty := plan.c, plan.c.Quantity()
	fromBin := c.BinID()

	if err := plan.unit.RelocateOut(qty); err != nil {
		return nil, err
	}
	if err := plan.from.Release(qty); err != nil {
		return nil, err
	}
	if err := plan.loose.Occupy(qty); err != nil {
		return nil, err
	}
	produced, looseUnit, err := uc.addLoose(ctx, plan)
	if err != nil {
		return nil, err
	}
	if err := c.Break(produced); err != nil {
		return nil, err
	}
	if err := c.Decant(); err != nil {
		return nil, err
	}
	if err := uc.persist(ctx, plan, looseUnit); err != nil {
		return nil, err
	}
	return c, uc.announce(ctx, c, fromBin, plan.loose.ID())
}

// plan loads and checks everything a break needs, changing nothing.
func (uc *BreakCase) plan(ctx context.Context, lpn string, looseBinID shared.BinId) (*breakPlan, error) {
	c, err := loadContainer(ctx, uc.Containers, lpn)
	if err != nil {
		return nil, err
	}
	if c.State() != container.StateSealed {
		return nil, container.ErrContainerNotSealed
	}
	if c.BinID() == "" {
		return nil, container.ErrContainerNotStowed
	}
	unit, err := uc.Stock.FindByID(ctx, c.StockUnitID())
	if err != nil {
		return nil, err
	}
	if unit == nil {
		return nil, ErrStockUnitNotFound
	}
	if unit.Reserved().Int() > 0 {
		return nil, container.ErrContainerReserved
	}
	if unit.Quantity() != c.Quantity() {
		// The 1:1 link between container and unit is broken: breaking would
		// not conserve quantity.
		return nil, container.ErrBreakNotConserving
	}
	from, loose, err := uc.bins(ctx, c, looseBinID)
	if err != nil {
		return nil, err
	}
	if err := uc.checkLooseBin(ctx, c, loose); err != nil {
		return nil, err
	}
	return &breakPlan{c: c, unit: unit, from: from, loose: loose}, nil
}

// bins loads the bin the case sits in and the bin for the loose eaches; when
// they are the same bin one instance serves both, so occupancy nets to zero.
func (uc *BreakCase) bins(ctx context.Context, c *container.Container, looseBinID shared.BinId) (from, loose *location.Bin, err error) {
	from, err = uc.Locations.FindByID(ctx, c.BinID())
	if err != nil {
		return nil, nil, err
	}
	if from == nil {
		return nil, nil, ErrBinNotFound
	}
	if looseBinID == from.ID() {
		return from, from, nil
	}
	loose, err = uc.Locations.FindByID(ctx, looseBinID)
	if err != nil {
		return nil, nil, err
	}
	if loose == nil {
		return nil, nil, ErrBinNotFound
	}
	return from, loose, nil
}

func (uc *BreakCase) checkLooseBin(ctx context.Context, c *container.Container, loose *location.Bin) error {
	if !loose.BelongsToSite(c.SiteID()) {
		return container.ErrContainerNotAtSite
	}
	hu, err := uc.HandlingUnits.HandlingUnit(ctx, loose.ID())
	if err != nil {
		return err
	}
	if !hu.AcceptsLoose() {
		return container.ErrBinHandlingUnitMismatch
	}
	if uc.Placement == nil {
		return nil
	}
	if err := uc.Placement.checkPlacement(ctx, c.SKU(), loose.ID()); err != nil {
		return err
	}
	return uc.Placement.checkSegregation(ctx, c.SKU(), loose.ID())
}

// addLoose puts the case's eaches on the LOOSE unit of (sku, owner) in the
// loose bin, creating it when there is none. It returns the eaches produced,
// measured as the growth of that unit, and the unit (not yet saved).
func (uc *BreakCase) addLoose(ctx context.Context, plan *breakPlan) (shared.Quantity, *stock.StockUnit, error) {
	c := plan.c
	existing, err := uc.looseUnit(ctx, plan.loose.ID(), c)
	if err != nil {
		return 0, nil, err
	}
	if existing == nil {
		id, err := uc.Stock.NextID(ctx)
		if err != nil {
			return 0, nil, err
		}
		unit, err := stock.NewLooseUnit(id, c.SKU(), plan.loose.ID(), c.Quantity(), c.SiteID(), c.OwnerRef())
		if err != nil {
			return 0, nil, err
		}
		return unit.Quantity(), unit, nil
	}
	before := existing.Quantity()
	if err := existing.AddQuantity(c.Quantity()); err != nil {
		return 0, nil, err
	}
	produced, err := existing.Quantity().Sub(before)
	return produced, existing, err
}

// looseUnit finds the present LOOSE unit of the container's (sku, owner) in bin.
func (uc *BreakCase) looseUnit(ctx context.Context, bin shared.BinId, c *container.Container) (*stock.StockUnit, error) {
	units, err := uc.Stock.FindByBin(ctx, bin)
	if err != nil {
		return nil, err
	}
	for _, u := range units {
		present := u.State() != stock.StateUnlocated && u.State() != stock.StateRemoved
		if present && u.SKU() == c.SKU() && u.ReservableBy(c.OwnerRef()) {
			return u, nil
		}
	}
	return nil, nil
}

func (uc *BreakCase) persist(ctx context.Context, plan *breakPlan, looseUnit *stock.StockUnit) error {
	if plan.from != plan.loose {
		if err := saveBinsOrdered(ctx, uc.Locations, plan.from, plan.loose); err != nil {
			return err
		}
	} else if err := uc.Locations.Save(ctx, plan.from); err != nil {
		return err
	}
	if err := uc.Stock.Save(ctx, plan.unit); err != nil {
		return err
	}
	if err := uc.Stock.Save(ctx, looseUnit); err != nil {
		return err
	}
	return uc.Containers.Save(ctx, plan.c)
}

// announce tracks both bins' levels and publishes CaseBroken then ContainerEmptied.
func (uc *BreakCase) announce(ctx context.Context, c *container.Container, fromBin, looseBin shared.BinId) error {
	for _, id := range distinctBins(fromBin, looseBin) {
		if err := uc.Levels.TrackFor(ctx, shared.BinLevelRelocate, id, c.SKU(), c.OwnerRef()); err != nil {
			return err
		}
	}
	now := uc.Clock.Now()
	if err := uc.Events.Publish(ctx, shared.NewCaseBroken(now, c.LPN(), c.SKU(), c.Quantity(), c.OwnerRef(), fromBin, looseBin)); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, shared.NewContainerEmptied(now, c.LPN(), c.SKU(), c.OwnerRef(), c.SiteID(), shared.ContainerEmptiedBroken))
}

func distinctBins(a, b shared.BinId) []shared.BinId {
	if a == b {
		return []shared.BinId{a}
	}
	return []shared.BinId{a, b}
}
