package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// emptyContainerOfUnit moves the container backing a sealed-case unit to
// EMPTY and raises ContainerEmptied, when the unit left storage whole (a
// confirmed whole-case pick, or a cycle count that could not find it). Loose
// units, and a nil repo (containers not wired), do nothing. It must run in the
// caller's UnitOfWork so the unit and its container never disagree (the 1:1
// link of ADR 0045).
func emptyContainerOfUnit(ctx context.Context, containers ports.ContainerRepo, events ports.EventPublisher, clock ports.Clock, unit *stock.StockUnit, reason string) error {
	if containers == nil || unit.PackState() != stock.PackSealedCase {
		return nil
	}
	c, err := containers.FindByStockUnitID(ctx, unit.ID())
	if err != nil || c == nil {
		return err
	}
	if err := c.EmptyWhole(); err != nil {
		return err
	}
	site := c.SiteID()
	if err := containers.Save(ctx, c); err != nil {
		return err
	}
	return events.Publish(ctx, shared.NewContainerEmptied(clock.Now(), c.LPN(), c.SKU(), c.OwnerRef(), site, reason))
}
