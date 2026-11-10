package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// HoldStock holds a received quantity out of usable stock (ADR 0052): it
// creates the HeldUnit and raises StockHeld. It is the one place a hold is
// created, whatever the producer (today the inbound receipt consumer, later
// quality inspection).
//
// It does not open its own metric: the caller counts after its transaction
// committed, so a rolled-back attempt never inflates the series.
type HoldStock struct {
	Held   ports.HeldStockRepo
	Events ports.EventPublisher
	Clock  ports.Clock
	// UnitOfWork brackets the save and the event atomically (ADR 0017). It
	// joins the caller's transaction when there is one. Optional.
	UnitOfWork ports.UnitOfWork
}

// Execute holds source for reason.
func (uc *HoldStock) Execute(ctx context.Context, source stock.HeldSource, reason stock.HoldReason) (*stock.HeldUnit, error) {
	var held *stock.HeldUnit
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		id, err := uc.Held.NextID(ctx)
		if err != nil {
			return err
		}
		now := uc.Clock.Now()
		held, err = stock.NewHeldUnit(id, source, reason, now)
		if err != nil {
			return err
		}
		if err := uc.Held.Save(ctx, held); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewStockHeld(now, id, source.SKU, source.Quantity, string(reason),
			source.ReceiptID, source.ASNNumber, source.LineNo, source.OwnerRef, source.SSCC))
	})
	if err != nil {
		return nil, err
	}
	return held, nil
}
