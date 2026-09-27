package usecases

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// StagedReceipt is the result of a receive: goods acknowledged into the
// building but not yet assigned to a bin. It is not a persisted aggregate —
// tracking becomes durable at StowStock, when item-scan + location-scan
// produce a StockUnit.
type StagedReceipt struct {
	SKU        shared.SKU
	Quantity   shared.Quantity
	ReceivedAt time.Time
}

// ReceiveStock acknowledges inbound goods for a SKU, staged awaiting stow.
type ReceiveStock struct {
	Events ports.EventPublisher
	Clock  ports.Clock
	// UnitOfWork brackets Publish atomically (ADR 0017). Optional: a nil
	// value means "no transactional backing", matching the in-memory /
	// log-publisher dev configuration.
	UnitOfWork ports.UnitOfWork
}

func (uc *ReceiveStock) Execute(ctx context.Context, sku shared.SKU, qty shared.Quantity) (StagedReceipt, error) {
	if sku == "" {
		return StagedReceipt{}, shared.ErrEmptySKU
	}
	if qty.Int() <= 0 {
		return StagedReceipt{}, shared.ErrZeroQuantity
	}

	now := uc.Clock.Now()
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		return uc.Events.Publish(ctx, shared.NewStockReceived(now, sku, qty))
	})
	if err != nil {
		return StagedReceipt{}, err
	}

	return StagedReceipt{SKU: sku, Quantity: qty, ReceivedAt: now}, nil
}
