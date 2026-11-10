package ports

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// HeldStockRepo persists HeldUnit aggregates (ADR 0052). Held stock lives
// outside stock_units, so no StockRepo read can see it.
type HeldStockRepo interface {
	// Save inserts a new held unit, or updates an existing one version-guarded
	// (ADR 0019): a stale version fails with usecases.ErrConcurrentModification.
	Save(ctx context.Context, h *stock.HeldUnit) error
	// FindByID returns nil, nil for an unknown id. The unit carries the LPNs
	// of the Damaged cartons recorded for its receipt line.
	FindByID(ctx context.Context, id string) (*stock.HeldUnit, error)
	// ListHeld returns the units still HELD that match q, oldest first, in
	// (held_at, id) order, strictly after q.After.
	ListHeld(ctx context.Context, q stock.HeldQuery) ([]*stock.HeldUnit, error)
	// MarkCarton records that a Damaged carton content named (receiptID,
	// lineNo) with lpn and sscc. It is a no-op for a mark already recorded and
	// books no quantity: the line event is the sole quantity authority.
	MarkCarton(ctx context.Context, receiptID string, lineNo int, lpn, sscc string) error
	NextID(ctx context.Context) (string, error)
}

// HeldStockMetrics records the units currently held per reason, so the
// quarantine backlog is observable (inventory.held_units{reason}). Use cases
// treat a nil value as "not instrumented".
type HeldStockMetrics interface {
	// HeldUnits adds delta (negative when units leave the hold) to the series
	// of reason.
	HeldUnits(ctx context.Context, reason string, delta int)
}
