package usecases

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// ListHeldStockQuery filters the held-stock list; zero values mean "any".
type ListHeldStockQuery struct {
	Reason stock.HoldReason
	SKU    shared.SKU
	// OlderThan keeps the units that have been held for MORE than this long
	// (the ageing filter); zero means any age.
	OlderThan time.Duration
	// After is the keyset position of the last row of the previous page and
	// Limit the page size (zero means no limit).
	After *stock.HeldPageAfter
	Limit int
}

// ListHeldStock reads the units still held (ADR 0052), optionally filtered by
// reason, SKU and age, oldest first. It is a pure read: it never changes a
// unit and held units never appear in any usable-quantity read.
type ListHeldStock struct {
	Held ports.HeldStockRepo
	// Clock turns OlderThan into an instant. Required only when a query sets
	// OlderThan.
	Clock ports.Clock
}

// Execute returns the held units matching q.
func (uc *ListHeldStock) Execute(ctx context.Context, q ListHeldStockQuery) ([]*stock.HeldUnit, error) {
	query := stock.HeldQuery{Reason: q.Reason, SKU: q.SKU, After: q.After, Limit: q.Limit}
	if q.OlderThan > 0 {
		query.HeldBefore = uc.Clock.Now().Add(-q.OlderThan)
	}
	return uc.Held.ListHeld(ctx, query)
}

// Get returns one held unit (held or already settled), or ErrHeldUnitNotFound.
func (uc *ListHeldStock) Get(ctx context.Context, id string) (*stock.HeldUnit, error) {
	h, err := uc.Held.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, ErrHeldUnitNotFound
	}
	return h, nil
}
