package ports

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// ToteRepo persists Tote aggregates (ADR 0054). FindByLPN returns nil for an
// unknown tote.
type ToteRepo interface {
	// Add inserts a freshly registered tote and claims its license plate. It
	// reports false, writing nothing, when the LPN is already registered, as
	// a tote or as a sealed case (one namespace).
	Add(ctx context.Context, t *container.Tote) (bool, error)
	// Save updates an existing tote, version-guarded (ADR 0019): a stale
	// version fails with usecases.ErrConcurrentModification.
	Save(ctx context.Context, t *container.Tote) error
	FindByLPN(ctx context.Context, lpn string) (*container.Tote, error)
	// FindByBinAfter returns at most limit totes in binID whose LPN sorts
	// strictly after afterLPN, in ascending LPN order.
	FindByBinAfter(ctx context.Context, binID shared.BinId, afterLPN string, limit int) ([]*container.Tote, error)
}

// ContainerStock reads the stock units a container holds (ADR 0054), in
// ascending stock unit id order, whatever their state.
type ContainerStock interface {
	FindByContainer(ctx context.Context, lpn string) ([]*stock.StockUnit, error)
}

// BinLocationTypeLookup answers facility-layout's LocationType of a bin from
// the event-fed local copy (ADR 0013, ADR 0054). It answers "" for a bin it
// has not seen; it never performs I/O.
type BinLocationTypeLookup interface {
	LocationType(ctx context.Context, bin shared.BinId) (string, error)
}
