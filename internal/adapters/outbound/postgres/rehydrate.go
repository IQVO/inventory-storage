package postgres

import (
	"fmt"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Rehydration helpers: rebuild value objects from persisted column values and
// FAIL when a row violates a domain invariant (empty SKU/bin id, negative
// quantity). A corrupt row must surface as a wrapped error from the
// repository — never as a zero-value VO silently embedded in an aggregate
// (audit 2026-10-05, F5).

func rehydrateSKU(raw string) (shared.SKU, error) {
	sku, err := shared.NewSKU(raw)
	if err != nil {
		return "", fmt.Errorf("rehydrate sku %q: %w", raw, err)
	}
	return sku, nil
}

func rehydrateBinID(raw string) (shared.BinId, error) {
	binID, err := shared.NewBinId(raw)
	if err != nil {
		return "", fmt.Errorf("rehydrate bin id %q: %w", raw, err)
	}
	return binID, nil
}

// rehydrateQuantity names the column being read ("capacity", "occupied",
// "reserved", ...) so the error says which field of the row is corrupt.
func rehydrateQuantity(field string, raw int) (shared.Quantity, error) {
	qty, err := shared.NewQuantity(raw)
	if err != nil {
		return 0, fmt.Errorf("rehydrate %s %d: %w", field, raw, err)
	}
	return qty, nil
}
