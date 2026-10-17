package usecases

import (
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// RelocationLevelReason exposes relocationLevelReason to the external test
// package, so the whole reason mapping (including an unknown reason) is
// exercised directly, besides the end-to-end REPLENISH test.
func RelocationLevelReason(reason string) shared.BinLevelReason {
	return relocationLevelReason(relocation.Reason(reason))
}
