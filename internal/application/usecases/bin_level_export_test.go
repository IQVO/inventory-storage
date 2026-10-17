package usecases

import (
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// RelocationLevelReason exposes relocationLevelReason to the external test
// package: REPLENISHMENT cannot be built through the relocation aggregate yet
// (ParseReason rejects it), so the mapping is exercised directly.
func RelocationLevelReason(reason string) shared.BinLevelReason {
	return relocationLevelReason(relocation.Reason(reason))
}
