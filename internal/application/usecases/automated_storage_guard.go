package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// refuseLooseInto refuses loose stock entering bin when the bin's
// facility-layout LocationType holds container stock only (AutomatedStorage,
// ADR 0054): the WCS retrieves containers, so loose eaches there could never
// be picked. A nil lookup or an unknown type allows it (today's behaviour,
// fail open as ADR 0013).
func refuseLooseInto(ctx context.Context, types ports.BinLocationTypeLookup, bin shared.BinId) error {
	if types == nil {
		return nil
	}
	typ, err := types.LocationType(ctx, bin)
	if err != nil {
		return err
	}
	if location.HoldsContainersOnly(typ) {
		return ErrAutomatedStorageRequiresContainer
	}
	return nil
}
