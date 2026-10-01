package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// GetBin reads a Bin's capacity and occupancy. Side-effect-free; an
// unknown bin is ErrBinNotFound (there is no meaningful "empty" bin to
// return, unlike GetUsable's zero for an unknown SKU).
type GetBin struct {
	Locations ports.LocationRepo
}

// Execute returns the bin with the given id, or ErrBinNotFound.
func (uc *GetBin) Execute(ctx context.Context, id shared.BinId) (*location.Bin, error) {
	if id == "" {
		return nil, shared.ErrEmptyBinID
	}
	bin, err := uc.Locations.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}
	return bin, nil
}
