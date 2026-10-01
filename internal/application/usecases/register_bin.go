package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// RegisterBinOutcome tells the caller what RegisterBin actually did, so the
// inbound adapter can answer 201 (created) vs 200 (already present /
// resized) without a second, racy lookup of its own.
type RegisterBinOutcome int

const (
	// BinCreated means no bin existed with this id; a new, empty one was
	// created with the requested capacity.
	BinCreated RegisterBinOutcome = iota + 1
	// BinUnchanged means the bin already existed with exactly the
	// requested capacity; nothing was written.
	BinUnchanged
	// BinResized means the bin already existed with a different capacity
	// and was resized (occupancy untouched).
	BinResized
)

// RegisterBin declaratively registers a Bin (a coded storage slot) with a
// capacity — the inventory-control action that brings a slot under this
// service's management. It is idempotent: the caller states the desired
// capacity and RegisterBin converges the bin to it.
//
//   - absent bin: created via location.NewBin (BinCreated);
//   - present bin, same capacity: no-op, nothing persisted (BinUnchanged);
//   - present bin, different capacity: Bin.Resize, which rejects a
//     capacity below current occupancy with
//     location.ErrCapacityBelowOccupancy (BinResized on success).
//
// Writes go through the version-guarded LocationRepo.Save (ADR 0019), so a
// resize racing a concurrent stow surfaces as ErrConcurrentModification
// rather than silently clobbering the stow's occupancy. No domain event is
// raised: bin registration is local topology master data and the
// integration contract (apis/asyncapi.yaml) is deliberately unchanged
// (ADR 0025).
type RegisterBin struct {
	Locations ports.LocationRepo
	// UnitOfWork brackets the read-modify-write atomically (ADR 0017).
	// Optional: nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

// Execute converges bin id to capacity and returns the resulting Bin and
// what was done to it.
func (uc *RegisterBin) Execute(ctx context.Context, id shared.BinId, capacity shared.Quantity) (*location.Bin, RegisterBinOutcome, error) {
	if id == "" {
		return nil, 0, shared.ErrEmptyBinID
	}
	if capacity.Int() <= 0 {
		return nil, 0, location.ErrInvalidCapacity
	}

	var (
		result  *location.Bin
		outcome RegisterBinOutcome
	)
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		existing, err := uc.Locations.FindByID(ctx, id)
		if err != nil {
			return err
		}
		if existing == nil {
			bin, err := location.NewBin(id, capacity)
			if err != nil {
				return err
			}
			if err := uc.Locations.Save(ctx, bin); err != nil {
				return err
			}
			result, outcome = bin, BinCreated
			return nil
		}
		if existing.Capacity() == capacity {
			result, outcome = existing, BinUnchanged
			return nil
		}
		if err := existing.Resize(capacity); err != nil {
			return err
		}
		if err := uc.Locations.Save(ctx, existing); err != nil {
			return err
		}
		result, outcome = existing, BinResized
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result, outcome, nil
}
