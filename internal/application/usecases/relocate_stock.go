package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ErrRelocationNotFound: no relocation has this id.
var ErrRelocationNotFound = errors.New("relocation not found")

// RelocateStockCommand is one requested intra-site move (ADR 0039).
type RelocateStockCommand struct {
	// SiteID is the site both bins must be in. Empty means "the source
	// bin's site" (the RELOCATE-task path: TaskCompleted carries no site).
	SiteID    shared.SiteID
	SKU       shared.SKU
	FromBin   shared.BinId
	ToBin     shared.BinId
	Quantity  shared.Quantity
	Reason    relocation.Reason
	SourceRef string
}

// RelocateStock moves a quantity of one SKU from one bin to another bin of
// the same site (ADR 0039). In ONE UnitOfWork it saves both bins, every
// touched source StockUnit, the new destination StockUnit, the relocation
// record and the StockRelocated outbox row. Every aggregate save is
// version-guarded (ADR 0019): a concurrent writer on either bin or a
// touched unit fails the save with ErrConcurrentModification and the whole
// move rolls back.
type RelocateStock struct {
	Stock       ports.StockRepo
	Locations   ports.LocationRepo
	Relocations ports.RelocationRepo
	Events      ports.EventPublisher
	Clock       ports.Clock
	// UnitOfWork brackets every write atomically (ADR 0017). Optional: nil
	// means "no transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
}

// Execute performs one relocation and returns the applied record.
func (uc *RelocateStock) Execute(ctx context.Context, cmd RelocateStockCommand) (*relocation.Relocation, error) {
	var applied *relocation.Relocation
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		rel, err := uc.relocate(ctx, cmd)
		applied = rel
		return err
	})
	if err != nil {
		return nil, err
	}
	return applied, nil
}

func (uc *RelocateStock) relocate(ctx context.Context, cmd RelocateStockCommand) (*relocation.Relocation, error) {
	site, err := uc.site(ctx, cmd)
	if err != nil {
		return nil, err
	}
	// Bins first: an unknown bin is a 404 whatever else is wrong with the
	// command (the same-bin and quantity rules are checked by New below).
	from, err := uc.bin(ctx, cmd.FromBin)
	if err != nil {
		return nil, err
	}
	to, err := uc.bin(ctx, cmd.ToBin)
	if err != nil {
		return nil, err
	}
	id, err := uc.Relocations.NextID(ctx)
	if err != nil {
		return nil, err
	}
	rel, err := relocation.New(relocation.Spec{
		ID: id, SiteID: site, SKU: cmd.SKU, FromBin: cmd.FromBin, ToBin: cmd.ToBin,
		Quantity: cmd.Quantity, Reason: cmd.Reason, SourceRef: cmd.SourceRef,
	})
	if err != nil {
		return nil, err
	}
	if err := uc.apply(ctx, rel, from, to); err != nil {
		return nil, err
	}
	return rel, nil
}

// site is the command's site, or the source bin's site when none is given.
func (uc *RelocateStock) site(ctx context.Context, cmd RelocateStockCommand) (shared.SiteID, error) {
	if cmd.SiteID != "" {
		return cmd.SiteID, nil
	}
	from, err := uc.bin(ctx, cmd.FromBin)
	if err != nil {
		return "", err
	}
	site := from.EffectiveSite()
	if site == "" {
		return "", relocation.ErrBinNotAtSite
	}
	return site, nil
}

func (uc *RelocateStock) bin(ctx context.Context, id shared.BinId) (*location.Bin, error) {
	bin, err := uc.Locations.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}
	return bin, nil
}

// apply moves the stock in memory and persists every changed aggregate.
func (uc *RelocateStock) apply(ctx context.Context, rel *relocation.Relocation, from, to *location.Bin) error {
	units, err := uc.Stock.FindByBin(ctx, from.ID())
	if err != nil {
		return err
	}
	destID, err := uc.Stock.NextID(ctx)
	if err != nil {
		return err
	}
	out, err := rel.Apply(units, from, to, destID, uc.Clock.Now())
	if err != nil {
		return err
	}
	if err := uc.saveBins(ctx, from, to); err != nil {
		return err
	}
	for _, unit := range append(out.Touched, out.Destination) {
		if err := uc.Stock.Save(ctx, unit); err != nil {
			return err
		}
	}
	if err := uc.Relocations.Save(ctx, rel); err != nil {
		return err
	}
	return uc.Events.Publish(ctx, rel.Event())
}

// saveBins saves both bins in ascending id order, so two opposite
// relocations (A->B and B->A) lock the bin rows in the same order.
func (uc *RelocateStock) saveBins(ctx context.Context, a, b *location.Bin) error {
	if b.ID() < a.ID() {
		a, b = b, a
	}
	if err := uc.Locations.Save(ctx, a); err != nil {
		return err
	}
	return uc.Locations.Save(ctx, b)
}

// GetRelocation reads one relocation record (GET /relocations/{id}, MCP
// get_relocation).
type GetRelocation struct {
	Relocations ports.RelocationRepo
}

// Execute returns the relocation or ErrRelocationNotFound.
func (uc *GetRelocation) Execute(ctx context.Context, id string) (*relocation.Relocation, error) {
	rel, err := uc.Relocations.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, ErrRelocationNotFound
	}
	return rel, nil
}
