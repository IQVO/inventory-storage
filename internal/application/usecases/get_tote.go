package usecases

import (
	"context"
	"sort"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// ToteView is a tote with its contents: the units it holds that still have
// eaches, ordered by SKU then stock unit id.
type ToteView struct {
	Tote     *container.Tote
	Contents []*stock.StockUnit
}

// GetTote reads one tote and its contents (GET /totes/{lpn}).
type GetTote struct {
	Totes ports.ToteRepo
	Stock ports.ContainerStock
}

// Execute returns the tote, or ErrToteNotFound.
func (uc *GetTote) Execute(ctx context.Context, lpn string) (ToteView, error) {
	t, err := uc.Totes.FindByLPN(ctx, lpn)
	if err != nil {
		return ToteView{}, err
	}
	if t == nil {
		return ToteView{}, ErrToteNotFound
	}
	units, err := uc.Stock.FindByContainer(ctx, lpn)
	if err != nil {
		return ToteView{}, err
	}
	return ToteView{Tote: t, Contents: present(units)}, nil
}

// present keeps the units that still hold eaches, ordered by SKU then id.
func present(units []*stock.StockUnit) []*stock.StockUnit {
	out := make([]*stock.StockUnit, 0, len(units))
	for _, u := range units {
		if u.Quantity().Int() > 0 && u.State() != stock.StateRemoved {
			out = append(out, u)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SKU() != out[j].SKU() {
			return out[i].SKU() < out[j].SKU()
		}
		return out[i].ID() < out[j].ID()
	})
	return out
}

// ListBinTotes reads the totes whose logical location is a bin
// (GET /bins/{binId}/totes).
type ListBinTotes struct {
	Totes     ports.ToteRepo
	Locations ports.LocationRepo
}

// Page returns up to fetch totes of binID whose LPN sorts after afterLPN, or
// ErrBinNotFound for an unregistered bin.
func (uc *ListBinTotes) Page(ctx context.Context, binID shared.BinId, afterLPN string, fetch int) ([]*container.Tote, error) {
	bin, err := uc.Locations.FindByID(ctx, binID)
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}
	return uc.Totes.FindByBinAfter(ctx, binID, afterLPN, fetch)
}
