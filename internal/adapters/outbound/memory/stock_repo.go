// Package memory provides thread-safe in-memory implementations of every
// outbound port, used for unit tests and local development.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// StockRepo is an in-memory implementation of ports.StockRepo.
type StockRepo struct {
	mu     sync.RWMutex
	units  map[string]*stock.StockUnit
	nextID int
}

func NewStockRepo() *StockRepo {
	return &StockRepo{units: make(map[string]*stock.StockUnit)}
}

func (r *StockRepo) Save(_ context.Context, unit *stock.StockUnit) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.units[unit.ID()] = unit
	return nil
}

func (r *StockRepo) FindByID(_ context.Context, id string) (*stock.StockUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	unit, ok := r.units[id]
	if !ok {
		return nil, nil
	}
	return unit, nil
}

func (r *StockRepo) FindBySKU(_ context.Context, sku shared.SKU) ([]*stock.StockUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*stock.StockUnit
	for _, unit := range r.units {
		if unit.SKU() == sku {
			result = append(result, unit)
		}
	}
	return result, nil
}

// FindBySKUAtSite returns sku's units whose recorded site custody is
// originSiteID. Legacy site-less units (empty SiteID) are never included.
func (r *StockRepo) FindBySKUAtSite(_ context.Context, sku shared.SKU, originSiteID shared.SiteID) ([]*stock.StockUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*stock.StockUnit
	for _, unit := range r.units {
		if unit.SKU() == sku && unit.SiteID() == originSiteID && originSiteID != "" {
			result = append(result, unit)
		}
	}
	return result, nil
}

func (r *StockRepo) FindByBin(_ context.Context, binID shared.BinId) ([]*stock.StockUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*stock.StockUnit
	for _, unit := range r.units {
		if unit.BinID() == binID {
			result = append(result, unit)
		}
	}
	return result, nil
}

func (r *StockRepo) NextID(_ context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return fmt.Sprintf("su-%d", r.nextID), nil
}
