package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ContainerRepo is an in-memory implementation of ports.ContainerRepo.
type ContainerRepo struct {
	mu      sync.RWMutex
	byLPN   map[string]container.Snapshot
	created []string
}

func NewContainerRepo() *ContainerRepo {
	return &ContainerRepo{byLPN: make(map[string]container.Snapshot)}
}

func snapshotOf(c *container.Container) container.Snapshot {
	return container.Snapshot{
		LPN: c.LPN(), SKU: c.SKU(), GTIN: c.GTIN(), UnitsPerPack: c.UnitsPerPack(), Quantity: c.Quantity(),
		OwnerRef: c.OwnerRef(), SiteID: c.SiteID(), BinID: c.BinID(), State: c.State(), Lot: c.Lot(),
		Expiry: c.Expiry(), StockUnitID: c.StockUnitID(), ReceiptID: c.ReceiptID(), LineNo: c.LineNo(), Version: c.Version(),
	}
}

// Add inserts c unless its LPN is taken.
func (r *ContainerRepo) Add(_ context.Context, c *container.Container) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, taken := r.byLPN[c.LPN()]; taken {
		return false, nil
	}
	r.byLPN[c.LPN()] = snapshotOf(c)
	r.created = append(r.created, c.LPN())
	return true, nil
}

// Save updates an existing container, version-guarded like the Postgres repo.
func (r *ContainerRepo) Save(_ context.Context, c *container.Container) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.byLPN[c.LPN()]
	if !ok || current.Version != c.Version() {
		return usecases.ErrConcurrentModification
	}
	next := snapshotOf(c)
	next.Version = c.Version() + 1
	r.byLPN[c.LPN()] = next
	return nil
}

func (r *ContainerRepo) FindByLPN(_ context.Context, lpn string) (*container.Container, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.byLPN[lpn]
	if !ok {
		return nil, nil
	}
	return container.Rehydrate(s), nil
}

func (r *ContainerRepo) FindByStockUnitID(_ context.Context, stockUnitID string) (*container.Container, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.byLPN {
		if s.StockUnitID != "" && s.StockUnitID == stockUnitID {
			return container.Rehydrate(s), nil
		}
	}
	return nil, nil
}

func (r *ContainerRepo) FindByBin(_ context.Context, binID shared.BinId) ([]*container.Container, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*container.Container
	for _, s := range r.byLPN {
		if s.BinID == binID && binID != "" {
			out = append(out, container.Rehydrate(s))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LPN() < out[j].LPN() })
	return out, nil
}

func (r *ContainerRepo) FindStowedBySKU(_ context.Context, sku shared.SKU, owner shared.OwnerRef) ([]*container.Container, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*container.Container
	for _, lpn := range r.created {
		s := r.byLPN[lpn]
		if s.SKU == sku && s.OwnerRef == owner && s.State == container.StateSealed && s.BinID != "" {
			out = append(out, container.Rehydrate(s))
		}
	}
	return out, nil
}

func (r *ContainerRepo) SumQuantity(_ context.Context, receiptID string, lineNo int) (shared.Quantity, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var total shared.Quantity
	for _, s := range r.byLPN {
		if s.ReceiptID == receiptID && s.LineNo == lineNo {
			total = total.Add(s.Quantity)
		}
	}
	return total, nil
}
