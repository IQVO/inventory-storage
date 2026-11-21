package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// ToteRepo is an in-memory ports.ToteRepo. cases (optional) is the sealed-case
// repo it shares the license-plate namespace with.
type ToteRepo struct {
	mu    sync.RWMutex
	byLPN map[string]container.ToteSnapshot
	cases *ContainerRepo
}

func NewToteRepo(cases *ContainerRepo) *ToteRepo {
	return &ToteRepo{byLPN: make(map[string]container.ToteSnapshot), cases: cases}
}

// Add inserts t unless its LPN names a tote or a sealed case.
func (r *ToteRepo) Add(_ context.Context, t *container.Tote) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, taken := r.byLPN[t.LPN()]; taken || r.caseHolds(t.LPN()) {
		return false, nil
	}
	r.byLPN[t.LPN()] = t.Snapshot()
	return true, nil
}

func (r *ToteRepo) caseHolds(lpn string) bool {
	if r.cases == nil {
		return false
	}
	r.cases.mu.RLock()
	defer r.cases.mu.RUnlock()
	_, ok := r.cases.byLPN[lpn]
	return ok
}

// Save updates an existing tote, version-guarded like the Postgres repo.
func (r *ToteRepo) Save(_ context.Context, t *container.Tote) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	stored, ok := r.byLPN[t.LPN()]
	if !ok || stored.Version != t.Version() {
		return usecases.ErrConcurrentModification
	}
	next := t.Snapshot()
	next.Version = stored.Version + 1
	r.byLPN[t.LPN()] = next
	return nil
}

func (r *ToteRepo) FindByLPN(_ context.Context, lpn string) (*container.Tote, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.byLPN[lpn]
	if !ok {
		return nil, nil
	}
	return container.RehydrateTote(s), nil
}

func (r *ToteRepo) FindByBinAfter(_ context.Context, binID shared.BinId, afterLPN string, limit int) ([]*container.Tote, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var lpns []string
	for lpn, s := range r.byLPN {
		if s.BinID == binID && lpn > afterLPN {
			lpns = append(lpns, lpn)
		}
	}
	sort.Strings(lpns)
	if limit >= 0 && len(lpns) > limit {
		lpns = lpns[:limit]
	}
	out := make([]*container.Tote, 0, len(lpns))
	for _, lpn := range lpns {
		out = append(out, container.RehydrateTote(r.byLPN[lpn]))
	}
	return out, nil
}

// FindByContainer implements ports.ContainerStock over the in-memory units.
func (r *StockRepo) FindByContainer(_ context.Context, lpn string) ([]*stock.StockUnit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*stock.StockUnit
	for _, u := range r.units {
		if u.ContainerLPN() == lpn {
			out = append(out, u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out, nil
}

// BinLocationTypes is an in-memory ports.BinLocationTypeLookup seeded by
// tests.
type BinLocationTypes struct {
	mu    sync.RWMutex
	types map[shared.BinId]string
}

func NewBinLocationTypes() *BinLocationTypes {
	return &BinLocationTypes{types: make(map[shared.BinId]string)}
}

// Set records bin's facility-layout LocationType.
func (b *BinLocationTypes) Set(bin shared.BinId, locationType string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.types[bin] = locationType
}

func (b *BinLocationTypes) LocationType(_ context.Context, bin shared.BinId) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.types[bin], nil
}

var (
	_ ports.ToteRepo              = (*ToteRepo)(nil)
	_ ports.ContainerStock        = (*StockRepo)(nil)
	_ ports.BinLocationTypeLookup = (*BinLocationTypes)(nil)
)
