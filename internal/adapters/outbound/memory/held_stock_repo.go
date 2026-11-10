package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// HeldStockRepo is an in-memory ports.HeldStockRepo.
type HeldStockRepo struct {
	mu     sync.Mutex
	units  map[string]*stock.HeldUnit
	marks  map[lineKey][]string
	nextID int
}

func NewHeldStockRepo() *HeldStockRepo {
	return &HeldStockRepo{units: make(map[string]*stock.HeldUnit), marks: make(map[lineKey][]string)}
}

func (r *HeldStockRepo) Save(_ context.Context, h *stock.HeldUnit) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.units[h.ID()] = h.WithLPNs(nil)
	return nil
}

func (r *HeldStockRepo) load(h *stock.HeldUnit) *stock.HeldUnit {
	return h.WithLPNs(append([]string(nil), r.marks[lineKey{h.ReceiptID(), h.LineNo()}]...))
}

func (r *HeldStockRepo) FindByID(_ context.Context, id string) (*stock.HeldUnit, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.units[id]
	if !ok {
		return nil, nil
	}
	return r.load(h), nil
}

// matches reports whether h is still held and passes q's filters.
func matches(h *stock.HeldUnit, q stock.HeldQuery) bool {
	switch {
	case h.State() != stock.StateHeld:
		return false
	case q.Reason != "" && h.Reason() != q.Reason:
		return false
	case q.SKU != "" && h.SKU() != q.SKU:
		return false
	case !q.HeldBefore.IsZero() && !h.HeldAt().Before(q.HeldBefore):
		return false
	}
	return true
}

// after reports whether h sorts strictly after the keyset position.
func after(h *stock.HeldUnit, pos *stock.HeldPageAfter) bool {
	if pos == nil {
		return true
	}
	return h.HeldAt().After(pos.HeldAt) || (h.HeldAt().Equal(pos.HeldAt) && h.ID() > pos.ID)
}

func (r *HeldStockRepo) ListHeld(_ context.Context, q stock.HeldQuery) ([]*stock.HeldUnit, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*stock.HeldUnit
	for _, h := range r.units {
		if matches(h, q) && after(h, q.After) {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].HeldAt().Equal(out[j].HeldAt()) {
			return out[i].HeldAt().Before(out[j].HeldAt())
		}
		return out[i].ID() < out[j].ID()
	})
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	for i, h := range out {
		out[i] = r.load(h)
	}
	return out, nil
}

func (r *HeldStockRepo) MarkCarton(_ context.Context, receiptID string, lineNo int, lpn, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := lineKey{receiptID, lineNo}
	for _, existing := range r.marks[k] {
		if existing == lpn {
			return nil
		}
	}
	r.marks[k] = append(r.marks[k], lpn)
	return nil
}

func (r *HeldStockRepo) NextID(_ context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return fmt.Sprintf("hu-%d", r.nextID), nil
}

// All returns every held unit (any state), for tests.
func (r *HeldStockRepo) All() []*stock.HeldUnit {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*stock.HeldUnit, 0, len(r.units))
	for _, h := range r.units {
		out = append(out, r.load(h))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

var _ ports.HeldStockRepo = (*HeldStockRepo)(nil)
