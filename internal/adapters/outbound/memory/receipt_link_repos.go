package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ReceiptBookingRepo is an in-memory ports.ReceiptBookingRepo.
type ReceiptBookingRepo struct {
	mu     sync.Mutex
	booked map[lineKey]shared.Quantity
}

type lineKey struct {
	receiptID string
	lineNo    int
}

func NewReceiptBookingRepo() *ReceiptBookingRepo {
	return &ReceiptBookingRepo{booked: make(map[lineKey]shared.Quantity)}
}

func (r *ReceiptBookingRepo) AddBooking(_ context.Context, receiptID string, lineNo int, _ shared.SKU, _ shared.OwnerRef, qty shared.Quantity) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := lineKey{receiptID, lineNo}
	r.booked[k] = r.booked[k].Add(qty)
	return nil
}

func (r *ReceiptBookingRepo) Booked(_ context.Context, receiptID string, lineNo int) (shared.Quantity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.booked[lineKey{receiptID, lineNo}], nil
}

// PendingLinkRepo is an in-memory ports.PendingLinkRepo.
type PendingLinkRepo struct {
	mu    sync.Mutex
	links map[pendingKey]container.PendingLink
	order []pendingKey
}

type pendingKey struct {
	receiptID string
	lineNo    int
	lpn       string
}

func NewPendingLinkRepo() *PendingLinkRepo {
	return &PendingLinkRepo{links: make(map[pendingKey]container.PendingLink)}
}

func (r *PendingLinkRepo) Add(_ context.Context, l container.PendingLink) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := pendingKey{l.ReceiptID, l.LineNo, l.LPN}
	if _, exists := r.links[k]; exists {
		return nil
	}
	if l.Status == "" {
		l.Status = container.LinkPending
	}
	r.links[k] = l
	r.order = append(r.order, k)
	return nil
}

func (r *PendingLinkRepo) pending(match func(container.PendingLink) bool) []container.PendingLink {
	var out []container.PendingLink
	for _, k := range r.order {
		l, ok := r.links[k]
		if ok && l.Status == container.LinkPending && match(l) {
			out = append(out, l)
		}
	}
	return out
}

func (r *PendingLinkRepo) FindPendingByLine(_ context.Context, receiptID string, lineNo int) ([]container.PendingLink, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pending(func(l container.PendingLink) bool { return l.ReceiptID == receiptID && l.LineNo == lineNo }), nil
}

func (r *PendingLinkRepo) FindPendingBySKU(_ context.Context, sku shared.SKU) ([]container.PendingLink, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pending(func(l container.PendingLink) bool { return l.SKU == sku }), nil
}

func (r *PendingLinkRepo) Remove(_ context.Context, receiptID string, lineNo int, lpn string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.links, pendingKey{receiptID, lineNo, lpn})
	return nil
}

func (r *PendingLinkRepo) Reject(_ context.Context, receiptID string, lineNo int, lpn, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := pendingKey{receiptID, lineNo, lpn}
	if l, ok := r.links[k]; ok {
		l.Status, l.Reason = container.LinkRejected, reason
		r.links[k] = l
	}
	return nil
}

// All returns every stored link (any status), for tests.
func (r *PendingLinkRepo) All() []container.PendingLink {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]container.PendingLink, 0, len(r.order))
	for _, k := range r.order {
		if l, ok := r.links[k]; ok {
			out = append(out, l)
		}
	}
	return out
}

// PackagingView is an in-memory PackagingViewRepo and PackagingProjection.
type PackagingView struct {
	mu     sync.Mutex
	levels map[string]packaging.PackLevel
}

func NewPackagingView() *PackagingView {
	return &PackagingView{levels: make(map[string]packaging.PackLevel)}
}

func (v *PackagingView) ApplyDeclared(_ context.Context, p packaging.PackLevel) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if cur, ok := v.levels[p.GTIN.String()]; ok && cur.Version >= p.Version {
		return false, nil
	}
	v.levels[p.GTIN.String()] = p
	return true, nil
}

func (v *PackagingView) ApplyRetired(_ context.Context, gtin string, effectiveTo time.Time, version int64) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	cur, ok := v.levels[gtin]
	if !ok || cur.Version > version {
		return false, nil
	}
	cur.EffectiveTo = &effectiveTo
	cur.Version = version
	v.levels[gtin] = cur
	return true, nil
}

func (v *PackagingView) FindByGTIN(_ context.Context, gtin string) (*packaging.PackLevel, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	p, ok := v.levels[gtin]
	if !ok {
		return nil, nil
	}
	return &p, nil
}

func (v *PackagingView) FindActive(_ context.Context, sku shared.SKU, level packaging.Level, at time.Time) (*packaging.PackLevel, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	var best *packaging.PackLevel
	for _, p := range v.levels {
		p := p
		if p.SKU == sku && p.Level == level && p.Active(at) && (best == nil || p.EffectiveFrom.After(best.EffectiveFrom)) {
			best = &p
		}
	}
	return best, nil
}

// BinHandlingUnits is an in-memory ports.BinHandlingUnitLookup seeded by
// tests and local runs; a bin nobody described answers HandlingUnknown.
type BinHandlingUnits struct {
	mu    sync.RWMutex
	units map[shared.BinId]container.HandlingUnit
}

func NewBinHandlingUnits() *BinHandlingUnits {
	return &BinHandlingUnits{units: make(map[shared.BinId]container.HandlingUnit)}
}

// Set records the handling unit of a bin.
func (b *BinHandlingUnits) Set(bin shared.BinId, hu container.HandlingUnit) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.units[bin] = hu
}

func (b *BinHandlingUnits) HandlingUnit(_ context.Context, bin shared.BinId) (container.HandlingUnit, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.units[bin], nil
}
