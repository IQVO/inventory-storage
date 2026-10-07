package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// legacySource is the classification_source of a row written by this
// service before product-master took ownership (ADR 0034).
const legacySource = "inventory-storage"

type classificationRow struct {
	classification *product.ProductClassification
	version        int64
	source         string
}

// ProductClassificationRepo is an in-memory implementation of
// ports.ProductClassificationRepo, ports.ProductClassificationLocalCopy and
// ports.ProductClassificationCatalogue.
type ProductClassificationRepo struct {
	mu   sync.RWMutex
	rows map[shared.SKU]classificationRow
}

func NewProductClassificationRepo() *ProductClassificationRepo {
	return &ProductClassificationRepo{rows: make(map[shared.SKU]classificationRow)}
}

// Save seeds a legacy row (version 0, source inventory-storage), the state
// every classification written before ADR 0034 is in. Test/fixture helper:
// no port exposes it any more.
func (r *ProductClassificationRepo) Save(_ context.Context, c *product.ProductClassification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[c.SKU()] = classificationRow{classification: c, version: 0, source: legacySource}
	return nil
}

// ApplyIfNewer writes c when version is greater than the stored version.
func (r *ProductClassificationRepo) ApplyIfNewer(_ context.Context, c *product.ProductClassification, version int64, source string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.rows[c.SKU()]; ok && existing.version >= version {
		return false, nil
	}
	r.rows[c.SKU()] = classificationRow{classification: c, version: version, source: source}
	return true, nil
}

func (r *ProductClassificationRepo) FindBySKU(_ context.Context, sku shared.SKU) (*product.ProductClassification, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[sku]
	if !ok {
		return nil, nil
	}
	return row.classification, nil
}

// VersionOf returns the stored version and source for sku (test helper);
// ok is false when the SKU has no row.
func (r *ProductClassificationRepo) VersionOf(sku shared.SKU) (version int64, source string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[sku]
	return row.version, row.source, ok
}

// ListAfter returns up to limit classifications with SKU > afterSKU, in SKU
// order.
func (r *ProductClassificationRepo) ListAfter(_ context.Context, afterSKU shared.SKU, limit int) ([]*product.ProductClassification, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	skus := make([]shared.SKU, 0, len(r.rows))
	for sku := range r.rows {
		if sku > afterSKU {
			skus = append(skus, sku)
		}
	}
	sort.Slice(skus, func(i, j int) bool { return skus[i] < skus[j] })
	if limit > 0 && len(skus) > limit {
		skus = skus[:limit]
	}
	out := make([]*product.ProductClassification, 0, len(skus))
	for _, sku := range skus {
		out = append(out, r.rows[sku].classification)
	}
	return out, nil
}

// ProcessedEventRepo is an in-memory ports.ProcessedEventRepo.
type ProcessedEventRepo struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewProcessedEventRepo() *ProcessedEventRepo {
	return &ProcessedEventRepo{seen: make(map[string]struct{})}
}

// Claim records (consumer, eventID), reporting false on a repeat.
func (r *ProcessedEventRepo) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := consumer + "\x00" + eventID
	if _, ok := r.seen[key]; ok {
		return false, nil
	}
	r.seen[key] = struct{}{}
	return true, nil
}
