package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

type siteMapRow struct {
	planID     string
	approvedAt time.Time
	bySKU      map[shared.SKU][]shared.BinId
}

// ForwardFaceRepo is an in-memory ports.ForwardFaceStore, for unit tests of
// the use cases only: the runtime is Postgres (ADR 0042).
type ForwardFaceRepo struct {
	mu    sync.RWMutex
	sites map[string]siteMapRow
}

func NewForwardFaceRepo() *ForwardFaceRepo {
	return &ForwardFaceRepo{sites: make(map[string]siteMapRow)}
}

func (r *ForwardFaceRepo) ForwardFacesFor(_ context.Context, sku shared.SKU) (allocation.ForwardFaces, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var bins []shared.BinId
	for _, row := range r.sites {
		bins = append(bins, row.bySKU[sku]...)
	}
	return allocation.NewForwardFaces(bins...), nil
}

func (r *ForwardFaceRepo) ReplaceSiteMap(_ context.Context, m allocation.SiteMap) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if held, ok := r.sites[m.SiteID]; ok && !m.Supersedes(held.planID, held.approvedAt) {
		return false, nil
	}
	by := make(map[shared.SKU][]shared.BinId)
	for _, a := range m.Assignments {
		by[a.SKU] = append(by[a.SKU], a.Bin)
	}
	r.sites[m.SiteID] = siteMapRow{planID: m.PlanID, approvedAt: m.ApprovedAt, bySKU: by}
	return true, nil
}
