package ports

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ForwardFaceReader answers, from data this context already holds locally,
// which bins are a SKU's forward pick faces. It is read at reservation time,
// so it must never call a sibling context (no REST/MCP).
type ForwardFaceReader interface {
	// ForwardFacesFor returns the bins that are sku's forward pick faces in
	// the latest applied approved slot plan of every site. A SKU no plan
	// assigns has the empty set (every bin is then RoleReserve).
	ForwardFacesFor(ctx context.Context, sku shared.SKU) (allocation.ForwardFaces, error)
}

// ForwardFaceStore is the local read model fed by slotting-optimization's
// SlotPlanApproved (ADR 0050).
type ForwardFaceStore interface {
	ForwardFaceReader
	// ReplaceSiteMap replaces the site's whole map by m when m supersedes the
	// held one (SiteMap.Supersedes), reporting whether it did. Called inside
	// the same UnitOfWork as the processed_events claim.
	ReplaceSiteMap(ctx context.Context, m allocation.SiteMap) (applied bool, err error)
}
