package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/domain/container"
)

// toteHolds reports whether lpn already names a tote (ADR 0054): one
// license-plate namespace, so such a carton is a duplicate, never merged. A
// linker built without Totes sees none (every caller before ADR 0054).
func (l *ContainerLinker) toteHolds(ctx context.Context, link container.PendingLink) (bool, error) {
	if l.Totes == nil {
		return false, nil
	}
	t, err := l.Totes.FindByLPN(ctx, link.LPN)
	return t != nil, err
}
