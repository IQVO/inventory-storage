package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// GetContainer reads one container by LPN (GET /containers/{lpn}, MCP
// get_container).
type GetContainer struct {
	Containers ports.ContainerRepo
}

// Execute returns the container or ErrContainerNotFound.
func (uc *GetContainer) Execute(ctx context.Context, lpn string) (*container.Container, error) {
	return loadContainer(ctx, uc.Containers, lpn)
}

// ListBinContainers reads the containers stowed in a bin
// (GET /bins/{bin}/containers).
type ListBinContainers struct {
	Containers ports.ContainerRepo
	Locations  ports.LocationRepo
}

// Execute returns the containers in the bin, or ErrBinNotFound for an
// unregistered bin.
func (uc *ListBinContainers) Execute(ctx context.Context, binID shared.BinId) ([]*container.Container, error) {
	bin, err := uc.Locations.FindByID(ctx, binID)
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}
	return uc.Containers.FindByBin(ctx, binID)
}

// Page is the keyset-paged read behind GET /bins/{binId}/containers: up to
// fetch containers whose LPN sorts strictly after afterLPN (empty = from the
// start), or ErrBinNotFound for an unregistered bin. The caller passes
// page.FetchSize(limit) so it can tell whether a next page exists.
func (uc *ListBinContainers) Page(ctx context.Context, binID shared.BinId, afterLPN string, fetch int) ([]*container.Container, error) {
	bin, err := uc.Locations.FindByID(ctx, binID)
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}
	return uc.Containers.FindByBinAfter(ctx, binID, afterLPN, fetch)
}
