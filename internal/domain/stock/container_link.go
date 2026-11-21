package stock

import (
	"errors"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ErrContainerRequired: a tote unit was created without the tote's license
// plate.
var ErrContainerRequired = errors.New("a tote unit requires the tote's license plate")

// NewToteUnit stows loose eaches of sku for owner into the tote lpn, which
// sits in binID at siteID (ADR 0054). The unit is ordinary LOOSE stock: it is
// reservable by quantity and counted in the bin's occupancy; its container
// tag never changes.
func NewToteUnit(id string, sku shared.SKU, binID shared.BinId, qty shared.Quantity, siteID shared.SiteID, owner shared.OwnerRef, lpn string) (*StockUnit, error) {
	if lpn == "" {
		return nil, ErrContainerRequired
	}
	unit, err := NewLooseUnit(id, sku, binID, qty, siteID, owner)
	if err != nil {
		return nil, err
	}
	unit.containerLPN = lpn
	return unit, nil
}

// WithContainer returns u tagged with the container lpn. Repositories use it
// when rehydrating; StowContainer uses it on a fresh SEALED_CASE unit.
func (u *StockUnit) WithContainer(lpn string) *StockUnit {
	u.containerLPN = lpn
	return u
}

// ContainerLPN is the license plate of the container holding this unit, or ""
// when it is in no container.
func (u *StockUnit) ContainerLPN() string { return u.containerLPN }

// InContainer reports whether a container holds this unit.
func (u *StockUnit) InContainer() bool { return u.containerLPN != "" }
