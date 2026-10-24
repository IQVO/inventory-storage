package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// UsableInventory is the read model for GetUsable: stock immediately
// available to fulfil (on-hand minus active reservations minus
// held/damaged/unlocated), in eaches, for one owner.
type UsableInventory struct {
	SKU shared.SKU
	// OwnerRef is the owner the figures are for; empty = fleet-owned stock.
	OwnerRef shared.OwnerRef
	// Usable counts every pack state, in eaches (ADR 0045: the ledger unit does
	// not change).
	Usable shared.Quantity
	// LooseUsable is the part of Usable a quantity reservation can draw on.
	LooseUsable shared.Quantity
	// ReservableCases is the number of whole sealed cases ReserveFullCases can
	// still reserve.
	ReservableCases int
}

// GetUsable computes usable inventory for a SKU, projected from StockUnits.
type GetUsable struct {
	Stock ports.StockRepo
}

// Execute is ExecuteFor fleet-owned stock.
func (uc *GetUsable) Execute(ctx context.Context, sku shared.SKU) (UsableInventory, error) {
	return uc.ExecuteFor(ctx, sku, "")
}

// ExecuteFor computes the usable inventory of sku held by owner. An empty
// owner means fleet-owned stock only, never "any owner".
func (uc *GetUsable) ExecuteFor(ctx context.Context, sku shared.SKU, owner shared.OwnerRef) (UsableInventory, error) {
	units, err := uc.Stock.FindBySKU(ctx, sku)
	if err != nil {
		return UsableInventory{}, err
	}
	out := UsableInventory{SKU: sku, OwnerRef: owner}
	for _, unit := range units {
		if unit.OwnerRef() != owner {
			continue
		}
		out.Usable = out.Usable.Add(unit.Usable())
		if unit.IsLoose() {
			out.LooseUsable = out.LooseUsable.Add(unit.Usable())
		} else if unit.Usable() == unit.Quantity() && unit.Usable().Int() > 0 {
			out.ReservableCases++
		}
	}
	return out, nil
}
