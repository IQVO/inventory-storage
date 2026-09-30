package usecases

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// CycleCountResult reports whether a cycle count reconciled cleanly.
type CycleCountResult struct {
	BinID       shared.BinId
	CountedQty  shared.Quantity
	SystemQty   shared.Quantity
	Discrepancy bool
}

// RunCycleCount verifies a bin's contents against system records and
// reconciles discrepancies. A shortfall (counted < system) flags the
// difference Unlocated: the physical item is lost, not present, and no
// longer known to be at that bin.
type RunCycleCount struct {
	Stock  ports.StockRepo
	Events ports.EventPublisher
	Clock  ports.Clock
	// UnitOfWork brackets every Save/Publish this use case makes
	// atomically (ADR 0017). Optional: nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

func (uc *RunCycleCount) Execute(ctx context.Context, binID shared.BinId, countedQty shared.Quantity) (CycleCountResult, error) {
	units, err := uc.Stock.FindByBin(ctx, binID)
	if err != nil {
		return CycleCountResult{}, err
	}

	systemQty, locatable := locatableUnits(units)

	now := uc.Clock.Now()
	var result CycleCountResult

	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if countedQty == systemQty {
			r, err := uc.complete(ctx, now, binID, countedQty, systemQty, false)
			if err != nil {
				return err
			}
			result = r
			return nil
		}

		if err := uc.Events.Publish(ctx, shared.NewDiscrepancyDetected(now, binID, countedQty, systemQty)); err != nil {
			return err
		}

		if countedQty.GreaterThan(systemQty) {
			// Overage: more physically present than recorded. Reconciling
			// this upward requires a separate receiving/audit process; the
			// count is still reported as a discrepancy for that process to
			// pick up.
			r, err := uc.complete(ctx, now, binID, countedQty, systemQty, true)
			if err != nil {
				return err
			}
			result = r
			return nil
		}

		// Simplification: a unit touched by the shortfall is marked fully
		// Unlocated (rather than split across located/lost portions),
		// leaving finer-grained reconciliation to a follow-up stow/count.
		shortfall, _ := systemQty.Sub(countedQty)
		if err := uc.markShortfallUnlocated(ctx, binID, locatable, shortfall, now); err != nil {
			return err
		}
		r, cerr := uc.complete(ctx, now, binID, countedQty, systemQty, true)
		if cerr != nil {
			return cerr
		}
		result = r
		return nil
	})
	if err != nil {
		return CycleCountResult{}, err
	}

	return result, nil
}

// locatableUnits sums the quantity of every located, still-present stock
// unit in a bin and collects those units for shortfall reconciliation:
// Unlocated and Removed units are not part of the bin's system quantity.
func locatableUnits(units []*stock.StockUnit) (shared.Quantity, []*stock.StockUnit) {
	systemQty := shared.Quantity(0)
	var locatable []*stock.StockUnit
	for _, unit := range units {
		if unit.State() == stock.StateUnlocated || unit.State() == stock.StateRemoved {
			continue
		}
		systemQty = systemQty.Add(unit.Quantity())
		locatable = append(locatable, unit)
	}
	return systemQty, locatable
}

// markShortfallUnlocated flags locatable units as Unlocated, in order, until
// the counted shortfall is covered, persisting each unit and raising an
// ItemUnlocated event per unit with the portion it contributed to the
// shortfall.
func (uc *RunCycleCount) markShortfallUnlocated(ctx context.Context, binID shared.BinId, locatable []*stock.StockUnit, shortfall shared.Quantity, now time.Time) error {
	for _, unit := range locatable {
		if shortfall.Int() == 0 {
			break
		}
		take := unit.Quantity()
		if !shortfall.GreaterThan(take) {
			take = shortfall
		}
		unit.MarkUnlocated()
		if err := uc.Stock.Save(ctx, unit); err != nil {
			return err
		}
		if err := uc.Events.Publish(ctx, shared.NewItemUnlocated(now, unit.ID(), unit.SKU(), binID, take)); err != nil {
			return err
		}
		shortfall, _ = shortfall.Sub(take)
	}
	return nil
}

// complete publishes CycleCountCompleted and assembles the use case's
// result for the caller.
func (uc *RunCycleCount) complete(ctx context.Context, now time.Time, binID shared.BinId, countedQty, systemQty shared.Quantity, discrepancy bool) (CycleCountResult, error) {
	if err := uc.Events.Publish(ctx, shared.NewCycleCountCompleted(now, binID, countedQty, systemQty, discrepancy)); err != nil {
		return CycleCountResult{}, err
	}
	return CycleCountResult{BinID: binID, CountedQty: countedQty, SystemQty: systemQty, Discrepancy: discrepancy}, nil
}
