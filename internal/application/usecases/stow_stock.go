package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// StowStock places received stock into a bin. It requires both an
// item-scan (SKU) and a location-scan (BinId), and it respects bin
// capacity.
//
// If the SKU carries a registered ProductClassification, StowStock also
// enforces hazmat/temperature placement rules by consulting the
// Classifications repo and the LocationClassificationLookup port (a
// synchronous cross-context read from facility-layout — see ADR 0009):
//   - a Hazmat SKU may only be stowed into a hazmat-rated bin;
//   - a TemperatureSensitive SKU may only be stowed into a bin whose zone
//     temperature class matches the SKU's required TemperatureClass.
//
// AFTER those checks pass, and only when the Classifications repo is
// wired, StowStock additionally enforces same-bin DOT hazard-class
// segregation (ADR 0010): if the incoming SKU's classification carries a
// DOTHazardClass, every OTHER SKU already occupying the target bin is
// checked via product.Incompatible, purely from this service's own
// StockRepo/ProductClassificationRepo — no cross-context call.
//
// Both Classifications and LocationLookup are nil-safe: a StowStock built
// without them (as every pre-existing test in this package does) behaves
// exactly as before — permissive, no placement or segregation check at
// all. This is the same "additive, does not alter existing behaviour"
// discipline documented in the Kafka/EVENT_PUBLISHER pattern.
type StowStock struct {
	Stock           ports.StockRepo
	Locations       ports.LocationRepo
	Events          ports.EventPublisher
	Clock           ports.Clock
	Classifications ports.ProductClassificationRepo
	LocationLookup  ports.LocationClassificationLookup
	// UnitOfWork brackets every Save and Publish below atomically (ADR
	// 0017). Optional: nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

func (uc *StowStock) Execute(ctx context.Context, sku shared.SKU, qty shared.Quantity, binID shared.BinId) (*stock.StockUnit, error) {
	bin, err := uc.Locations.FindByID(ctx, binID)
	if err != nil {
		return nil, err
	}
	if bin == nil {
		return nil, ErrBinNotFound
	}

	if err := uc.checkPlacement(ctx, sku, binID); err != nil {
		return nil, err
	}
	if err := uc.checkSegregation(ctx, sku, binID); err != nil {
		return nil, err
	}

	id, err := uc.Stock.NextID(ctx)
	if err != nil {
		return nil, err
	}

	unit, err := stock.NewStockUnit(id, sku, binID, qty)
	if err != nil {
		return nil, err
	}

	if err := bin.Occupy(qty); err != nil {
		return nil, err
	}

	now := uc.Clock.Now()
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Locations.Save(ctx, bin); err != nil {
			return err
		}
		if err := uc.Stock.Save(ctx, unit); err != nil {
			return err
		}
		if err := uc.Events.Publish(ctx, shared.NewItemStowed(now, sku, binID, qty)); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewLocationRecorded(now, unit.ID(), binID))
	})
	if err != nil {
		return nil, err
	}

	return unit, nil
}

// checkPlacement enforces hazmat/temperature placement rules for sku
// against binID, when both a classification repo and a lookup port are
// wired and the SKU is actually classified with a rule-relevant tag.
//
// A SKU with no registered classification, or one whose classification
// carries neither Hazmat nor TemperatureSensitive, is never blocked here —
// including when the lookup itself is unavailable (fail-closed applies
// only to classified SKUs; see ADR 0009).
func (uc *StowStock) checkPlacement(ctx context.Context, sku shared.SKU, binID shared.BinId) error {
	if uc.Classifications == nil || uc.LocationLookup == nil {
		return nil
	}

	classification, err := uc.Classifications.FindBySKU(ctx, sku)
	if err != nil {
		return err
	}
	if classification == nil {
		return nil
	}

	requiresHazmatZone := classification.IsHazmat()
	requiresTemperatureClass := classification.IsTemperatureSensitive()
	if !requiresHazmatZone && !requiresTemperatureClass {
		return nil
	}

	attrs, err := uc.LocationLookup.GetSlotAttributes(ctx, binID)
	if err != nil {
		// Fail-closed, but only because this SKU is classified as Hazmat
		// or TemperatureSensitive: an unavailable lookup for an
		// unclassified SKU never reaches this branch.
		return ErrLocationClassificationUnavailable
	}
	if !attrs.Known {
		// Fail-open: no constraint info for this bin, permit the stow.
		return nil
	}

	if requiresHazmatZone && !attrs.Hazmat {
		return ErrHazmatZoneRequired
	}
	if requiresTemperatureClass && attrs.TemperatureClass != classification.TemperatureClass() {
		return ErrTemperatureClassMismatch
	}

	return nil
}

// checkSegregation enforces same-bin DOT hazard-class segregation (ADR
// 0010), AFTER the hazmat-zone/temperature-class checks above have
// passed. It is local to this service: bin occupancy (ports.StockRepo)
// and classification lookup (ports.ProductClassificationRepo) are both
// already-owned repos here, so no cross-context call is needed.
//
// It only runs when the SKU being stowed has a registered classification
// with a non-zero DOTHazardClass — an unclassified SKU, or one classified
// without a DOT class, is never blocked here (fail-open, consistent with
// product.Incompatible's own fail-open zero-value behaviour). For every
// OTHER StockUnit already occupying binID, this looks up that occupant
// SKU's classification and rejects the stow if product.Incompatible
// reports the incoming and an occupant's DOTHazardClass as incompatible.
// An occupant with no classification, or a classification with no DOT
// class recorded, never blocks the stow either.
func (uc *StowStock) checkSegregation(ctx context.Context, sku shared.SKU, binID shared.BinId) error {
	if uc.Classifications == nil {
		return nil
	}

	incoming, err := uc.Classifications.FindBySKU(ctx, sku)
	if err != nil {
		return err
	}
	if incoming == nil || incoming.DOTHazardClass() == product.DOTHazardClassUnspecified {
		return nil
	}

	occupants, err := uc.Stock.FindByBin(ctx, binID)
	if err != nil {
		return err
	}

	seen := make(map[shared.SKU]struct{}, len(occupants))
	for _, occupant := range occupants {
		occupantSKU := occupant.SKU()
		if occupantSKU == sku {
			// The same SKU already in this bin is never "another" SKU to
			// segregate against.
			continue
		}
		if _, already := seen[occupantSKU]; already {
			continue
		}
		seen[occupantSKU] = struct{}{}

		occupantClassification, err := uc.Classifications.FindBySKU(ctx, occupantSKU)
		if err != nil {
			return err
		}
		if occupantClassification == nil {
			continue
		}

		if product.Incompatible(incoming.DOTHazardClass(), occupantClassification.DOTHazardClass()) {
			return ErrHazmatClassIncompatible
		}
	}

	return nil
}
