package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ClassifyProduct registers or replaces a SKU's ProductClassification —
// SKU-level master data this service owns as source of truth (ADR 0009).
// It is idempotent by SKU: classifying the same SKU again replaces its
// prior classification rather than erroring, since re-classification is a
// legitimate operational action (e.g. an item newly designated hazmat).
type ClassifyProduct struct {
	Classifications ports.ProductClassificationRepo
	Events          ports.EventPublisher
	Clock           ports.Clock
	// UnitOfWork brackets Save and Publish atomically (ADR 0017). Optional:
	// nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
}

// Execute validates and persists a ProductClassification for sku, and
// publishes ProductClassified. dotHazardClass is optional (pass
// product.DOTHazardClassUnspecified when none applies); the aggregate
// constructor enforces that a non-zero value is only valid alongside the
// Hazmat tag (ADR 0010) — this use case does not duplicate that check.
func (uc *ClassifyProduct) Execute(ctx context.Context, sku shared.SKU, tags []product.HandlingTag, temperatureClass product.TemperatureClass, dotHazardClass product.DOTHazardClass) (*product.ProductClassification, error) {
	c, err := product.New(sku, tags, temperatureClass, dotHazardClass)
	if err != nil {
		return nil, err
	}

	now := uc.Clock.Now()
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Classifications.Save(ctx, c); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, product.NewProductClassified(c, now))
	})
	if err != nil {
		return nil, err
	}

	return c, nil
}
