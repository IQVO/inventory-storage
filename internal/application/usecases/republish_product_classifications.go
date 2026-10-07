package usecases

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// DefaultRepublishBatchSize is how many classifications
// RepublishProductClassifications enqueues per transaction.
const DefaultRepublishBatchSize = 500

// RepublishProductClassifications is the one-shot backfill of
// product-master ADR 0003 stage B (this repo's ADR 0034): it re-emits every
// stored classification as the legacy ProductClassified event through the
// event publisher (the transactional outbox in production), so
// product-master's legacy importer learns the classifications that existed
// before this service started publishing them (ADR 0031 did not backfill).
//
// Each message is a full-state replacement keyed by SKU, so running it again
// is harmless. Each batch commits in its own UnitOfWork: a crash midway
// leaves the finished batches enqueued and a re-run enqueues everything
// again. Removed at product-master ADR 0003 stage E.
type RepublishProductClassifications struct {
	Catalogue ports.ProductClassificationCatalogue
	Events    ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets one batch of publishes. Optional.
	UnitOfWork ports.UnitOfWork
	// BatchSize is the page size; <= 0 means DefaultRepublishBatchSize.
	BatchSize int
}

// Execute pages through every classification and enqueues one
// ProductClassified per row. It returns how many were enqueued; on error the
// count covers only the batches already committed.
func (uc *RepublishProductClassifications) Execute(ctx context.Context) (int, error) {
	size := uc.BatchSize
	if size <= 0 {
		size = DefaultRepublishBatchSize
	}

	total := 0
	var after shared.SKU
	for {
		page, err := uc.Catalogue.ListAfter(ctx, after, size)
		if err != nil {
			return total, err
		}
		if len(page) == 0 {
			return total, nil
		}

		now := uc.Clock.Now()
		err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
			for _, c := range page {
				if err := uc.Events.Publish(ctx, product.NewProductClassified(c, now)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return total, err
		}

		total += len(page)
		after = page[len(page)-1].SKU()
		if len(page) < size {
			return total, nil
		}
	}
}
