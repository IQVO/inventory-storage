package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ProductMasterClassificationConsumer is the processed_events consumer name
// under which ApplyProductClassification claims CloudEvents ids.
const ProductMasterClassificationConsumer = "product-master-classification"

// ErrMalformedProductClassification marks a product-master classification
// that can never be applied (missing id, invalid SKU, unknown tag, broken
// invariant, version < 1). It is deterministic: the consumer logs it and
// commits past the message, never retries it.
var ErrMalformedProductClassification = errors.New("malformed product-master classification")

// ProductClassificationUpdate is one product-master ProductClassified
// occurrence, as decoded by the inbound adapter (wire strings, not yet
// validated).
type ProductClassificationUpdate struct {
	// EventID is the CloudEvents id, the dedupe key.
	EventID          string
	SKU              string
	HandlingTags     []string
	TemperatureClass string // "" when unset
	DOTHazardClass   int    // 0 when unset
	// ClassificationSource is product-master's label (native |
	// legacy-import), stored verbatim on the local copy.
	ClassificationSource string
	// Version is product-master's Product.version (>= 1).
	Version int64
}

// ApplyOutcome says what ApplyProductClassification did with one update.
type ApplyOutcome string

const (
	// ClassificationApplied: the local copy now holds this version.
	ClassificationApplied ApplyOutcome = "APPLIED"
	// ClassificationStale: the stored version is equal or newer; no write.
	ClassificationStale ApplyOutcome = "STALE"
	// ClassificationDuplicate: this CloudEvents id was already applied.
	ClassificationDuplicate ApplyOutcome = "DUPLICATE"
)

// ApplyProductClassification keeps this service's local copy of a SKU's
// handling classification in step with product-master, the owner
// (ADR 0033). The CloudEvents id claim and the version-guarded upsert run in
// ONE UnitOfWork, so a failure rolls both back and the redelivery is applied
// in full. It raises no domain event: product-master -> inventory-storage is
// one-way, nothing reaches the outbox.
type ApplyProductClassification struct {
	Classifications ports.ProductClassificationLocalCopy
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and the upsert. Optional: nil means "no
	// transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
}

// Execute validates u and applies it. It returns an error wrapping
// ErrMalformedProductClassification for a deterministic problem, and the
// repository error unchanged for a transient one.
func (uc *ApplyProductClassification) Execute(ctx context.Context, u ProductClassificationUpdate) (ApplyOutcome, error) {
	c, err := u.classification()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedProductClassification, err)
	}

	outcome := ClassificationDuplicate
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, ProductMasterClassificationConsumer, u.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			outcome = ClassificationDuplicate
			return nil
		}
		applied, err := uc.Classifications.ApplyIfNewer(ctx, c, u.Version, u.ClassificationSource)
		if err != nil {
			return err
		}
		outcome = ClassificationStale
		if applied {
			outcome = ClassificationApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// classification validates the wire fields through the domain constructor,
// so the local copy only ever holds classifications that satisfy the
// ADR 0009/0010 invariants.
func (u ProductClassificationUpdate) classification() (*product.ProductClassification, error) {
	if u.EventID == "" {
		return nil, errors.New("empty event id")
	}
	if u.Version < 1 {
		return nil, fmt.Errorf("version %d is not positive", u.Version)
	}
	sku, err := shared.NewSKU(u.SKU)
	if err != nil {
		return nil, err
	}
	tags := make([]product.HandlingTag, 0, len(u.HandlingTags))
	for _, raw := range u.HandlingTags {
		tag, err := product.ParseHandlingTag(raw)
		if err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	var temperatureClass product.TemperatureClass
	if u.TemperatureClass != "" {
		temperatureClass, err = product.ParseTemperatureClass(u.TemperatureClass)
		if err != nil {
			return nil, err
		}
	}
	return product.New(sku, tags, temperatureClass, product.DOTHazardClass(u.DOTHazardClass))
}
