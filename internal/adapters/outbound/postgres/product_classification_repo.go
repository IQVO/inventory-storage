package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ProductClassificationRepo is the pgxpool-backed local copy of
// product-master's classifications (ADR 0034). It implements
// ports.ProductClassificationRepo (FindBySKU, read by StowStock and the
// deprecated GET), ports.ProductClassificationLocalCopy (ApplyIfNewer, the
// product-master consumer) and ports.ProductClassificationCatalogue
// (ListAfter, the one-shot backfill).
type ProductClassificationRepo struct {
	pool *pgxpool.Pool
}

func NewProductClassificationRepo(pool *pgxpool.Pool) *ProductClassificationRepo {
	return &ProductClassificationRepo{pool: pool}
}

// ApplyIfNewer upserts c only when version is greater than the stored
// version, in ONE statement (no read-then-write race): the ON CONFLICT
// branch's WHERE guard turns a stale or equal version into a zero-row
// write. Legacy rows carry version 0, so any product-master version wins.
func (r *ProductClassificationRepo) ApplyIfNewer(ctx context.Context, c *product.ProductClassification, version int64, source string) (bool, error) {
	tags := c.HandlingTags()
	rawTags := make([]string, 0, len(tags))
	for _, tag := range tags {
		rawTags = append(rawTags, string(tag))
	}

	var dotHazardClass *int
	if c.DOTHazardClass() != product.DOTHazardClassUnspecified {
		v := int(c.DOTHazardClass())
		dotHazardClass = &v
	}

	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO product_classifications (sku, handling_tags, temperature_class, dot_hazard_class, version, classification_source)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (sku) DO UPDATE SET
			handling_tags = EXCLUDED.handling_tags,
			temperature_class = EXCLUDED.temperature_class,
			dot_hazard_class = EXCLUDED.dot_hazard_class,
			version = EXCLUDED.version,
			classification_source = EXCLUDED.classification_source
		WHERE product_classifications.version < EXCLUDED.version
	`, c.SKU().String(), rawTags, string(c.TemperatureClass()), dotHazardClass, version, source)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (r *ProductClassificationRepo) FindBySKU(ctx context.Context, sku shared.SKU) (*product.ProductClassification, error) {
	var rawTags []string
	var temperatureClass string
	var dotHazardClass *int
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT handling_tags, temperature_class, dot_hazard_class FROM product_classifications WHERE sku = $1
	`, sku.String()).Scan(&rawTags, &temperatureClass, &dotHazardClass)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rehydrateClassification(sku, rawTags, temperatureClass, dotHazardClass), nil
}

// ListAfter returns up to limit rows whose sku sorts strictly after
// afterSKU, in sku order (keyset pagination on the primary key).
func (r *ProductClassificationRepo) ListAfter(ctx context.Context, afterSKU shared.SKU, limit int) ([]*product.ProductClassification, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT sku, handling_tags, temperature_class, dot_hazard_class
		FROM product_classifications
		WHERE sku > $1
		ORDER BY sku
		LIMIT $2
	`, afterSKU.String(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*product.ProductClassification
	for rows.Next() {
		var rawSKU, temperatureClass string
		var rawTags []string
		var dotHazardClass *int
		if err := rows.Scan(&rawSKU, &rawTags, &temperatureClass, &dotHazardClass); err != nil {
			return nil, err
		}
		out = append(out, rehydrateClassification(shared.SKU(rawSKU), rawTags, temperatureClass, dotHazardClass))
	}
	return out, rows.Err()
}

func rehydrateClassification(sku shared.SKU, rawTags []string, temperatureClass string, dotHazardClass *int) *product.ProductClassification {
	tags := make([]product.HandlingTag, 0, len(rawTags))
	for _, raw := range rawTags {
		tags = append(tags, product.HandlingTag(raw))
	}
	dotClass := product.DOTHazardClassUnspecified
	if dotHazardClass != nil {
		dotClass = product.DOTHazardClass(*dotHazardClass)
	}
	return product.Rehydrate(sku, tags, product.TemperatureClass(temperatureClass), dotClass)
}

// ProcessedEventRepo is the pgxpool-backed ports.ProcessedEventRepo over
// processed_events (migration 0033).
type ProcessedEventRepo struct {
	pool *pgxpool.Pool
}

func NewProcessedEventRepo(pool *pgxpool.Pool) *ProcessedEventRepo {
	return &ProcessedEventRepo{pool: pool}
}

// Claim inserts (consumer, eventID) and reports whether this call recorded
// it. It joins the ctx-bound UnitOfWork transaction, so a rollback of the
// effect also removes the claim.
func (r *ProcessedEventRepo) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)
		ON CONFLICT (consumer, event_id) DO NOTHING
	`, consumer, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
