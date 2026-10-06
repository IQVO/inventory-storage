//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func TestPostgres_ProductClassificationRoundTrip(t *testing.T) {
	databaseURL := migratedDB(t)

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()

	repo := postgres.NewProductClassificationRepo(pool)
	sku, _ := shared.NewSKU("IT-PRODUCT-SKU")

	c, err := product.New(sku, []product.HandlingTag{product.Hazmat, product.TemperatureSensitive}, product.Frozen, 3)
	if err != nil {
		t.Fatalf("unexpected error building classification: %v", err)
	}

	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("unexpected error saving classification: %v", err)
	}

	found, err := repo.FindBySKU(ctx, sku)
	if err != nil {
		t.Fatalf("unexpected error finding classification: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find the saved classification")
	}
	if !found.HasTag(product.Hazmat) || !found.HasTag(product.TemperatureSensitive) {
		t.Fatalf("expected both tags to round-trip, got %v", found.HandlingTags())
	}
	if found.TemperatureClass() != product.Frozen {
		t.Fatalf("expected TemperatureClass=Frozen, got %v", found.TemperatureClass())
	}
	if found.DOTHazardClass() != 3 {
		t.Fatalf("expected DOTHazardClass=3, got %v", found.DOTHazardClass())
	}

	// A second Save (re-classification) replaces the row via upsert.
	c2, err := product.New(sku, []product.HandlingTag{product.Fragile}, "", 0)
	if err != nil {
		t.Fatalf("unexpected error building second classification: %v", err)
	}
	if err := repo.Save(ctx, c2); err != nil {
		t.Fatalf("unexpected error saving second classification: %v", err)
	}
	refetched, err := repo.FindBySKU(ctx, sku)
	if err != nil {
		t.Fatalf("unexpected error re-finding classification: %v", err)
	}
	if !refetched.HasTag(product.Fragile) || refetched.HasTag(product.Hazmat) {
		t.Fatalf("expected reclassification to replace tags, got %v", refetched.HandlingTags())
	}
	if refetched.DOTHazardClass() != product.DOTHazardClassUnspecified {
		t.Fatalf("expected reclassification to clear DOTHazardClass, got %v", refetched.DOTHazardClass())
	}
}

func TestPostgres_ProductClassification_FindBySKU_UnknownReturnsNil(t *testing.T) {
	databaseURL := migratedDB(t)

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()

	repo := postgres.NewProductClassificationRepo(pool)
	sku, _ := shared.NewSKU("does-not-exist")
	found, err := repo.FindBySKU(ctx, sku)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found != nil {
		t.Fatalf("expected nil for an unknown SKU, got %+v", found)
	}
}
