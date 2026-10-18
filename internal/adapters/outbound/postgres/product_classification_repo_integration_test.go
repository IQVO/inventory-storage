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

	applied, err := repo.ApplyIfNewer(ctx, c, 1, "native")
	if err != nil || !applied {
		t.Fatalf("first ApplyIfNewer: applied=%t err=%v, want an insert", applied, err)
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

	// A newer version replaces the row (full-state replacement).
	c2, err := product.New(sku, []product.HandlingTag{product.Fragile}, "", 0)
	if err != nil {
		t.Fatalf("unexpected error building second classification: %v", err)
	}
	if applied, err := repo.ApplyIfNewer(ctx, c2, 2, "native"); err != nil || !applied {
		t.Fatalf("newer ApplyIfNewer: applied=%t err=%v", applied, err)
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

	// An older or equal version is a no-op.
	for _, v := range []int64{1, 2} {
		if applied, err := repo.ApplyIfNewer(ctx, c, v, "native"); err != nil || applied {
			t.Fatalf("version %d: applied=%t err=%v, want a no-op", v, applied, err)
		}
	}
	if again, _ := repo.FindBySKU(ctx, sku); again.HasTag(product.Hazmat) {
		t.Fatal("a stale version overwrote the row")
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

func TestPostgres_ProcessedEvents_ClaimOnce(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewProcessedEventRepo(pool)

	if ok, err := repo.Claim(ctx, "c1", "id-1"); err != nil || !ok {
		t.Fatalf("first claim: ok=%t err=%v", ok, err)
	}
	if ok, err := repo.Claim(ctx, "c1", "id-1"); err != nil || ok {
		t.Fatalf("second claim: ok=%t err=%v, want false", ok, err)
	}
	if ok, err := repo.Claim(ctx, "c2", "id-1"); err != nil || !ok {
		t.Fatalf("same id under another consumer: ok=%t err=%v, want true", ok, err)
	}
}
