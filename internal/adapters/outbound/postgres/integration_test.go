//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// migrationsDir resolves /migrations relative to this package, so the
// tests work regardless of the working directory `go test` is invoked from.
func migrationsDir(t *testing.T) string {
	t.Helper()
	dir, err := migrationsPath()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestPostgres_BinRoundTrip runs against a private, fully-migrated database
// (outboxDB, testdb_integration_test.go) in the package's shared
// testcontainers Postgres — never an external DATABASE_URL, never t.Skip.
func TestPostgres_BinRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	repo := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("IT-BIN-1")
	capacity, _ := shared.NewQuantity(10)
	bin, err := location.NewBin(binID, capacity)
	if err != nil {
		t.Fatalf("unexpected error building bin: %v", err)
	}
	if err := bin.Occupy(mustQty(t, 4)); err != nil {
		t.Fatalf("unexpected error occupying bin: %v", err)
	}

	if err := repo.Save(ctx, bin); err != nil {
		t.Fatalf("unexpected error saving bin: %v", err)
	}

	found, err := repo.FindByID(ctx, binID)
	if err != nil {
		t.Fatalf("unexpected error finding bin: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find the saved bin")
	}
	if found.Occupied().Int() != 4 {
		t.Fatalf("expected occupied=4, got %d", found.Occupied().Int())
	}
}

func mustQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return q
}
