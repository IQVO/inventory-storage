//go:build integration

package postgres_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// migrationsDir resolves /migrations relative to this test file, so the
// test works regardless of the working directory `go test` is invoked from.
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
}

// postgresURL boots a throwaway Postgres via testcontainers (the test owns
// its own database end to end — never an external DATABASE_URL, never
// t.Skip), registers its termination with t.Cleanup, and returns the
// connection URL of the EMPTY database (no migrations applied). Tests that
// need a migrated schema and a pool use outboxDB(t) instead.
func postgresURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return url
}

// TestPostgres_BinRoundTrip boots its own throwaway, fully-migrated Postgres
// via outboxDB (testcontainers) — never an external DATABASE_URL, never
// t.Skip.
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
