//go:build integration

// Real-Postgres proof of the ReserveStock idempotency guard: a client
// retry against the same demandRef must not create a second Reservation
// row nor double-reserve the same StockUnit. Boots its own throwaway
// Postgres via testcontainers (ocDB in
// optimistic_concurrency_integration_test.go) — never an external
// DATABASE_URL, never t.Skip. Mirrors the seeding pattern
// used by internal/adapters/outbound/postgres's own reservation
// integration tests (bin -> stock unit, since allocations foreign-key to
// stock_units), but exercises the full ReserveStock use case (not just the
// repo) against a real database so the guard is proven end to end.
package usecases_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// migrationsDirForUsecases resolves /migrations relative to this test
// file, mirroring the postgres package's own migrationsDir helper (not
// reusable across packages since it is unexported there).
func migrationsDirForUsecases(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")
}

func TestIntegration_ReserveStock_SameDemandRefRetried_NoDoubleReserve(t *testing.T) {
	pool := ocDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	stockRepo := postgres.NewStockRepo(pool)
	reservations := postgres.NewReservationRepo(pool)

	binID, _ := shared.NewBinId("IT-RESERVE-IDEMP-BIN")
	bin, err := location.NewBin(binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error building bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("unexpected error saving bin: %v", err)
	}

	sku, _ := shared.NewSKU("IT-RESERVE-IDEMP-SKU")
	unitID, err := stockRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating stock unit ID: %v", err)
	}
	unit, err := stock.NewStockUnit(unitID, sku, binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error building stock unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("unexpected error saving stock unit: %v", err)
	}

	// Unique per test run so a retry within this test is the only thing
	// that can match the demandRef.
	demandRefSuffix, err := reservations.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating demandRef suffix: %v", err)
	}
	demandRef := "it-reserve-idemp-order-" + demandRefSuffix

	clock := memory.NewFixedClock(time.Now().UTC())
	uc := &usecases.ReserveStock{
		Stock:        stockRepo,
		Reservations: reservations,
		Events:       events.NewBufferedPublisher(),
		Clock:        clock,
	}

	// (1) First call creates a reservation.
	first, err := uc.Execute(ctx, sku, mustQty(t, 6), demandRef)
	if err != nil {
		t.Fatalf("unexpected error on first call: %v", err)
	}
	if first == nil {
		t.Fatal("expected a reservation from the first call")
	}

	// (2) A second call with the same demandRef must NOT create a second
	// row and must return the same reservation.
	second, err := uc.Execute(ctx, sku, mustQty(t, 6), demandRef)
	if err != nil {
		t.Fatalf("unexpected error on retried call: %v", err)
	}
	if second.ID() != first.ID() {
		t.Fatalf("expected retry to return the same reservation ID, got first=%s second=%s", first.ID(), second.ID())
	}

	all, err := reservations.FindByDemandRef(ctx, demandRef)
	if err != nil {
		t.Fatalf("unexpected error listing reservations by demandRef: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected exactly 1 persisted reservation for demandRef %q after a retry, got %d", demandRef, len(all))
	}

	// Stock must reflect only ONE reservation of 6 (not double-reserved):
	// re-read the stock unit from Postgres and confirm reserved == 6, not 12.
	refetched, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil {
		t.Fatalf("unexpected error re-fetching stock unit: %v", err)
	}
	if refetched == nil {
		t.Fatal("expected to find the stock unit")
	}
	if refetched.Reserved().Int() != 6 {
		t.Fatalf("expected stock unit reserved=6 (single reservation, not double-reserved), got %d", refetched.Reserved().Int())
	}
}
