//go:build integration

// Proves ADR 0019 (optimistic concurrency) end to end through a REAL use
// case, not just at the repo layer: two goroutines each load the SAME
// StockUnit via ReserveStock's own FindBySKU/Save path, each reserve
// against it in memory, both Save — exactly one must succeed and the
// other must surface usecases.ErrConcurrentModification, proving the
// version flows through Rehydrate -> mutate -> Save transparently with
// ZERO changes to ReserveStock's own logic. Boots its own throwaway
// Postgres via testcontainers, never an external DATABASE_URL, never
// t.Skip.
package usecases_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// ocDB boots a throwaway, fully-migrated Postgres via testcontainers, the
// same recipe internal/adapters/outbound/postgres's own outboxDB(t) uses
// (that helper is unexported to its package, so this is a small
// same-shape duplicate scoped to this file rather than a cross-package
// export).
func ocDB(t *testing.T) *pgxpool.Pool {
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
	if err := postgres.RunMigrations(url, migrationsDirForUsecases(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// barrierStockRepo wraps a real ports.StockRepo but blocks inside
// FindBySKU until releaseAfter goroutines have all called it, then lets
// every one proceed. ReserveStock.Execute is otherwise fast enough
// end-to-end that one goroutine can complete Save before the other even
// starts — without this barrier the race can go fully sequential
// (version 1 -> 2 -> 3, no conflict at all) and prove nothing. This
// forces both goroutines' Rehydrate reads to happen before either
// Save, mirroring the real-world case of two requests arriving close
// together.
type barrierStockRepo struct {
	*postgres.StockRepo
	wg *sync.WaitGroup
}

func (b *barrierStockRepo) FindBySKU(ctx context.Context, sku shared.SKU) ([]*stock.StockUnit, error) {
	units, err := b.StockRepo.FindBySKU(ctx, sku)
	b.wg.Done()
	b.wg.Wait()
	return units, err
}

// TestIntegration_ReserveStock_ConcurrentReservations_ExactlyOneSucceeds
// is the real concurrent-conflict proof through the actual use case
// (ADR 0019's requirement #4/#5): two ReserveStock.Execute calls racing
// against the SAME underlying StockUnit for the SAME sku, both drawing
// from its usable quantity. Exactly one must persist; the other must
// surface ErrConcurrentModification unchanged from the repo layer,
// proving ReserveStock needed zero logic changes to be protected.
func TestIntegration_ReserveStock_ConcurrentReservations_ExactlyOneSucceeds(t *testing.T) {
	pool := ocDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	stockRepo := postgres.NewStockRepo(pool)
	reservations := postgres.NewReservationRepo(pool)

	binID, _ := shared.NewBinId("OC-UC-BIN-1")
	bin, err := location.NewBin(binID, mustQty(t, 100))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	sku, _ := shared.NewSKU("OC-UC-SKU-1")
	unitID, err := stockRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("next id: %v", err)
	}
	// Single StockUnit holding enough usable quantity for BOTH racing
	// reservations to individually fit (10 usable; each asks for 4) —
	// the point is proving the LOST UPDATE is closed, not proving an
	// insufficient-usable rejection.
	unit, err := stock.NewStockUnit(unitID, sku, binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("build unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}

	clock := memory.NewFixedClock(time.Now().UTC())

	var readBarrier sync.WaitGroup
	readBarrier.Add(2)
	racingStock := &barrierStockRepo{StockRepo: stockRepo, wg: &readBarrier}

	var wg sync.WaitGroup
	results := make([]error, 2)
	reservationIDs := make([]string, 2)
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)

	race := func(i int, demandRef string) {
		defer wg.Done()
		uc := &usecases.ReserveStock{
			Stock:        racingStock,
			Reservations: reservations,
			Events:       events.NewBufferedPublisher(),
			Clock:        clock,
		}
		startBarrier.Wait()
		res, err := uc.Execute(ctx, sku, mustQty(t, 4), demandRef)
		results[i] = err
		if res != nil {
			reservationIDs[i] = res.ID()
		}
	}

	wg.Add(2)
	go race(0, "oc-uc-demand-a")
	go race(1, "oc-uc-demand-b")
	startBarrier.Done()
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range results {
		switch err {
		case nil:
			successes++
		case usecases.ErrConcurrentModification:
			conflicts++
		default:
			t.Fatalf("unexpected error from a racing ReserveStock call: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected exactly 1 success and 1 conflict through the real use case, got successes=%d conflicts=%d (results=%v)", successes, conflicts, results)
	}

	// The winning reservation actually persisted with its allocation,
	// and the stock unit reflects EXACTLY one reservation of 4 (not 8 —
	// that would mean the lost-update race let both writes land).
	final, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil || final == nil {
		t.Fatalf("re-find stock unit: %v", err)
	}
	if final.Reserved().Int() != 4 {
		t.Fatalf("expected reserved=4 (exactly one winning reservation), got %d", final.Reserved().Int())
	}
	if final.Version() != 2 {
		t.Fatalf("expected version=2 after exactly one winning write, got %d", final.Version())
	}
}
