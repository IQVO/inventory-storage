//go:build integration

// Optimistic-concurrency (ADR 0019) proof, at the repo layer, for all
// three protected aggregates (StockUnit, Bin, Reservation): a Save with a
// stale version must fail with usecases.ErrConcurrentModification and
// leave the row untouched; a Save with the current version must succeed
// and increment the persisted version. Every test here runs on its own
// private database in the package's shared testcontainers Postgres
// (outboxDB, testdb_integration_test.go) — never an external DATABASE_URL,
// never t.Skip.
package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// --- StockUnit ---------------------------------------------------------

func TestPostgres_StockRepo_Save_StaleVersion_FailsWithConcurrentModification(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("OC-STOCK-BIN-1")
	bin, err := location.NewBin(binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("OC-STOCK-SKU-1")
	unit, err := stock.NewStockUnit("oc-su-1", sku, binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("build unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	// Two independent readers load the same row at version 1.
	readerA, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil || readerA == nil {
		t.Fatalf("find readerA: %v", err)
	}
	readerB, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil || readerB == nil {
		t.Fatalf("find readerB: %v", err)
	}

	// A saves first: succeeds, row moves to version 2.
	if err := readerA.Reserve(mustQty(t, 3)); err != nil {
		t.Fatalf("reserve on A: %v", err)
	}
	if err := stockRepo.Save(ctx, readerA); err != nil {
		t.Fatalf("expected A's save to succeed, got %v", err)
	}

	// B, still holding the stale version-1 snapshot, must be rejected —
	// this is the exact lost-update race the blind ON CONFLICT DO UPDATE
	// used to allow silently.
	if err := readerB.Reserve(mustQty(t, 2)); err != nil {
		t.Fatalf("reserve on B: %v", err)
	}
	err = stockRepo.Save(ctx, readerB)
	if err == nil {
		t.Fatal("expected B's stale-version save to fail")
	}
	if err != usecases.ErrConcurrentModification {
		t.Fatalf("expected ErrConcurrentModification, got %v", err)
	}

	// The row reflects ONLY A's write — B's write never landed.
	final, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil {
		t.Fatalf("re-find: %v", err)
	}
	if final.Reserved().Int() != 3 {
		t.Fatalf("expected reserved=3 (only A's write), got %d — B's write was not actually rejected", final.Reserved().Int())
	}
	if final.Version() != 2 {
		t.Fatalf("expected version=2 after exactly one successful write, got %d", final.Version())
	}
}

func TestPostgres_StockRepo_Save_CurrentVersion_SucceedsAndIncrements(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("OC-STOCK-BIN-2")
	bin, _ := location.NewBin(binID, mustQty(t, 10))
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("OC-STOCK-SKU-2")
	unit, _ := stock.NewStockUnit("oc-su-2", sku, binID, mustQty(t, 10))
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	if unit.Version() != 1 {
		t.Fatalf("expected a freshly constructed aggregate to start at version 1, got %d", unit.Version())
	}

	found, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil || found == nil {
		t.Fatalf("find: %v", err)
	}
	if found.Version() != 1 {
		t.Fatalf("expected version=1 on first read, got %d", found.Version())
	}

	if err := found.Reserve(mustQty(t, 4)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := stockRepo.Save(ctx, found); err != nil {
		t.Fatalf("expected current-version save to succeed, got %v", err)
	}

	refetched, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil || refetched == nil {
		t.Fatalf("re-find: %v", err)
	}
	if refetched.Version() != 2 {
		t.Fatalf("expected version incremented to 2, got %d", refetched.Version())
	}
	if refetched.Reserved().Int() != 4 {
		t.Fatalf("expected the write to persist, got reserved=%d", refetched.Reserved().Int())
	}
}

// TestPostgres_StockRepo_Save_ConcurrentGoroutines_ExactlyOneSucceeds is
// the real two-goroutine race: both goroutines FindByID the SAME row,
// both mutate in memory, both Save at (roughly) the same time. Exactly
// one must succeed; the other must get ErrConcurrentModification. This
// proves the guard against genuine concurrent commits, not just a
// sequential simulation of one.
func TestPostgres_StockRepo_Save_ConcurrentGoroutines_ExactlyOneSucceeds(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("OC-STOCK-BIN-3")
	bin, _ := location.NewBin(binID, mustQty(t, 100))
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("OC-STOCK-SKU-3")
	unit, _ := stock.NewStockUnit("oc-su-3", sku, binID, mustQty(t, 20))
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]error, 2)
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	// readBarrier forces BOTH goroutines to complete their FindByID
	// before either is allowed to Save — without this, goroutine 0 could
	// legitimately read-then-write to completion before goroutine 1 even
	// reads, which would make both succeed sequentially (v1->v2->v3) and
	// prove nothing about a genuine concurrent-commit race.
	var readBarrier sync.WaitGroup
	readBarrier.Add(2)

	race := func(i int, qty int) {
		defer wg.Done()
		startBarrier.Wait() // maximize the chance both goroutines read before either writes
		loaded, err := stockRepo.FindByID(ctx, unit.ID())
		readBarrier.Done()
		readBarrier.Wait()
		if err != nil || loaded == nil {
			results[i] = err
			return
		}
		if err := loaded.Reserve(mustQty(t, qty)); err != nil {
			results[i] = err
			return
		}
		results[i] = stockRepo.Save(ctx, loaded)
	}

	wg.Add(2)
	go race(0, 3)
	go race(1, 5)
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
			t.Fatalf("unexpected error from a racing goroutine: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected exactly 1 success and 1 conflict, got successes=%d conflicts=%d (results=%v)", successes, conflicts, results)
	}

	final, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil || final == nil {
		t.Fatalf("re-find: %v", err)
	}
	if final.Version() != 2 {
		t.Fatalf("expected version=2 after exactly one winning write, got %d", final.Version())
	}
	// Reserved must equal EXACTLY one of the two attempted quantities (3
	// or 5), never both summed (8) — that would mean both writes landed,
	// which is the lost-update bug this closes.
	if final.Reserved().Int() != 3 && final.Reserved().Int() != 5 {
		t.Fatalf("expected reserved to reflect exactly one winning write (3 or 5), got %d", final.Reserved().Int())
	}
}

// --- Bin -----------------------------------------------------------

func TestPostgres_LocationRepo_Save_StaleVersion_FailsWithConcurrentModification(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("OC-BIN-1")
	bin, err := location.NewBin(binID, mustQty(t, 20))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	readerA, err := locations.FindByID(ctx, binID)
	if err != nil || readerA == nil {
		t.Fatalf("find readerA: %v", err)
	}
	readerB, err := locations.FindByID(ctx, binID)
	if err != nil || readerB == nil {
		t.Fatalf("find readerB: %v", err)
	}

	if err := readerA.Occupy(mustQty(t, 5)); err != nil {
		t.Fatalf("occupy A: %v", err)
	}
	if err := locations.Save(ctx, readerA); err != nil {
		t.Fatalf("expected A's save to succeed, got %v", err)
	}

	if err := readerB.Occupy(mustQty(t, 7)); err != nil {
		t.Fatalf("occupy B: %v", err)
	}
	err = locations.Save(ctx, readerB)
	if err != usecases.ErrConcurrentModification {
		t.Fatalf("expected ErrConcurrentModification for B's stale write, got %v", err)
	}

	final, err := locations.FindByID(ctx, binID)
	if err != nil || final == nil {
		t.Fatalf("re-find: %v", err)
	}
	if final.Occupied().Int() != 5 {
		t.Fatalf("expected occupied=5 (only A's write survived), got %d", final.Occupied().Int())
	}
	if final.Version() != 2 {
		t.Fatalf("expected version=2, got %d", final.Version())
	}
}

func TestPostgres_LocationRepo_Save_CurrentVersion_SucceedsAndIncrements(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("OC-BIN-2")
	bin, _ := location.NewBin(binID, mustQty(t, 20))
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	found, err := locations.FindByID(ctx, binID)
	if err != nil || found == nil {
		t.Fatalf("find: %v", err)
	}
	if err := found.Occupy(mustQty(t, 6)); err != nil {
		t.Fatalf("occupy: %v", err)
	}
	if err := locations.Save(ctx, found); err != nil {
		t.Fatalf("expected current-version save to succeed, got %v", err)
	}

	refetched, err := locations.FindByID(ctx, binID)
	if err != nil || refetched == nil {
		t.Fatalf("re-find: %v", err)
	}
	if refetched.Version() != 2 || refetched.Occupied().Int() != 6 {
		t.Fatalf("expected version=2 occupied=6, got version=%d occupied=%d", refetched.Version(), refetched.Occupied().Int())
	}
}

// --- Reservation -----------------------------------------------------

func TestPostgres_ReservationRepo_Save_StaleVersion_FailsWithConcurrentModification(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("OC-RES-BIN-1")
	bin, _ := location.NewBin(binID, mustQty(t, 10))
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("OC-RES-SKU-1")
	unit, _ := stock.NewStockUnit("oc-res-su-1", sku, binID, mustQty(t, 10))
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}

	reservations := postgres.NewReservationRepo(pool)
	id, err := reservations.NextID(ctx)
	if err != nil {
		t.Fatalf("next id: %v", err)
	}
	allocs := []reservation.Allocation{{StockUnitID: unit.ID(), Quantity: mustQty(t, 4)}}
	res, err := reservation.New(id, sku, mustQty(t, 4), "oc-demand-1", allocs, time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatalf("build reservation: %v", err)
	}
	if err := reservations.Save(ctx, res); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	readerA, err := reservations.FindByID(ctx, id)
	if err != nil || readerA == nil {
		t.Fatalf("find readerA: %v", err)
	}
	readerB, err := reservations.FindByID(ctx, id)
	if err != nil || readerB == nil {
		t.Fatalf("find readerB: %v", err)
	}

	if err := readerA.Revoke(); err != nil {
		t.Fatalf("revoke A: %v", err)
	}
	if err := reservations.Save(ctx, readerA); err != nil {
		t.Fatalf("expected A's save to succeed, got %v", err)
	}

	// B tries to confirm the same (now-stale) snapshot.
	err = reservations.Save(ctx, readerB)
	if err != usecases.ErrConcurrentModification {
		t.Fatalf("expected ErrConcurrentModification for B's stale write, got %v", err)
	}

	final, err := reservations.FindByID(ctx, id)
	if err != nil || final == nil {
		t.Fatalf("re-find: %v", err)
	}
	if final.Status() != reservation.StatusRevoked {
		t.Fatalf("expected status Revoked (only A's write survived), got %v", final.Status())
	}
	if final.Version() != 2 {
		t.Fatalf("expected version=2, got %d", final.Version())
	}
}

func TestPostgres_ReservationRepo_Save_CurrentVersion_SucceedsAndIncrements(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("OC-RES-BIN-2")
	bin, _ := location.NewBin(binID, mustQty(t, 10))
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("OC-RES-SKU-2")
	unit, _ := stock.NewStockUnit("oc-res-su-2", sku, binID, mustQty(t, 10))
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}

	reservations := postgres.NewReservationRepo(pool)
	id, err := reservations.NextID(ctx)
	if err != nil {
		t.Fatalf("next id: %v", err)
	}
	allocs := []reservation.Allocation{{StockUnitID: unit.ID(), Quantity: mustQty(t, 4)}}
	res, err := reservation.New(id, sku, mustQty(t, 4), "oc-demand-2", allocs, time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatalf("build reservation: %v", err)
	}
	if err := reservations.Save(ctx, res); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	if res.Version() != 1 {
		t.Fatalf("expected a freshly constructed reservation to start at version 1, got %d", res.Version())
	}

	found, err := reservations.FindByID(ctx, id)
	if err != nil || found == nil {
		t.Fatalf("find: %v", err)
	}
	if found.Version() != 1 {
		t.Fatalf("expected version=1 on first read, got %d", found.Version())
	}
	if err := found.Revoke(); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := reservations.Save(ctx, found); err != nil {
		t.Fatalf("expected current-version save to succeed, got %v", err)
	}

	refetched, err := reservations.FindByID(ctx, id)
	if err != nil || refetched == nil {
		t.Fatalf("re-find: %v", err)
	}
	if refetched.Version() != 2 || refetched.Status() != reservation.StatusRevoked {
		t.Fatalf("expected version=2 status=Revoked, got version=%d status=%v", refetched.Version(), refetched.Status())
	}
}
