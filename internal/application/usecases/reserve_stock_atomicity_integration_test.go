//go:build integration

// Real-Postgres proof that ReserveStock is atomic on its OWN UnitOfWork,
// with no idempotency-middleware transaction on the ctx: when the publish
// step fails, the StockUnit's reserved quantity must be rolled back too and
// no reservation row may remain. Boots its own throwaway Postgres via
// testcontainers (ocDB), never an external DATABASE_URL, never t.Skip.
package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func TestIntegration_ReserveStock_PublishFails_RollsBackStockWithoutMiddlewareTx(t *testing.T) {
	pool := ocDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	stockRepo := postgres.NewStockRepo(pool)
	reservations := postgres.NewReservationRepo(pool)

	binID, _ := shared.NewBinId("IT-RESERVE-ATOMIC-BIN")
	bin, err := location.NewBin(binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	sku, _ := shared.NewSKU("IT-RESERVE-ATOMIC-SKU")
	unitID, err := stockRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("next id: %v", err)
	}
	unit, err := stock.NewStockUnit(unitID, sku, binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("build unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}

	// Plain context.Background(): no transaction is on the ctx, i.e. no
	// HTTP idempotency middleware. Atomicity must come from the use
	// case's own UnitOfWork.
	uc := &usecases.ReserveStock{
		Stock:        stockRepo,
		Reservations: reservations,
		Events:       failingEvents{},
		Clock:        memory.NewFixedClock(time.Now().UTC()),
		UnitOfWork:   postgres.NewUnitOfWork(pool),
	}

	if _, err := uc.Execute(ctx, sku, mustQty(t, 6), "it-reserve-atomic-order"); err != errFake {
		t.Fatalf("expected errFake from the failing publisher, got %v", err)
	}

	refetched, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil || refetched == nil {
		t.Fatalf("re-fetch stock unit: %v (unit=%v)", err, refetched)
	}
	if refetched.Reserved().Int() != 0 {
		t.Fatalf("stock unit reserved = %d after a failed ReserveStock, want 0 (the stock save must roll back with the failed publish)", refetched.Reserved().Int())
	}
	rows, err := reservations.FindByDemandRef(ctx, "it-reserve-atomic-order")
	if err != nil {
		t.Fatalf("list reservations: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("found %d reservation rows after a failed ReserveStock, want 0", len(rows))
	}
}
