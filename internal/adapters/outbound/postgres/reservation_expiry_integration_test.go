//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// fixedClock is a trivial ports.Clock returning a settable time, used only
// by this integration test to control when a reservation is considered
// expired against the real Postgres-backed repos.
type fixedClock struct{ t time.Time }

func (c *fixedClock) Now() time.Time { return c.t }

// TestPostgres_LazyReservationExpiry_PersistsAndWritesOutbox proves the
// full lazy-expiry write-on-read path against a REAL Postgres: a reservation
// looked up after its timeout via GetReservationsByDemandRef.Execute must
// (1) durably transition to EXPIRED in the reservations table, (2) return
// its allocated quantity to the stock_units table's usable pool, and (3) a
// ReservationExpired row must land in outbox_events (this service's
// transactional outbox, ADR 0017), via the same postgres.OutboxPublisher
// every other domain event now goes through — wrapped in a
// postgres.UnitOfWork so the reservation update and the outbox insert
// commit in the one transaction the use case's atomically() call opens.
func TestPostgres_LazyReservationExpiry_PersistsAndWritesOutbox(t *testing.T) {
	databaseURL := postgresURL(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("unexpected error running migrations: %v", err)
	}

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()

	// Seed a bin and a stock unit (reservation allocations foreign-key to
	// stock_units), mirroring TestPostgres_ReservationRoundTrip's pattern.
	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("IT-EXPIRY-BIN")
	bin, err := location.NewBin(binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error building bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("unexpected error saving bin: %v", err)
	}

	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("IT-EXPIRY-SKU")
	unit, err := stock.NewStockUnit("it-expiry-su-1", sku, binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error building stock unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("unexpected error saving stock unit: %v", err)
	}

	reservationRepo := postgres.NewReservationRepo(pool)
	publisher := postgres.NewOutboxPublisher(pool, stubEncoder{})
	uow := postgres.NewUnitOfWork(pool)

	// Build the reservation directly (not through ReserveStock, since
	// ReserveStock itself would also reserve the unit's quantity — doing
	// the reserve step through the unit avoids a second, redundant
	// dependency on the use case layer in this adapter-level test).
	if err := unit.Reserve(mustQty(t, 6)); err != nil {
		t.Fatalf("unexpected error reserving on the unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("unexpected error saving reserved unit: %v", err)
	}

	// Unique per test run (suffixed with a fresh generated id) so reruns
	// against a persistent database don't accumulate reservations under
	// the same demandRef and break the "exactly 1 reservation" assertion
	// below.
	demandRefSuffix, err := reservationRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating demandRef suffix: %v", err)
	}
	demandRef := "it-expiry-order-1-" + demandRefSuffix
	id, err := reservationRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating ID: %v", err)
	}
	created := time.Now().UTC().Truncate(time.Second)
	allocs := []reservation.Allocation{{StockUnitID: unit.ID(), Quantity: mustQty(t, 6)}}
	res, err := reservation.New(id, sku, mustQty(t, 6), demandRef, allocs, created, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error building reservation: %v", err)
	}
	if err := reservationRepo.Save(ctx, res); err != nil {
		t.Fatalf("unexpected error saving reservation: %v", err)
	}

	// Read it back AFTER the timeout has elapsed via the real use case,
	// wired over the real Postgres repos, the real outbox publisher, and
	// the real UnitOfWork — this exercises the exact write-on-read path a
	// live GET /reservations call would take (ADR 0017).
	clock := &fixedClock{t: created.Add(2 * time.Minute)}
	getUC := &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow}
	found, err := getUC.Execute(ctx, demandRef)
	if err != nil {
		t.Fatalf("unexpected error executing GetReservationsByDemandRef: %v", err)
	}
	if len(found) != 1 || found[0].Status() != reservation.StatusExpired {
		t.Fatalf("expected 1 Expired reservation, got %+v", found)
	}

	// (1) The EXPIRED transition is durable.
	persisted, err := reservationRepo.FindByID(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error re-finding reservation: %v", err)
	}
	if persisted.Status() != reservation.StatusExpired {
		t.Fatalf("expected persisted status EXPIRED, got %v", persisted.Status())
	}

	// (2) The stock unit's reserved quantity was released back to usable.
	refetchedUnit, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil {
		t.Fatalf("unexpected error re-finding stock unit: %v", err)
	}
	if refetchedUnit.Usable().Int() != 10 {
		t.Fatalf("expected usable restored to 10 on the real stock_units row, got %d", refetchedUnit.Usable().Int())
	}

	// (3) A ReservationExpired row landed in outbox_events (the outbox).
	var count int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events WHERE event_type = $1
	`, "ReservationExpired").Scan(&count)
	if err != nil {
		t.Fatalf("unexpected error querying outbox_events table: %v", err)
	}
	if count < 1 {
		t.Fatalf("expected at least 1 ReservationExpired row in outbox_events, got %d", count)
	}
}

// TestPostgres_LazyReservationExpiry_NotYetExpired_LeavesReservationActive
// pins the negative case against real Postgres: reading a reservation
// before its timeout must leave both the reservations row and the stock
// unit's reserved quantity untouched, and must not write an outbox row.
func TestPostgres_LazyReservationExpiry_NotYetExpired_LeavesReservationActive(t *testing.T) {
	databaseURL := postgresURL(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("unexpected error running migrations: %v", err)
	}

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()

	locations := postgres.NewLocationRepo(pool)
	binID, _ := shared.NewBinId("IT-NOEXP-BIN")
	bin, err := location.NewBin(binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error building bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("unexpected error saving bin: %v", err)
	}

	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("IT-NOEXP-SKU")
	unit, err := stock.NewStockUnit("it-noexp-su-1", sku, binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error building stock unit: %v", err)
	}
	if err := unit.Reserve(mustQty(t, 6)); err != nil {
		t.Fatalf("unexpected error reserving on the unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("unexpected error saving stock unit: %v", err)
	}

	reservationRepo := postgres.NewReservationRepo(pool)
	publisher := postgres.NewOutboxPublisher(pool, stubEncoder{})
	uow := postgres.NewUnitOfWork(pool)

	// Unique per test run, same rationale as the other integration test in
	// this file (and TestPostgres_Reservation_FindByDemandRef).
	demandRefSuffix, err := reservationRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating demandRef suffix: %v", err)
	}
	demandRef := "it-noexp-order-1-" + demandRefSuffix
	id, err := reservationRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("unexpected error generating ID: %v", err)
	}
	created := time.Now().UTC().Truncate(time.Second)
	allocs := []reservation.Allocation{{StockUnitID: unit.ID(), Quantity: mustQty(t, 6)}}
	res, err := reservation.New(id, sku, mustQty(t, 6), demandRef, allocs, created, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error building reservation: %v", err)
	}
	if err := reservationRepo.Save(ctx, res); err != nil {
		t.Fatalf("unexpected error saving reservation: %v", err)
	}

	// Read it back well before its (1 hour) timeout.
	clock := &fixedClock{t: created.Add(time.Minute)}
	getUC := &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow}
	found, err := getUC.Execute(ctx, demandRef)
	if err != nil {
		t.Fatalf("unexpected error executing GetReservationsByDemandRef: %v", err)
	}
	if len(found) != 1 || found[0].Status() != reservation.StatusActive {
		t.Fatalf("expected 1 Active reservation, got %+v", found)
	}

	refetchedUnit, err := stockRepo.FindByID(ctx, unit.ID())
	if err != nil {
		t.Fatalf("unexpected error re-finding stock unit: %v", err)
	}
	if refetchedUnit.Usable().Int() != 4 {
		t.Fatalf("expected usable unchanged at 4 (10 - 6 reserved), got %d", refetchedUnit.Usable().Int())
	}

	var count int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events WHERE event_type = $1 AND key = $2
	`, "ReservationExpired", []byte(id)).Scan(&count)
	if err != nil {
		t.Fatalf("unexpected error querying outbox_events table: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no ReservationExpired row for a not-yet-expired reservation, got %d", count)
	}
}
