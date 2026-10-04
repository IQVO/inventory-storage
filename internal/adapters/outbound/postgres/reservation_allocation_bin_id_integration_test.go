//go:build integration

// Pick location on allocations (ADR 0025), proven against a real Postgres:
// the 0008 migration's nullable reservation_allocations.bin_id column, its
// backfill from stock_units.bin_id for rows written before it existed, and
// the reservation repo's read/write of it. Every test boots its own
// throwaway Postgres via testcontainers — never an external DATABASE_URL,
// never t.Skip.
package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func TestPostgres_ReservationAllocation_BinID_RoundTrips(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	locations := postgres.NewLocationRepo(pool)
	stockRepo := postgres.NewStockRepo(pool)
	sku, _ := shared.NewSKU("PICK-SKU-1")
	var allocs []reservation.Allocation
	for i, id := range []string{"PICK-BIN-1", "PICK-BIN-2"} {
		binID, _ := shared.NewBinId(id)
		bin, err := location.NewBin(binID, mustQty(t, 10))
		if err != nil {
			t.Fatalf("build bin: %v", err)
		}
		if err := locations.Save(ctx, bin); err != nil {
			t.Fatalf("save bin: %v", err)
		}
		unit, err := stock.NewStockUnit(fmt.Sprintf("pick-su-%d", i+1), sku, binID, mustQty(t, 5))
		if err != nil {
			t.Fatalf("build unit: %v", err)
		}
		if err := stockRepo.Save(ctx, unit); err != nil {
			t.Fatalf("save unit: %v", err)
		}
		allocs = append(allocs, reservation.Allocation{StockUnitID: unit.ID(), BinID: binID, Quantity: mustQty(t, 3)})
	}

	repo := postgres.NewReservationRepo(pool)
	res, err := reservation.New("res-pick-1", sku, mustQty(t, 6), "order-pick-1", allocs, time.Now().UTC().Truncate(time.Second), time.Hour)
	if err != nil {
		t.Fatalf("build reservation: %v", err)
	}
	if err := repo.Save(ctx, res); err != nil {
		t.Fatalf("save reservation: %v", err)
	}

	assertPickLocations := func(t *testing.T, got []reservation.Allocation) {
		t.Helper()
		want := map[string]string{"pick-su-1": "PICK-BIN-1", "pick-su-2": "PICK-BIN-2"}
		if len(got) != len(want) {
			t.Fatalf("expected %d allocations, got %d", len(want), len(got))
		}
		for _, a := range got {
			if want[a.StockUnitID] != a.BinID.String() {
				t.Fatalf("allocation %s: expected bin %q, got %q", a.StockUnitID, want[a.StockUnitID], a.BinID)
			}
		}
	}

	byID, err := repo.FindByID(ctx, res.ID())
	if err != nil || byID == nil {
		t.Fatalf("find by id: %v", err)
	}
	assertPickLocations(t, byID.Allocations())

	byRef, err := repo.FindByDemandRef(ctx, "order-pick-1")
	if err != nil || len(byRef) != 1 {
		t.Fatalf("find by demand ref: %v (got %d)", err, len(byRef))
	}
	assertPickLocations(t, byRef[0].Allocations())

	// A later upsert (revoke) must not lose the recorded pick location.
	if err := byID.Revoke(); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := repo.Save(ctx, byID); err != nil {
		t.Fatalf("save revoked: %v", err)
	}
	revoked, err := repo.FindByID(ctx, res.ID())
	if err != nil {
		t.Fatalf("re-find: %v", err)
	}
	assertPickLocations(t, revoked.Allocations())
}

// The 0008 migration backfills bin_id for allocations written before the
// column existed, from the stock unit each one references.
func TestPostgres_Migration0008_BackfillsAllocationBinID(t *testing.T) {
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

	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Migrate(7); err != nil {
		t.Fatalf("migrate to 0007: %v", err)
	}

	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// Legacy rows, written with the pre-0008 schema (no bin_id column).
	for _, stmt := range []string{
		`INSERT INTO bins (id, capacity, occupied) VALUES ('LEGACY-BIN', 10, 5)`,
		`INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state) VALUES ('legacy-su', 'LEGACY-SKU', 'LEGACY-BIN', 5, 2, 'RESERVED')`,
		`INSERT INTO reservations (id, sku, quantity, demand_ref, status, created_at, expires_at) VALUES ('res-legacy', 'LEGACY-SKU', 2, 'order-legacy', 'ACTIVE', now(), now() + interval '1 hour')`,
		`INSERT INTO reservation_allocations (reservation_id, stock_unit_id, quantity) VALUES ('res-legacy', 'legacy-su', 2)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed legacy row (%s): %v", stmt, err)
		}
	}

	if err := m.Migrate(8); err != nil {
		t.Fatalf("migrate to 0008: %v", err)
	}

	var binID *string
	if err := pool.QueryRow(ctx, `SELECT bin_id FROM reservation_allocations WHERE reservation_id = 'res-legacy'`).Scan(&binID); err != nil {
		t.Fatalf("read backfilled bin_id: %v", err)
	}
	if binID == nil || *binID != "LEGACY-BIN" {
		t.Fatalf("expected bin_id backfilled to LEGACY-BIN, got %v", binID)
	}

	res, err := postgres.NewReservationRepo(pool).FindByID(ctx, "res-legacy")
	if err != nil || res == nil {
		t.Fatalf("find legacy reservation: %v", err)
	}
	if got := res.Allocations()[0].BinID.String(); got != "LEGACY-BIN" {
		t.Fatalf("expected legacy allocation to hydrate with bin LEGACY-BIN, got %q", got)
	}

	// The column is nullable: a row whose bin_id is genuinely unknown
	// hydrates with an empty BinID instead of failing the read.
	if _, err := pool.Exec(ctx, `UPDATE reservation_allocations SET bin_id = NULL WHERE reservation_id = 'res-legacy'`); err != nil {
		t.Fatalf("null out bin_id: %v", err)
	}
	res, err = postgres.NewReservationRepo(pool).FindByID(ctx, "res-legacy")
	if err != nil || res == nil {
		t.Fatalf("find legacy reservation with null bin_id: %v", err)
	}
	if got := res.Allocations()[0].BinID; got != "" {
		t.Fatalf("expected empty BinID for a NULL bin_id, got %q", got)
	}

	// The down migration drops the column cleanly.
	if err := m.Migrate(7); err != nil {
		t.Fatalf("migrate down to 0007: %v", err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'reservation_allocations' AND column_name = 'bin_id')`).Scan(&exists); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if exists {
		t.Fatal("expected bin_id column to be dropped by the down migration")
	}
}

// RegisterBin's resize persists through the version-guarded
// LocationRepo.Save (ADR 0019): a resize bumps the version and keeps
// occupancy intact.
func TestPostgres_LocationRepo_Resize_Persists(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewLocationRepo(pool)

	binID, _ := shared.NewBinId("RESIZE-BIN-1")
	bin, err := location.NewBin(binID, mustQty(t, 10))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := bin.Occupy(mustQty(t, 4)); err != nil {
		t.Fatalf("occupy: %v", err)
	}
	if err := repo.Save(ctx, bin); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := repo.FindByID(ctx, binID)
	if err != nil || loaded == nil {
		t.Fatalf("find: %v", err)
	}
	if err := loaded.Resize(mustQty(t, 6)); err != nil {
		t.Fatalf("resize: %v", err)
	}
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("save resized: %v", err)
	}

	final, err := repo.FindByID(ctx, binID)
	if err != nil {
		t.Fatalf("re-find: %v", err)
	}
	if final.Capacity().Int() != 6 || final.Occupied().Int() != 4 || final.Version() != 2 {
		t.Fatalf("expected capacity=6 occupied=4 version=2, got capacity=%d occupied=%d version=%d",
			final.Capacity().Int(), final.Occupied().Int(), final.Version())
	}
}
