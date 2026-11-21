//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func totePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), migratedDB(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// seedToteBins inserts the bins the stock units below reference
// (stock_units.bin_id REFERENCES bins).
func seedToteBins(t *testing.T, pool *pgxpool.Pool, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := pool.Exec(context.Background(), `INSERT INTO bins (id, capacity, occupied) VALUES ($1, 100, 0)`, id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestToteRepoRoundTripAndVersionGuard(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	repo := postgres.NewToteRepo(pool)
	tote, _ := container.RegisterTote("TOTE-1")
	if ok, err := repo.Add(ctx, tote); !ok || err != nil {
		t.Fatalf("add = %v %v", ok, err)
	}
	if ok, _ := repo.Add(ctx, tote); ok {
		t.Fatal("second add of the same LPN reported created")
	}
	loaded, _ := repo.FindByLPN(ctx, "TOTE-1")
	if err := loaded.PlaceIn("WH1-AUTO-ASRS-ASRS1-00-00-S", "WH1"); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, loaded); !errors.Is(err, usecases.ErrConcurrentModification) {
		t.Fatalf("stale save err = %v", err)
	}
	got, _ := repo.FindByLPN(ctx, "TOTE-1")
	want := container.ToteSnapshot{LPN: "TOTE-1", SiteID: "WH1", BinID: "WH1-AUTO-ASRS-ASRS1-00-00-S", Version: 2}
	if got.Snapshot() != want {
		t.Fatalf("stored = %+v, want %+v", got.Snapshot(), want)
	}
	page, _ := repo.FindByBinAfter(ctx, "WH1-AUTO-ASRS-ASRS1-00-00-S", "", 10)
	if len(page) != 1 || page[0].LPN() != "TOTE-1" {
		t.Fatalf("page = %v", page)
	}
	if missing, _ := repo.FindByLPN(ctx, "NOPE"); missing != nil {
		t.Fatal("unknown tote found")
	}
}

// carry (Task 6 review): two readers load the same tote; the first save wins
// and the second, at a stale version, is refused with ErrConcurrentModification
// inside a unit of work, which rolls back every other write of that unit
// (here a second tote and its license-plate claim). The stored tote keeps the
// winner's bin.
func TestStaleToteSaveIsRefusedAndRollsBackItsUnitOfWork(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	repo, uow := postgres.NewToteRepo(pool), postgres.NewUnitOfWork(pool)
	tote, _ := container.RegisterTote("TOTE-1")
	if ok, err := repo.Add(ctx, tote); !ok || err != nil {
		t.Fatalf("add = %v %v", ok, err)
	}
	winner, _ := repo.FindByLPN(ctx, "TOTE-1")
	loser, _ := repo.FindByLPN(ctx, "TOTE-1")
	_ = winner.PlaceIn("WH1-RCV-DEC-D01-01-01-A", "WH1")
	if err := repo.Save(ctx, winner); err != nil {
		t.Fatalf("winner save: %v", err)
	}
	_ = loser.PlaceIn("WH1-AUTO-ASRS-ASRS1-00-00-S", "WH1")
	err := uow.Execute(ctx, func(ctx context.Context) error {
		other, _ := container.RegisterTote("TOTE-2")
		if _, err := repo.Add(ctx, other); err != nil {
			return err
		}
		return repo.Save(ctx, loser)
	})
	if !errors.Is(err, usecases.ErrConcurrentModification) {
		t.Fatalf("stale save in a unit of work = %v, want ErrConcurrentModification", err)
	}
	got, _ := repo.FindByLPN(ctx, "TOTE-1")
	want := container.ToteSnapshot{LPN: "TOTE-1", SiteID: "WH1", BinID: "WH1-RCV-DEC-D01-01-01-A", Version: 2}
	if got == nil || got.Snapshot() != want {
		t.Fatalf("stored = %+v, want the winner's %+v", got, want)
	}
	if rolledBack, _ := repo.FindByLPN(ctx, "TOTE-2"); rolledBack != nil {
		t.Fatal("TOTE-2 survived the rolled-back unit of work")
	}
	again, _ := container.RegisterTote("TOTE-2")
	if ok, err := repo.Add(ctx, again); !ok || err != nil {
		t.Fatalf("TOTE-2 plate still claimed after the rollback: %v %v", ok, err)
	}
}

func TestOneLicensePlateNamespaceAcrossCasesAndTotes(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	totes, cases := postgres.NewToteRepo(pool), postgres.NewContainerRepo(pool)
	sealed, err := container.Register(container.RegisterSpec{LPN: "003400000000000017", SKU: "SKU-1", GTIN: "24006381333935",
		UnitsPerPack: 12, Quantity: 12, ReceiptID: "R-1", LineNo: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := cases.Add(ctx, sealed); !ok || err != nil {
		t.Fatalf("case add = %v %v", ok, err)
	}
	asTote, _ := container.RegisterTote("003400000000000017")
	if ok, _ := totes.Add(ctx, asTote); ok {
		t.Fatal("a tote took a sealed case's LPN")
	}
	tote, _ := container.RegisterTote("TOTE-2")
	_, _ = totes.Add(ctx, tote)
	asCase, _ := container.Register(container.RegisterSpec{LPN: "TOTE-2", SKU: "SKU-1", GTIN: "24006381333935",
		UnitsPerPack: 12, Quantity: 12, ReceiptID: "R-1", LineNo: 1})
	if ok, _ := cases.Add(ctx, asCase); ok {
		t.Fatal("a sealed case took a tote's LPN")
	}
	if ok, _ := cases.Add(ctx, sealed); ok {
		t.Fatal("a redelivered case reported created")
	}
}

func TestStockUnitContainerAndAllocationContainerRoundTrip(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	stocks, reservations := postgres.NewStockRepo(pool), postgres.NewReservationRepo(pool)
	seedToteBins(t, pool, "WH1-AUTO-ASRS-ASRS1-00-00-S", "WH1-STOR-RSV-A01-01-01-A")
	unit, _ := stock.NewToteUnit("su-t1", "SKU-1", "WH1-AUTO-ASRS-ASRS1-00-00-S", 6, "WH1", "", "TOTE-1")
	plain, _ := stock.NewLooseUnit("su-p1", "SKU-1", "WH1-STOR-RSV-A01-01-01-A", 3, "WH1", "")
	for _, u := range []*stock.StockUnit{unit, plain} {
		if err := stocks.Save(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := stocks.FindByContainer(ctx, "TOTE-1")
	if len(got) != 1 || got[0].ID() != "su-t1" || got[0].ContainerLPN() != "TOTE-1" || !got[0].IsLoose() {
		t.Fatalf("contents = %v", got)
	}
	again, _ := stocks.FindByID(ctx, "su-p1")
	if again.ContainerLPN() != "" {
		t.Fatalf("plain unit container = %q", again.ContainerLPN())
	}
	_ = unit.ReserveFor("", 2)
	_ = stocks.Save(ctx, unit)
	res, err := reservation.New("res-1", "SKU-1", 2, "order-1",
		[]reservation.Allocation{{StockUnitID: "su-t1", BinID: "WH1-AUTO-ASRS-ASRS1-00-00-S", Quantity: 2, ContainerLPN: "TOTE-1"}},
		time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservations.Save(ctx, res); err != nil {
		t.Fatal(err)
	}
	loaded, _ := reservations.FindByID(ctx, "res-1")
	if a := loaded.Allocations(); len(a) != 1 || a[0].ContainerLPN != "TOTE-1" {
		t.Fatalf("allocations = %+v", a)
	}
}

// Migration 0042 backfills the license plates of existing cases and tags
// existing SEALED_CASE units with their case.
func TestMigration0042BackfillsExistingCases(t *testing.T) {
	ctx, url := context.Background(), emptyDB(t)
	dir, err := migrationsPath()
	if err != nil {
		t.Fatal(err)
	}
	m, err := migrate.New("file://"+dir, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Migrate(41); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, stmt := range []string{
		`INSERT INTO bins (id, capacity, occupied) VALUES ('B-1', 100, 12)`,
		`INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state, pack_state) VALUES ('su-c1', 'SKU-1', 'B-1', 12, 0, 'AVAILABLE', 'SEALED_CASE')`,
		`INSERT INTO containers (lpn, receipt_id, line_no, sku, gtin, units_per_pack, qty_eaches, state, bin_id, stock_unit_id)
		 VALUES ('003400000000000017', 'R-1', 1, 'SKU-1', '24006381333935', 12, 12, 'SEALED', 'B-1', 'su-c1')`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := m.Up(); err != nil {
		t.Fatal(err)
	}
	var kind, lpn string
	if err := pool.QueryRow(ctx, `SELECT kind FROM license_plates WHERE lpn = '003400000000000017'`).Scan(&kind); err != nil || kind != "CASE" {
		t.Fatalf("license plate kind = %q %v", kind, err)
	}
	if err := pool.QueryRow(ctx, `SELECT container_lpn FROM stock_units WHERE id = 'su-c1'`).Scan(&lpn); err != nil || lpn != "003400000000000017" {
		t.Fatalf("container_lpn = %q %v", lpn, err)
	}
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
}
