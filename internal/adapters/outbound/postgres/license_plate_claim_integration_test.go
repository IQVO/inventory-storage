//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// carry (Task 8 ledger): an allocation saved without a container reads back
// with ContainerLPN "" next to one that names its tote (D7 round-trip).
func TestAllocationWithoutContainerReadsBackEmpty(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	stocks, reservations := postgres.NewStockRepo(pool), postgres.NewReservationRepo(pool)
	seedToteBins(t, pool, "WH1-AUTO-ASRS-ASRS1-00-00-S", "WH1-STOR-RSV-A01-01-01-A")
	inTote, _ := stock.NewToteUnit("su-a", "SKU-1", "WH1-AUTO-ASRS-ASRS1-00-00-S", 6, "WH1", "", "TOTE-9")
	loose, _ := stock.NewLooseUnit("su-b", "SKU-1", "WH1-STOR-RSV-A01-01-01-A", 3, "WH1", "")
	for _, u := range []*stock.StockUnit{inTote, loose} {
		if err := stocks.Save(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	res, err := reservation.New("res-2", "SKU-1", 3, "order-2", []reservation.Allocation{
		{StockUnitID: "su-a", BinID: "WH1-AUTO-ASRS-ASRS1-00-00-S", Quantity: 2, ContainerLPN: "TOTE-9"},
		{StockUnitID: "su-b", BinID: "WH1-STOR-RSV-A01-01-01-A", Quantity: 1},
	}, time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservations.Save(ctx, res); err != nil {
		t.Fatal(err)
	}
	loaded, _ := reservations.FindByID(ctx, "res-2")
	a := loaded.Allocations()
	if len(a) != 2 || a[0].ContainerLPN != "TOTE-9" || a[1].ContainerLPN != "" {
		t.Fatalf("allocations = %+v", a)
	}
}

// failInsertsInto makes every INSERT into table raise, so the statement after
// the license-plate claim fails.
func failInsertsInto(t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE FUNCTION fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'insert refused'; END $$`,
		`CREATE TRIGGER fail_insert BEFORE INSERT ON ` + table + ` FOR EACH ROW EXECUTE FUNCTION fail_insert()`,
	} {
		if _, err := pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

func plateCount(t *testing.T, pool *pgxpool.Pool, lpn string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM license_plates WHERE lpn = $1`, lpn).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func sealedCase(t *testing.T, lpn string) *container.Container {
	t.Helper()
	c, err := container.Register(container.RegisterSpec{LPN: lpn, SKU: "SKU-1", GTIN: "24006381333935",
		UnitsPerPack: 12, Quantity: 12, ReceiptID: "R-1", LineNo: 1})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// carry (Task 6 review, D1): the claim and the tote insert are one
// transaction even outside a UnitOfWork, so a failed insert leaves no plate.
func TestToteAddClaimRollsBackWithAFailedInsert(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	failInsertsInto(t, pool, "totes")
	tote, _ := container.RegisterTote("TOTE-ATOMIC")
	if ok, err := postgres.NewToteRepo(pool).Add(ctx, tote); ok || err == nil {
		t.Fatalf("add = %v %v, want the insert's error", ok, err)
	}
	if n := plateCount(t, pool, "TOTE-ATOMIC"); n != 0 {
		t.Fatalf("license plates after a failed tote insert = %d, want 0", n)
	}
}

// carry (Task 6 review, D1): the same for a sealed case.
func TestCaseAddClaimRollsBackWithAFailedInsert(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	failInsertsInto(t, pool, "containers")
	if ok, err := postgres.NewContainerRepo(pool).Add(ctx, sealedCase(t, "003400000000000024")); ok || err == nil {
		t.Fatalf("add = %v %v, want the insert's error", ok, err)
	}
	if n := plateCount(t, pool, "003400000000000024"); n != 0 {
		t.Fatalf("license plates after a failed case insert = %d, want 0", n)
	}
}

// carry (Task 6 review, D1): inside a UnitOfWork the claim joins the use
// case's transaction and rolls back with it.
func TestLicensePlateClaimRollsBackWithTheUnitOfWork(t *testing.T) {
	ctx, pool := context.Background(), totePool(t)
	tote, _ := container.RegisterTote("TOTE-UOW")
	err := postgres.NewUnitOfWork(pool).Execute(ctx, func(ctx context.Context) error {
		if ok, err := postgres.NewToteRepo(pool).Add(ctx, tote); !ok || err != nil {
			t.Fatalf("add = %v %v", ok, err)
		}
		return fmt.Errorf("use case failed after the add")
	})
	if err == nil {
		t.Fatal("unit of work reported success")
	}
	if n := plateCount(t, pool, "TOTE-UOW"); n != 0 {
		t.Fatalf("license plates after a rolled-back unit of work = %d, want 0", n)
	}
}

type claimResult struct {
	ok  bool
	err error
}

// raceToteAndCase registers lpn as a tote and as a sealed case at once, each
// in its own UnitOfWork that holds its transaction open after the add, so the
// later claim blocks on the license_plates key until the first commits.
func raceToteAndCase(t *testing.T, pool *pgxpool.Pool, lpn string) (tote, sealed claimResult) {
	t.Helper()
	ctx, uow := context.Background(), postgres.NewUnitOfWork(pool)
	aTote, _ := container.RegisterTote(lpn)
	aCase := sealedCase(t, lpn)
	start := make(chan struct{})
	var wg sync.WaitGroup
	run := func(out *claimResult, add func(context.Context) (bool, error)) {
		defer wg.Done()
		<-start
		out.err = uow.Execute(ctx, func(ctx context.Context) error {
			ok, err := add(ctx)
			out.ok = ok
			time.Sleep(20 * time.Millisecond)
			return err
		})
	}
	wg.Add(2)
	go run(&tote, func(ctx context.Context) (bool, error) { return postgres.NewToteRepo(pool).Add(ctx, aTote) })
	go run(&sealed, func(ctx context.Context) (bool, error) { return postgres.NewContainerRepo(pool).Add(ctx, aCase) })
	close(start)
	wg.Wait()
	return tote, sealed
}

// carry (Task 6 review, D1): a concurrent tote and carton claim of the same
// LPN yields exactly one winner, and only the winner's row exists.
func TestConcurrentToteAndCaseClaimHasExactlyOneWinner(t *testing.T) {
	pool := totePool(t)
	for i := range 20 {
		lpn := fmt.Sprintf("00340000000000%04d", i)
		tote, sealed := raceToteAndCase(t, pool, lpn)
		if tote.err != nil || sealed.err != nil {
			t.Fatalf("%s: errors tote=%v case=%v", lpn, tote.err, sealed.err)
		}
		if tote.ok == sealed.ok {
			t.Fatalf("%s: tote created=%v, case created=%v, want exactly one winner", lpn, tote.ok, sealed.ok)
		}
		assertOnlyWinnerStored(t, pool, lpn, tote.ok)
	}
}

func assertOnlyWinnerStored(t *testing.T, pool *pgxpool.Pool, lpn string, toteWon bool) {
	t.Helper()
	var kind string
	var totes, cases int
	err := pool.QueryRow(context.Background(), `SELECT
		(SELECT kind FROM license_plates WHERE lpn = $1),
		(SELECT count(*) FROM totes WHERE lpn = $1),
		(SELECT count(*) FROM containers WHERE lpn = $1)`, lpn).Scan(&kind, &totes, &cases)
	if err != nil {
		t.Fatal(err)
	}
	want := map[bool][3]any{true: {"TOTE", 1, 0}, false: {"CASE", 0, 1}}[toteWon]
	if got := [3]any{kind, totes, cases}; got != want {
		t.Fatalf("%s: stored (kind, totes, cases) = %v, want %v", lpn, got, want)
	}
}
