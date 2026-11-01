//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// The keyset SQL behind the two paginated collections (REST API guideline
// section 3.4) against a real Postgres: more rows than the page size, rows
// that share the sort key, and an id order that must be the byte-wise order of
// the in-memory repositories (hence the "C" collation in the queries).

// TestContainerRepo_FindByBinAfter_WalksEveryPageInByteOrder: the LPN is the
// whole keyset. The LPNs mix digits, upper and lower case, whose byte order
// (digits < upper < lower) differs from most locales' collation.
func TestContainerRepo_FindByBinAfter_WalksEveryPageInByteOrder(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewContainerRepo(pool)

	lpns := []string{"PG-a1", "PG-A1", "PG-b1", "PG-B1", "PG-a2", "PG-A2", "PG-10", "PG-9"}
	for i, lpn := range lpns {
		if added, err := repo.Add(ctx, ctRegister(t, lpn)); err != nil || !added {
			t.Fatalf("add %s = %v %v", lpn, added, err)
		}
		c, err := repo.FindByLPN(ctx, lpn)
		if err != nil || c == nil {
			t.Fatalf("find %s = %v %v", lpn, c, err)
		}
		if err := c.Stow(ctCaseBin, "S1", fmt.Sprintf("su-pg-%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, c); err != nil {
			t.Fatalf("save %s: %v", lpn, err)
		}
	}
	want := slices.Clone(lpns)
	slices.Sort(want) // Go compares bytes, like the "C" collation

	for _, limit := range []int{1, 3, len(lpns), len(lpns) + 1} {
		var got []string
		after := ""
		for pages := 0; ; pages++ {
			if pages > len(lpns)+1 {
				t.Fatalf("limit %d: walk did not terminate", limit)
			}
			rows, err := repo.FindByBinAfter(ctx, ctCaseBin, after, limit+1) // limit+1: one row of look-ahead
			if err != nil {
				t.Fatal(err)
			}
			more := len(rows) > limit
			if more {
				rows = rows[:limit]
			}
			for _, c := range rows {
				got = append(got, c.LPN())
			}
			if !more {
				break
			}
			after = rows[len(rows)-1].LPN()
		}
		if !slices.Equal(got, want) {
			t.Fatalf("limit %d: walk = %v, want %v", limit, got, want)
		}
	}
}

// TestPostgres_Reservation_FindByDemandRefAfter_WalksEveryPageWithTies: seven
// reservations in two created_at groups (3 + 4 rows tie on the sort key), ids
// of mixed case. The (created_at, id) tuple comparison must neither repeat nor
// skip a row at any page size.
func TestPostgres_Reservation_FindByDemandRefAfter_WalksEveryPageWithTies(t *testing.T) {
	databaseURL := migratedDB(t)
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("unexpected error opening pool: %v", err)
	}
	defer pool.Close()

	binID, _ := shared.NewBinId("IT-PAGE-BIN")
	bin, err := location.NewBin(binID, mustQty(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewLocationRepo(pool).Save(ctx, bin); err != nil {
		t.Fatal(err)
	}
	sku, _ := shared.NewSKU("IT-PAGE-SKU")
	unit, err := stock.NewStockUnit("it-page-su-1", sku, binID, mustQty(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewStockRepo(pool).Save(ctx, unit); err != nil {
		t.Fatal(err)
	}

	repo := postgres.NewReservationRepo(pool)
	first := time.Now().UTC().Truncate(time.Second)
	second := first.Add(time.Second)
	rows := []struct {
		id      string
		created time.Time
	}{
		{"res-pg-b", first}, {"res-pg-A", first}, {"res-pg-a", first},
		{"res-pg-3", second}, {"res-pg-Z", second}, {"res-pg-1", second}, {"res-pg-z", second},
	}
	var want []string
	for _, r := range rows {
		res, err := reservation.New(r.id, sku, mustQty(t, 1), "it-page-demand", []reservation.Allocation{{StockUnitID: unit.ID(), Quantity: mustQty(t, 1)}}, r.created, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, res); err != nil {
			t.Fatalf("save %s: %v", r.id, err)
		}
	}
	// (created_at, id) with a byte-wise id: the first group, then the second group
	want = []string{"res-pg-A", "res-pg-a", "res-pg-b", "res-pg-1", "res-pg-3", "res-pg-Z", "res-pg-z"}

	for _, limit := range []int{1, 2, 3, 4, len(rows), len(rows) + 1} {
		var got []string
		var after *reservation.PageAfter
		for pages := 0; ; pages++ {
			if pages > len(rows)+1 {
				t.Fatalf("limit %d: walk did not terminate", limit)
			}
			page, err := repo.FindByDemandRefAfter(ctx, "it-page-demand", after, limit+1) // limit+1: one row of look-ahead
			if err != nil {
				t.Fatal(err)
			}
			more := len(page) > limit
			if more {
				page = page[:limit]
			}
			for _, r := range page {
				got = append(got, r.ID())
			}
			if !more {
				break
			}
			last := page[len(page)-1]
			after = &reservation.PageAfter{CreatedAt: last.CreatedAt(), ID: last.ID()}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("limit %d: walk = %v, want %v", limit, got, want)
		}
	}
}
