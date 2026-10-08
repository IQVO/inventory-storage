//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// line_no (ADR 0036, migration 0035) round-trips through FindByID and
// FindByDemandRef; a reservation saved without one reads back with nil (the
// legacy shape), and the column's CHECK rejects a non-positive value even if
// the application guard were bypassed.
func TestPostgres_Reservation_LineNoRoundTrip(t *testing.T) {
	databaseURL := migratedDB(t)
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	binID, _ := shared.NewBinId("IT-LINENO-BIN")
	bin, err := location.NewBin(binID, mustQty(t, 30))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := postgres.NewLocationRepo(pool).Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}
	sku, _ := shared.NewSKU("IT-LINENO-SKU")
	unit, err := stock.NewStockUnit("it-lineno-su-1", sku, binID, mustQty(t, 30))
	if err != nil {
		t.Fatalf("build stock unit: %v", err)
	}
	if err := postgres.NewStockRepo(pool).Save(ctx, unit); err != nil {
		t.Fatalf("save stock unit: %v", err)
	}

	repo := postgres.NewReservationRepo(pool)
	suffix, _ := repo.NextID(ctx)
	demandRef := "it-lineno-order-" + suffix
	created := time.Now().UTC().Truncate(time.Second)
	allocs := []reservation.Allocation{{StockUnitID: unit.ID(), Quantity: mustQty(t, 2)}}

	save := func(lineNo *int, offset time.Duration) string {
		t.Helper()
		id, _ := repo.NextID(ctx)
		res, err := reservation.NewForLine(id, sku, mustQty(t, 2), demandRef, lineNo, allocs, created.Add(offset), time.Hour)
		if err != nil {
			t.Fatalf("build reservation: %v", err)
		}
		if err := repo.Save(ctx, res); err != nil {
			t.Fatalf("save reservation: %v", err)
		}
		return id
	}
	three, one := 3, 1
	lineID := save(&three, 0)
	legacyID := save(nil, time.Second)
	lineOneID := save(&one, 2*time.Second)

	byID, err := repo.FindByID(ctx, lineID)
	if err != nil || byID == nil {
		t.Fatalf("FindByID: %v, %v", byID, err)
	}
	if got := byID.LineNo(); got == nil || *got != 3 {
		t.Fatalf("FindByID LineNo = %v, want 3", got)
	}

	// Saving again (a status change) must not lose or alter the line.
	if err := byID.Revoke(); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := repo.Save(ctx, byID); err != nil {
		t.Fatalf("save revoked: %v", err)
	}
	again, _ := repo.FindByID(ctx, lineID)
	if got := again.LineNo(); got == nil || *got != 3 || again.Status() != reservation.StatusRevoked {
		t.Fatalf("after a status update LineNo = %v status = %s, want 3 REVOKED", got, again.Status())
	}

	all, err := repo.FindByDemandRef(ctx, demandRef)
	if err != nil || len(all) != 3 {
		t.Fatalf("FindByDemandRef = %d rows, %v; want 3", len(all), err)
	}
	want := map[string]int{lineID: 3, lineOneID: 1}
	for _, r := range all {
		switch r.ID() {
		case legacyID:
			if r.LineNo() != nil {
				t.Errorf("legacy reservation LineNo = %d, want nil", *r.LineNo())
			}
		default:
			if got := r.LineNo(); got == nil || *got != want[r.ID()] {
				t.Errorf("%s LineNo = %v, want %d", r.ID(), got, want[r.ID()])
			}
		}
	}

	// Defence in depth: the column itself refuses 0 and negatives.
	for _, bad := range []int{0, -1} {
		if _, err := pool.Exec(ctx, `UPDATE reservations SET line_no = $1 WHERE id = $2`, bad, legacyID); err == nil {
			t.Errorf("line_no = %d was accepted by the database, want the CHECK constraint to reject it", bad)
		}
	}

	// The column is a 32-bit INTEGER: the largest valid line number
	// round-trips, one more is refused by the database (which is why the
	// domain/edge/consumer must never let it through).
	max := reservation.MaxLineNo
	maxID := save(&max, 3*time.Second)
	if r, err := repo.FindByID(ctx, maxID); err != nil || r == nil || r.LineNo() == nil || *r.LineNo() != 2147483647 {
		t.Fatalf("FindByID of line 2147483647 = %v, %v", r, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE reservations SET line_no = $1 WHERE id = $2`, int64(2147483648), legacyID); err == nil {
		t.Errorf("line_no = 2147483648 was accepted by the database, want integer out of range")
	}
}
