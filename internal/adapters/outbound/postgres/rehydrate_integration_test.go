//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// A row that violates a domain invariant (here: an empty SKU, which the
// schema allows but shared.NewSKU rejects) must make the repository's read
// path return a wrapped error instead of hydrating an aggregate around a
// zero-value SKU (audit 2026-10-05, F5). Boots its own Postgres via
// outboxDB (testcontainers).
func TestPostgres_CorruptRows_FailRehydrationInsteadOfZeroValueVOs(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	for _, stmt := range []string{
		`INSERT INTO bins (id, capacity, occupied) VALUES ('BIN-OK', 10, 0)`,
		`INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state) VALUES ('su-corrupt', '', 'BIN-OK', 5, 0, 'AVAILABLE')`,
		`INSERT INTO reservations (id, sku, quantity, demand_ref, status, created_at, expires_at) VALUES ('res-corrupt', '', 2, 'order-corrupt', 'ACTIVE', now(), now() + interval '1 hour')`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed corrupt row %q: %v", stmt, err)
		}
	}

	stocks := postgres.NewStockRepo(pool)
	reservations := postgres.NewReservationRepo(pool)

	t.Run("StockRepo.FindByID", func(t *testing.T) {
		unit, err := stocks.FindByID(ctx, "su-corrupt")
		if err == nil || unit != nil {
			t.Fatalf("expected (nil, error), got (%+v, %v)", unit, err)
		}
		if !errors.Is(err, shared.ErrEmptySKU) {
			t.Fatalf("error %q does not wrap ErrEmptySKU", err)
		}
	})

	t.Run("StockRepo.FindByBin", func(t *testing.T) {
		binID, _ := shared.NewBinId("BIN-OK")
		units, err := stocks.FindByBin(ctx, binID)
		if err == nil || units != nil {
			t.Fatalf("expected (nil, error), got (%+v, %v)", units, err)
		}
		if !errors.Is(err, shared.ErrEmptySKU) {
			t.Fatalf("error %q does not wrap ErrEmptySKU", err)
		}
	})

	t.Run("ReservationRepo.FindByID", func(t *testing.T) {
		res, err := reservations.FindByID(ctx, "res-corrupt")
		if err == nil || res != nil {
			t.Fatalf("expected (nil, error), got (%+v, %v)", res, err)
		}
		if !errors.Is(err, shared.ErrEmptySKU) {
			t.Fatalf("error %q does not wrap ErrEmptySKU", err)
		}
	})

	t.Run("ReservationRepo.FindByDemandRef", func(t *testing.T) {
		all, err := reservations.FindByDemandRef(ctx, "order-corrupt")
		if err == nil || all != nil {
			t.Fatalf("expected (nil, error), got (%+v, %v)", all, err)
		}
		if !errors.Is(err, shared.ErrEmptySKU) {
			t.Fatalf("error %q does not wrap ErrEmptySKU", err)
		}
	})
}
