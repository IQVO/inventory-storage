//go:build integration

// POST /relocations (ADR 0039) through the real Idempotency-Key middleware
// and Postgres, with exactly the semantics of the other creating POSTs
// (ADR 0018): the header is required, the same key + body replays the stored
// response without moving stock again, and the same key with a different
// body is 422 idempotency-key-reused.
package http_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

const (
	idemRelFrom = "WH1-STOR-AMB-A01-01-01-A"
	idemRelTo   = "WH1-STOR-AMB-A02-01-01-A"
)

func seedRelocationBins(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	bins := postgres.NewLocationRepo(pool)
	from, _ := location.NewBin(idemRelFrom, 50)
	_ = from.Occupy(10)
	to, _ := location.NewBin(idemRelTo, 50)
	for _, b := range []*location.Bin{from, to} {
		if err := bins.Save(ctx, b); err != nil {
			t.Fatalf("seed bin: %v", err)
		}
	}
	unit, _ := stock.NewStockUnit("su-idem-rel", "SKU-IDEM-REL", shared.BinId(idemRelFrom), 10)
	if err := postgres.NewStockRepo(pool).Save(ctx, unit); err != nil {
		t.Fatalf("seed unit: %v", err)
	}
}

func relocationBody(qty int) string {
	return `{"siteCode":"WH1","sku":"SKU-IDEM-REL","fromBin":"` + idemRelFrom + `","toBin":"` + idemRelTo + `","quantity":` +
		strconv.Itoa(qty) + `}`
}

func doRelocate(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/relocations", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func countRelocations(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM relocations`).Scan(&n); err != nil {
		t.Fatalf("count relocations: %v", err)
	}
	return n
}

func sourceOnHand(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT COALESCE(SUM(quantity), 0) FROM stock_units WHERE sku = 'SKU-IDEM-REL' AND bin_id = $1`, idemRelFrom).Scan(&n); err != nil {
		t.Fatalf("sum: %v", err)
	}
	return n
}

func TestIdempotency_Relocate_ReplayMovesStockOnce(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedRelocationBins(t, pool)

	first := doRelocate(fx.router, "rel-key-1", relocationBody(4))
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201 (body: %s)", first.Code, first.Body.String())
	}
	second := doRelocate(fx.router, "rel-key-1", relocationBody(4))
	if second.Code != first.Code || second.Body.String() != first.Body.String() ||
		second.Header().Get("Location") != first.Header().Get("Location") {
		t.Fatalf("replay differs:\nfirst:  %d %s\nsecond: %d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if got := countRelocations(t, pool); got != 1 {
		t.Fatalf("relocations rows = %d, want 1", got)
	}
	if got := sourceOnHand(t, pool); got != 6 {
		t.Fatalf("source on hand = %d, want 6 (moved once)", got)
	}

	get := httptest.NewRecorder()
	fx.router.ServeHTTP(get, httptest.NewRequest(http.MethodGet, first.Header().Get("Location"), nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET %s = %d (%s)", first.Header().Get("Location"), get.Code, get.Body.String())
	}
}

func TestIdempotency_Relocate_SameKeyDifferentBodyIs422(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedRelocationBins(t, pool)

	if first := doRelocate(fx.router, "rel-key-2", relocationBody(4)); first.Code != http.StatusCreated {
		t.Fatalf("first status = %d (body: %s)", first.Code, first.Body.String())
	}
	second := doRelocate(fx.router, "rel-key-2", relocationBody(3))
	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body: %s)", second.Code, second.Body.String())
	}
	if slug := problemSlug(t, second.Body.Bytes()); slug != "idempotency-key-reused" {
		t.Fatalf("problem slug = %q, want idempotency-key-reused", slug)
	}
	if got := sourceOnHand(t, pool); got != 6 {
		t.Fatalf("source on hand = %d, want 6", got)
	}
}

func TestIdempotency_Relocate_HeaderIsRequired(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedRelocationBins(t, pool)

	rec := doRelocate(fx.router, "", relocationBody(4))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	if slug := problemSlug(t, rec.Body.Bytes()); slug != "idempotency-key-required" {
		t.Fatalf("problem slug = %q, want idempotency-key-required", slug)
	}
	if got := countRelocations(t, pool); got != 0 {
		t.Fatalf("relocations rows = %d, want 0", got)
	}
}
