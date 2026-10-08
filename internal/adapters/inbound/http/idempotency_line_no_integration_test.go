//go:build integration

// POST /reservations with lineNo (decision 18, ADR 0036) through the real
// Idempotency-Key middleware and Postgres: the line is persisted, a replay
// with the identical body returns the cached response, and adding lineNo to a
// key that was first used without it is a DIFFERENT body, so 422
// idempotency-key-reused (expected; ADR 0036 records it).
package http_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
)

func reserveLineBody(sku, demandRef string, lineNo int) string {
	return fmt.Sprintf(`{"sku":%q,"quantity":6,"demandRef":%q,"lineNo":%d}`, sku, demandRef, lineNo)
}

func TestIdempotency_Reserve_LineNo_IsPersistedAndReplayedIdentically(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-L1", 100)
	body := reserveLineBody("SKU-RESERVE-L1", "order-l1", 2)

	first := doReserve(fx.router, "res-line-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201 (body: %s)", first.Code, first.Body.String())
	}
	second := doReserve(fx.router, "res-line-1", body)
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("replay differs:\nfirst:  %d %s\nsecond: %d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if got := countReservationRows(t, pool); got != 1 {
		t.Fatalf("reservations rows after replay = %d, want 1", got)
	}

	var lineNo *int
	if err := pool.QueryRow(context.Background(), `SELECT line_no FROM reservations WHERE demand_ref = 'order-l1'`).Scan(&lineNo); err != nil {
		t.Fatalf("read line_no: %v", err)
	}
	if lineNo == nil || *lineNo != 2 {
		t.Fatalf("persisted line_no = %v, want 2", lineNo)
	}
}

func TestIdempotency_Reserve_AddingLineNoToAUsedKey_Returns422(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-L2", 100)

	first := doReserve(fx.router, "res-line-2", sprintfReserve("SKU-RESERVE-L2", "order-l2"))
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201 (body: %s)", first.Code, first.Body.String())
	}
	second := doReserve(fx.router, "res-line-2", reserveLineBody("SKU-RESERVE-L2", "order-l2", 1))
	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body: %s)", second.Code, second.Body.String())
	}
	if slug := problemSlug(t, second.Body.Bytes()); slug != "idempotency-key-reused" {
		t.Fatalf("problem.type slug = %q, want idempotency-key-reused", slug)
	}
	if got := countReservationRows(t, pool); got != 1 {
		t.Fatalf("reservations rows = %d, want 1", got)
	}
}

func TestIdempotency_Reserve_InvalidLineNo_Returns400AndReservesNothing(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-L3", 100)

	first := doReserve(fx.router, "res-line-3", reserveLineBody("SKU-RESERVE-L3", "order-l3", 0))
	if first.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", first.Code, first.Body.String())
	}
	if slug := problemSlug(t, first.Body.Bytes()); slug != "invalid-line-no" {
		t.Fatalf("problem.type slug = %q, want invalid-line-no", slug)
	}
	if got := countReservationRows(t, pool); got != 0 {
		t.Fatalf("reservations rows = %d, want 0", got)
	}
}
