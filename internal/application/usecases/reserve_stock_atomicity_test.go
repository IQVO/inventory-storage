package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// scopeKey marks a ctx as "inside the use case's own UnitOfWork scope".
type scopeKey struct{}

// scopeUnitOfWork is a UnitOfWork fake that stamps the ctx it hands to fn,
// so a repo wrapper can tell whether a write ran inside the scope the use
// case itself opened (as opposed to a scope some outer middleware opened
// earlier and left on the request ctx).
type scopeUnitOfWork struct{}

func (scopeUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(context.WithValue(ctx, scopeKey{}, true))
}

// scopeRecordingStock records, per Save, whether the ctx carried the
// scopeUnitOfWork stamp.
type scopeRecordingStock struct {
	ports.StockRepo
	savesInScope  int
	savesOutScope int
}

func (s *scopeRecordingStock) Save(ctx context.Context, unit *stock.StockUnit) error {
	if ctx.Value(scopeKey{}) != nil {
		s.savesInScope++
	} else {
		s.savesOutScope++
	}
	return s.StockRepo.Save(ctx, unit)
}

// TestReserveStock_StockUnitSaves_RunInsideOwnUnitOfWork pins that the
// touched StockUnit saves are made inside the use case's OWN UnitOfWork,
// with no idempotency-middleware tx on the ctx. Before the fix the saves
// ran first, on the bare request ctx, and were atomic with the
// reservation row and the outbox event only when the HTTP idempotency
// middleware happened to have put its transaction on the ctx (REST); any
// other caller (MCP, a future consumer) got a half-applied reservation if
// the reservation save or Publish failed.
func TestReserveStock_StockUnitSaves_RunInsideOwnUnitOfWork(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	rec := &scopeRecordingStock{StockRepo: e.Stock}
	uc := &usecases.ReserveStock{Stock: rec, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, UnitOfWork: scopeUnitOfWork{}}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.savesInScope != 1 || rec.savesOutScope != 0 {
		t.Fatalf("stock saves inside the use case's UnitOfWork = %d, outside = %d; want 1 and 0", rec.savesInScope, rec.savesOutScope)
	}
}
