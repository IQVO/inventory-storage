package usecases_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
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

// Regression test for the audit's ReserveStock atomicity bug: the
// mutated StockUnits used to be saved OUTSIDE (before) the UnitOfWork
// closure, so a later failure inside the closure (reservation save,
// outbox publish) could not roll the stock decrements back — leaving
// stock decremented with no reservation holding it.
//
// This test pins the ORDERING property a unit test can see: every
// StockRepo.Save must happen between the UnitOfWork's begin and its
// commit. The full rollback semantics are proven against a real Postgres
// transaction by TestIntegration_ReserveStock_OutboxFailure_RollsBackStock
// (postgres package, testcontainers).
func TestReserveStock_SavesStockInsideTheUnitOfWorkClosure(t *testing.T) {
	e := newEnv()
	bin, _ := shared.NewBinId("BIN-1")
	unit, err := stock.NewStockUnit("su-1", mustSKU(t, "SKU-1"), bin, mustQty(t, 10))
	if err != nil {
		t.Fatalf("NewStockUnit: %v", err)
	}
	if err := e.Stock.Save(context.Background(), unit); err != nil {
		t.Fatalf("seed stock: %v", err)
	}

	tracer := &orderingTrace{}
	uc := &usecases.ReserveStock{
		Stock:        &tracingStockRepo{delegate: e.Stock, trace: tracer},
		Reservations: &tracingReservationRepo{delegate: e.Reservations, trace: tracer},
		Events:       e.Events,
		Clock:        e.Clock,
		UnitOfWork:   &tracingUnitOfWork{trace: tracer},
	}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 4), "order-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := tracer.expect("uow-begin", "stock-save", "res-save", "uow-commit"); err != nil {
		t.Fatalf("atomicity ordering violated: %v\ntrace: %v", err, tracer.events)
	}
}

// orderingTrace records the sequence of adapter calls.
type orderingTrace struct {
	events []string
}

func (t *orderingTrace) record(e string) { t.events = append(t.events, e) }

// expect asserts the recorded events contain the given subsequence in
// order (other events may interleave).
func (t *orderingTrace) expect(want ...string) error {
	i := 0
	for _, ev := range t.events {
		if i < len(want) && ev == want[i] {
			i++
		}
	}
	if i != len(want) {
		return fmt.Errorf("only matched %d of %d expected events (first missing: %q)", i, len(want), want[i])
	}
	return nil
}

type tracingStockRepo struct {
	delegate ports.StockRepo
	trace    *orderingTrace
}

func (r *tracingStockRepo) Save(ctx context.Context, unit *stock.StockUnit) error {
	r.trace.record("stock-save")
	return r.delegate.Save(ctx, unit)
}
func (r *tracingStockRepo) FindByID(ctx context.Context, id string) (*stock.StockUnit, error) {
	return r.delegate.FindByID(ctx, id)
}
func (r *tracingStockRepo) FindBySKU(ctx context.Context, sku shared.SKU) ([]*stock.StockUnit, error) {
	return r.delegate.FindBySKU(ctx, sku)
}
func (r *tracingStockRepo) FindByBin(ctx context.Context, binID shared.BinId) ([]*stock.StockUnit, error) {
	return r.delegate.FindByBin(ctx, binID)
}
func (r *tracingStockRepo) FindBySKUAtSite(ctx context.Context, sku shared.SKU, originSiteID shared.SiteID) ([]*stock.StockUnit, error) {
	return r.delegate.FindBySKUAtSite(ctx, sku, originSiteID)
}
func (r *tracingStockRepo) NextID(ctx context.Context) (string, error) { return r.delegate.NextID(ctx) }

type tracingReservationRepo struct {
	delegate ports.ReservationRepo
	trace    *orderingTrace
}

func (r *tracingReservationRepo) Save(ctx context.Context, res *reservation.Reservation) error {
	r.trace.record("res-save")
	return r.delegate.Save(ctx, res)
}
func (r *tracingReservationRepo) FindByID(ctx context.Context, id string) (*reservation.Reservation, error) {
	return r.delegate.FindByID(ctx, id)
}
func (r *tracingReservationRepo) FindByDemandRef(ctx context.Context, ref string) ([]*reservation.Reservation, error) {
	return r.delegate.FindByDemandRef(ctx, ref)
}
func (r *tracingReservationRepo) NextID(ctx context.Context) (string, error) {
	return r.delegate.NextID(ctx)
}

type tracingUnitOfWork struct {
	trace *orderingTrace
}

func (u *tracingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	u.trace.record("uow-begin")
	if err := fn(ctx); err != nil {
		u.trace.record("uow-rollback")
		return err
	}
	u.trace.record("uow-commit")
	return nil
}
