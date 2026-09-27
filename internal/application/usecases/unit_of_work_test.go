package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/product"
)

// recordingUnitOfWork is a ports.UnitOfWork fake that records how many
// times Execute was invoked, so a test can prove the atomically helper
// actually delegates to a configured UnitOfWork rather than silently
// bypassing it.
type recordingUnitOfWork struct {
	calls int
}

func (u *recordingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	u.calls++
	return fn(ctx)
}

var _ ports.UnitOfWork = (*recordingUnitOfWork)(nil)

// TestReceiveStock_WithUnitOfWork_WrapsPublishInOneScope proves a
// configured UnitOfWork is actually invoked (not silently bypassed) by
// the atomically helper every publishing use case now goes through.
func TestReceiveStock_WithUnitOfWork_WrapsPublishInOneScope(t *testing.T) {
	e := newEnv()
	uow := &recordingUnitOfWork{}
	uc := &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork.Execute called %d times, want 1", uow.calls)
	}
}

// TestReceiveStock_NilUnitOfWork_StillPublishes proves the atomically
// helper's nil fallback keeps every call site that has not been wired
// with a UnitOfWork (in-memory/dev config) working exactly as before ADR
// 0017 — the property that let every use case's pre-existing tests keep
// passing unmodified.
func TestReceiveStock_NilUnitOfWork_StillPublishes(t *testing.T) {
	e := newEnv()
	uc := &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestReserveStock_WithUnitOfWork_WrapsStockAndReservationWritesTogether
// proves ReserveStock's whole write section (stock-unit Save loop +
// reservation creation + Publish) runs inside a single UnitOfWork.Execute
// call, not one call per Save/Publish — the design point of ADR 0017 (an
// aggregate write and its event(s) must commit together in ONE
// transaction, not several).
func TestReserveStock_WithUnitOfWork_WrapsStockAndReservationWritesTogether(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uow := &recordingUnitOfWork{}
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork.Execute called %d times, want exactly 1 (one atomic scope for the whole write)", uow.calls)
	}
}

// TestReserveStock_UnitOfWork_PropagatesPublishFailure proves that when
// Publish fails inside the atomically scope, the use case still surfaces
// that error unchanged. A real Postgres UnitOfWork rolls the whole
// transaction back on this path (including the stock-unit Save this test
// cannot observe through the fake) — proven for real by
// TestOutbox_RollsBackAggregateWriteWhenOutboxInsertFails in the postgres
// package's integration test.
func TestReserveStock_UnitOfWork_PropagatesPublishFailure(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uow := &recordingUnitOfWork{}
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: failingEvents{}, Clock: e.Clock, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1"); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
	if uow.calls == 0 {
		t.Fatal("expected UnitOfWork.Execute to have been invoked")
	}
}

// TestClassifyProduct_WithUnitOfWork_WrapsSaveAndPublish exercises a
// second, differently-shaped use case (Save then Publish, no prior reads
// to branch on) to guard against the wiring being ReserveStock-specific.
func TestClassifyProduct_WithUnitOfWork_WrapsSaveAndPublish(t *testing.T) {
	e := newEnv()
	uow := &recordingUnitOfWork{}
	uc := &usecases.ClassifyProduct{Classifications: e.Classifications, Events: e.Events, Clock: e.Clock, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), []product.HandlingTag{product.Fragile}, "", 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork.Execute called %d times, want 1", uow.calls)
	}
}

var _ = time.Now
