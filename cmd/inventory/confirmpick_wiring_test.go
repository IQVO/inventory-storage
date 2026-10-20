package main

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// lazyPool returns a pgxpool that never dials until used: enough to pass the
// wiring without a database.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:***@127.0.0.1:1/inventory_storage?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// The first broker dial runs under bootretry: one transient reset (the
// fleet's known first-dial failure) must not fail boot.
func TestBuildTaskCompletedConsumer_RetriesTheBootDial(t *testing.T) {
	orig := dialBroker
	t.Cleanup(func() { dialBroker = orig })
	calls := 0
	dialBroker = func(context.Context, []string) error {
		calls++
		if calls == 1 {
			return errors.New("read: connection reset by peer")
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cfg := config{
		databaseURL:                "postgres://x",
		kafkaBrokers:               []string{"127.0.0.1:1"},
		taskCompletedConsumerGroup: "inventory-storage-confirm-pick",
	}
	h, err := buildTaskCompletedConsumer(ctx, quietLogger(), cfg, adapterSet{pool: lazyPool(t)}, nil)
	if err != nil {
		cancel()
		t.Fatalf("a transient first dial must not fail boot: %v", err)
	}
	if calls != 2 {
		t.Errorf("dial attempts = %d, want 2", calls)
	}
	cancel()
	h.wait()
	h.close()
}

// The RELOCATE branch (ADR 0039) shares the relocation use case and the
// processed-events claim repo with the rest of the consumer wiring. The
// in-memory set is a test double here: it only proves which repos are wired.
func TestNewApplyRelocationTask_WiresTheSharedRelocateStock(t *testing.T) {
	set := memoryAdapters(quietLogger())
	processed := memory.NewProcessedEventRepo()
	uc := newApplyRelocationTask(set, system.Clock{}, processed)

	if uc.ProcessedEvents != processed {
		t.Fatal("ApplyRelocationTask must claim through the consumer's processed-events repo")
	}
	rs, ok := uc.Relocate.(*usecases.RelocateStock)
	if !ok {
		t.Fatalf("Relocate is %T, want *usecases.RelocateStock", uc.Relocate)
	}
	if rs.Relocations != set.relocations || rs.Stock != set.stock || rs.Locations != set.locations {
		t.Fatal("RelocateStock must use the adapter set's repos")
	}
}

// The REPLENISH branch (ADR 0041) shares RelocateStock and the processed-events
// repo too, but claims ids under its own consumer name inside the use case.
func TestNewApplyReplenishTask_WiresTheSharedRelocateStock(t *testing.T) {
	set := memoryAdapters(quietLogger())
	processed := memory.NewProcessedEventRepo()
	uc := newApplyReplenishTask(set, system.Clock{}, processed)

	if uc.ProcessedEvents != processed || uc.UnitOfWork != set.uow {
		t.Fatal("ApplyReplenishTask must claim through the consumer's processed-events repo, in the adapter set's unit of work")
	}
	rs, ok := uc.Relocate.(*usecases.RelocateStock)
	if !ok {
		t.Fatalf("Relocate is %T, want *usecases.RelocateStock", uc.Relocate)
	}
	if rs.Relocations != set.relocations || rs.Stock != set.stock || rs.Locations != set.locations {
		t.Fatal("RelocateStock must use the adapter set's repos")
	}
}
