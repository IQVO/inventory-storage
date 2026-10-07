package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func seedClassifications(t *testing.T, e env, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		c, err := product.New(shared.SKU(fmt.Sprintf("SKU-%03d", i)), []product.HandlingTag{product.Hazmat}, "", product.DOTHazardClass(3))
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Classifications.Save(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
}

// countingUnitOfWork counts Execute calls (one per batch).
type countingUnitOfWork struct{ calls int }

func (u *countingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	u.calls++
	return fn(ctx)
}

func TestRepublishProductClassifications_EnqueuesEveryRowInBatches(t *testing.T) {
	e := newEnv()
	seedClassifications(t, e, 7)
	uow := &countingUnitOfWork{}
	uc := &usecases.RepublishProductClassifications{Catalogue: e.Classifications, Events: e.Events, Clock: e.Clock, UnitOfWork: uow, BatchSize: 3}

	n, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 7 {
		t.Fatalf("count = %d, want 7", n)
	}
	if uow.calls != 3 {
		t.Fatalf("batches = %d, want 3 (3+3+1)", uow.calls)
	}
	published := e.Events.Events()
	if len(published) != 7 {
		t.Fatalf("published %d events, want 7", len(published))
	}
	seen := map[shared.SKU]bool{}
	for _, ev := range published {
		pc, ok := ev.(product.ProductClassified)
		if !ok {
			t.Fatalf("published %T, want product.ProductClassified", ev)
		}
		if seen[pc.SKU] {
			t.Fatalf("SKU %s republished twice in one run", pc.SKU)
		}
		seen[pc.SKU] = true
		if !pc.At.Equal(e.Clock.Now()) || pc.DOTHazardClass != 3 || len(pc.HandlingTags) != 1 || pc.HandlingTags[0] != product.Hazmat {
			t.Fatalf("event = %+v", pc)
		}
	}
}

// An exact multiple of the batch size ends on an empty page, not a loop.
func TestRepublishProductClassifications_ExactMultipleOfBatch(t *testing.T) {
	e := newEnv()
	seedClassifications(t, e, 4)
	uow := &countingUnitOfWork{}
	uc := &usecases.RepublishProductClassifications{Catalogue: e.Classifications, Events: e.Events, Clock: e.Clock, UnitOfWork: uow, BatchSize: 2}

	n, err := uc.Execute(context.Background())
	if err != nil || n != 4 {
		t.Fatalf("n=%d err=%v, want 4/nil", n, err)
	}
	if uow.calls != 2 {
		t.Fatalf("batches = %d, want 2", uow.calls)
	}
}

func TestRepublishProductClassifications_EmptyTableIsZero(t *testing.T) {
	e := newEnv()
	uc := &usecases.RepublishProductClassifications{Catalogue: e.Classifications, Events: e.Events, Clock: e.Clock}
	n, err := uc.Execute(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v, want 0/nil", n, err)
	}
}

// Re-running re-enqueues the same full-state messages: harmless by
// design (product-master skips native rows and ignores identical ones).
func TestRepublishProductClassifications_RerunEnqueuesAgain(t *testing.T) {
	e := newEnv()
	seedClassifications(t, e, 2)
	uc := &usecases.RepublishProductClassifications{Catalogue: e.Classifications, Events: e.Events, Clock: e.Clock}
	for run := 1; run <= 2; run++ {
		n, err := uc.Execute(context.Background())
		if err != nil || n != 2 {
			t.Fatalf("run %d: n=%d err=%v", run, n, err)
		}
	}
	if got := len(e.Events.Events()); got != 4 {
		t.Fatalf("published %d, want 4 after two runs", got)
	}
}

func TestRepublishProductClassifications_DefaultBatchSize(t *testing.T) {
	e := newEnv()
	seedClassifications(t, e, usecases.DefaultRepublishBatchSize+1)
	uow := &countingUnitOfWork{}
	uc := &usecases.RepublishProductClassifications{Catalogue: e.Classifications, Events: e.Events, Clock: e.Clock, UnitOfWork: uow}
	n, err := uc.Execute(context.Background())
	if err != nil || n != usecases.DefaultRepublishBatchSize+1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if uow.calls != 2 {
		t.Fatalf("batches = %d, want 2", uow.calls)
	}
}

func TestRepublishProductClassifications_PublishFailureStopsAndReportsCommittedCount(t *testing.T) {
	e := newEnv()
	seedClassifications(t, e, 3)
	uc := &usecases.RepublishProductClassifications{Catalogue: e.Classifications, Events: failingEvents{}, Clock: e.Clock, BatchSize: 2}
	n, err := uc.Execute(context.Background())
	if !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want errFake", err)
	}
	if n != 0 {
		t.Fatalf("count = %d, want 0 (the first batch failed)", n)
	}
}

type failingCatalogue struct{}

func (failingCatalogue) ListAfter(context.Context, shared.SKU, int) ([]*product.ProductClassification, error) {
	return nil, errFake
}

func TestRepublishProductClassifications_ListFailurePropagates(t *testing.T) {
	e := newEnv()
	uc := &usecases.RepublishProductClassifications{Catalogue: failingCatalogue{}, Events: e.Events, Clock: e.Clock}
	if _, err := uc.Execute(context.Background()); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want errFake", err)
	}
}
