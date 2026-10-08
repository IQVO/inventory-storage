//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
)

func TestPostgres_OrderPickProgress_RecordPickCounts(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewOrderPickProgressRepo(pool)

	t0 := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	for want := 1; want <= 3; want++ {
		got, err := repo.RecordPick(ctx, "order-1", t0.Add(time.Duration(want)*time.Minute))
		if err != nil {
			t.Fatalf("RecordPick #%d: %v", want, err)
		}
		if got != want {
			t.Fatalf("RecordPick #%d = %d, want %d", want, got, want)
		}
	}
	if got, err := repo.RecordPick(ctx, "order-2", t0); err != nil || got != 1 {
		t.Fatalf("another order starts at 1: got %d, %v", got, err)
	}

	var picked int
	var updated time.Time
	if err := pool.QueryRow(ctx, `SELECT picked_tasks, updated_at FROM order_pick_progress WHERE demand_ref = 'order-1'`).Scan(&picked, &updated); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if picked != 3 || !updated.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("row = picked %d updated %v, want 3 and the last pick's timestamp", picked, updated)
	}
}

// RecordPick joins the UnitOfWork transaction: a rollback un-counts the pick,
// which is what makes the counter exactly-once together with the claim.
func TestPostgres_OrderPickProgress_RollsBackWithTheUnitOfWork(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewOrderPickProgressRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	if _, err := repo.RecordPick(ctx, "order-1", time.Now()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	boom := errors.New("boom")
	err := uow.Execute(ctx, func(ctx context.Context) error {
		if got, err := repo.RecordPick(ctx, "order-1", time.Now()); err != nil || got != 2 {
			t.Fatalf("in-tx RecordPick = %d, %v; want 2", got, err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Execute = %v, want the callback error", err)
	}
	if got, err := repo.RecordPick(ctx, "order-1", time.Now()); err != nil || got != 2 {
		t.Fatalf("after rollback the next pick = %d, %v; want 2 (the rolled-back pick was not counted)", got, err)
	}
}

// Concurrent handlers for one order serialize on the row lock: every pick gets
// a distinct count, so exactly one of them observes the final value.
func TestPostgres_OrderPickProgress_ConcurrentPicksGetDistinctCounts(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewOrderPickProgressRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	const picks = 8
	counts := make(chan int, picks)
	var wg sync.WaitGroup
	for i := 0; i < picks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := uow.Execute(ctx, func(ctx context.Context) error {
				got, err := repo.RecordPick(ctx, "order-race", time.Now())
				counts <- got
				return err
			})
			if err != nil {
				t.Errorf("RecordPick: %v", err)
			}
		}()
	}
	wg.Wait()
	close(counts)

	seen := map[int]bool{}
	for c := range counts {
		if seen[c] {
			t.Errorf("count %d observed twice", c)
		}
		seen[c] = true
	}
	for want := 1; want <= picks; want++ {
		if !seen[want] {
			t.Errorf("count %d never observed (got %v)", want, seen)
		}
	}
}
