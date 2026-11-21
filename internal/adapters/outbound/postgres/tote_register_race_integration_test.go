//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
)

// gatedTotes makes the first `racers` FindByLPN calls wait until every racer
// has read, so each racer's unit of work sees "no tote" before any Add: the
// registrations then race on the license_plates primary key. Later reads (the
// loser's re-read) pass straight through.
type gatedTotes struct {
	ports.ToteRepo
	racers int32
	reads  atomic.Int32
	all    sync.WaitGroup
}

func (g *gatedTotes) FindByLPN(ctx context.Context, lpn string) (*container.Tote, error) {
	if g.reads.Add(1) > g.racers {
		return g.ToteRepo.FindByLPN(ctx, lpn)
	}
	got, err := g.ToteRepo.FindByLPN(ctx, lpn)
	g.all.Done()
	done := make(chan struct{})
	go func() { g.all.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		return nil, errors.New("gatedTotes: the other racers never read")
	}
	return got, err
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// carry (Task 5/10 review, ADR 0054): concurrent PUT /totes/{lpn} of one new
// LPN over Postgres, each in its own unit of work: the claim loser re-reads
// the winner's committed tote and answers it created=false (200), never 409
// license-plate-taken. One tote row, one TOTE plate, one ToteRegistered.
func TestConcurrentRegisterToteOverPostgresAnswersTheExistingTote(t *testing.T) {
	pool := outboxDB(t)
	const racers, lpn = 4, "TOTE-RACE"
	gate := &gatedTotes{ToteRepo: postgres.NewToteRepo(pool), racers: racers}
	gate.all.Add(racers)
	uc := &usecases.RegisterTote{
		Totes: gate, Containers: postgres.NewContainerRepo(pool),
		Events:     postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisher(nil, nil)),
		Clock:      &fixedClock{t: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	type result struct {
		created bool
		lpn     string
		err     error
	}
	results := make([]result, racers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tote, created, err := uc.Execute(context.Background(), lpn)
			if tote != nil {
				results[i].lpn = tote.LPN()
			}
			results[i].created, results[i].err = created, err
		}(i)
	}
	wg.Wait()
	created := 0
	for i, r := range results {
		if r.err != nil || r.lpn != lpn {
			t.Fatalf("racer %d: lpn %q created %v err %v, want the tote and no error (all: %+v)", i, r.lpn, r.created, r.err, results)
		}
		if r.created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created = %d of %d racers, want exactly 1 (all: %+v)", created, racers, results)
	}
	if n := countRows(t, pool, `SELECT count(*) FROM totes WHERE lpn = $1`, lpn); n != 1 {
		t.Fatalf("tote rows = %d, want 1", n)
	}
	if n := countRows(t, pool, `SELECT count(*) FROM license_plates WHERE lpn = $1 AND kind = 'TOTE'`, lpn); n != 1 {
		t.Fatalf("TOTE plates = %d, want 1", n)
	}
	if n := countRows(t, pool, `SELECT count(*) FROM outbox_events WHERE key = $1`, []byte(lpn)); n != 1 {
		t.Fatalf("outbox rows keyed %s = %d, want exactly one ToteRegistered", lpn, n)
	}
}
