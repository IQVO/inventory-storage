//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestBuildAdapters_StartsHousekeepingSweeperFromEnv proves the ADR-0026
// wiring end to end against a real Postgres (testcontainers): with
// DATABASE_URL set, buildAdapters starts the sweeper using the
// HOUSEKEEPING_INTERVAL / IDEMPOTENCY_KEY_TTL / OUTBOX_RETENTION env, an
// expired idempotency key and a published outbox row past retention are
// removed, an unpublished row of the same age is kept, and the returned
// closer stops the sweeper before closing the pool.
func TestBuildAdapters_StartsHousekeepingSweeperFromEnv(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	t.Setenv("HOUSEKEEPING_INTERVAL", "100ms")
	t.Setenv("IDEMPOTENCY_KEY_TTL", "1h")
	t.Setenv("OUTBOX_RETENTION", "1h")

	adapters, err := buildAdapters(ctx, url, url, migrationsDirForTest(t), "log", quietLogger())
	if err != nil {
		t.Fatalf("buildAdapters: %v", err)
	}
	closeAdapters := adapters.close
	closed := false
	t.Cleanup(func() {
		if !closed {
			closeAdapters()
		}
	})

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open assertion pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// Seed AFTER boot so the sweeper's immediate first pass (on an empty
	// table) is not what removes them: a later tick must.
	mustExec(t, pool, `INSERT INTO idempotency_keys (key, method, path, request_hash, status_code, created_at)
		VALUES ('stale', 'POST', '/reservations', 'h', 201, now() - interval '3 hours'),
		       ('fresh', 'POST', '/reservations', 'h', 201, now())`)
	mustExec(t, pool, `INSERT INTO outbox_events (topic, event_type, value, created_at, published_at) VALUES
		('t', 'StalePublished', '\x7b7d', now() - interval '3 hours', now() - interval '3 hours'),
		('t', 'StaleUnpublished', '\x7b7d', now() - interval '3 hours', NULL)`)

	deadline := time.Now().Add(30 * time.Second)
	for {
		var keys, published, unpublished int
		if err := pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM idempotency_keys),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'StalePublished'),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'StaleUnpublished')`).Scan(&keys, &published, &unpublished); err != nil {
			t.Fatalf("count: %v", err)
		}
		if keys == 1 && published == 0 && unpublished == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sweeper did not converge: idempotency_keys=%d (want 1) stale published=%d (want 0) stale unpublished=%d (want 1)", keys, published, unpublished)
		}
		time.Sleep(100 * time.Millisecond)
	}

	closeAdapters() // must stop the sweeper, then close the pool, without hanging
	closed = true
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}
