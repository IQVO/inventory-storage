//go:build integration

package http_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it and applies every migration ONCE into a template
// database; each test then gets a private database cloned from that template
// (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds). Isolation
// is total — no TRUNCATE bookkeeping, no dependence on test order. The same
// shape is used by the outbound/postgres package's integration tests.
//
// Never an external DATABASE_URL, never t.Skip.

const (
	templateDB     = "inventory_migrated_template"
	migrationsPath = "../../../../migrations"
)

var (
	sharedBaseURL string // connection URL of the container's default database
	dbSeq         atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	sharedBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		return 1
	}
	if err := execAdmin(ctx, "CREATE DATABASE "+templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	// RunMigrations closes its connection before returning, which a
	// CREATE DATABASE ... TEMPLATE needs (the template must have no sessions).
	if err := postgres.RunMigrations(urlForDatabase(templateDB), migrationsPath); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template database: %v\n", err)
		return 1
	}
	return m.Run()
}

// urlForDatabase is the shared container's connection URL pointed at name.
func urlForDatabase(name string) string {
	u, err := url.Parse(sharedBaseURL)
	if err != nil {
		panic(fmt.Sprintf("parse shared postgres URL: %v", err))
	}
	u.Path = "/" + name
	return u.String()
}

// execAdmin runs one statement on a short-lived connection to the default
// database (its own connection per call, so concurrent callers never share
// one).
func execAdmin(ctx context.Context, stmt string) error {
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, stmt)
	return err
}

// idempotencyDB returns a pool on a private, fully-migrated database
// (including the idempotency_keys migration) cloned from the template. The
// database is dropped, forcing out leftover sessions, when the test ends.
func idempotencyDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("it_%d", dbSeq.Add(1))
	if err := execAdmin(ctx, "CREATE DATABASE "+name+" TEMPLATE "+templateDB); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() { _ = execAdmin(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })

	pool, err := postgres.NewPool(ctx, urlForDatabase(name))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
