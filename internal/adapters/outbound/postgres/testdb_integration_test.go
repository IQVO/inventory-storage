//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is therefore total — no TRUNCATE bookkeeping, no dependence on
// test order, and tests that assert on global state (outbox row counts,
// sequences, versions) still start from a pristine schema. Tests that need
// a database with NO migrations (pool settings, migration up/down) take an
// empty database from emptyDB instead; that is pristine too, so none of
// them needs a container of its own.
//
// Never an external DATABASE_URL, never t.Skip.

const templateDB = "inventory_migrated_template"

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
	dir, err := migrationsPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	// RunMigrations closes its connection before returning, which a
	// CREATE DATABASE ... TEMPLATE needs (the template must have no sessions).
	if err := postgres.RunMigrations(urlForDatabase(templateDB), dir); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template database: %v\n", err)
		return 1
	}
	return m.Run()
}

// migrationsPath resolves /migrations relative to this file, so the tests
// work regardless of the directory `go test` is invoked from.
func migrationsPath() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations"), nil
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

// newDatabase creates a fresh database (cloned from template when
// non-empty) and drops it, forcing out any leftover sessions, when the test
// ends. The returned URL is private to the calling test.
func newDatabase(t *testing.T, template string) string {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("it_%d", dbSeq.Add(1))
	stmt := "CREATE DATABASE " + name
	if template != "" {
		stmt += " TEMPLATE " + template
	}
	if err := execAdmin(ctx, stmt); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() { _ = execAdmin(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
	return urlForDatabase(name)
}

// emptyDB returns the URL of a private database with NO migrations applied.
func emptyDB(t *testing.T) string {
	t.Helper()
	return newDatabase(t, "")
}

// migratedDB returns the URL of a private, fully-migrated database.
func migratedDB(t *testing.T) string {
	t.Helper()
	return newDatabase(t, templateDB)
}

// outboxDB returns a pool on a private, fully-migrated database: every test
// starts from a clean, fully-migrated schema regardless of the order tests
// run in.
func outboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
