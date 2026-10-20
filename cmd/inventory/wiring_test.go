package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// migrationsDirForTest resolves this repo's real migrations directory
// (../../migrations relative to this package) so migrate.New has a valid
// "file://" source to parse before it ever tries to dial a database.
func migrationsDirForTest(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations dir %q: %v", dir, err)
	}
	return dir
}

// TestMigrationsDatabaseURLFallback proves the fallback wiring loadConfig
// applies before main calls buildAdapters: MIGRATIONS_DATABASE_URL's own
// value when it is set, and DATABASE_URL when it is unset. This is the exact
// lookup the fix for the PgBouncer/pg_advisory_lock incompatibility (ADR
// 0023-migrations-direct-postgres-connection.md, mirroring
// order-management's ADR-0029) depends on: an environment that doesn't
// provision the split (CI integration tests, a cluster whose Terraform
// predates this fix) keeps using DATABASE_URL for everything including
// migrations.
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:***@pgbouncer.example:6432/inventory_storage?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		env := validEnv()
		env["DATABASE_URL"] = databaseURL
		cfg, err := loadConfig(mapEnv(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.migrationsDatabaseURL != databaseURL {
			t.Fatalf("migrationsDatabaseURL = %q, want the DATABASE_URL value %q", cfg.migrationsDatabaseURL, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/inventory_storage?sslmode=disable"
		env := validEnv()
		env["DATABASE_URL"] = databaseURL
		env["MIGRATIONS_DATABASE_URL"] = direct
		cfg, err := loadConfig(mapEnv(env))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.migrationsDatabaseURL != direct {
			t.Fatalf("migrationsDatabaseURL = %q, want the direct value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", cfg.migrationsDatabaseURL, direct)
		}
	})
}

// TestBuildAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves buildAdapters itself — not just the lookup above — actually
// threads migrationsDatabaseURL into the migration step (via
// openPostgresPool) and databaseURL into the pgxpool, rather than the two
// ever being conflated. Gives DATABASE_URL an address nothing listens on
// (so opening the pgxpool, which happens AFTER migrations succeed, would
// hang/fail loudly if ever reached) and MIGRATIONS_DATABASE_URL a
// schemeless string that migrate.New rejects immediately with a
// distinctive parse error ("failed to parse scheme from database URL") —
// if buildAdapters ignored migrationsDatabaseURL and ran migrations
// against databaseURL instead, this test would see a dial/"connection
// refused" error, not the parse error.
func TestBuildAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:***@127.0.0.1:1/inventory_storage?sslmode=disable&connect_timeout=1"
	)

	_, err := buildAdapters(context.Background(), unreachableAppURL, bogusMigrationsURL, migrationsDirForTest(t), []string{"127.0.0.1:1"}, quietLogger())
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against databaseURL/unreachableAppURL instead of migrationsDatabaseURL", err)
	}
}
