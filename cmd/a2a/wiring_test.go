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
// applies before run() calls buildAdapters (cmd/a2a also runs migrations on
// start, so it needs the same direct-connection split as cmd/mcp): MIGRATIONS_DATABASE_URL's own
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
		cfg, err := loadConfig(mapEnv(map[string]string{"DATABASE_URL": databaseURL, "A2A_AGENT_URL": testAgentURL}))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.migrationsDatabaseURL != databaseURL {
			t.Fatalf("migrationsDatabaseURL = %q, want the DATABASE_URL value %q", cfg.migrationsDatabaseURL, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/inventory_storage?sslmode=disable"
		cfg, err := loadConfig(mapEnv(map[string]string{"DATABASE_URL": databaseURL, "MIGRATIONS_DATABASE_URL": direct, "A2A_AGENT_URL": testAgentURL}))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		if cfg.migrationsDatabaseURL != direct {
			t.Fatalf("migrationsDatabaseURL = %q, want the direct value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", cfg.migrationsDatabaseURL, direct)
		}
	})
}

func mapEnv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

// Postgres is the only runtime: every refusal names the variable.
func TestLoadConfig_RefusesAnythingButRealWiring(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"no database", map[string]string{"A2A_AGENT_URL": testAgentURL}, "DATABASE_URL"},
		{"blank database", map[string]string{"DATABASE_URL": "  ", "A2A_AGENT_URL": testAgentURL}, "DATABASE_URL"},
		{"log publisher", map[string]string{"DATABASE_URL": "postgres://x", "A2A_AGENT_URL": testAgentURL, "EVENT_PUBLISHER": "log"}, "EVENT_PUBLISHER"},
		{"no agent url", map[string]string{"DATABASE_URL": "postgres://x"}, "A2A_AGENT_URL is required"},
		{"blank agent url", map[string]string{"DATABASE_URL": "postgres://x", "A2A_AGENT_URL": "   "}, "A2A_AGENT_URL is required"},
		{"relative agent url", map[string]string{"DATABASE_URL": "postgres://x", "A2A_AGENT_URL": "/a2a"}, `A2A_AGENT_URL="/a2a" is not an absolute http(s) URL`},
		{"schemeless agent url", map[string]string{"DATABASE_URL": "postgres://x", "A2A_AGENT_URL": "inventory-storage-a2a:8093"}, "is not an absolute http(s) URL"},
		{"wrong scheme agent url", map[string]string{"DATABASE_URL": "postgres://x", "A2A_AGENT_URL": "ftp://host/"}, "is not an absolute http(s) URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(mapEnv(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to name %s", err, tt.want)
			}
		})
	}
}

// warehouse-infra still sets EVENT_PUBLISHER=kafka; this binary never dials a
// broker, so KAFKA_BROKERS is not required.
func TestLoadConfig_AcceptsKafkaPublisherAndNeedsNoBrokers(t *testing.T) {
	cfg, err := loadConfig(mapEnv(map[string]string{"DATABASE_URL": "postgres://x", "A2A_AGENT_URL": testAgentURL, "EVENT_PUBLISHER": "KAFKA"}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.migrationsPath != "migrations" || cfg.agentURL != testAgentURL {
		t.Fatalf("config = %+v, want the default migrations path and the agent URL", cfg)
	}
}

// TestBuildAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves buildAdapters itself — not just the lookup above — actually
// threads migrationsDatabaseURL into the migration step, rather than the
// two ever being conflated. Gives DATABASE_URL an address nothing listens
// on (so opening the pgxpool, which happens AFTER migrations succeed,
// would hang/fail loudly if ever reached) and MIGRATIONS_DATABASE_URL a
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

	_, err := buildAdapters(context.Background(), unreachableAppURL, bogusMigrationsURL, migrationsDirForTest(t), quietLogger())
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against databaseURL/unreachableAppURL instead of migrationsDatabaseURL", err)
	}
}
