//go:build integration

package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
)

// TestRunCommand_RepublishProductClassifications runs the real subcommand
// against a real Postgres (testcontainers): it needs only DATABASE_URL,
// enqueues one legacy ProductClassified outbox row per classification on
// the integration topic, prints the count and exits 0; a second run is
// harmless and enqueues the same set again.
func TestRunCommand_RepublishProductClassifications(t *testing.T) {
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
	// The running pod migrated the schema; the command itself does not.
	if err := postgres.RunMigrations(url, migrationsDirForTest(t)); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open assertion pool: %v", err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, pool, `INSERT INTO product_classifications (sku, handling_tags, temperature_class, dot_hazard_class) VALUES
		('CMD-SKU-1', ARRAY['Hazmat'], '', 3),
		('CMD-SKU-2', ARRAY['TemperatureSensitive'], 'Frozen', NULL)`)

	t.Setenv("DATABASE_URL", url)
	for run := 1; run <= 2; run++ {
		var stdout, stderr bytes.Buffer
		if code := runCommand(ctx, []string{republishCommand}, &stdout, &stderr); code != 0 {
			t.Fatalf("run %d: exit code %d, stderr: %s", run, code, stderr.String())
		}
		if got := stdout.String(); got != "republished 2 product classifications\n" {
			t.Fatalf("run %d: stdout = %q", run, got)
		}
	}

	var integration, analytics int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM outbox_events WHERE topic = 'warehouse.inventory.events'
			AND event_type = 'com.warehouse.wms.inventory-storage.product.ProductClassified' AND published_at IS NULL),
		(SELECT count(*) FROM outbox_events WHERE topic = 'warehouse.inventory.analytics')`).Scan(&integration, &analytics); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if integration != 4 || analytics != 0 {
		t.Fatalf("outbox rows: integration=%d (want 4 = 2 rows x 2 runs) analytics=%d (want 0)", integration, analytics)
	}
}
