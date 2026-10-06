//go:build integration

package analyticsstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/analytics/report"
)

// These tests run against a throwaway Postgres the test binary starts itself
// via testcontainers — never an external ANALYTICS_DATABASE_URL, never
// t.Skip. One container is started in TestMain, migrated once with the
// analytics migrations, and shared by every test in the package (containers
// are slow to boot); isolation comes from each test using unique SKUs or
// truncating what it asserts on.

var analyticsURL string

// TestMain owns the package-wide Postgres lifecycle.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_analytics"),
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

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(url, "../../../../migrations/analytics"); err != nil {
		fmt.Fprintf(os.Stderr, "migrate analytics: %v\n", err)
		return 1
	}
	analyticsURL = url
	return m.Run()
}

func TestPostgresProjectionAndReport_RoundTrip(t *testing.T) {
	url := analyticsURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	sku := "SKU-INT-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM flow_accuracy_rollup WHERE sku = $1`, sku)
	})

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// Apply the same events twice with the same event ids: idempotent.
	apply := func() {
		must(proj.ApplyStockReceived(ctx, "int-received", sku, 10, base))
		must(proj.ApplyStockPicked(ctx, "int-picked", sku, 4, base.Add(time.Minute)))
	}
	apply()
	apply()

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From: base.Add(-time.Hour), To: base.Add(time.Hour), SKU: sku, Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rep.Rows))
	}
	if rep.Rows[0].ReceivedQuantity != 10 {
		t.Errorf("ReceivedQuantity = %d, want 10 (idempotent)", rep.Rows[0].ReceivedQuantity)
	}
	if rep.Rows[0].PickedQuantity != 4 {
		t.Errorf("PickedQuantity = %d, want 4 (idempotent)", rep.Rows[0].PickedQuantity)
	}

	lag, err := rdr.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag < 0 {
		t.Errorf("lag = %v, want >= 0", lag)
	}
}

// TestReadOnlyPool_RejectsWrites asserts the reader pool is genuinely
// read-only: an attempt to write through it must be rejected by Postgres.
func TestReadOnlyPool_RejectsWrites(t *testing.T) {
	url := analyticsURL

	roPool, err := analyticsstore.NewReadOnlyPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewReadOnlyPool: %v", err)
	}
	t.Cleanup(roPool.Close)

	ctx := context.Background()
	_, err = roPool.Exec(ctx,
		`INSERT INTO flow_accuracy_rollup (sku, bin_id, hour_bucket) VALUES ($1, $2, $3)`,
		"RO", "", time.Now().UTC().Truncate(time.Hour))
	if err == nil {
		t.Fatal("expected read-only pool to reject INSERT, but it succeeded")
	}

	// The read side still works over the same read-only pool.
	rdr := analyticsstore.NewPostgresReport(roPool)
	if _, err := rdr.FreshnessLag(ctx); err != nil {
		t.Fatalf("FreshnessLag over read-only pool: %v", err)
	}
}

// TestFreshnessLag_EmptyStore covers the NULL path (pilot bug #2): max(occurred_at)
// over an empty table returns a single NULL row (not zero rows), which must be
// read as a zero lag rather than a scan error.
func TestFreshnessLag_EmptyStore(t *testing.T) {
	url := analyticsURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	// Ensure the processed-events table is empty so max() yields NULL.
	if _, err := pool.Exec(ctx, `TRUNCATE analytics_processed_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	lag, err := analyticsstore.NewPostgresReport(pool).FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag on empty store: %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty-store lag = %v, want 0", lag)
	}
}
