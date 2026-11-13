//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
)

// The real entry point of the analytics READER (ADR-0011) against a real,
// analytically-migrated Postgres booted by this test via testcontainers
// (never an external ANALYTICS_DATABASE_URL, never t.Skip):
//
//	seed facts through the repo's own analytics writer (the PostgresProjection
//	  cmd/inventory-projector applies — the reader must never write)
//	  -> run(): read-only pool over the analytical database, REST up
//	  -> the documented probes answer from the projection:
//	     /healthz, GET /reports/flow-accuracy (rows + sku/binId filters),
//	     GET /reports/flow-accuracy/freshness
//	  -> SIGTERM (the process's own drain signal) shuts down cleanly.
//
// The reader never migrates (the writer owns the schema), so the test applies
// the same analytics migrations the projector runs, then seeds through the
// writer's own projection port — never raw INSERTs.
var repItPG testcontainers.Container

func TestMain(m *testing.M) {
	code := m.Run()
	if repItPG != nil {
		if err := testcontainers.TerminateContainer(repItPG); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

// repItMigrationsDir resolves the repo's own analytics migrations relative to
// this file, so the path works regardless of the directory go test ran from.
func repItMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve the test file's path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}

// repItFreeAddr reserves an ephemeral localhost port for the reports server.
func repItFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// repItGetJSON fetches url and decodes a JSON object body; a transport error
// returns 0 (the server may still be starting).
func repItGetJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// repItRow finds the row with the given sku and binId in a flow-accuracy
// response body.
func repItRow(t *testing.T, body map[string]any, sku, binID string) map[string]any {
	t.Helper()
	rows, _ := body["rows"].([]any)
	for _, r := range rows {
		row, _ := r.(map[string]any)
		if row["sku"] == sku && row["binId"] == binID {
			return row
		}
	}
	return nil
}

// TestReportsServeTheProjectionAndStopCleanly proves the documented reader
// lifecycle end to end (ADR-0011, docs/docs/analytics): boot over an
// analytically-migrated database, answer /healthz, serve the report and its
// filters and the freshness lag from the seeded projection, and drain cleanly
// on SIGTERM.
func TestReportsServeTheProjectionAndStopCleanly(t *testing.T) {
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_analytics"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	repItPG = pg
	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}

	// The analytical schema the projector owns, applied with the same
	// migration runner cmd/inventory-projector runs at boot.
	if err := postgres.RunMigrations(url, repItMigrationsDir(t)); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}

	// Seed the facts with the repo's own analytics writer — the exact
	// projection the projector applies per consumed event.
	writer, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	projection := analyticsstore.NewPostgresProjection(writer)
	declared := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	if err := projection.ApplyStockReceived(ctx, "evt-it-reports-received", "SKU-IT-REP-A", 7, declared); err != nil {
		t.Fatal(err)
	}
	if err := projection.ApplyItemStowed(ctx, "evt-it-reports-stowed", "SKU-IT-REP-A", "BIN-IT-REP-A", declared.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	addr := repItFreeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a dial
	// at shutdown (same convention as the cmd/inventory e2e suite).
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	// run() takes no context: it installs its own SIGINT/SIGTERM handler, so
	// the clean drain is driven through the process's real signal path below.
	done := make(chan error, 1)
	go func() { done <- run() }()

	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if code, _ := repItGetJSON(t, "http://"+addr+"/healthz"); code == http.StatusOK {
			break
		}
	}

	base := "http://" + addr + "/reports/flow-accuracy"
	code, body := repItGetJSON(t, base+"?from=2026-11-03T09:00:00Z&to=2026-11-03T12:00:00Z")
	rows, _ := body["rows"].([]any)
	if code != http.StatusOK || len(rows) != 2 {
		t.Fatalf("flow-accuracy = %d %v", code, body)
	}
	received := repItRow(t, body, "SKU-IT-REP-A", "")
	if received == nil || received["hourBucket"] != "2026-11-03T10:00:00Z" || received["receivedQuantity"] != 7.0 {
		t.Fatalf("the received row is wrong: %v", received)
	}
	stowed := repItRow(t, body, "SKU-IT-REP-A", "BIN-IT-REP-A")
	if stowed == nil || stowed["hourBucket"] != "2026-11-03T11:00:00Z" || stowed["stowedCount"] != 1.0 {
		t.Fatalf("the stowed row is wrong: %v", stowed)
	}

	code, body = repItGetJSON(t, base+"?from=2026-11-03T09:00:00Z&to=2026-11-03T12:00:00Z&binId=BIN-IT-REP-A")
	rows, _ = body["rows"].([]any)
	if code != http.StatusOK || len(rows) != 1 || repItRow(t, body, "SKU-IT-REP-A", "BIN-IT-REP-A") == nil {
		t.Fatalf("flow-accuracy with the binId filter = %d %v", code, body)
	}

	code, body = repItGetJSON(t, "http://"+addr+"/reports/flow-accuracy/freshness")
	lag, ok := body["lagSeconds"].(float64)
	if code != http.StatusOK || !ok || lag <= 0 {
		t.Fatalf("freshness = %d %v", code, body)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a SIGTERM'd reports server must drain cleanly, got %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the reports server did not stop")
	}
}
