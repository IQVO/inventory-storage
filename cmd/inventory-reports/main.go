// Command inventory-reports is the READER composition root of the
// inventory-storage "Inventory Flow & Accuracy" data product. It opens the
// analytical Postgres database over a read-only pool and serves the report and
// its freshness over REST. It writes nothing: the writer
// (cmd/inventory-projector) is a separate deployable and owns the schema
// (ADR-0011).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

// telemetryFlushTimeout bounds the final export attempt on shutdown, matching
// cmd/inventory.
const telemetryFlushTimeout = 5 * time.Second

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is unset.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

func main() {
	if err := run(); err != nil {
		slog.Error("inventory-reports exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	serviceName := getenv("OTEL_SERVICE_NAME", "inventory-reports")
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint)
	shutdownTelemetry, err := telemetry.Setup(context.Background(), serviceName, getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion), otlpEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("telemetry flush failed on shutdown", "error", err)
		}
	}()

	httpAddr := getenv("HTTP_ADDR", ":8092")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}

	// Read-only pool: even a bug in the reader cannot mutate the read model, on
	// top of the read-only database role ANALYTICS_DATABASE_URL should use.
	ctx := context.Background()
	pool, err := analyticsstore.NewReadOnlyPool(ctx, analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Retried: this fleet's Istio native sidecars reset EVERY pod's first
	// outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars).
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first-dial reset would surface inside the first
	// served request instead of at boot, and a single attempt would turn
	// that transient condition into CrashLoopBackOff.
	if err := bootretry.Retry(ctx, logger, "ping analytics database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		return err
	}
	if err := analyticsstore.RecordPoolStats(pool); err != nil {
		logger.Error("analytics pgxpool metrics unavailable", "error", err)
	}

	handlers := &inboundhttp.ReportsHandlers{Store: analyticsstore.NewPostgresReport(pool)}
	router := inboundhttp.NewReportsRouter(handlers, logger, serviceName)

	srv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("reports server listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(telemetry.WithTraceContext(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
