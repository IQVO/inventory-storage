// Command mcp is the composition root for the Inventory & Storage MCP server:
// it wires env config to outbound adapters, adapters to the use cases, and
// those to the inbound MCP adapter, then serves MCP over Streamable HTTP. It
// is a second, independent deployable alongside cmd/inventory (the HTTP
// service), per ADR-0008.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// telemetryFlushTimeout bounds the final export attempt on shutdown, matching
// cmd/inventory. Without a deadline the exporter would retry against an
// unreachable Collector well past what an orchestrator will wait for.
const telemetryFlushTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Same non-blocking telemetry setup as the HTTP service: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't start.
	serviceName := getenv("OTEL_SERVICE_NAME", "inventory-storage-mcp")
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
	logger.Info("telemetry configured", "service_name", serviceName, "otlp_endpoint", otlpEndpoint)

	httpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	stockRepo, reservationRepo, publisher, closeAdapters, err := buildAdapters(context.Background(), databaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()

	clock := memory.SystemClock{}

	// The MCP adapter reuses the SAME use cases the HTTP adapter uses:
	// GetUsable (read) and RevokeReservation (write), plus the read-only
	// StockQueries port satisfied by the same StockRepo. RevokeReservation
	// needs a publisher and clock; the MCP server is not the platform's
	// primary event publisher (cmd/inventory is), so it logs the
	// ReservationRevoked event rather than publishing to Kafka.
	deps := inboundmcp.Deps{
		GetUsable:         &usecases.GetUsable{Stock: stockRepo},
		RevokeReservation: &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		Stock:             stockRepo,
	}
	// When the inventory-reports REST service is reachable, expose the curated
	// read-only Inventory Flow & Accuracy report tool, which reads through that
	// REST surface rather than the analytical database directly (ADR-0011).
	if reportsBaseURL := os.Getenv("REPORTS_BASE_URL"); reportsBaseURL != "" {
		deps.Reports = inboundmcp.NewReportsRESTClient(reportsBaseURL, nil)
		logger.Info("analytics report tool enabled", "reports_base_url", reportsBaseURL)
	}
	server := inboundmcp.NewServer(deps)

	handler := newRouter(inboundmcp.Handler(server))

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", httpAddr)
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

// newRouter wraps the MCP handler in the process's HTTP surface:
//
//   - GET /healthz answers 200 {"status":"ok"}, so the Kubernetes
//     liveness/readiness probes (charts/.../mcp-deployment.yaml) have a
//     cheap target.
//   - The MCP Streamable HTTP endpoint is mounted at BOTH "/" (the address
//     the binary has always served) and "/mcp" (warehouse-ops-agent's
//     *_MCP_ENDPOINT convention and the docs' examples), so either URL works.
func newRouter(mcpHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/mcp", mcpHandler)
	mux.Handle("/mcp/", mcpHandler)
	mux.Handle("/", mcpHandler)
	return mux
}

// buildAdapters wires the Postgres repos when DATABASE_URL is set, or falls
// back to the in-memory repos for local development without a database —
// exactly the selection cmd/inventory makes. The MCP server always logs its
// events (it is not the primary Kafka publisher), so a plain LogPublisher is
// used regardless of the repo choice.
func buildAdapters(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (
	ports.StockRepo, ports.ReservationRepo, ports.EventPublisher, func(), error,
) {
	noop := func() {}
	publisher := events.NewLogPublisher(logger)

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return memory.NewStockRepo(), memory.NewReservationRepo(), publisher, noop, nil
	}

	// Retried: this fleet's Istio native sidecars reset EVERY pod's first
	// outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars). A
	// single attempt turns that transient condition into CrashLoopBackOff;
	// the retry still fails closed once its budget is exhausted.
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(databaseURL, migrationsPath)
	}); err != nil {
		return nil, nil, nil, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, nil, nil, noop, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first-dial reset would surface inside the first
	// served request instead of at boot.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, nil, nil, noop, err
	}
	return postgres.NewStockRepo(pool), postgres.NewReservationRepo(pool), publisher, pool.Close, nil
}

// newLogger builds the process-wide structured logger, mirroring
// cmd/inventory: JSON to stdout, wrapped so a record written with a
// span-carrying context also carries trace_id/span_id.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
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
