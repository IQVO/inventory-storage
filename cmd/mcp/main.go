// Command mcp is the composition root for the Inventory & Storage MCP server:
// it wires env config to outbound adapters, adapters to the use cases, and
// those to the inbound MCP adapter, then serves MCP over Streamable HTTP. It
// is a second, independent deployable alongside cmd/inventory (the HTTP
// service), per ADR-0008.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"
	kafkago "github.com/segmentio/kafka-go"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
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
	// See cmd/inventory/main.go's identical fallback and buildAdapters'
	// doc comment for the full "why" (session-scoped pg_advisory_lock vs
	// PgBouncer transaction-pooling incompatibility, ADR
	// 0023-migrations-direct-postgres-connection.md, mirroring
	// order-management's ADR-0029). This binary also runs migrations on
	// start (buildAdapters below), so it needs the same direct-connection
	// split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")
	// EVENT_PUBLISHER / KAFKA_BROKERS select the SAME event publishing
	// wiring cmd/inventory uses (ADR-0017): with DATABASE_URL set and
	// EVENT_PUBLISHER=kafka, MCP-initiated writes enqueue outbox rows.
	eventPublisher := getenv("EVENT_PUBLISHER", "log")
	var kafkaBrokers []string
	if raw := os.Getenv("KAFKA_BROKERS"); raw != "" {
		kafkaBrokers = strings.Split(raw, ",")
	}

	ad, err := buildAdapters(context.Background(), databaseURL, migrationsDatabaseURL, migrationsPath, eventPublisher, kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer ad.close()

	reservationMetrics, err := telemetry.NewReservationMetrics()
	if err != nil {
		return err
	}

	deps := buildDeps(ad, memory.SystemClock{}, reservationMetrics)
	// When the inventory-reports REST service is reachable, expose the curated
	// read-only Inventory Flow & Accuracy report tool, which reads through that
	// REST surface rather than the analytical database directly (ADR-0011).
	if reportsBaseURL := os.Getenv("REPORTS_BASE_URL"); reportsBaseURL != "" {
		deps.Reports = inboundmcp.NewReportsRESTClient(reportsBaseURL, nil)
		logger.Info("analytics report tool enabled", "reports_base_url", reportsBaseURL)
	}
	server := inboundmcp.NewServer(deps)

	handler := newRouter(inboundmcp.Handler(server), serviceName)

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
//
// Every request is traced (otelchi) and metered (otelchimetric:
// http.server.request.duration + http.server.active_requests), in the same
// order as the REST routers (ADR-0016 Tier 1). The MCP endpoint is
// unauthenticated by decision (ADR-0015).
func newRouter(mcpHandler http.Handler, serviceName string) http.Handler {
	r := chi.NewRouter()
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	metricCfg := otelchimetric.NewBaseConfig(serviceName)
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle("/mcp", mcpHandler)
	r.Handle("/mcp/*", mcpHandler)
	// "/*" also matches "/" itself in chi.
	r.Handle("/*", mcpHandler)
	return r
}

// adapters is what buildAdapters wires: the repos and event publisher the
// MCP tools' use cases run on, the UnitOfWork bracketing their writes, and
// the shutdown hook releasing every resource opened along the way.
type adapters struct {
	stock        ports.StockRepo
	reservations ports.ReservationRepo
	publisher    ports.EventPublisher
	// uow is nil in the in-memory (no DATABASE_URL) configuration, which
	// the use cases' atomically() helper treats as "run fn directly".
	uow   ports.UnitOfWork
	close func()
}

// buildDeps wires the MCP tools' use cases over ad. RevokeReservation gets
// the SAME collaborators cmd/inventory gives it — publisher, UnitOfWork and
// reservation metrics — so an MCP-initiated revocation is as observable and
// as durable as a REST DELETE /reservations/{id} (ADR-0017).
func buildDeps(ad adapters, clock ports.Clock, metrics ports.ReservationMetrics) inboundmcp.Deps {
	return inboundmcp.Deps{
		GetUsable: &usecases.GetUsable{Stock: ad.stock},
		RevokeReservation: &usecases.RevokeReservation{
			Stock: ad.stock, Reservations: ad.reservations, Events: ad.publisher,
			Clock: clock, Metrics: metrics, UnitOfWork: ad.uow,
		},
		Stock: ad.stock,
	}
}

// buildAdapters wires the Postgres repos when DATABASE_URL is set, or falls
// back to the in-memory repos for local development without a database —
// exactly the selection cmd/inventory makes — and selects the event
// publisher the same way (EVENT_PUBLISHER):
//
//   - "log" (default): events are logged only.
//   - "kafka" with DATABASE_URL: the transactional outbox (ADR-0017). A
//     UnitOfWork brackets each use case's Save + Publish, and
//     postgres.OutboxPublisher inserts one outbox_events row per (event,
//     topic) — integration AND analytics — in that same transaction. This
//     binary does NOT run the relay: cmd/inventory's OutboxRelay drains
//     the shared outbox_events table onto Kafka, so MCP-initiated events
//     reach warehouse.inventory.events / warehouse.inventory.analytics
//     without a second relay (and without this process needing a broker).
//   - "kafka" without DATABASE_URL: direct publish to both topics, as
//     cmd/inventory does in that configuration. Requires KAFKA_BROKERS.
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below,
// mirroring cmd/inventory/main.go's openPostgresPool exactly — see its doc
// comment for the full "why" a direct, non-pooled connection is needed
// here even though the pgxpool opened just after (databaseURL) stays on
// PgBouncer.
func buildAdapters(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath, eventPublisher string, brokers []string, logger *slog.Logger) (adapters, error) {
	noop := func() {}
	kafkaMode := strings.EqualFold(eventPublisher, "kafka")

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		reservationRepo := memory.NewReservationRepo()
		ad := adapters{stock: memory.NewStockRepo(), reservations: reservationRepo, publisher: events.NewLogPublisher(logger), close: noop}
		if !kafkaMode {
			return ad, nil
		}
		if len(brokers) == 0 {
			return adapters{}, fmt.Errorf("EVENT_PUBLISHER=kafka without DATABASE_URL requires KAFKA_BROKERS to be set")
		}
		writer := kafkaadapter.NewWriter(brokers...)
		analyticsPub := kafkaadapter.NewAnalyticsPublisher(brokers, reservationRepo, nil)
		ad.publisher = events.NewMultiPublisher(kafkaadapter.NewPublisher(writer, reservationRepo), analyticsPub)
		ad.close = func() { closeKafkaWriters(writer, analyticsPub, logger) }
		logger.Info("event publisher configured", "publisher", "kafka (direct, no outbox)", "brokers", brokers)
		return ad, nil
	}

	// Retried: this fleet's Istio native sidecars reset EVERY pod's first
	// outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars). A
	// single attempt turns that transient condition into CrashLoopBackOff;
	// the retry still fails closed once its budget is exhausted.
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return adapters{}, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return adapters{}, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first-dial reset would surface inside the first
	// served request instead of at boot.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return adapters{}, err
	}

	stockRepo := postgres.NewStockRepo(pool)
	reservationRepo := postgres.NewReservationRepo(pool)
	ad := adapters{
		stock: stockRepo, reservations: reservationRepo,
		publisher: events.NewLogPublisher(logger),
		uow:       postgres.NewUnitOfWork(pool),
		close:     pool.Close,
	}
	if kafkaMode {
		// The outbox only ENCODES: both encoders are built without a Kafka
		// writer because delivery is cmd/inventory's relay's job. Their
		// direct Publish methods are never called on this path.
		ad.publisher = postgres.NewOutboxPublisher(pool,
			kafkaadapter.NewPublisher(nil, reservationRepo),
			&kafkaadapter.AnalyticsPublisher{Reservations: reservationRepo},
		)
		logger.Info("event publisher configured", "publisher", "kafka (transactional outbox, relay runs in cmd/inventory)",
			"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic)
	}
	return ad, nil
}

// closeKafkaWriters closes the integration and analytics Kafka writers of
// the direct (no-outbox) configuration, logging (not failing) on error.
func closeKafkaWriters(writer *kafkago.Writer, analyticsPub *kafkaadapter.AnalyticsPublisher, logger *slog.Logger) {
	if err := writer.Close(); err != nil {
		logger.Error("error closing kafka integration writer", "error", err)
	}
	if err := analyticsPub.Close(); err != nil {
		logger.Error("error closing kafka analytics writer", "error", err)
	}
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
