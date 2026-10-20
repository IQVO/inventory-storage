// Command inventory-projector is the WRITER composition root of the
// inventory-storage "Inventory Flow & Accuracy" data product. It consumes the
// analytics Kafka topic, projects each event into the analytical Postgres
// database via the idempotent PostgresProjection, and serves only a health
// endpoint on an admin port. It is the single writer of the analytical
// database and serves no reports; the reader (cmd/inventory-reports) is a
// separate deployable (ADR-0011).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

// telemetryFlushTimeout bounds the final export attempt on shutdown, matching
// cmd/inventory.
const telemetryFlushTimeout = 5 * time.Second

// drainTimeout bounds the consumer's and the admin server's shutdown.
const drainTimeout = 10 * time.Second

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is unset: the
// projector is the writer of the analytical database and cannot start without
// it.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

// projectorConfig is the mandatory part of the process environment.
type projectorConfig struct {
	analyticsURL   string
	kafkaBrokers   []string
	migrationsPath string
}

// loadConfig reads and validates the environment through get (os.Getenv in
// production, a map lookup in the tests). Postgres and Kafka are the only
// runtime (the real-wiring ADR): ANALYTICS_DATABASE_URL and KAFKA_BROKERS are
// required (no localhost default) and the error names the variable, so the
// process exits before any dial. The consumer group is a fixed constant of the
// binary (inboundkafka.AnalyticsConsumerGroup), so it needs no variable.
func loadConfig(get func(string) string) (projectorConfig, error) {
	cfg := projectorConfig{
		analyticsURL:   strings.TrimSpace(get("ANALYTICS_DATABASE_URL")),
		migrationsPath: "migrations/analytics",
	}
	if cfg.analyticsURL == "" {
		return projectorConfig{}, errMissingAnalyticsURL
	}
	for _, b := range strings.Split(get("KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.kafkaBrokers = append(cfg.kafkaBrokers, b)
		}
	}
	if len(cfg.kafkaBrokers) == 0 {
		return projectorConfig{}, errors.New("KAFKA_BROKERS is required: the projector runs on a real Kafka (comma-separated host:port list), there is no localhost default")
	}
	if v := get("ANALYTICS_MIGRATIONS_PATH"); v != "" {
		cfg.migrationsPath = v
	}
	return cfg, nil
}

func main() {
	// ctx is cancelled by SIGINT/SIGTERM: the start of the graceful drain.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		slog.Error("inventory-projector exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	serviceName := getenv("OTEL_SERVICE_NAME", "inventory-projector")
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

	// The projector owns the analytical schema: run its migrations on start,
	// then open the pool it projects into.
	pool, err := openAnalyticsPool(ctx, logger, cfg.analyticsURL, cfg.migrationsPath)
	if err != nil {
		return err
	}
	defer pool.Close()

	projection := analyticsstore.NewPostgresProjection(pool)
	consumed := analyticsstore.NewConsumedEventsRepo(pool)
	consumer := inboundkafka.NewAnalyticsConsumer(cfg.kafkaBrokers, outboundkafka.AnalyticsTopic, projection, consumed, logger)
	defer func() { _ = consumer.Close() }()

	// /readyz: the drain gate AND the analytical Postgres and Kafka answering.
	readiness := inboundhttp.NewReadiness(inboundhttp.DefaultReadinessTTL, inboundhttp.DefaultReadinessTimeout,
		inboundhttp.Dependency{Name: "postgres", Check: pool.Ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(cfg.kafkaBrokers).Check},
	)
	srv := newAdminServer(readiness)
	go func() {
		logger.Info("projector admin server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("projector admin server failed", "error", err)
		}
	}()

	return serveUntilDone(ctx, logger, consumer.Run, srv, readiness, cfg.kafkaBrokers)
}

// openAnalyticsPool runs the analytical schema migrations this projector
// owns, then opens the analytical Postgres pool and confirms the first dial
// with a ping. Retried: this fleet's Istio native sidecars reset EVERY pod's
// first outbound TCP dial ~10s after the app starts
// (holdApplicationUntilProxyStarts is a no-op for native sidecars). A single
// attempt turns that transient condition into CrashLoopBackOff; the retry
// still fails closed once its budget is exhausted.
func openAnalyticsPool(ctx context.Context, logger *slog.Logger, analyticsURL, migrationsPath string) (*pgxpool.Pool, error) {
	if err := bootretry.Retry(ctx, logger, "run analytics migrations", func() error {
		return postgres.RunMigrations(analyticsURL, migrationsPath)
	}); err != nil {
		return nil, err
	}

	pool, err := analyticsstore.NewPool(ctx, analyticsURL)
	if err != nil {
		return nil, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first-dial reset would surface inside the first
	// consumed message instead of at boot.
	if err := bootretry.Retry(ctx, logger, "ping analytics database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		return nil, err
	}
	if err := analyticsstore.RecordPoolStats(pool); err != nil {
		logger.Error("analytics pgxpool metrics unavailable", "error", err)
	}
	return pool, nil
}

// newAdminServer builds the projector's admin server: health and readiness
// only, on the admin port — the writer serves no reports and no business
// surface (ADR-0011 keeps the writer and reader deployables separate).
//
//   - GET /healthz is liveness: 200 while the process is up.
//   - GET /readyz is the drain gate plus the analytical Postgres and Kafka:
//     503 not_ready while draining, 503 dependency-unavailable (naming the
//     dependencies) while one is unreachable.
func newAdminServer(readiness *inboundhttp.Readiness) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !readiness.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		if failing := readiness.Failing(r.Context()); len(failing) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "dependency-unavailable", "dependencies": failing})
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	return &http.Server{Addr: getenv("ADMIN_ADDR", ":8091"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// serveUntilDone runs the analytics consumer (runConsumer) until ctx is
// cancelled (SIGINT/SIGTERM) or the consumer stops by itself. A signal drains
// in order: readiness flips to not-ready, the consumer's context is cancelled
// and awaited (bounded), the admin server shuts down. A consumer that stops
// WITHOUT a signal is a broken projector — Postgres and Kafka are the only
// runtime and the consumer is always on — so the same drain runs and the
// consumer's error is returned, which makes the process exit non-zero and the
// orchestrator restart it.
func serveUntilDone(ctx context.Context, logger *slog.Logger, runConsumer func(context.Context) error, srv *http.Server, readiness *inboundhttp.Readiness, kafkaBrokers []string) error {
	consumerCtx, cancelConsumer := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelConsumer()
	consumerDone := make(chan error, 1)
	go func() {
		logger.Info("analytics consumer starting", "topic", outboundkafka.AnalyticsTopic, "group", inboundkafka.AnalyticsConsumerGroup, "brokers", kafkaBrokers)
		consumerDone <- runConsumer(consumerCtx)
	}()

	var consumerErr error
	consumerExited := false
	select {
	case <-ctx.Done():
	case err := <-consumerDone:
		consumerExited = true
		consumerErr = fmt.Errorf("analytics consumer stopped: %w", errors.Join(err, errConsumerStopped))
		logger.Error("analytics consumer stopped", "error", err)
	}

	readiness.SetNotReady()
	cancelConsumer()
	if !consumerExited {
		select {
		case err := <-consumerDone:
			logger.Debug("analytics consumer stopped after the shutdown signal", "error", err)
		case <-time.After(drainTimeout):
			logger.Warn("analytics consumer did not stop before the shutdown drain deadline")
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	return errors.Join(consumerErr, srv.Shutdown(shutdownCtx))
}

// errConsumerStopped marks a consumer that returned before any shutdown signal.
var errConsumerStopped = errors.New("consumer returned without a shutdown signal")

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
