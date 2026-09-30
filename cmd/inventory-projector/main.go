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
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is unset: the
// projector is the writer of the analytical database and cannot start without
// it.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

func main() {
	if err := run(); err != nil {
		slog.Error("inventory-projector exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
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

	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	migrationsPath := getenv("ANALYTICS_MIGRATIONS_PATH", "migrations/analytics")

	// The projector owns the analytical schema: run its migrations on start,
	// then open the pool it projects into.
	pool, err := openAnalyticsPool(context.Background(), logger, analyticsURL, migrationsPath)
	if err != nil {
		return err
	}
	defer pool.Close()

	projection := analyticsstore.NewPostgresProjection(pool)
	consumed := analyticsstore.NewConsumedEventsRepo(pool)
	consumer := inboundkafka.NewAnalyticsConsumer(kafkaBrokers, outboundkafka.AnalyticsTopic, projection, consumed, logger)
	defer func() { _ = consumer.Close() }()

	srv := newAdminServer()
	go func() {
		logger.Info("projector admin server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("projector admin server failed", "error", err)
		}
	}()

	return serveUntilSignal(logger, consumer, srv, kafkaBrokers)
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

// newAdminServer builds the projector's admin server: a health endpoint
// only, on the admin port — the writer serves no reports and no business
// surface (ADR-0011 keeps the writer and reader deployables separate).
func newAdminServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return &http.Server{Addr: getenv("ADMIN_ADDR", ":8091"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// serveUntilSignal runs the analytics consumer until SIGINT/SIGTERM, then
// drains: it cancels the consumer's context and shuts the admin server down
// within a bounded grace period.
func serveUntilSignal(logger *slog.Logger, consumer *inboundkafka.AnalyticsConsumer, srv *http.Server, kafkaBrokers []string) error {
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	go func() {
		logger.Info("analytics consumer starting", "topic", outboundkafka.AnalyticsTopic, "group", inboundkafka.AnalyticsConsumerGroup, "brokers", kafkaBrokers)
		if err := consumer.Run(consumerCtx); err != nil {
			logger.Error("analytics consumer stopped", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancelConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
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
