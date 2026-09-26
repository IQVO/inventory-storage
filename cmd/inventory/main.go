// Command inventory is the composition root: it wires env config into
// adapters, adapters into use cases, and use cases into the HTTP router.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/facilitycache"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/facilitylayout"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// telemetryFlushTimeout bounds the final export attempt. Without a deadline
// the exporter would retry against an unreachable Collector and stretch a
// shutdown out well past what an orchestrator will wait for.
const telemetryFlushTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint)

	// Telemetry comes up before any adapter, so the pgx pool and the Kafka
	// writer are built against the real providers rather than the no-op
	// globals. Export is non-blocking: an unreachable Collector costs
	// telemetry, never availability.
	shutdownTelemetry, err := telemetry.Setup(context.Background(), serviceName, getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion), otlpEndpoint)
	if err != nil {
		return err
	}
	// Registered before every other defer so it runs last: the final flush
	// happens once the HTTP server has stopped and the adapters are closed.
	// A failed flush is logged, never returned — an unreachable Collector
	// costs telemetry, and turning that into a non-zero exit would make
	// every clean shutdown look like a crash.
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("telemetry flush failed on shutdown", "error", err)
		}
	}()
	logger.Info("telemetry configured", "service_name", serviceName, "otlp_endpoint", otlpEndpoint)

	httpAddr := getenv("HTTP_ADDR", ":8080")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")
	eventPublisher := getenv("EVENT_PUBLISHER", "log")

	stockRepo, locationRepo, reservationRepo, classificationRepo, publisher, closeAdapters, err := buildAdapters(context.Background(), databaseURL, migrationsPath, eventPublisher, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()

	reservationMetrics, err := telemetry.NewReservationMetrics()
	if err != nil {
		return err
	}

	clock := memory.SystemClock{}
	// The lookup's Kafka consumer (when LOCATION_LOOKUP_MODE=kafka) must
	// outlive this call and stop on shutdown, so it gets its own
	// cancellable context rather than the signal context established
	// further down — which does not exist yet at this point.
	lookupCtx, stopLookup := context.WithCancel(context.Background())
	defer stopLookup()

	var kafkaBrokers []string
	if raw := os.Getenv("KAFKA_BROKERS"); raw != "" {
		kafkaBrokers = strings.Split(raw, ",")
	}

	locationLookup, closeLocationLookup, err := buildLocationLookup(
		lookupCtx,
		getenv("LOCATION_LOOKUP_MODE", "permissive"),
		os.Getenv("FACILITY_LAYOUT_BASE_URL"),
		kafkaBrokers,
		logger,
	)
	if err != nil {
		return err
	}
	defer closeLocationLookup()

	server := &inboundhttp.Server{
		ReceiveStock: &usecases.ReceiveStock{Events: publisher, Clock: clock},
		StowStock: &usecases.StowStock{
			Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock,
			Classifications: classificationRepo, LocationLookup: locationLookup,
		},
		ReserveStock:               &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics},
		RevokeReservation:          &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics},
		ConfirmPick:                &usecases.ConfirmPick{Stock: stockRepo, Locations: locationRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		GetUsable:                  &usecases.GetUsable{Stock: stockRepo},
		GetReservationsByDemandRef: &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		RunCycleCount:              &usecases.RunCycleCount{Stock: stockRepo, Events: publisher, Clock: clock},
		ClassifyProduct:            &usecases.ClassifyProduct{Classifications: classificationRepo, Events: publisher, Clock: clock},
		Classifications:            classificationRepo,
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
	return httpServer.Shutdown(shutdownCtx)
}

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|error (case-insensitive) to the matching slog.Level,
// defaulting to Info for unset or unrecognized values. Logs are emitted as
// JSON to stdout, wrapped so that any record written with a span-carrying
// context also carries trace_id/span_id.
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

// buildAdapters wires the Postgres adapters when DATABASE_URL is set, or
// falls back to the in-memory adapters for local development without a
// database. The event publisher defaults to that same memory/Postgres
// choice ("log"), or can be switched to the Kafka integration-events
// publisher via eventPublisher="kafka" (EVENT_PUBLISHER env), independent of
// which repos are in use.
func buildAdapters(ctx context.Context, databaseURL, migrationsPath, eventPublisher string, logger *slog.Logger) (
	ports.StockRepo, ports.LocationRepo, ports.ReservationRepo, ports.ProductClassificationRepo, ports.EventPublisher, func(), error,
) {
	noop := func() {}

	var (
		stockRepo          ports.StockRepo
		locationRepo       ports.LocationRepo
		reservationRepo    ports.ReservationRepo
		classificationRepo ports.ProductClassificationRepo
		defaultPub         ports.EventPublisher
		closeRepos         = noop
	)

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		stockRepo = memory.NewStockRepo()
		locationRepo = memory.NewLocationRepo()
		reservationRepo = memory.NewReservationRepo()
		classificationRepo = memory.NewProductClassificationRepo()
		defaultPub = events.NewLogPublisher(logger)
	} else {
		// Retried: this fleet's Istio native sidecars reset EVERY pod's
		// first outbound TCP dial ~10s after the app starts
		// (holdApplicationUntilProxyStarts is a no-op for native
		// sidecars). A single attempt turns that transient condition into
		// CrashLoopBackOff; the retry still fails closed once its budget
		// is exhausted.
		if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
			return postgres.RunMigrations(databaseURL, migrationsPath)
		}); err != nil {
			return nil, nil, nil, nil, nil, noop, err
		}

		pool, err := postgres.NewPool(ctx, databaseURL)
		if err != nil {
			return nil, nil, nil, nil, nil, noop, err
		}
		// ParseConfig/NewWithConfig do not themselves establish a
		// connection, so without this the first-dial reset would surface
		// inside the first real request instead of at boot.
		if err := bootretry.Retry(ctx, logger, "ping database", func() error {
			return pool.Ping(ctx)
		}); err != nil {
			pool.Close()
			return nil, nil, nil, nil, nil, noop, err
		}

		stockRepo = postgres.NewStockRepo(pool)
		locationRepo = postgres.NewLocationRepo(pool)
		reservationRepo = postgres.NewReservationRepo(pool)
		classificationRepo = postgres.NewProductClassificationRepo(pool)
		defaultPub = postgres.NewEventPublisher(pool)
		closeRepos = pool.Close
	}

	if !strings.EqualFold(eventPublisher, "kafka") {
		return stockRepo, locationRepo, reservationRepo, classificationRepo, defaultPub, closeRepos, nil
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	writer := kafkaadapter.NewWriter(brokers...)
	integrationPub := kafkaadapter.NewPublisher(writer, reservationRepo)

	// Fan-out: the same domain event is forwarded to BOTH the integration
	// topic (via the untouched integration Publisher) AND the dedicated
	// analytics topic (via a separate AnalyticsPublisher), behind the single
	// ports.EventPublisher the use cases depend on. The integration contract
	// (warehouse.inventory.events) is unchanged; the analytics stream
	// (warehouse.inventory.analytics) evolves independently (ADR-0011).
	analyticsPub := kafkaadapter.NewAnalyticsPublisher(brokers, reservationRepo, nil)
	publisher := events.NewMultiPublisher(integrationPub, analyticsPub)
	logger.Info("event publisher configured", "publisher", "kafka",
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic, "brokers", brokers)

	closeAll := func() {
		if err := writer.Close(); err != nil {
			logger.Error("error closing kafka integration writer", "error", err)
		}
		if err := analyticsPub.Close(); err != nil {
			logger.Error("error closing kafka analytics writer", "error", err)
		}
		closeRepos()
	}

	return stockRepo, locationRepo, reservationRepo, classificationRepo, publisher, closeAll, nil
}

// buildLocationLookup selects the outbound LocationClassificationLookup
// adapter via LOCATION_LOOKUP_MODE (kafka|http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected — mirroring the EVENT_PUBLISHER=kafka|log
// pattern.
//
//   - "kafka"      maintains a local cache fed by facility-layout's
//     warehouse.facility.events topic. No per-stow HTTP call, so
//     facility-layout stops being a runtime dependency of StowStock.
//     Requires KAFKA_BROKERS. Blocks until the initial replay completes
//     (see the facilitycache package doc for why).
//   - "http"       calls facility-layout synchronously per stow. Requires
//     FACILITY_LAYOUT_BASE_URL. Retained as the rollback for "kafka".
//   - "permissive" (default) answers Known=false for everything.
//
// The returned closer releases the Kafka reader when one was started, and
// is a no-op otherwise.
func buildLocationLookup(ctx context.Context, mode, facilityLayoutBaseURL string, brokers []string, logger *slog.Logger) (ports.LocationClassificationLookup, func(), error) {
	switch {
	case strings.EqualFold(mode, "kafka"):
		if len(brokers) == 0 {
			return nil, nil, fmt.Errorf("LOCATION_LOOKUP_MODE=kafka requires KAFKA_BROKERS to be set")
		}
		// Retried for the same reason the Postgres dials above are: this
		// call's newTargetOffsets dials the broker synchronously
		// (kafkago.DialContext) to capture the readiness watermark before
		// any consuming begins, and that dial is exactly this fleet's
		// known ~10s post-start first-outbound-dial reset. A single
		// attempt turned that transient into CrashLoopBackOff here too.
		var consumer *facilitycache.Consumer
		if err := bootretry.Retry(ctx, logger, "dial facility-layout kafka topic", func() error {
			c, err := facilitycache.NewConsumer(ctx, brokers, logger)
			if err != nil {
				return err
			}
			consumer = c
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("failed to start the Kafka-sourced location cache: %w", err)
		}
		logger.Info("location classification lookup configured",
			"mode", "kafka", "topic", facilitycache.Topic, "brokers", brokers)

		go func() {
			logger.Info("facility location cache consumer running", "topic", facilitycache.Topic)
			// Run only ever returns on error (including the plain
			// context.Canceled of an orderly shutdown), never nil.
			if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
				logger.Error("facility location cache consumer stopped", "error", err)
			}
		}()

		// Preserve the retired HTTP client's "never answer against
		// incomplete data" property: an empty cache reports Known=false,
		// which is a FAIL-OPEN answer, so serving traffic mid-replay
		// would silently wave through stows that should have been
		// classified.
		logger.Info("waiting for the facility location cache to replay its initial history before accepting traffic")
		waitCtx, cancel := context.WithTimeout(ctx, facilitycache.WaitReadyTimeout)
		defer cancel()
		if err := consumer.WaitReady(waitCtx); err != nil {
			_ = consumer.Close()
			return nil, nil, fmt.Errorf("facility location cache did not become ready within %s: %w", facilitycache.WaitReadyTimeout, err)
		}
		if consumer.Slots() == 0 {
			// Not fatal — a genuinely empty facility-layout is a valid
			// state — but it means every lookup fails open, so say so
			// loudly rather than letting it look like a working cache.
			logger.Warn("facility location cache is ready but EMPTY; every location will report Known=false (fail-open) until facility-layout publishes",
				"topic", facilitycache.Topic)
		} else {
			logger.Info("facility location cache is ready", "slots", consumer.Slots(), "zones", consumer.Zones())
		}
		return consumer, func() { _ = consumer.Close() }, nil

	case strings.EqualFold(mode, "http"):
		logger.Info("location classification lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL)
		return facilitylayout.NewClient(facilityLayoutBaseURL, nil), func() {}, nil

	default:
		return facilitylayout.NewPermissiveLookup(), func() {}, nil
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
