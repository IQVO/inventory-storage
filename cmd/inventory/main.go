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

	"github.com/jackc/pgx/v5/pgxpool"

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
	"github.com/claudioed/inventory-storage/internal/resilience"
)

// telemetryFlushTimeout bounds the final export attempt. Without a deadline
// the exporter would retry against an unreachable Collector and stretch a
// shutdown out well past what an orchestrator will wait for.
const telemetryFlushTimeout = 5 * time.Second

// shutdownDrainTimeout bounds how long graceful shutdown (ADR-0020
// §graceful shutdown) waits for the outbox relay and the facility
// location cache consumer's Run loop to actually stop, after their
// contexts are cancelled — mirroring the HTTP server's own Shutdown
// budget below. A relay/consumer that does not stop within this window
// is logged and shutdown proceeds anyway; the process is exiting either
// way and this is strictly better than hanging past the orchestrator's
// own terminationGracePeriodSeconds.
const shutdownDrainTimeout = 10 * time.Second

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

	stockRepo, locationRepo, reservationRepo, classificationRepo, publisher, uow, idempotencyPool, closeAdapters, err := buildAdapters(context.Background(), databaseURL, migrationsPath, eventPublisher, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()

	reservationMetrics, err := telemetry.NewReservationMetrics()
	if err != nil {
		return err
	}

	// circuitBreakerMetrics wires the facility-layout breaker's
	// OnStateChange into the circuit_breaker.state gauge (ADR-0020),
	// reusing the SAME OTel MeterProvider telemetry.Setup already
	// installed above rather than standing up a second Prometheus
	// registry. Errors here mirror NewReservationMetrics' contract
	// (invalid instrument name only, a programming error) — non-fatal:
	// a nil recorder just means this process runs without the gauge,
	// never without the breaker itself.
	circuitBreakerMetrics, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics unavailable; the facility-layout breaker will run without the circuit_breaker.state gauge", "error", err)
	}

	// readiness gates GET /readyz (ADR-0020 §graceful shutdown). The
	// zero value is ready; SetNotReady is called as the FIRST step of
	// the shutdown sequence below, before the HTTP server itself stops
	// accepting connections, so a Kubernetes readinessProbe has a
	// chance to observe the flip and stop routing new traffic during
	// the drain window that follows.
	readiness := &inboundhttp.Readiness{}

	clock := memory.SystemClock{}
	// The lookup's Kafka consumer (when LOCATION_LOOKUP_MODE=kafka) must
	// outlive this call and stop on shutdown, so it gets its own
	// cancellable context rather than the signal context established
	// further down — which does not exist yet at this point.
	lookupCtx, stopLookup := context.WithCancel(context.Background())

	var kafkaBrokers []string
	if raw := os.Getenv("KAFKA_BROKERS"); raw != "" {
		kafkaBrokers = strings.Split(raw, ",")
	}

	locationLookup, lookupRunDone, closeLocationLookup, err := buildLocationLookup(
		lookupCtx,
		getenv("LOCATION_LOOKUP_MODE", "permissive"),
		os.Getenv("FACILITY_LAYOUT_BASE_URL"),
		kafkaBrokers,
		circuitBreakerMetrics,
		logger,
	)
	if err != nil {
		stopLookup()
		return err
	}

	server := &inboundhttp.Server{
		ReceiveStock: &usecases.ReceiveStock{Events: publisher, Clock: clock, UnitOfWork: uow},
		StowStock: &usecases.StowStock{
			Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock,
			Classifications: classificationRepo, LocationLookup: locationLookup, UnitOfWork: uow,
		},
		ReserveStock:               &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics, UnitOfWork: uow},
		RevokeReservation:          &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, Metrics: reservationMetrics, UnitOfWork: uow},
		ConfirmPick:                &usecases.ConfirmPick{Stock: stockRepo, Locations: locationRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		GetUsable:                  &usecases.GetUsable{Stock: stockRepo},
		GetReservationsByDemandRef: &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		RunCycleCount:              &usecases.RunCycleCount{Stock: stockRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		ClassifyProduct:            &usecases.ClassifyProduct{Classifications: classificationRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		Classifications:            classificationRepo,
		// IdempotencyPool wires RequireIdempotencyKey onto POST
		// /stock/receive and POST /reservations (see
		// inboundhttp.NewRouter). nil (in-memory/no-DATABASE_URL
		// configuration) leaves those routes unprotected, mirroring
		// every other optional Postgres-backed capability here.
		IdempotencyPool: idempotencyPool,
		// Readiness backs GET /readyz (ADR-0020 §graceful shutdown):
		// flipped to not-ready as the FIRST step of shutdown, below,
		// before anything else stops.
		Readiness: readiness,
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
		stopLookup()
		closeLocationLookup()
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown (ADR-0020 §graceful shutdown), in order:
	//
	//  1. Flip readiness to not-ready FIRST, before anything else
	//     stops — a Kubernetes readinessProbe polling /readyz needs a
	//     window to observe this and stop routing NEW traffic to this
	//     pod before step 2 below ever closes the listener, so a
	//     request racing the SIGTERM is far less likely to be routed
	//     here only to hit a closing connection.
	//  2. Stop accepting new HTTP connections and drain in-flight
	//     requests, bounded by shutdownCtx.
	//  3. Stop the facility location cache consumer's Run loop
	//     cleanly: cancel lookupCtx (no new message is fetched/handled
	//     after this) and wait, bounded by shutdownDrainTimeout, for
	//     its goroutine to actually finish rather than merely asking
	//     it to stop and moving on.
	//  4. Only THEN does the deferred closeAdapters (registered at the
	//     top of run(), so by defer's LIFO order it runs LAST of all,
	//     after this function returns and every consumer/relay
	//     goroutine has already stopped touching the pgx pool) close
	//     Postgres.
	readiness.SetNotReady()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = httpServer.Shutdown(shutdownCtx)

	// Stop the facility location cache consumer's loop cleanly: cancel
	// so no NEW message is fetched, then wait (bounded) for the Run
	// goroutine to actually return before closing its Kafka reader.
	stopLookup()
	if lookupRunDone != nil {
		select {
		case <-lookupRunDone:
		case <-time.After(shutdownDrainTimeout):
			logger.Warn("facility location cache consumer did not stop before the shutdown drain deadline")
		}
	}
	closeLocationLookup()

	return err
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
// buildAdapters' *pgxpool.Pool return value (named idempotencyPool at the
// call site) is the same pool as uow's — handed out separately, not
// derived from uow, because the idempotency middleware needs a real
// *pgxpool.Pool to begin its own transaction directly (see
// inboundhttp.Server.IdempotencyPool's doc comment) and ports.UnitOfWork
// is an interface with no way to recover the concrete pool from it. nil
// in the in-memory (no DATABASE_URL) configuration, exactly mirroring
// uow's own nil-means-unconfigured convention.
func buildAdapters(ctx context.Context, databaseURL, migrationsPath, eventPublisher string, logger *slog.Logger) (
	ports.StockRepo, ports.LocationRepo, ports.ReservationRepo, ports.ProductClassificationRepo, ports.EventPublisher, ports.UnitOfWork, *pgxpool.Pool, func(), error,
) {
	noop := func() {}

	var (
		stockRepo          ports.StockRepo
		locationRepo       ports.LocationRepo
		reservationRepo    ports.ReservationRepo
		classificationRepo ports.ProductClassificationRepo
		defaultPub         ports.EventPublisher
		uow                ports.UnitOfWork // nil (in-memory mode): atomically() runs its fn directly.
		pool               *pgxpool.Pool
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
			return nil, nil, nil, nil, nil, nil, nil, noop, err
		}

		p, err := postgres.NewPool(ctx, databaseURL)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, noop, err
		}
		pool = p
		// ParseConfig/NewWithConfig do not themselves establish a
		// connection, so without this the first-dial reset would surface
		// inside the first real request instead of at boot.
		if err := bootretry.Retry(ctx, logger, "ping database", func() error {
			return pool.Ping(ctx)
		}); err != nil {
			pool.Close()
			return nil, nil, nil, nil, nil, nil, nil, noop, err
		}

		stockRepo = postgres.NewStockRepo(pool)
		locationRepo = postgres.NewLocationRepo(pool)
		reservationRepo = postgres.NewReservationRepo(pool)
		classificationRepo = postgres.NewProductClassificationRepo(pool)
		// UnitOfWork brackets every use case's Save(s) + Publish(es) in
		// one Postgres transaction (ADR 0017). It is safe to hand out
		// even when eventPublisher="log" below: OutboxPublisher is only
		// ever constructed in the eventPublisher="kafka" branch, so a
		// non-kafka run's atomically() calls still just wrap the
		// existing repo writes (still one transaction, still fine) with
		// no outbox row involved.
		uow = postgres.NewUnitOfWork(pool)
		defaultPub = events.NewLogPublisher(logger)
		closeRepos = pool.Close
	}

	if !strings.EqualFold(eventPublisher, "kafka") {
		return stockRepo, locationRepo, reservationRepo, classificationRepo, defaultPub, uow, pool, closeRepos, nil
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	writer := kafkaadapter.NewWriter(brokers...)
	integrationPub := kafkaadapter.NewPublisher(writer, reservationRepo)
	analyticsPub := kafkaadapter.NewAnalyticsPublisher(brokers, reservationRepo, nil)

	if pool == nil {
		// In-memory repos with EVENT_PUBLISHER=kafka: no Postgres, so no
		// transactional outbox is possible — publish straight to Kafka,
		// same as before ADR 0017.
		publisher := events.NewMultiPublisher(integrationPub, analyticsPub)
		logger.Info("event publisher configured", "publisher", "kafka (direct, no outbox)",
			"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic, "brokers", brokers)
		closeAll := func() {
			if err := writer.Close(); err != nil {
				logger.Error("error closing kafka integration writer", "error", err)
			}
			if err := analyticsPub.Close(); err != nil {
				logger.Error("error closing kafka analytics writer", "error", err)
			}
		}
		return stockRepo, locationRepo, reservationRepo, classificationRepo, publisher, nil, pool, closeAll, nil
	}

	// Transactional outbox (ADR 0017): the use cases publish through
	// OutboxPublisher, which — running inside the UnitOfWork transaction
	// above — inserts one outbox_events row per (event, topic) instead of
	// calling Kafka directly, so the aggregate write and the enqueued
	// event(s) commit atomically. A background OutboxRelay then drains
	// outbox_events onto both topics via a single RelaySink, preserving
	// the same dual-topic fan-out the direct MultiPublisher performed.
	publisher := postgres.NewOutboxPublisher(pool, integrationPub, analyticsPub)
	logger.Info("event publisher configured", "publisher", "kafka (transactional outbox)",
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic, "brokers", brokers)

	relaySink := kafkaadapter.NewRelaySink(brokers)
	relayInterval := getenv("OUTBOX_RELAY_INTERVAL", "1s")
	interval, err := time.ParseDuration(relayInterval)
	if err != nil {
		logger.Warn("invalid OUTBOX_RELAY_INTERVAL, using 1s default", "value", relayInterval, "error", err)
		interval = time.Second
	}
	relay := postgres.NewOutboxRelay(pool, relaySink, postgres.WithLogger(logger), postgres.WithInterval(interval))
	relayCtx, stopRelay := context.WithCancel(context.Background())
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		// Run only ever returns nil, on ctx cancellation.
		_ = relay.Run(relayCtx)
	}()
	logger.Info("outbox relay started", "interval", interval)

	closeAll := func() {
		stopRelay()
		select {
		case <-relayDone:
		case <-time.After(shutdownDrainTimeout):
			logger.Warn("outbox relay did not stop before the shutdown drain deadline")
		}
		if err := relaySink.Close(); err != nil {
			logger.Error("error closing outbox relay sink", "error", err)
		}
		if err := writer.Close(); err != nil {
			logger.Error("error closing kafka integration writer", "error", err)
		}
		if err := analyticsPub.Close(); err != nil {
			logger.Error("error closing kafka analytics writer", "error", err)
		}
		closeRepos()
	}

	return stockRepo, locationRepo, reservationRepo, classificationRepo, publisher, uow, pool, closeAll, nil
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
//   - "http"       calls facility-layout synchronously per stow, wrapped
//     in a per-dependency circuit breaker with jittered retry
//     (ADR-0020): on a trip, calls fall back to the SAME fail-open
//     behaviour this client already had, rather than a new fallback
//     path. recorder feeds the breaker's state transitions into the
//     circuit_breaker.state gauge; nil is fine (see
//     resilience.RecordStateChange's doc comment). Requires
//     FACILITY_LAYOUT_BASE_URL. Retained as the rollback for "kafka".
//   - "permissive" (default) answers Known=false for everything.
//
// The second return value is a channel that closes once the kafka mode's
// consumer.Run goroutine has actually returned (nil in every other mode,
// since there is no such goroutine) — graceful shutdown waits on it,
// bounded, rather than firing the cancel and moving on immediately. The
// third return value (the closer) releases the Kafka reader when one was
// started, and is a no-op otherwise.
func buildLocationLookup(ctx context.Context, mode, facilityLayoutBaseURL string, brokers []string, recorder resilience.StateRecorder, logger *slog.Logger) (ports.LocationClassificationLookup, chan struct{}, func(), error) {
	switch {
	case strings.EqualFold(mode, "kafka"):
		if len(brokers) == 0 {
			return nil, nil, nil, fmt.Errorf("LOCATION_LOOKUP_MODE=kafka requires KAFKA_BROKERS to be set")
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
			return nil, nil, nil, fmt.Errorf("failed to start the Kafka-sourced location cache: %w", err)
		}
		logger.Info("location classification lookup configured",
			"mode", "kafka", "topic", facilitycache.Topic, "brokers", brokers)

		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
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
			return nil, runDone, nil, fmt.Errorf("facility location cache did not become ready within %s: %w", facilitycache.WaitReadyTimeout, err)
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
		return consumer, runDone, func() { _ = consumer.Close() }, nil

	case strings.EqualFold(mode, "http"):
		logger.Info("location classification lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL, "circuit_breaker", "enabled", "retry", "enabled")
		client := facilitylayout.NewBreakerClient(facilitylayout.NewClient(facilityLayoutBaseURL, nil), recorder)
		return client, nil, func() {}, nil

	default:
		return facilitylayout.NewPermissiveLookup(), nil, func() {}, nil
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
