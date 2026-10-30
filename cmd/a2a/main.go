// Command a2a is the composition root for the Inventory & Storage A2A agent:
// it wires env config to outbound adapters, adapters to the use cases, and
// those to the inbound A2A adapter, then serves Agent2Agent JSON-RPC over HTTP
// with the Agent Card at the well-known path. It is a third, independent
// deployable alongside cmd/inventory (the HTTP service) and cmd/mcp, per
// ADR-0048.
//
// Environment:
//
//	DATABASE_URL             REQUIRED. Postgres is the only runtime.
//	A2A_AGENT_URL            REQUIRED. Absolute http(s) URL of this agent's
//	                         JSON-RPC endpoint, advertised on the Agent Card
//	                         (in-cluster default:
//	                         http://<release>-a2a.<ns>.svc.cluster.local:8093/)
//	EVENT_PUBLISHER          optional: unset or kafka (events always leave
//	                         through the transactional outbox; the relay runs
//	                         in cmd/inventory, so this binary never dials a
//	                         broker and needs no KAFKA_BROKERS)
//	MIGRATIONS_DATABASE_URL  optional direct (non-pooled) URL for migrations
//	MIGRATIONS_PATH          optional, default "migrations"
//	A2A_ADDR                 listen address (default :8093)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inbounda2a "github.com/claudioed/inventory-storage/internal/adapters/inbound/a2a"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// telemetryFlushTimeout bounds the final export attempt on shutdown, matching
// cmd/inventory. Without a deadline the exporter would retry against an
// unreachable Collector well past what an orchestrator will wait for.
const telemetryFlushTimeout = 5 * time.Second

// a2aConfig is the mandatory part of the process environment.
type a2aConfig struct {
	databaseURL           string
	migrationsDatabaseURL string
	migrationsPath        string
	agentURL              string
}

// errNoAgentURL is the boot error for a missing A2A_AGENT_URL: the Agent Card
// must advertise the absolute URL clients dial (ADR-0048).
var errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR-0048), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract; a relative or schemeless URL is a misconfiguration
// every client would hit, so it is refused at boot, never silently served.
func checkAgentURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR-0048)", raw)
	}
	return nil
}

// loadConfig reads and validates the mandatory environment through get
// (os.Getenv in production, a map lookup in the tests). Postgres is the only
// runtime (the real-wiring ADR): DATABASE_URL is required and the error names
// it, so the process exits before telemetry, the listener or any dial.
// EVENT_PUBLISHER stays accepted (warehouse-infra sets it) but only as unset
// or kafka: A2A-initiated events always enqueue outbox rows, which
// cmd/inventory's relay delivers, so this binary never dials a broker and
// does not need KAFKA_BROKERS. A2A_AGENT_URL is mandatory and must be an
// absolute http(s) URL (the Agent Card advertises it).
func loadConfig(get func(string) string) (a2aConfig, error) {
	databaseURL := strings.TrimSpace(get("DATABASE_URL"))
	if databaseURL == "" {
		return a2aConfig{}, errors.New("DATABASE_URL is required: this server runs on a real Postgres, there is no in-memory mode")
	}
	if v := strings.ToLower(strings.TrimSpace(get("EVENT_PUBLISHER"))); v != "" && v != "kafka" {
		return a2aConfig{}, fmt.Errorf("EVENT_PUBLISHER=%s is not supported: events always leave through the transactional outbox (unset it or use kafka)", get("EVENT_PUBLISHER"))
	}
	if err := checkAgentURL(get("A2A_AGENT_URL")); err != nil {
		return a2aConfig{}, err
	}
	cfg := a2aConfig{databaseURL: databaseURL, migrationsDatabaseURL: databaseURL, migrationsPath: "migrations", agentURL: strings.TrimSpace(get("A2A_AGENT_URL"))}
	// See cmd/inventory's identical fallback and buildAdapters' doc comment
	// for the full "why" (session-scoped pg_advisory_lock vs PgBouncer
	// transaction-pooling incompatibility, ADR
	// 0023-migrations-direct-postgres-connection.md, mirroring
	// order-management's ADR-0029). This binary also runs migrations on
	// start, so it needs the same direct-connection split.
	if v := get("MIGRATIONS_DATABASE_URL"); v != "" {
		cfg.migrationsDatabaseURL = v
	}
	if v := get("MIGRATIONS_PATH"); v != "" {
		cfg.migrationsPath = v
	}
	return cfg, nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Same non-blocking telemetry setup as the HTTP service: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't start.
	serviceName := getenv("OTEL_SERVICE_NAME", "inventory-storage-a2a")
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

	httpAddr := getenv("A2A_ADDR", ":8093")

	ad, err := buildAdapters(context.Background(), cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath, logger)
	if err != nil {
		return err
	}
	defer ad.close()

	reservationMetrics, err := telemetry.NewReservationMetrics()
	if err != nil {
		return err
	}

	executor := inbounda2a.NewExecutor(buildDeps(ad, system.Clock{}, reservationMetrics))
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: cfg.agentURL, Version: getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion)})

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           newRouter(inbounda2a.Handler(executor), card, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("a2a agent listening (JSON-RPC over HTTP)", "addr", httpAddr)
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

// adapters is what buildAdapters wires: the repos and event publisher the
// A2A skills' use cases run on, the UnitOfWork bracketing their writes, and
// the shutdown hook releasing every resource opened along the way.
type adapters struct {
	stock        ports.StockRepo
	reservations ports.ReservationRepo
	// locations and binLevels back the BinStockLevelChanged a revocation
	// raises (ADR 0040): site resolution and the per-(bin, sku) version.
	locations ports.LocationRepo
	binLevels ports.BinLevelLedger
	publisher ports.EventPublisher
	// uow brackets every write use case's Save + Publish in one Postgres
	// transaction (ADR-0017).
	uow   ports.UnitOfWork
	close func()
}

// levelEmitter builds the BinStockLevelChanged emitter over the FINAL
// publisher, or nil when the adapters carry no level ledger.
func (a adapters) levelEmitter(clock ports.Clock) *usecases.BinLevelEmitter {
	if a.binLevels == nil || a.locations == nil {
		return nil
	}
	return &usecases.BinLevelEmitter{Ledger: a.binLevels, Locations: a.locations, Events: a.publisher, Clock: clock}
}

// buildDeps wires the A2A skills' use cases over ad. RevokeReservation gets
// the SAME collaborators cmd/inventory gives it — publisher, UnitOfWork and
// reservation metrics — so an A2A-initiated revocation is as observable and
// as durable as a REST DELETE /reservations/{id} (ADR-0017).
func buildDeps(ad adapters, clock ports.Clock, metrics ports.ReservationMetrics) inbounda2a.Deps {
	return inbounda2a.Deps{
		GetUsable: &usecases.GetUsable{Stock: ad.stock},
		RevokeReservation: &usecases.RevokeReservation{
			Stock: ad.stock, Reservations: ad.reservations, Events: ad.publisher,
			Clock: clock, Metrics: metrics, UnitOfWork: ad.uow, Levels: ad.levelEmitter(clock),
		},
	}
}

// buildAdapters wires the Postgres repos: Postgres is the only runtime (the
// real-wiring ADR). Events are enqueued by the transactional outbox
// (ADR-0017): a UnitOfWork brackets each use case's Save + Publish, and
// postgres.OutboxPublisher inserts one outbox_events row per (event, topic) —
// integration AND analytics — in that same transaction. This binary does NOT
// run the relay: cmd/inventory's OutboxRelay drains the shared outbox_events
// table onto Kafka, so A2A-initiated events reach warehouse.inventory.events /
// warehouse.inventory.analytics without a second relay (and without this
// process needing a broker).
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below,
// mirroring cmd/inventory/main.go's openPostgresPool exactly — see its doc
// comment for the full "why" a direct, non-pooled connection is needed
// here even though the pgxpool opened just after (databaseURL) stays on
// PgBouncer.
func buildAdapters(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (adapters, error) {
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
	// The outbox only ENCODES: both encoders are built without a Kafka
	// writer because delivery is cmd/inventory's relay's job. Their direct
	// Publish methods are never called on this path.
	publisher := postgres.NewOutboxPublisher(pool,
		kafkaadapter.NewPublisher(nil, reservationRepo),
		&kafkaadapter.AnalyticsPublisher{Reservations: reservationRepo},
	)
	logger.Info("event publisher configured", "publisher", "kafka (transactional outbox, relay runs in cmd/inventory)",
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic)
	return adapters{
		stock: stockRepo, reservations: reservationRepo,
		locations: postgres.NewLocationRepo(pool), binLevels: postgres.NewBinLevelLedger(pool),
		publisher: publisher,
		uow:       postgres.NewUnitOfWork(pool),
		close:     pool.Close,
	}, nil
}

// newLogger builds the process-wide structured logger, mirroring
// cmd/inventory: LOG_LEVEL debug|info|warn|warning|error (case-insensitive,
// default info), JSON to stdout, wrapped so a record written with a
// span-carrying context also carries trace_id/span_id.
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
