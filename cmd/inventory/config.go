package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

// config is the process's env-derived startup configuration. Reading it in
// one place keeps every env-var name and default next to the others;
// semantics (which variable, which default, empty == unset) are unchanged.
type config struct {
	logLevel       string
	serviceName    string
	serviceVersion string
	otlpEndpoint   string
	httpAddr       string
	databaseURL    string
	// migrationsDatabaseURL, when MIGRATIONS_DATABASE_URL is set, is a
	// DIRECT (non-pooled, session-mode) Postgres connection string used
	// ONLY for the golang-migrate startup step — everything else (the
	// pgxpool this process serves requests through) keeps using
	// databaseURL unchanged. See openPostgresPool's doc comment for the
	// full "why": golang-migrate's postgres driver takes a session-scoped
	// `SELECT pg_advisory_lock($1)` to serialize concurrent migration
	// runs, which PgBouncer's transaction-pooling mode does not support
	// (warehouse-infra's PgBouncer rollout, PR #43; this fallback closes
	// the fleet-wide bug that rollout introduced — see ADR
	// 0023-migrations-direct-postgres-connection.md, mirroring
	// order-management's ADR-0029). Falls back to databaseURL when unset,
	// which is every environment that doesn't provision the split (local
	// dev, CI integration tests, and any cluster whose Terraform predates
	// this fix).
	migrationsDatabaseURL string
	migrationsPath        string
	eventPublisher        string

	locationLookupMode    string
	facilityLayoutBaseURL string
	kafkaBrokers          []string // KAFKA_BROKERS split on ","; nil when unset

	// transferConsumerMode is TRANSFER_ALLOCATION_CONSUMER_MODE: "off"
	// (default) or "kafka" — see buildTransferAllocationConsumer.
	transferConsumerMode string
	// transferConsumerGroup is TRANSFER_ALLOCATION_CONSUMER_GROUP; empty
	// means inboundkafka.DefaultConsumerGroup.
	transferConsumerGroup string

	// productMasterConsumerGroup is PRODUCT_MASTER_CONSUMER_GROUP: the
	// stable consumer group of the product-master classification consumer
	// (ADR 0034). Empty means the consumer is not started.
	productMasterConsumerGroup string

	// inboundReceiptConsumerGroup is INBOUND_RECEIPT_CONSUMER_GROUP: the
	// stable consumer group of the inbound-receiving receipt consumer
	// (ADR 0037). Empty means the consumer is not started.
	inboundReceiptConsumerGroup string

	// taskCompletedConsumerMode is TASK_COMPLETED_CONSUMER_MODE: "off"
	// (default) or "kafka" — see buildTaskCompletedConsumer (ADR 0035).
	taskCompletedConsumerMode string
	// taskCompletedConsumerGroup is TASK_COMPLETED_CONSUMER_GROUP; empty
	// means inboundkafka.DefaultTaskCompletedConsumerGroup.
	taskCompletedConsumerGroup string
}

// loadConfig reads the process environment.
func loadConfig() config {
	databaseURL := os.Getenv("DATABASE_URL")
	cfg := config{
		logLevel:              getenv("LOG_LEVEL", "info"),
		serviceName:           getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName),
		serviceVersion:        getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		otlpEndpoint:          getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint),
		httpAddr:              getenv("HTTP_ADDR", ":8080"),
		databaseURL:           databaseURL,
		migrationsDatabaseURL: getenv("MIGRATIONS_DATABASE_URL", databaseURL),
		migrationsPath:        getenv("MIGRATIONS_PATH", "migrations"),
		eventPublisher:        getenv("EVENT_PUBLISHER", "log"),
		locationLookupMode:    getenv("LOCATION_LOOKUP_MODE", "permissive"),
		facilityLayoutBaseURL: os.Getenv("FACILITY_LAYOUT_BASE_URL"),
		transferConsumerMode:  getenv("TRANSFER_ALLOCATION_CONSUMER_MODE", "off"),
		transferConsumerGroup: os.Getenv("TRANSFER_ALLOCATION_CONSUMER_GROUP"),

		productMasterConsumerGroup: os.Getenv("PRODUCT_MASTER_CONSUMER_GROUP"),

		inboundReceiptConsumerGroup: os.Getenv("INBOUND_RECEIPT_CONSUMER_GROUP"),

		taskCompletedConsumerMode:  getenv("TASK_COMPLETED_CONSUMER_MODE", "off"),
		taskCompletedConsumerGroup: os.Getenv("TASK_COMPLETED_CONSUMER_GROUP"),
	}
	if raw := os.Getenv("KAFKA_BROKERS"); raw != "" {
		cfg.kafkaBrokers = strings.Split(raw, ",")
	}
	return cfg
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

// outboxRelayInterval parses OUTBOX_RELAY_INTERVAL, falling back to 1s
// with a warning on an invalid value.
func outboxRelayInterval(logger *slog.Logger) time.Duration {
	raw := getenv("OUTBOX_RELAY_INTERVAL", "1s")
	interval, err := time.ParseDuration(raw)
	if err != nil {
		logger.Warn("invalid OUTBOX_RELAY_INTERVAL, using 1s default", "value", raw, "error", err)
		return time.Second
	}
	return interval
}

// envDuration parses a Go duration env var. Unset yields def; an invalid or
// negative value logs a warning and yields def. "0" is valid and means
// "disabled" to the caller.
func envDuration(logger *slog.Logger, key string, def time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("invalid duration env var, using default", "key", key, "value", raw, "default", def.String(), "error", err)
		return def
	}
	return d
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
