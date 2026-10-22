package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
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

	// locationLookupMode is LOCATION_LOOKUP_MODE: "kafka" or "http" (the
	// real-wiring ADR removed "permissive" and the unset default).
	locationLookupMode    string
	facilityLayoutBaseURL string
	kafkaBrokers          []string // KAFKA_BROKERS split on ","; never empty after loadConfig

	// The five consumer-group ids are mandatory: every consumer always runs
	// under a stable group so a restarted pod resumes from its offsets.
	// transferConsumerGroup is TRANSFER_ALLOCATION_CONSUMER_GROUP.
	transferConsumerGroup string

	// productMasterConsumerGroup is PRODUCT_MASTER_CONSUMER_GROUP: the
	// stable consumer group of the product-master classification consumer
	// (ADR 0034).
	productMasterConsumerGroup string

	// inboundReceiptConsumerGroup is INBOUND_RECEIPT_CONSUMER_GROUP: the
	// stable consumer group of the inbound-receiving receipt consumer
	// (ADR 0037).
	inboundReceiptConsumerGroup string

	// returnLineConsumerGroup is RETURN_LINE_CONSUMER_GROUP: the stable
	// consumer group of the returns-processing return-line consumer
	// (ADR 0044).
	returnLineConsumerGroup string

	// taskCompletedConsumerGroup is TASK_COMPLETED_CONSUMER_GROUP: the
	// stable consumer group of the TaskCompleted confirm-pick consumer
	// (ADR 0035).
	taskCompletedConsumerGroup string
}

// loadConfig reads and validates the process environment through get
// (os.Getenv in production, a map lookup in the tests). Postgres and Kafka
// are the only runtime (the real-wiring ADR): DATABASE_URL, KAFKA_BROKERS,
// LOCATION_LOOKUP_MODE and the five consumer-group ids are mandatory, and
// every problem is an error that names the variable, so the process exits
// before telemetry, the listener or any dial. EVENT_PUBLISHER and the two
// *_CONSUMER_MODE variables stay accepted (warehouse-infra sets them) but only
// as unset or kafka.
func loadConfig(get func(string) string) (config, error) {
	or := func(key, fallback string) string {
		if v := get(key); v != "" {
			return v
		}
		return fallback
	}
	databaseURL := strings.TrimSpace(get("DATABASE_URL"))
	cfg := config{
		logLevel:              or("LOG_LEVEL", "info"),
		serviceName:           or("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName),
		serviceVersion:        or("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		otlpEndpoint:          or("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint),
		httpAddr:              or("HTTP_ADDR", ":8080"),
		databaseURL:           databaseURL,
		migrationsDatabaseURL: or("MIGRATIONS_DATABASE_URL", databaseURL),
		migrationsPath:        or("MIGRATIONS_PATH", "migrations"),
		facilityLayoutBaseURL: get("FACILITY_LAYOUT_BASE_URL"),
	}
	if databaseURL == "" {
		return config{}, errors.New("DATABASE_URL is required: this context runs on a real Postgres, there is no in-memory mode")
	}
	cfg.kafkaBrokers = splitBrokers(get("KAFKA_BROKERS"))
	if len(cfg.kafkaBrokers) == 0 {
		return config{}, errors.New("KAFKA_BROKERS is required: this context runs on a real Kafka (comma-separated host:port list), there is no localhost default")
	}
	if err := onlyUnsetOrKafka("EVENT_PUBLISHER", get("EVENT_PUBLISHER"), "events always leave through the transactional outbox"); err != nil {
		return config{}, err
	}
	if err := onlyUnsetOrKafka("TRANSFER_ALLOCATION_CONSUMER_MODE", get("TRANSFER_ALLOCATION_CONSUMER_MODE"), "the transfer allocation consumer always runs"); err != nil {
		return config{}, err
	}
	if err := onlyUnsetOrKafka("TASK_COMPLETED_CONSUMER_MODE", get("TASK_COMPLETED_CONSUMER_MODE"), "the TaskCompleted confirm-pick consumer always runs"); err != nil {
		return config{}, err
	}
	mode := strings.ToLower(strings.TrimSpace(get("LOCATION_LOOKUP_MODE")))
	if mode != "kafka" && mode != "http" {
		return config{}, fmt.Errorf("LOCATION_LOOKUP_MODE=%q is not supported: set kafka (facility-events cache) or http (facility-layout REST); permissive and unset were removed because they validate no location at all", get("LOCATION_LOOKUP_MODE"))
	}
	cfg.locationLookupMode = mode
	for _, g := range []struct {
		env string
		dst *string
	}{
		{"TRANSFER_ALLOCATION_CONSUMER_GROUP", &cfg.transferConsumerGroup},
		{"PRODUCT_MASTER_CONSUMER_GROUP", &cfg.productMasterConsumerGroup},
		{"INBOUND_RECEIPT_CONSUMER_GROUP", &cfg.inboundReceiptConsumerGroup},
		{"RETURN_LINE_CONSUMER_GROUP", &cfg.returnLineConsumerGroup},
		{"TASK_COMPLETED_CONSUMER_GROUP", &cfg.taskCompletedConsumerGroup},
	} {
		v := strings.TrimSpace(get(g.env))
		if v == "" {
			return config{}, fmt.Errorf("%s is required: the consumer always runs and needs a STABLE consumer group id (a restarted pod resumes from its committed offsets)", g.env)
		}
		*g.dst = v
	}
	return cfg, nil
}

// splitBrokers splits a comma-separated broker list, dropping blanks.
func splitBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// onlyUnsetOrKafka keeps a legacy variable accepted (warehouse-infra still
// sets it) but only with the value it can have now: unset or kafka.
func onlyUnsetOrKafka(env, raw, why string) error {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" || v == "kafka" {
		return nil
	}
	return fmt.Errorf("%s=%s is not supported: %s (unset it or use kafka)", env, raw, why)
}

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|warning|error (case-insensitive) to the matching
// slog.Level, defaulting to Info for unset or unrecognized values. Logs are
// emitted as JSON to stdout, wrapped so that any record written with a
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

// outboxMaxAttempts parses OUTBOX_MAX_ATTEMPTS (ADR 0038): how many failed
// sends exclude an outbox row from the relay's claims. Unset means
// postgres.DefaultOutboxMaxAttempts (20); 0 means unlimited (the pre-ADR
// behaviour, where one poison row blocks the relay forever).
func outboxMaxAttempts(logger *slog.Logger) int {
	return envInt(logger, "OUTBOX_MAX_ATTEMPTS", postgres.DefaultOutboxMaxAttempts)
}

// envInt parses a non-negative integer env var. Unset yields def; an invalid
// or negative value logs a warning and yields def. "0" is valid and its
// meaning is the caller's.
func envInt(logger *slog.Logger, key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		logger.Warn("invalid integer env var, using default", "key", key, "value", raw, "default", def, "error", err)
		return def
	}
	return n
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
