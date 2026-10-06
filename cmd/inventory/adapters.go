package main

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/ports"
)

// shutdownDrainTimeout bounds how long graceful shutdown (ADR-0020
// §graceful shutdown) waits for the outbox relay, the housekeeping sweeper
// and the facility location cache consumer's Run loop to actually stop,
// after their contexts are cancelled — mirroring the HTTP server's own
// Shutdown budget. A relay/consumer that does not stop within this window
// is logged and shutdown proceeds anyway; the process is exiting either way
// and this is strictly better than hanging past the orchestrator's own
// terminationGracePeriodSeconds.
const shutdownDrainTimeout = 10 * time.Second

// adapterSet is everything buildAdapters assembles: the outbound ports the
// use cases need, plus the closer that releases them.
//
// pool is the same pool as uow's — handed out separately, not derived from
// uow, because the idempotency middleware needs a real *pgxpool.Pool to
// begin its own transaction directly (see inboundhttp.Server.IdempotencyPool's
// doc comment) and ports.UnitOfWork is an interface with no way to recover
// the concrete pool from it. nil in the in-memory (no DATABASE_URL)
// configuration, exactly mirroring uow's own nil-means-unconfigured
// convention (atomically() then runs its fn directly).
type adapterSet struct {
	stock           ports.StockRepo
	locations       ports.LocationRepo
	reservations    ports.ReservationRepo
	classifications ports.ProductClassificationRepo
	publisher       ports.EventPublisher
	uow             ports.UnitOfWork
	pool            *pgxpool.Pool
	close           func()
}

// buildAdapters wires the Postgres adapters when DATABASE_URL is set, or
// falls back to the in-memory adapters for local development without a
// database. The event publisher defaults to that same memory/Postgres
// choice ("log"), or can be switched to the Kafka integration-events
// publisher via eventPublisher="kafka" (EVENT_PUBLISHER env), independent of
// which repos are in use. On error the returned set carries a no-op closer.
func buildAdapters(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath, eventPublisher string, logger *slog.Logger) (adapterSet, error) {
	set, err := buildRepos(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return adapterSet{close: func() {}}, err
	}
	if !strings.EqualFold(eventPublisher, "kafka") {
		return set, nil
	}
	return withKafkaPublishing(set, logger), nil
}

// buildRepos selects the repository set: Postgres-backed when databaseURL
// is set, in-memory otherwise. Either way the publisher is the log
// publisher; withKafkaPublishing replaces it on request.
func buildRepos(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (adapterSet, error) {
	if databaseURL == "" {
		return memoryAdapters(logger), nil
	}
	pool, err := openPostgresPool(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return adapterSet{}, err
	}
	return adapterSet{
		stock:           postgres.NewStockRepo(pool),
		locations:       postgres.NewLocationRepo(pool),
		reservations:    postgres.NewReservationRepo(pool),
		classifications: postgres.NewProductClassificationRepo(pool),
		publisher:       events.NewLogPublisher(logger),
		// UnitOfWork brackets every use case's Save(s) + Publish(es) in
		// one Postgres transaction (ADR 0017). It is safe to hand out
		// even when eventPublisher="log": OutboxPublisher is only ever
		// constructed on the kafka path, so a non-kafka run's
		// atomically() calls still just wrap the existing repo writes
		// (still one transaction, still fine) with no outbox row involved.
		uow:   postgres.NewUnitOfWork(pool),
		pool:  pool,
		close: closeWithSweeper(pool, logger),
	}, nil
}

// memoryAdapters builds the in-memory outbound set for local runs
// without a database.
func memoryAdapters(logger *slog.Logger) adapterSet {
	logger.Info("database url not configured; using in-memory adapters")
	return adapterSet{
		stock:           memory.NewStockRepo(),
		locations:       memory.NewLocationRepo(),
		reservations:    memory.NewReservationRepo(),
		classifications: memory.NewProductClassificationRepo(),
		publisher:       events.NewLogPublisher(logger),
		close:           func() {},
	}
}

// kafkaPublishers groups the Kafka-side adapters shared by the direct and
// outbox publishing paths.
type kafkaPublishers struct {
	brokers     []string
	writer      *kafkago.Writer
	integration *kafkaadapter.Publisher
	analytics   *kafkaadapter.AnalyticsPublisher
}

func (k kafkaPublishers) closeWriters(logger *slog.Logger) {
	closeKafkaWriters(k.writer, k.analytics, logger)
}

func (k kafkaPublishers) logConfigured(logger *slog.Logger, mode string) {
	logger.Info("event publisher configured", "publisher", mode,
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic, "brokers", k.brokers)
}

// withKafkaPublishing switches set's publisher to the Kafka integration +
// analytics fan-out: direct when there is no Postgres, transactional outbox
// (ADR 0017) when there is.
func withKafkaPublishing(set adapterSet, logger *slog.Logger) adapterSet {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	writer := kafkaadapter.NewWriter(brokers...)
	k := kafkaPublishers{
		brokers:     brokers,
		writer:      writer,
		integration: kafkaadapter.NewPublisher(writer, set.reservations),
		analytics:   kafkaadapter.NewAnalyticsPublisher(brokers, set.reservations, nil),
	}
	if set.pool == nil {
		return withDirectKafka(set, k, logger)
	}
	return withOutboxKafka(set, k, logger)
}

// withDirectKafka: in-memory repos with EVENT_PUBLISHER=kafka have no
// Postgres, so no transactional outbox is possible — publish straight to
// Kafka, same as before ADR 0017.
func withDirectKafka(set adapterSet, k kafkaPublishers, logger *slog.Logger) adapterSet {
	set.publisher = events.NewMultiPublisher(k.integration, k.analytics)
	set.uow = nil
	set.close = func() { k.closeWriters(logger) }
	k.logConfigured(logger, "kafka (direct, no outbox)")
	return set
}

// withOutboxKafka (ADR 0017): the use cases publish through
// OutboxPublisher, which — running inside the UnitOfWork transaction —
// inserts one outbox_events row per (event, topic) instead of calling Kafka
// directly, so the aggregate write and the enqueued event(s) commit
// atomically. A background OutboxRelay then drains outbox_events onto both
// topics via a single RelaySink, preserving the same dual-topic fan-out the
// direct MultiPublisher performed.
func withOutboxKafka(set adapterSet, k kafkaPublishers, logger *slog.Logger) adapterSet {
	set.publisher = postgres.NewOutboxPublisher(set.pool, k.integration, k.analytics)
	k.logConfigured(logger, "kafka (transactional outbox)")

	stopRelay := startOutboxRelay(set.pool, k.brokers, logger)
	closeRepos := set.close
	set.close = func() {
		stopRelay()
		k.closeWriters(logger)
		closeRepos()
	}
	return set
}

// startOutboxRelay runs the outbox relay in a goroutine and returns the
// stop func: cancel, wait (bounded by shutdownDrainTimeout) for the relay
// to return, then close its sink.
func startOutboxRelay(pool *pgxpool.Pool, brokers []string, logger *slog.Logger) func() {
	relaySink := kafkaadapter.NewRelaySink(brokers)
	interval := outboxRelayInterval(logger)
	relay := postgres.NewOutboxRelay(pool, relaySink, postgres.WithLogger(logger), postgres.WithInterval(interval))
	relayCtx, stopRelay := context.WithCancel(context.Background())
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		// Run only ever returns nil, on ctx cancellation.
		_ = relay.Run(relayCtx)
	}()
	logger.Info("outbox relay started", "interval", interval)

	return func() {
		stopRelay()
		select {
		case <-relayDone:
		case <-time.After(shutdownDrainTimeout):
			logger.Warn("outbox relay did not stop before the shutdown drain deadline")
		}
		if err := relaySink.Close(); err != nil {
			logger.Error("error closing outbox relay sink", "error", err)
		}
	}
}

// closeKafkaWriters closes the integration and analytics Kafka writers,
// logging (not failing) on error — shutdown-time cleanup only.
func closeKafkaWriters(writer *kafkago.Writer, analyticsPub *kafkaadapter.AnalyticsPublisher, logger *slog.Logger) {
	if err := writer.Close(); err != nil {
		logger.Error("error closing kafka integration writer", "error", err)
	}
	if err := analyticsPub.Close(); err != nil {
		logger.Error("error closing kafka analytics writer", "error", err)
	}
}
