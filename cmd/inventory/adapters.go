package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
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
// the concrete pool from it. It is never nil: Postgres is the only runtime.
type adapterSet struct {
	stock           ports.StockRepo
	locations       ports.LocationRepo
	reservations    ports.ReservationRepo
	classifications ports.ProductClassificationRepo
	publisher       ports.EventPublisher
	uow             ports.UnitOfWork
	pool            *pgxpool.Pool
	close           func()
	// transfers is the transfer_allocations ledger repo. Shared by the
	// allocation consumer and the destination receipt path (ADR 0031).
	transfers ports.TransferAllocationRepo
	// transferReceipts is the destination transfer_receipts repo (ADR 0031).
	transferReceipts ports.TransferReceiptRepo
	// inventoryExceptions is the quarantine repo (ADR 0031).
	inventoryExceptions ports.InventoryExceptionRepo
	// relocations is the intra-site relocation record repo (ADR 0039).
	relocations ports.RelocationRepo
	// binLevels advances and reads the per-(bin, sku) level behind
	// BinStockLevelChanged (ADR 0040).
	binLevels ports.BinLevelLedger
}

// levelEmitter builds the BinStockLevelChanged emitter over the adapter set's
// FINAL publisher (the outbox publisher), so every level event leaves through
// the same path as the other domain events. Call it only after buildAdapters
// has returned.
func (a adapterSet) levelEmitter(clock ports.Clock) *usecases.BinLevelEmitter {
	if a.binLevels == nil {
		return nil
	}
	return &usecases.BinLevelEmitter{Ledger: a.binLevels, Locations: a.locations, Events: a.publisher, Clock: clock}
}

// buildAdapters wires the Postgres adapters and the Kafka publishing path:
// Postgres and Kafka are the only runtime (the real-wiring ADR). Events are
// enqueued into the transactional outbox (ADR 0017) in the same transaction
// as the aggregate write and a background relay drains the outbox onto the
// integration and analytics topics. On error the returned set carries a
// no-op closer.
func buildAdapters(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, brokers []string, logger *slog.Logger) (adapterSet, error) {
	set, err := buildRepos(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return adapterSet{close: func() {}}, err
	}
	return withOutboxKafka(set, newKafkaPublishers(brokers, set), logger), nil
}

// buildRepos builds the Postgres-backed repository set and unit of work.
// The publisher is set by withOutboxKafka.
func buildRepos(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (adapterSet, error) {
	pool, err := openPostgresPool(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return adapterSet{}, err
	}
	return adapterSet{
		stock:               postgres.NewStockRepo(pool),
		locations:           postgres.NewLocationRepo(pool),
		reservations:        postgres.NewReservationRepo(pool),
		classifications:     postgres.NewProductClassificationRepo(pool),
		transfers:           postgres.NewTransferAllocationRepo(pool),
		transferReceipts:    postgres.NewTransferReceiptRepo(pool),
		inventoryExceptions: postgres.NewInventoryExceptionRepo(pool),
		relocations:         postgres.NewRelocationRepo(pool),
		binLevels:           postgres.NewBinLevelLedger(pool),
		// UnitOfWork brackets every use case's Save(s) + Publish(es) in
		// one Postgres transaction (ADR 0017).
		uow:   postgres.NewUnitOfWork(pool),
		pool:  pool,
		close: closeWithSweeper(pool, logger),
	}, nil
}

// kafkaPublishers groups the Kafka-side adapters of the outbox path.
type kafkaPublishers struct {
	brokers     []string
	writer      *kafkago.Writer
	integration *kafkaadapter.Publisher
	analytics   *kafkaadapter.AnalyticsPublisher
}

func newKafkaPublishers(brokers []string, set adapterSet) kafkaPublishers {
	writer := kafkaadapter.NewWriter(brokers...)
	return kafkaPublishers{
		brokers:     brokers,
		writer:      writer,
		integration: kafkaadapter.NewPublisher(writer, set.reservations),
		analytics:   kafkaadapter.NewAnalyticsPublisher(brokers, set.reservations, nil),
	}
}

func (k kafkaPublishers) closeWriters(logger *slog.Logger) {
	closeKafkaWriters(k.writer, k.analytics, logger)
}

func (k kafkaPublishers) logConfigured(logger *slog.Logger, mode string) {
	logger.Info("event publisher configured", "publisher", mode,
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic, "brokers", k.brokers)
}

// withOutboxKafka (ADR 0017): the use cases publish through
// OutboxPublisher, which — running inside the UnitOfWork transaction —
// inserts one outbox_events row per (event, topic) instead of calling Kafka
// directly, so the aggregate write and the enqueued event(s) commit
// atomically. A background OutboxRelay then drains outbox_events onto both
// topics via a single RelaySink.
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
	maxAttempts := outboxMaxAttempts(logger)
	relay := postgres.NewOutboxRelay(pool, relaySink, postgres.WithLogger(logger), postgres.WithInterval(interval),
		postgres.WithMaxAttempts(maxAttempts))
	relayCtx, stopRelay := context.WithCancel(context.Background())
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		// Run only ever returns nil, on ctx cancellation.
		_ = relay.Run(relayCtx)
	}()
	logger.Info("outbox relay started", "interval", interval, "max_attempts", maxAttempts)

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
