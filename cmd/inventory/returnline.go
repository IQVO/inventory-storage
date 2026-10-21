package main

import (
	"context"
	"errors"
	"log/slog"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// buildReturnLineConsumer wires the returns-processing handover
// consumer (ADR 0044, returns-processing ADR 0001): it consumes
// DispositionDecided's RESTOCK follow-up ReturnLineStaged on
// warehouse.returns-processing.events and books every line as a staged
// receipt through the existing ReceiveStock use case — the SAME seam an
// inbound-receiving receipt line drives (the same effect as
// POST /stock/receive) — claiming each CloudEvents id in
// processed_events (consumer return-line-staged, the flow's provenance
// record) in the SAME transaction as the StockReceived outbox row.
//
// The consumer ALWAYS runs: Postgres and Kafka are the only runtime (the
// real-wiring ADR). loadConfig already refused a missing DATABASE_URL,
// KAFKA_BROKERS or RETURN_LINE_CONSUMER_GROUP (cfg.returnLineConsumerGroup
// is RETURN_LINE_CONSUMER_GROUP), and StockReceived leaves the service
// through the always-on outbox relay.
//
// ctx is the shared inbound-consumer context (see run()), so shutdown's
// stopLookup cancel unblocks this loop too.
func buildReturnLineConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
) (transferConsumerHandle, error) {
	book := &usecases.BookReturnLine{
		Receive:         &usecases.ReceiveStock{Events: set.publisher, Clock: system.Clock{}, UnitOfWork: set.uow},
		ProcessedEvents: postgres.NewProcessedEventRepo(set.pool),
		UnitOfWork:      set.uow,
	}
	consumer := inboundkafka.NewReturnLineConsumer(cfg.kafkaBrokers, cfg.returnLineConsumerGroup, book, logger)
	logger.Info("returns-processing return-line consumer configured",
		"topic", inboundkafka.ReturnsProcessingTopic, "consumer_group", cfg.returnLineConsumerGroup)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			workerDied(ctx, logger, "returns-processing return-line consumer", err)
		}
	}()

	return transferConsumerHandle{
		stop: func() {},
		wait: func() { <-runDone },
		close: func() {
			if err := consumer.Close(); err != nil {
				logger.Error("error closing returns-processing return-line consumer", "error", err)
			}
		},
	}, nil
}
