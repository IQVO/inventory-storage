package main

import (
	"context"
	"errors"
	"log/slog"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// buildInboundReceiptConsumer wires the inbound-receiving handover consumer
// (ADR 0037, inbound-receiving ADR 0003): it consumes ReceiptLineReceived on
// warehouse.inbound-receiving.events and books every Good line as a staged
// receipt through the existing ReceiveStock use case (the same effect as
// POST /stock/receive), claiming each CloudEvents id in processed_events in
// the SAME transaction as the StockReceived outbox row. Damaged lines are
// counted, not booked.
//
// The consumer ALWAYS runs: Postgres and Kafka are the only runtime (the
// real-wiring ADR). loadConfig already refused a missing DATABASE_URL,
// KAFKA_BROKERS or INBOUND_RECEIPT_CONSUMER_GROUP (cfg.inboundReceiptConsumerGroup
// is INBOUND_RECEIPT_CONSUMER_GROUP), and StockReceived leaves the service
// through the always-on outbox relay.
//
// ctx is the shared inbound-consumer context (see run()), so shutdown's
// stopLookup cancel unblocks this loop too.
func buildInboundReceiptConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
) (transferConsumerHandle, error) {
	metrics, err := telemetry.NewInboundReceiptMetrics()
	if err != nil {
		return noopTransferConsumer(), err
	}
	book := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: set.publisher, Clock: system.Clock{}, UnitOfWork: set.uow},
		ProcessedEvents: postgres.NewProcessedEventRepo(set.pool),
		UnitOfWork:      set.uow,
		Metrics:         metrics,
	}
	consumer := inboundkafka.NewInboundReceiptConsumer(cfg.kafkaBrokers, cfg.inboundReceiptConsumerGroup, book, logger)
	logger.Info("inbound-receiving receipt consumer configured",
		"topic", inboundkafka.InboundReceivingTopic, "consumer_group", cfg.inboundReceiptConsumerGroup)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("inbound-receiving receipt consumer stopped", "error", err)
		}
	}()

	return transferConsumerHandle{
		stop: func() {},
		wait: func() { <-runDone },
		close: func() {
			if err := consumer.Close(); err != nil {
				logger.Error("error closing inbound-receiving receipt consumer", "error", err)
			}
		},
	}, nil
}
