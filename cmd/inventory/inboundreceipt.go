package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
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
// cfg.inboundReceiptConsumerGroup is INBOUND_RECEIPT_CONSUMER_GROUP. Unset
// means the consumer is not started (receipts then only arrive over REST).
// Set, it requires DATABASE_URL (the claim + receive are only atomic through
// the Postgres UnitOfWork) and KAFKA_BROKERS; a misconfiguration fails boot
// with the real cause instead of silently skipping the consumer.
//
// ctx is the shared inbound-consumer context (see run()), so shutdown's
// stopLookup cancel unblocks this loop too.
func buildInboundReceiptConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
) (transferConsumerHandle, error) {
	if cfg.inboundReceiptConsumerGroup == "" {
		logger.Info("inbound-receiving receipt consumer not started", "reason", "INBOUND_RECEIPT_CONSUMER_GROUP unset")
		return noopTransferConsumer(), nil
	}
	if cfg.databaseURL == "" || set.pool == nil {
		return noopTransferConsumer(), errors.New("INBOUND_RECEIPT_CONSUMER_GROUP requires DATABASE_URL (the receipt is only atomic with its dedupe claim through the Postgres unit of work)")
	}
	if len(cfg.kafkaBrokers) == 0 {
		return noopTransferConsumer(), errors.New("INBOUND_RECEIPT_CONSUMER_GROUP requires KAFKA_BROKERS to be set")
	}
	if !strings.EqualFold(cfg.eventPublisher, "kafka") {
		logger.Warn("inbound-receiving receipts are booked but StockReceived will not leave the service",
			"reason", "EVENT_PUBLISHER is not kafka")
	}

	metrics, err := telemetry.NewInboundReceiptMetrics()
	if err != nil {
		return noopTransferConsumer(), err
	}
	book := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: set.publisher, Clock: memory.SystemClock{}, UnitOfWork: set.uow},
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
