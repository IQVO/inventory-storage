package main

import (
	"context"
	"errors"
	"log/slog"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// buildProductMasterConsumer wires the product-master classification
// consumer (ADR 0033, product-master ADR 0003 stage C): it consumes
// product-master's ProductClassified on warehouse.product-master.events and
// keeps product_classifications (the local copy StowStock reads) in step,
// claiming each CloudEvents id in processed_events in the SAME transaction
// as the version-guarded upsert.
//
// cfg.productMasterConsumerGroup is PRODUCT_MASTER_CONSUMER_GROUP. Unset
// means the consumer is not started (the local copy then only holds legacy
// rows). Set, it requires DATABASE_URL (the claim + upsert are only atomic
// through the Postgres UnitOfWork) and KAFKA_BROKERS; a misconfiguration
// fails boot with the real cause instead of silently skipping the consumer.
//
// ctx is the shared inbound-consumer context (see run()), so shutdown's
// stopLookup cancel unblocks this loop too.
func buildProductMasterConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
) (transferConsumerHandle, error) {
	if cfg.productMasterConsumerGroup == "" {
		logger.Info("product-master classification consumer not started", "reason", "PRODUCT_MASTER_CONSUMER_GROUP unset")
		return noopTransferConsumer(), nil
	}
	if cfg.databaseURL == "" || set.pool == nil {
		return noopTransferConsumer(), errors.New("PRODUCT_MASTER_CONSUMER_GROUP requires DATABASE_URL (the local copy is only atomic through the Postgres unit of work)")
	}
	if len(cfg.kafkaBrokers) == 0 {
		return noopTransferConsumer(), errors.New("PRODUCT_MASTER_CONSUMER_GROUP requires KAFKA_BROKERS to be set")
	}

	apply := &usecases.ApplyProductClassification{
		Classifications: postgres.NewProductClassificationRepo(set.pool),
		ProcessedEvents: postgres.NewProcessedEventRepo(set.pool),
		UnitOfWork:      set.uow,
	}
	consumer := inboundkafka.NewProductMasterConsumer(cfg.kafkaBrokers, cfg.productMasterConsumerGroup, apply, logger)
	logger.Info("product-master classification consumer configured",
		"topic", inboundkafka.ProductMasterTopic, "consumer_group", cfg.productMasterConsumerGroup)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("product-master classification consumer stopped", "error", err)
		}
	}()

	return transferConsumerHandle{
		stop: func() {},
		wait: func() { <-runDone },
		close: func() {
			if err := consumer.Close(); err != nil {
				logger.Error("error closing product-master classification consumer", "error", err)
			}
		},
	}, nil
}

// combineConsumerHandles merges several inbound consumer handles into one,
// so the shutdown path drains them all with a single wait/close.
func combineConsumerHandles(handles ...transferConsumerHandle) transferConsumerHandle {
	return transferConsumerHandle{
		stop: func() {
			for _, h := range handles {
				h.stop()
			}
		},
		wait: func() {
			for _, h := range handles {
				h.wait()
			}
		},
		close: func() {
			for _, h := range handles {
				h.close()
			}
		},
	}
}
