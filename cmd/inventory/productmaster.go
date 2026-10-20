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
// consumer (ADR 0034, product-master ADR 0003 stage C): it consumes
// product-master's ProductClassified on warehouse.product-master.events and
// keeps product_classifications (the local copy StowStock reads) in step,
// claiming each CloudEvents id in processed_events in the SAME transaction
// as the version-guarded upsert.
//
// The consumer ALWAYS runs: Postgres and Kafka are the only runtime (the
// real-wiring ADR). loadConfig already refused a missing DATABASE_URL,
// KAFKA_BROKERS or PRODUCT_MASTER_CONSUMER_GROUP (cfg.productMasterConsumerGroup
// is PRODUCT_MASTER_CONSUMER_GROUP); the claim + upsert are atomic through the
// Postgres UnitOfWork.
//
// ctx is the shared inbound-consumer context (see run()), so shutdown's
// stopLookup cancel unblocks this loop too.
func buildProductMasterConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
) (transferConsumerHandle, error) {
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
