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

// transferConsumerHandle bundles the transfer allocation command
// consumer's lifecycle hooks, mirroring locationLookupHandle.
//
// stop cancels the consumer's Run loop (it runs under the SAME
// cancellable context as the facility location lookup's consumer, so
// shutdown's existing stopLookup cancel unblocks both), wait blocks
// until the Run goroutine has actually returned, and close releases the
// Kafka reader. A nil consumer (mode "off", the default) returns hooks
// that are all no-ops.
type transferConsumerHandle struct {
	stop  func()
	wait  func()
	close func()
}

// noopTransferConsumer is the "off" handle: every hook is a no-op, so
// run()'s deferred stop and shutdown wiring never need a nil check.
func noopTransferConsumer() transferConsumerHandle {
	noop := func() {}
	return transferConsumerHandle{stop: noop, wait: noop, close: noop}
}

// buildTransferAllocationConsumer wires the inbound transfer allocation
// command consumer (Phase 2 command/reply leg of the network transfer
// saga): it consumes network-inventory-planning's
// TransferAllocationRequested commands (CloudEvents 1.0 on
// warehouse.network-inventory-planning.events) and decides them through
// AllocateTransferStock — stock decrements, the Reservation, the
// transfer_allocations ledger row and the TransferStockAllocated /
// TransferStockRejected reply's outbox row all commit in ONE UnitOfWork
// transaction, idempotent on transfer_line_id at the database level.
//
// The consumer ALWAYS runs: Postgres and Kafka are the only runtime (the
// real-wiring ADR). loadConfig already refused a missing DATABASE_URL,
// KAFKA_BROKERS or TRANSFER_ALLOCATION_CONSUMER_GROUP, and
// TRANSFER_ALLOCATION_CONSUMER_MODE is accepted only as unset or kafka. The
// use case's writes are atomic through the Postgres UnitOfWork/outbox.
//
// ctx is the lookup's own cancellable context (see run()): both inbound
// consumers' loops must stop on shutdown, so they share one cancel
// func, cancelled by gracefulShutdown's stopLookup.
func buildTransferAllocationConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
) (transferConsumerHandle, error) {
	transfers := postgres.NewTransferAllocationRepo(set.pool)
	clock := system.Clock{}
	allocate := &usecases.AllocateTransferStock{
		Stock:        set.stock,
		Reservations: set.reservations,
		Transfers:    transfers,
		Events:       set.publisher,
		Clock:        clock,
		UnitOfWork:   set.uow,
		Levels:       set.levelEmitter(clock),
	}

	consumer := inboundkafka.NewConsumer(cfg.kafkaBrokers, cfg.transferConsumerGroup, allocate, logger)
	logger.Info("transfer allocation command consumer configured",
		"topic", inboundkafka.Topic, "consumer_group", cfg.transferConsumerGroup)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		logger.Info("transfer allocation command consumer running", "topic", inboundkafka.Topic)
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("transfer allocation command consumer stopped", "error", err)
		}
	}()

	return transferConsumerHandle{
		stop: func() {},
		wait: func() { <-runDone },
		close: func() {
			if err := consumer.Close(); err != nil {
				logger.Error("error closing transfer allocation consumer", "error", err)
			}
		},
	}, nil
}
