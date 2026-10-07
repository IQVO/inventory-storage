package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// dialBrokerTimeout bounds one boot-time probe of the Kafka bootstrap broker.
const dialBrokerTimeout = 10 * time.Second

// dialBroker opens and closes one connection to the first broker. A variable
// so the wiring tests can run without a broker.
var dialBroker = func(ctx context.Context, brokers []string) error {
	dialCtx, cancel := context.WithTimeout(ctx, dialBrokerTimeout)
	defer cancel()
	conn, err := kafkago.DialContext(dialCtx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

// buildTaskCompletedConsumer wires the confirm-pick consumer (ADR 0035): it
// consumes fulfillment-execution's TaskCompleted (CloudEvents 1.0 on
// warehouse.fulfillment.events), counts each PICK task per order_ref and, when
// the LAST pick of the order completes, confirms every ACTIVE reservation of
// that order through ConfirmPick. The CloudEvents id claim, the pick counter
// and all confirmations (stock, bin, reservation, outbox rows) commit in ONE
// UnitOfWork transaction.
//
// mode is TASK_COMPLETED_CONSUMER_MODE: "off" (default — the feature stays
// dark until an operator opts a deployment in) or "kafka". "kafka" also
// requires KAFKA_BROKERS and DATABASE_URL: the claim and the confirmations are
// only atomic through the Postgres UnitOfWork/outbox, so an in-memory run must
// not consume real completions. A misconfiguration returns an error so boot
// fails with the real cause rather than silently skipping the consumer. The
// first dial of the broker runs under bootretry, like the other boot-time
// dials (ADR 0028).
//
// ctx is the shared inbound-consumer context (see run()), so shutdown's
// stopLookup cancel unblocks this loop too.
func buildTaskCompletedConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
	metrics ports.PickConfirmationMetrics,
) (transferConsumerHandle, error) {
	if !strings.EqualFold(cfg.taskCompletedConsumerMode, "kafka") {
		logger.Info("task-completed confirm-pick consumer not started", "reason", "TASK_COMPLETED_CONSUMER_MODE is not kafka")
		return noopTransferConsumer(), nil
	}
	if cfg.databaseURL == "" || set.pool == nil {
		return noopTransferConsumer(), errors.New("TASK_COMPLETED_CONSUMER_MODE=kafka requires DATABASE_URL (the confirmations are only atomic through the Postgres unit of work)")
	}
	if len(cfg.kafkaBrokers) == 0 {
		return noopTransferConsumer(), errors.New("TASK_COMPLETED_CONSUMER_MODE=kafka requires KAFKA_BROKERS to be set")
	}
	if err := bootretry.Retry(ctx, logger, "dial fulfillment kafka brokers", func() error {
		return dialBroker(ctx, cfg.kafkaBrokers)
	}); err != nil {
		return noopTransferConsumer(), fmt.Errorf("failed to start the task-completed consumer: %w", err)
	}

	clock := memory.SystemClock{}
	confirm := &usecases.ConfirmPicksForOrder{
		Stock:        set.stock,
		Reservations: set.reservations,
		Events:       set.publisher,
		Clock:        clock,
		Confirm: &usecases.ConfirmPick{
			Stock: set.stock, Locations: set.locations, Reservations: set.reservations,
			Events: set.publisher, Clock: clock, UnitOfWork: set.uow,
		},
		ProcessedEvents: postgres.NewProcessedEventRepo(set.pool),
		PickProgress:    postgres.NewOrderPickProgressRepo(set.pool),
		UnitOfWork:      set.uow,
		Metrics:         metrics,
	}
	consumer := inboundkafka.NewTaskCompletedConsumer(cfg.kafkaBrokers, cfg.taskCompletedConsumerGroup, confirm, logger)
	group := cfg.taskCompletedConsumerGroup
	if group == "" {
		group = inboundkafka.DefaultTaskCompletedConsumerGroup
	}
	logger.Info("task-completed confirm-pick consumer configured",
		"mode", "kafka", "topic", inboundkafka.FulfillmentTopic, "dlq_topic", inboundkafka.FulfillmentTopic+".dlq",
		"consumer_group", group, "overridden", cfg.taskCompletedConsumerGroup != "")

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("task-completed confirm-pick consumer stopped", "error", err)
		}
	}()

	return transferConsumerHandle{
		stop: func() {},
		wait: func() { <-runDone },
		close: func() {
			if err := consumer.Close(); err != nil {
				logger.Error("error closing task-completed confirm-pick consumer", "error", err)
			}
		},
	}, nil
}
