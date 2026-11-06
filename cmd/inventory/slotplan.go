package main

import (
	"context"
	"errors"
	"log/slog"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// buildSlotPlanConsumer wires the slotting-optimization slot-plan consumer
// (ADR 0050): it consumes SlotPlanApproved on
// warehouse.slotting-optimization.events and keeps forward_pick_faces (the
// local read model ReserveStock draws its pick face from) in step, claiming
// each CloudEvents id in processed_events in the SAME transaction as the
// replacement of the site's map. It always runs, on the shared inbound
// consumer context, like every other consumer here.
func buildSlotPlanConsumer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
) transferConsumerHandle {
	apply := &usecases.ApplySlotPlan{
		Faces:           postgres.NewForwardFaceRepo(set.pool),
		ProcessedEvents: postgres.NewProcessedEventRepo(set.pool),
		UnitOfWork:      set.uow,
	}
	consumer := inboundkafka.NewSlotPlanConsumer(cfg.kafkaBrokers, cfg.slotPlanConsumerGroup, apply, logger)
	logger.Info("slot plan consumer configured",
		"topic", inboundkafka.SlotPlanTopic, "consumer_group", cfg.slotPlanConsumerGroup)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			workerDied(ctx, logger, "slot plan consumer", err)
		}
	}()

	return transferConsumerHandle{
		stop: func() {},
		wait: func() { <-runDone },
		close: func() {
			if err := consumer.Close(); err != nil {
				logger.Error("error closing slot plan consumer", "error", err)
			}
		},
	}
}
