package main

import (
	"context"
	"log/slog"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

// buildEventDrivenConsumers builds the event-driven inbound consumers that
// follow the transfer and product-master ones — the TaskCompleted
// confirm-pick consumer (ADR 0035) and the inbound-receiving receipt consumer
// (ADR 0037) — and returns one handle draining all of them together with
// started (the consumers run() already built). All share run()'s lookupCtx, so
// shutdown's stopLookup cancel unblocks every loop. On error it stops
// started and the consumers built here, so the caller only has to release
// the lookup.
func buildEventDrivenConsumers(
	ctx context.Context,
	logger *slog.Logger,
	cfg config,
	set adapterSet,
	started transferConsumerHandle,
) (transferConsumerHandle, error) {
	// Default "off"; see buildTaskCompletedConsumer.
	pickMetrics, err := telemetry.NewPickConfirmationMetrics()
	if err != nil {
		stopTransferConsumer(logger, started)
		return noopTransferConsumer(), err
	}
	confirmPicks, err := buildTaskCompletedConsumer(ctx, logger, cfg, set, pickMetrics)
	if err != nil {
		stopTransferConsumer(logger, started)
		return noopTransferConsumer(), err
	}

	// Not started unless INBOUND_RECEIPT_CONSUMER_GROUP is set.
	inboundReceipt, err := buildInboundReceiptConsumer(ctx, logger, cfg, set)
	if err != nil {
		stopTransferConsumer(logger, combineConsumerHandles(started, confirmPicks))
		return noopTransferConsumer(), err
	}
	return combineConsumerHandles(started, confirmPicks, inboundReceipt), nil
}
