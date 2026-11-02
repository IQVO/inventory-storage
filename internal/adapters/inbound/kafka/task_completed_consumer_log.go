package kafka

import (
	"context"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// logReplenish logs what ApplyReplenishTask did with one settled message.
func (c *TaskCompletedConsumer) logReplenish(ctx context.Context, id string, data taskCompletedData, result usecases.ReplenishTaskResult) {
	if result.Outcome == usecases.ReplenishDuplicate {
		c.Logger.InfoContext(ctx, "task-completed REPLENISH already applied; redelivery ignored",
			"id", id, "task_id", data.TaskID, "request_ref", data.RequestRef)
		return
	}
	relocationID := ""
	if result.Relocation != nil {
		relocationID = result.Relocation.ID()
	}
	c.Logger.InfoContext(ctx, "task-completed REPLENISH applied to the ledger",
		"id", id, "task_id", data.TaskID, "relocation_id", relocationID, "sku", data.SKU,
		"from_bin", data.FromBin, "to_bin", data.ToBin, "request_ref", data.RequestRef)
}

// logRelocation logs what ApplyRelocationTask did with one settled message.
func (c *TaskCompletedConsumer) logRelocation(ctx context.Context, id string, data taskCompletedData, result usecases.RelocationTaskResult) {
	if result.Outcome == usecases.RelocationDuplicate {
		c.Logger.InfoContext(ctx, "task-completed RELOCATE already applied; redelivery ignored",
			"id", id, "task_id", data.TaskID, "relocation_ref", data.RelocationRef)
		return
	}
	relocationID := ""
	if result.Relocation != nil {
		relocationID = result.Relocation.ID()
	}
	c.Logger.InfoContext(ctx, "task-completed RELOCATE applied to the ledger",
		"id", id, "task_id", data.TaskID, "relocation_id", relocationID, "sku", data.SKU,
		"from_bin", data.FromBin, "to_bin", data.ToBin, "relocation_ref", data.RelocationRef)
}

// logOutcome logs what the use case did with one settled message.
func (c *TaskCompletedConsumer) logOutcome(ctx context.Context, id string, data taskCompletedData, result usecases.ConfirmPicksResult) {
	switch result.Outcome {
	case usecases.PicksIgnored:
		c.Logger.DebugContext(ctx, "task-completed ignored (not a PICK task or no order_ref)",
			"id", id, "task_id", data.TaskID, "task_type", data.TaskType)
	case usecases.PicksDuplicate:
		c.Logger.InfoContext(ctx, "task-completed already handled; redelivery ignored",
			"id", id, "task_id", data.TaskID, "order_ref", data.OrderRef)
	case usecases.PicksNoReservations:
		c.Logger.InfoContext(ctx, "task-completed for an order with no reservations; nothing to confirm",
			"id", id, "task_id", data.TaskID, "order_ref", data.OrderRef)
	case usecases.PicksAwaiting:
		c.Logger.InfoContext(ctx, "task-completed counted; waiting for the order's last pick before confirming",
			"id", id, "task_id", data.TaskID, "order_ref", data.OrderRef,
			"picks_seen", result.PicksSeen, "picks_needed", result.PicksNeeded)
	case usecases.PicksNothingToConfirm:
		c.Logger.InfoContext(ctx, "task-completed for an order with no ACTIVE reservation left; nothing to confirm",
			"id", id, "task_id", data.TaskID, "order_ref", data.OrderRef,
			"picks_seen", result.PicksSeen, "picks_needed", result.PicksNeeded)
	case usecases.PicksLineNotFound:
		c.Logger.InfoContext(ctx, "task-completed names a line no reservation of the order carries; nothing to confirm",
			"id", id, "task_id", data.TaskID, "order_ref", data.OrderRef, "line_no", data.lineNoOrZero())
	case usecases.PicksLineSettled:
		c.logLineSettled(ctx, id, data, result)
	default:
		c.logSettled(ctx, id, data, result)
	}
}

// lineNoOrZero is the event's line for logging; 0 when it has none.
func (d taskCompletedData) lineNoOrZero() int {
	if d.LineNo == nil {
		return 0
	}
	return *d.LineNo
}

// logLineSettled logs the per-line result (ADR 0036). Expired reservations are
// a WARN for the same reason as on the counting path; a pick that finds the
// line already confirmed or revoked is an INFO (redelivery under a new id, an
// operator's REST confirm, or a revoked earlier attempt).
func (c *TaskCompletedConsumer) logLineSettled(ctx context.Context, id string, data taskCompletedData, r usecases.ConfirmPicksResult) {
	level := slog.LevelInfo
	msg := "task-completed confirmed the picked line"
	if r.Expired > 0 {
		level = slog.LevelWarn
		msg = "task-completed skipped an expired reservation of the picked line (stock was already returned to usable)"
	}
	c.Logger.Log(ctx, level, msg,
		"id", id, "task_id", data.TaskID, "order_ref", data.OrderRef, "line_no", data.lineNoOrZero(),
		"confirmed", r.Confirmed, "already_picked", r.AlreadyPicked, "revoked", r.Revoked, "expired", r.Expired)
}

// logSettled logs the per-order result; expired reservations are a WARN
// because the physical pick happened but the stock had already gone back to
// usable (ADR 0035).
func (c *TaskCompletedConsumer) logSettled(ctx context.Context, id string, data taskCompletedData, r usecases.ConfirmPicksResult) {
	level := slog.LevelInfo
	msg := "task-completed confirmed the order's picks (last pick)"
	if r.Expired > 0 {
		level = slog.LevelWarn
		msg = "task-completed skipped expired reservations (stock was already returned to usable)"
	}
	c.Logger.Log(ctx, level, msg,
		"id", id, "task_id", data.TaskID, "order_ref", data.OrderRef,
		"confirmed", r.Confirmed, "already_picked", r.AlreadyPicked, "revoked", r.Revoked, "expired", r.Expired)
}

func (c *TaskCompletedConsumer) logNotCloudEvent(ctx context.Context, msg kafkago.Message, err error) {
	n := c.notCloudEvents.Add(1)
	if n > notCloudEventLogFirst && n%notCloudEventLogEvery != 0 {
		return
	}
	c.Logger.WarnContext(ctx, "skipping fulfillment message that is not a valid CloudEvent (sampled: first few, then every 1000th)",
		"skipped_so_far", n, "partition", msg.Partition, "offset", msg.Offset, "error", err)
}
