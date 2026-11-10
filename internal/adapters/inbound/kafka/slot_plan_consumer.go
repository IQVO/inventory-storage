package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/allocation"
)

// SlotPlanTopic is slotting-optimization's integration topic.
const SlotPlanTopic = "warehouse.slotting-optimization.events"

// typeSlotPlanApproved is the FULL CloudEvents type this consumer dispatches
// on, byte-identical to slotting-optimization's apis/asyncapi.yaml. The other
// types on the topic (SlotPlanGenerated, SlotPlanRejected) are committed past.
const typeSlotPlanApproved = "com.warehouse.wms.slotting-optimization.slotplan.SlotPlanApproved"

// slotPlanApprovedData is SlotPlanApproved v1. Fields this service does not
// use (moves, supersedes_plan_id, pick_unit, cases_capacity) are ignored.
type slotPlanApprovedData struct {
	PlanID      string    `json:"plan_id"`
	SiteID      string    `json:"site_id"`
	ApprovedAt  time.Time `json:"approved_at"`
	Assignments []struct {
		SKU  string `json:"sku"`
		Slot string `json:"slot"`
	} `json:"assignments"`
}

// SlotPlanApplier is the use-case port the consumer drives, satisfied by
// *usecases.ApplySlotPlan.
type SlotPlanApplier interface {
	Execute(ctx context.Context, u usecases.SlotPlanApprovedUpdate) (usecases.SlotPlanOutcome, error)
}

// SlotPlanConsumer keeps the local forward-face read model in step with
// slotting-optimization (ADR 0050). Fixed consumer group from configuration,
// state-mutating, at-least-once: FetchMessage, handler, then CommitMessages;
// a transient handler error retries the SAME message with capped backoff;
// deterministic poison is logged and committed past.
type SlotPlanConsumer struct {
	Reader  Reader
	Apply   SlotPlanApplier
	Logger  *slog.Logger
	Backoff func(attempt int) time.Duration
}

// NewSlotPlanConsumer builds the reader on SlotPlanTopic.
func NewSlotPlanConsumer(brokers []string, groupID string, apply SlotPlanApplier, logger *slog.Logger) *SlotPlanConsumer {
	return NewSlotPlanConsumerForTopic(SlotPlanTopic, brokers, groupID, apply, logger)
}

// NewSlotPlanConsumerForTopic is NewSlotPlanConsumer with an explicit topic,
// so integration tests drive the same semantics against a throwaway topic.
func NewSlotPlanConsumerForTopic(topic string, brokers []string, groupID string, apply SlotPlanApplier, logger *slog.Logger) *SlotPlanConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// A brand-new group replays the topic from the start, so the map is
		// rebuilt from the approved plans' history; once the group has
		// committed, its offsets take over.
		StartOffset: kafkago.FirstOffset,
	})
	return &SlotPlanConsumer{Reader: reader, Apply: apply, Logger: logger}
}

// Run consumes until ctx is cancelled or the reader fails.
func (c *SlotPlanConsumer) Run(ctx context.Context) error {
	return runConsumer(ctx, c.Reader, c.Handle, c.Logger, c.Backoff, "slotting-optimization", "slot plan")
}

// Handle decodes and applies one message. It returns an error ONLY for a
// transient failure; anything deterministic is logged and returns nil.
func (c *SlotPlanConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping slotting-optimization message that is not a valid CloudEvent",
			"partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	if e.Type() != typeSlotPlanApproved {
		return nil
	}
	var data slotPlanApprovedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping slotting-optimization SlotPlanApproved with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	raw := make([]allocation.RawAssignment, 0, len(data.Assignments))
	for _, a := range data.Assignments {
		raw = append(raw, allocation.RawAssignment{SKU: a.SKU, Slot: a.Slot})
	}
	outcome, err := c.Apply.Execute(ctx, usecases.SlotPlanApprovedUpdate{
		EventID: e.ID(), PlanID: data.PlanID, SiteID: data.SiteID, ApprovedAt: data.ApprovedAt, Assignments: raw,
	})
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedSlotPlan) {
			c.Logger.WarnContext(ctx, "skipping invalid slotting-optimization slot plan",
				"id", e.ID(), "plan_id", data.PlanID, "site_id", data.SiteID, "error", err)
			return nil
		}
		return err
	}
	c.Logger.InfoContext(ctx, "slotting-optimization slot plan handled",
		"id", e.ID(), "plan_id", data.PlanID, "site_id", data.SiteID, "assignments", len(raw), "outcome", string(outcome))
	return nil
}

// Close releases the underlying reader.
func (c *SlotPlanConsumer) Close() error {
	return c.Reader.Close()
}
