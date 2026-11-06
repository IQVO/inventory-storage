// Package inbound is the inbound Kafka adapter for the transfer
// allocation command: it consumes network-inventory-planning's
// TransferAllocationRequested events off
// warehouse.network-inventory-planning.events and runs the
// AllocateTransferStock use case, exactly once per CloudEvents id
// despite at-least-once delivery.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Topic is network-inventory-planning's integration topic — that
// context's Published Language. This service knows nothing else about
// the producer beyond the CloudEvents type and payload below.
const Topic = "warehouse.network-inventory-planning.events"

// DefaultConsumerGroup is the default consumer group id for the transfer
// allocation command consumer. Overridable via NewConsumer's groupID
// parameter (env-configured by the composition root:
// TRANSFER_ALLOCATION_CONSUMER_GROUP) so a locally-run process can never
// silently steal partitions from the live in-cluster Deployment's group.
const DefaultConsumerGroup = "inventory-storage-transfer-allocation"

// typeTransferAllocationRequested is the FULL CloudEvents type string
// this consumer dispatches on — never a suffix match.
const typeTransferAllocationRequested = "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested"

// typeTransferAllocationReleaseRequested is the FULL CloudEvents type of
// the compensation command (ADR 0050): a transfer cancelled after
// allocation asks this service to resolve the origin hold.
const typeTransferAllocationReleaseRequested = "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationReleaseRequested"

// commandData is the TransferAllocationRequested payload, per
// network-inventory-planning's contract:
// {transfer_id, transfer_line_id, origin_site_id, sku, quantity}.
type commandData struct {
	TransferID     string `json:"transfer_id"`
	TransferLineID string `json:"transfer_line_id"`
	OriginSiteID   string `json:"origin_site_id"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
}

// releaseCommandData is the TransferAllocationReleaseRequested payload,
// per network-inventory-planning's contract (ADR 0050):
// {transfer_id, transfer_line_id, reservation_id, origin_site_id, sku,
// quantity, reason}.
type releaseCommandData struct {
	TransferID     string `json:"transfer_id"`
	TransferLineID string `json:"transfer_line_id"`
	ReservationID  string `json:"reservation_id"`
	OriginSiteID   string `json:"origin_site_id"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	Reason         string `json:"reason"`
}

// Reader is the subset of *kafkago.Reader this consumer needs, so unit
// tests can substitute a fake without a live broker.
type Reader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Consumer applies transfer allocation commands. Its Run loop follows
// the fleet's at-least-once atomicity checklist: FetchMessage (never
// ReadMessage — that auto-commits), handler nil BEFORE CommitMessages,
// and capped exponential backoff retrying the SAME message on a
// transient error, so a failed handler can never drop or skip a command.
type Consumer struct {
	Reader Reader
	// Allocate is the allocation command handler. Required.
	Allocate TransferAllocator
	// Release is the release command handler (ADR 0051). Required in
	// production wiring; a release command arriving while it is nil is
	// logged and committed past (a wiring bug is not retryable).
	Release TransferReleaser
	Logger  *slog.Logger
}

// TransferAllocator is the use-case port the consumer drives —
// satisfied by *usecases.AllocateTransferStock.
type TransferAllocator interface {
	Execute(ctx context.Context, cmd usecases.TransferCommand) (*usecases.Result, error)
}

// TransferReleaser is the release use-case port the consumer drives —
// satisfied by *usecases.ReleaseTransferAllocation.
type TransferReleaser interface {
	Execute(ctx context.Context, cmd usecases.TransferReleaseCommand) (*usecases.ReleaseResult, error)
}

// NewConsumer constructs the reader and consumer. groupID is the
// consumer group (env-configurable at the composition root; the named
// const documents the default and its reasoning).
func NewConsumer(brokers []string, groupID string, allocate TransferAllocator, release TransferReleaser, logger *slog.Logger) *Consumer {
	return NewConsumerForTopic(Topic, brokers, groupID, allocate, release, logger)
}

// NewConsumerForTopic is NewConsumer with an explicit topic, so
// integration tests can drive the identical consume/commit semantics
// against a throwaway topic instead of the production one.
func NewConsumerForTopic(topic string, brokers []string, groupID string, allocate TransferAllocator, release TransferReleaser, logger *slog.Logger) *Consumer {
	if groupID == "" {
		groupID = DefaultConsumerGroup
	}
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// A brand-new group starts at the EARLIEST offset so a replayed
		// command stream is re-derived from the ledger rather than
		// skipped; once the group has committed, offsets take over.
		StartOffset: kafkago.FirstOffset,
	})
	return &Consumer{Reader: reader, Allocate: allocate, Release: release, Logger: logger}
}

// retryBackoff is the capped exponential backoff between retries of the
// SAME message on a transient handler error: 200ms → 5s.
func retryBackoff(attempt int) time.Duration {
	d := 200 * time.Millisecond
	for i := 0; i < attempt && d < 5*time.Second; i++ {
		d *= 2
	}
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// Run consumes Topic until ctx is cancelled (a clean stop: nil, even
// mid-handle -- the in-flight command stays uncommitted and is redelivered)
// or the reader fails fatally. A transient handler error retries the SAME message with
// capped backoff (cancellable); a deterministic one (malformed,
// unknown type, domain validation) is logged and the message committed
// past — never retried, never blocking the partition.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if !c.settle(ctx, msg) {
			// Shutdown landed mid-handle: a clean stop. The offset was not
			// committed, so the command is redelivered and replayed.
			return nil
		}

		// Commit only after the handler settled (nil or deterministic
		// poison) — a transient failure never reaches this line.
		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil // shutdown: the uncommitted command is redelivered and replayed
			}
			return fmt.Errorf("commit transfer allocation command: %w", err)
		}
	}
}

// settle handles msg until it is settled (handled, or deterministic poison
// logged and skipped), retrying a transient failure on the SAME message with
// capped backoff. It reports false only when ctx is done first.
func (c *Consumer) settle(ctx context.Context, msg kafkago.Message) bool {
	for attempt := 1; ; attempt++ {
		err := c.Handle(ctx, msg)
		if err == nil {
			return true
		}
		if isDeterministic(err) {
			// Poison: log, commit past, never retry. The command was
			// malformed or the envelope invalid — a retry cannot fix
			// it, and blocking the partition would wedge every
			// later transfer.
			c.Logger.ErrorContext(ctx, "transfer allocation command permanently unhandleable; committing past it",
				"error", err, "partition", msg.Partition, "offset", msg.Offset)
			return true
		}
		// Transient (DB/tx failure): retry the SAME message.
		c.Logger.WarnContext(ctx, "transfer allocation command handling failed; retrying same message",
			"error", err, "attempt", attempt, "partition", msg.Partition, "offset", msg.Offset)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(retryBackoff(attempt)):
		}
	}
}

// Handle decodes and applies one message. It returns an error ONLY for
// transient failures (use-case infra errors); deterministic problems
// (not CloudEvents, unknown type, malformed payload, command validation)
// are logged and returned as nil, per the error-classification rule.
func (c *Consumer) Handle(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping transfer message that is not a valid CloudEvent",
			"partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}

	// Dispatch on the FULL type; ignore everything else on the topic.
	switch e.Type() {
	case typeTransferAllocationRequested:
		return c.handleAllocate(ctx, e, msg)
	case typeTransferAllocationReleaseRequested:
		return c.handleRelease(ctx, e, msg)
	default:
		return nil
	}
}

// handleAllocate decodes and applies one TransferAllocationRequested.
// It needs the cloudevents event for its id and the raw message for its
// partition/offset in log lines.
func (c *Consumer) handleAllocate(ctx context.Context, e ce.Event, msg kafkago.Message) error {
	var data commandData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationRequested with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}

	site, err := shared.NewSiteID(data.OriginSiteID)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationRequested with invalid origin_site_id",
			"id", e.ID(), "origin_site_id", data.OriginSiteID, "error", err)
		return nil
	}
	sku, err := shared.NewSKU(data.SKU)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationRequested with invalid sku",
			"id", e.ID(), "sku", data.SKU, "error", err)
		return nil
	}
	qty, err := shared.NewPositiveQuantity(data.Quantity)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationRequested with non-positive quantity",
			"id", e.ID(), "quantity", data.Quantity, "error", err)
		return nil
	}

	result, err := c.Allocate.Execute(ctx, usecases.TransferCommand{
		TransferID:     data.TransferID,
		TransferLineID: data.TransferLineID,
		OriginSiteID:   site,
		SKU:            sku,
		Quantity:       qty,
	})
	if err != nil {
		if isDeterministic(err) {
			// Command-level validation failure: deterministic poison.
			c.Logger.WarnContext(ctx, "transfer allocation command rejected as malformed; committing past it",
				"id", e.ID(), "transfer_line_id", data.TransferLineID, "error", err)
			return nil
		}
		return err
	}

	outcome, reservationID := "UNKNOWN", ""
	if result.Allocation != nil {
		outcome = string(result.Allocation.Outcome())
		reservationID = result.Allocation.ReservationID()
	}
	if result.Replay {
		c.Logger.InfoContext(ctx, "transfer allocation command replayed; original outcome returned",
			"ce_id", e.ID(), "transfer_line_id", data.TransferLineID,
			"outcome", outcome)
	} else {
		c.Logger.InfoContext(ctx, "transfer allocation decided",
			"ce_id", e.ID(), "transfer_line_id", data.TransferLineID,
			"outcome", outcome,
			"reservation_id", reservationID)
	}
	return nil
}

// handleRelease decodes and applies one
// TransferAllocationReleaseRequested — the compensation command of a
// transfer cancelled after allocation (ADR 0050). Refusals (unknown
// line, ledger mismatch, malformed payload) are deterministic: logged
// and committed past, never retried, and the use case publishes no event
// for them.
func (c *Consumer) handleRelease(ctx context.Context, e ce.Event, msg kafkago.Message) error {
	if c.Release == nil {
		// A wiring bug, not a retryable failure: the composition root
		// always wires the release handler.
		c.Logger.ErrorContext(ctx, "transfer allocation release command arrived but no release handler is wired; committing past it",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset)
		return nil
	}

	var data releaseCommandData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationReleaseRequested with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}

	site, err := shared.NewSiteID(data.OriginSiteID)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationReleaseRequested with invalid origin_site_id",
			"id", e.ID(), "origin_site_id", data.OriginSiteID, "error", err)
		return nil
	}
	sku, err := shared.NewSKU(data.SKU)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationReleaseRequested with invalid sku",
			"id", e.ID(), "sku", data.SKU, "error", err)
		return nil
	}
	qty, err := shared.NewPositiveQuantity(data.Quantity)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping TransferAllocationReleaseRequested with non-positive quantity",
			"id", e.ID(), "quantity", data.Quantity, "error", err)
		return nil
	}

	result, err := c.Release.Execute(ctx, usecases.TransferReleaseCommand{
		TransferID:     data.TransferID,
		TransferLineID: data.TransferLineID,
		ReservationID:  data.ReservationID,
		OriginSiteID:   site,
		SKU:            sku,
		Quantity:       qty,
		Reason:         data.Reason,
	})
	if err != nil {
		if isDeterministic(err) {
			// Deterministic refusal (malformed command, unknown line,
			// ledger mismatch): log, commit past, never retry.
			c.Logger.WarnContext(ctx, "transfer allocation release command refused; committing past it",
				"id", e.ID(), "transfer_line_id", data.TransferLineID, "error", err)
			return nil
		}
		return err
	}

	outcome := "UNKNOWN"
	if result.Release != nil {
		outcome = string(result.Release.Outcome())
	}
	if result.Replay {
		c.Logger.InfoContext(ctx, "transfer allocation release command replayed; original outcome returned",
			"ce_id", e.ID(), "transfer_line_id", data.TransferLineID,
			"outcome", outcome)
	} else {
		c.Logger.InfoContext(ctx, "transfer allocation release decided",
			"ce_id", e.ID(), "transfer_line_id", data.TransferLineID,
			"outcome", outcome,
			"reservation_id", data.ReservationID)
	}
	return nil
}

// isDeterministic reports whether err can never succeed on retry:
// a malformed command (validation) or a non-CloudEvent envelope.
func isDeterministic(err error) bool {
	return errors.Is(err, usecases.ErrMalformedTransferCommand) ||
		errors.Is(err, usecases.ErrUnknownTransferLine) ||
		errors.Is(err, usecases.ErrTransferReleaseMismatch) ||
		errors.Is(err, cloudevents.ErrNotCloudEvent)
}

// Close releases the underlying reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}
