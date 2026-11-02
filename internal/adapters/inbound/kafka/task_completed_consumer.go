package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// FulfillmentTopic is fulfillment-execution's integration topic, its
// Published Language for task facts (ADR 0035).
const FulfillmentTopic = "warehouse.fulfillment.events"

// DefaultTaskCompletedConsumerGroup is the consumer group of the
// TaskCompleted confirm-pick consumer when TASK_COMPLETED_CONSUMER_GROUP is
// unset. Fixed (not per process): this consumer mutates state and must resume
// from its committed offset.
const DefaultTaskCompletedConsumerGroup = "inventory-storage-confirm-pick"

// typeTaskCompleted is the FULL CloudEvents type this consumer dispatches on,
// byte-identical to fulfillment-execution's apis/asyncapi.yaml. The other
// types on the topic (TaskCPTMissed, PackageManifested, Transfer*) are
// committed past untouched.
const typeTaskCompleted = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"

// dlqSuffix names the dead-letter topic relative to the source topic.
const dlqSuffix = ".dlq"

// DefaultMaxHandleAttempts bounds how often a transient failure of ONE message
// is retried before the message is dead-lettered, so a persistently failing
// message can not wedge the partition forever.
const DefaultMaxHandleAttempts = 5

// notCloudEventLogEvery rate-limits the WARN for messages that are not
// CloudEvents (e.g. pre-CloudEvents history replayed by a brand-new group):
// the first few are logged, then one in every notCloudEventLogEvery.
const (
	notCloudEventLogFirst = 5
	notCloudEventLogEvery = 1000
)

// errPoison marks a message that can never be handled (retrying is
// pointless): the consumer dead-letters it immediately.
var errPoison = errors.New("poison message")

// taskCompletedData is fulfillment-execution's TaskCompleted v1 payload,
// reduced to what this consumer needs. order_ref is the additive field of
// ADR 0035: the completed task's order reference (the OrderId the reservations
// were made against); absent on events from a producer that predates it.
// line_no is the additive field of ADR 0036: the order line the task was for;
// absent (nil) when the producer does not know it, in which case the ADR 0035
// last-pick counting applies. sku, from_bin, to_bin, quantity and
// relocation_ref are the additive fields of ADR 0039 that fulfillment-execution
// emits for RELOCATE tasks only (absent on every other task type). request_ref
// is the additive field of ADR 0041: the replenishment REQUEST id, on REPLENISH
// tasks only; it mirrors relocation_ref of RELOCATE tasks.
type taskCompletedData struct {
	TaskID        string `json:"task_id"`
	TaskType      string `json:"task_type"`
	OrderRef      string `json:"order_ref"`
	LineNo        *int   `json:"line_no"`
	SKU           string `json:"sku"`
	FromBin       string `json:"from_bin"`
	ToBin         string `json:"to_bin"`
	Quantity      *int   `json:"quantity"`
	RelocationRef string `json:"relocation_ref"`
	RequestRef    string `json:"request_ref"`
}

// PickCompletionHandler is the use-case port the consumer drives, satisfied
// by *usecases.ConfirmPicksForOrder.
type PickCompletionHandler interface {
	Execute(ctx context.Context, in usecases.PickCompletion) (usecases.ConfirmPicksResult, error)
}

// RelocationTaskHandler is the use-case port for RELOCATE completions
// (ADR 0039), satisfied by *usecases.ApplyRelocationTask.
type RelocationTaskHandler interface {
	Execute(ctx context.Context, in usecases.RelocationTaskCompletion) (usecases.RelocationTaskResult, error)
}

// ReplenishTaskHandler is the use-case port for REPLENISH completions
// (ADR 0041), satisfied by *usecases.ApplyReplenishTask.
type ReplenishTaskHandler interface {
	Execute(ctx context.Context, in usecases.ReplenishTaskCompletion) (usecases.ReplenishTaskResult, error)
}

// DeadLetterWriter stores a dead-lettered message; satisfied by *kafkago.Writer.
type DeadLetterWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// TaskCompletedConsumer confirms picked reservations from fulfillment-
// execution's TaskCompleted (ADR 0035). It follows the fleet's at-least-once
// atomicity checklist: FetchMessage (never ReadMessage), the handler's claim
// and writes in one UnitOfWork, commit only after the message is settled
// (handled, ignored, or stored in the DLQ).
//
// Error classification: a transient failure retries the SAME message with
// capped backoff up to MaxAttempts, then the message is dead-lettered; a
// poison message (malformed payload, invalid completion) is dead-lettered
// immediately. Either way the offset is committed only after the DLQ write
// succeeded, so nothing is ever lost.
type TaskCompletedConsumer struct {
	Reader  Reader
	Confirm PickCompletionHandler
	// Relocate applies RELOCATE completions (ADR 0039). Optional: nil keeps
	// the pre-ADR 0039 behaviour (every completion goes to Confirm, which
	// ignores non-PICK tasks).
	Relocate RelocationTaskHandler
	// Replenish applies REPLENISH completions (ADR 0041). Optional: nil
	// keeps the pre-ADR 0041 behaviour (a REPLENISH completion goes to
	// Confirm, which ignores non-PICK tasks).
	Replenish ReplenishTaskHandler
	// DeadLetter receives poison and exhausted messages. Required.
	DeadLetter DeadLetterWriter
	Logger     *slog.Logger
	// Topic is the source topic, recorded on dead-lettered messages.
	Topic string
	// MaxAttempts is the handler attempts per message before dead-lettering;
	// <= 0 means DefaultMaxHandleAttempts.
	MaxAttempts int
	// Backoff is the wait before retry attempt n; nil means retryBackoff.
	Backoff func(attempt int) time.Duration

	notCloudEvents atomic.Int64
}

// NewTaskCompletedConsumer builds the reader and DLQ writer on
// FulfillmentTopic. groupID comes from configuration.
func NewTaskCompletedConsumer(brokers []string, groupID string, confirm PickCompletionHandler, logger *slog.Logger) *TaskCompletedConsumer {
	return NewTaskCompletedConsumerForTopic(FulfillmentTopic, brokers, groupID, confirm, logger)
}

// NewTaskCompletedConsumerForTopic is NewTaskCompletedConsumer with an
// explicit topic, so integration tests drive the same semantics against a
// throwaway topic. The dead-letter topic is topic + ".dlq".
func NewTaskCompletedConsumerForTopic(topic string, brokers []string, groupID string, confirm PickCompletionHandler, logger *slog.Logger) *TaskCompletedConsumer {
	if groupID == "" {
		groupID = DefaultTaskCompletedConsumerGroup
	}
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// A brand-new group starts at the EARLIEST offset: a completion that
		// happened while this consumer was down must still confirm its
		// reservations. Replaying already-handled history is harmless (the
		// reservations are no longer ACTIVE).
		StartOffset: kafkago.FirstOffset,
	})
	return &TaskCompletedConsumer{
		Reader:     reader,
		Confirm:    confirm,
		DeadLetter: newDeadLetterWriter(brokers, topic),
		Logger:     logger,
		Topic:      topic,
	}
}

// newDeadLetterWriter builds the writer for topic+".dlq". Same contract as
// the fleet's other DLQ writers: auto-create the topic on first write,
// RequireAll so a nil error means stored, Hash so the key keeps one partition,
// and a short BatchTimeout so a synchronous single-message write is not held
// back for kafka-go's 1 s default.
func newDeadLetterWriter(brokers []string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic + dlqSuffix,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafkago.RequireAll,
		Balancer:               &kafkago.Hash{},
	}
}

// Run consumes until ctx is cancelled or the reader fails. A cancellation is a
// clean stop and returns nil, even when it lands mid-handle: the in-flight
// message stays uncommitted and is redelivered on the next start.
func (c *TaskCompletedConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.settle(ctx, msg); err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				// Shutdown landed mid-handle: the offset was not committed,
				// so the message is redelivered on the next start and the
				// use case's processed-event claim absorbs the replay.
				return nil
			}
			return err
		}
		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil // shutdown: the uncommitted message is redelivered and deduped
			}
			return fmt.Errorf("commit task-completed message: %w", err)
		}
	}
}

// Handle decodes and applies one message. It returns nil when the message is
// settled (handled, or not for us), an error wrapping errPoison when it can
// never be handled, and any other error for a transient failure.
func (c *TaskCompletedConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		// Not a CloudEvent (the retired flat envelope, garbage). Skipped, not
		// dead-lettered: a new group replaying pre-CloudEvents history would
		// otherwise write one DLQ message per legacy message. The WARN is
		// sampled for the same reason.
		c.logNotCloudEvent(ctx, msg, err)
		return nil
	}
	if e.Type() != typeTaskCompleted {
		return nil
	}

	var data taskCompletedData
	if err := e.DataAs(&data); err != nil {
		return fmt.Errorf("%w: undecodable TaskCompleted payload (id %s): %v", errPoison, e.ID(), err)
	}
	if data.TaskType == usecases.RelocateTaskType && c.Relocate != nil {
		return c.handleRelocation(ctx, e.ID(), data)
	}
	if data.TaskType == usecases.ReplenishTaskType && c.Replenish != nil {
		return c.handleReplenish(ctx, e.ID(), data)
	}

	result, err := c.Confirm.Execute(ctx, usecases.PickCompletion{
		EventID:  e.ID(),
		TaskID:   data.TaskID,
		TaskType: data.TaskType,
		OrderRef: data.OrderRef,
		LineNo:   data.LineNo,
	})
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedPickCompletion) {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		return err
	}

	c.logOutcome(ctx, e.ID(), data, result)
	return nil
}

// handleRelocation applies one RELOCATE completion (ADR 0039). A
// deterministic rejection (ErrMalformedRelocationTask) is poison and goes to
// the DLQ at once; anything else (database, concurrent modification,
// cancellation) is returned unchanged, so settle retries it or, on shutdown,
// leaves the offset uncommitted.
func (c *TaskCompletedConsumer) handleRelocation(ctx context.Context, id string, data taskCompletedData) error {
	result, err := c.Relocate.Execute(ctx, usecases.RelocationTaskCompletion{
		EventID:       id,
		TaskID:        data.TaskID,
		TaskType:      data.TaskType,
		SKU:           data.SKU,
		FromBin:       data.FromBin,
		ToBin:         data.ToBin,
		Quantity:      data.Quantity,
		RelocationRef: data.RelocationRef,
	})
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedRelocationTask) {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		return err
	}
	c.logRelocation(ctx, id, data, result)
	return nil
}

// handleReplenish applies one REPLENISH completion (ADR 0041). Error
// classification is that of handleRelocation: a deterministic rejection is
// poison and goes to the DLQ at once, anything else is retried.
func (c *TaskCompletedConsumer) handleReplenish(ctx context.Context, id string, data taskCompletedData) error {
	result, err := c.Replenish.Execute(ctx, usecases.ReplenishTaskCompletion{
		EventID:    id,
		TaskID:     data.TaskID,
		TaskType:   data.TaskType,
		SKU:        data.SKU,
		FromBin:    data.FromBin,
		ToBin:      data.ToBin,
		Quantity:   data.Quantity,
		RequestRef: data.RequestRef,
	})
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedRelocationTask) {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		return err
	}
	c.logReplenish(ctx, id, data, result)
	return nil
}

// Close releases the reader and the dead-letter writer.
func (c *TaskCompletedConsumer) Close() error {
	readerErr := c.Reader.Close()
	if c.DeadLetter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.DeadLetter.Close())
}
