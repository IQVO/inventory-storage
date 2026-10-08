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
// last-pick counting applies.
type taskCompletedData struct {
	TaskID   string `json:"task_id"`
	TaskType string `json:"task_type"`
	OrderRef string `json:"order_ref"`
	LineNo   *int   `json:"line_no"`
}

// PickCompletionHandler is the use-case port the consumer drives, satisfied
// by *usecases.ConfirmPicksForOrder.
type PickCompletionHandler interface {
	Execute(ctx context.Context, in usecases.PickCompletion) (usecases.ConfirmPicksResult, error)
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

// Run consumes until ctx is cancelled or the reader fails.
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
			return err
		}
		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit task-completed message: %w", err)
		}
	}
}

// settle drives one message to a final state: handled/ignored, or
// dead-lettered. It returns an error only when ctx ends first (the offset is
// then NOT committed and the message is redelivered).
func (c *TaskCompletedConsumer) settle(ctx context.Context, msg kafkago.Message) error {
	backoff := c.Backoff
	if backoff == nil {
		backoff = retryBackoff
	}
	maxAttempts := c.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxHandleAttempts
	}

	for attempt := 1; ; attempt++ {
		err := c.Handle(ctx, msg)
		if err == nil {
			return nil
		}
		if errors.Is(err, errPoison) {
			c.Logger.ErrorContext(ctx, "task-completed message is malformed; dead-lettering",
				"error", err, "partition", msg.Partition, "offset", msg.Offset)
			return c.deadLetter(ctx, msg, err)
		}
		if attempt >= maxAttempts {
			c.Logger.ErrorContext(ctx, "task-completed handling kept failing; dead-lettering after bounded retries",
				"error", err, "attempts", attempt, "partition", msg.Partition, "offset", msg.Offset)
			return c.deadLetter(ctx, msg, fmt.Errorf("gave up after %d attempts: %w", attempt, err))
		}
		c.Logger.WarnContext(ctx, "task-completed handling failed; retrying same message",
			"error", err, "attempt", attempt, "partition", msg.Partition, "offset", msg.Offset)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
}

// deadLetter stores msg (raw value and key, plus x-dlq-* headers) on the DLQ
// topic. It retries the DLQ write until it succeeds or ctx ends: committing
// past a message whose dead-letter write failed would lose it.
func (c *TaskCompletedConsumer) deadLetter(ctx context.Context, msg kafkago.Message, cause error) error {
	backoff := c.Backoff
	if backoff == nil {
		backoff = retryBackoff
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	out := kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers}
	for attempt := 1; ; attempt++ {
		err := c.DeadLetter.WriteMessages(ctx, out)
		if err == nil {
			return nil
		}
		c.Logger.ErrorContext(ctx, "task-completed dead-letter write failed; retrying",
			"error", err, "attempt", attempt, "partition", msg.Partition, "offset", msg.Offset)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff(attempt)):
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

// Close releases the reader and the dead-letter writer.
func (c *TaskCompletedConsumer) Close() error {
	readerErr := c.Reader.Close()
	if c.DeadLetter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.DeadLetter.Close())
}
