package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

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
		if ctx.Err() != nil {
			return ctx.Err() // shutting down: never dead-letter a message only cancellation interrupted
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
