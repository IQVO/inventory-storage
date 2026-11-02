package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// runConsumer is the shared at-least-once Run loop of the cross-context
// consumers (inbound-receiving, product-master, returns-processing):
// FetchMessage, retry the SAME message on a transient handler error with
// capped backoff, then CommitMessages. A cancellation is a clean stop and
// returns nil, even when it lands mid-handle: the in-flight message stays
// uncommitted and is redelivered on the next start, where processed_events
// deduplicates it. source names the owning context in the commit error;
// label prefixes the retry warning (e.g. "inbound-receiving receipt").
func runConsumer(
	ctx context.Context,
	reader Reader,
	handle func(context.Context, kafkago.Message) error,
	logger *slog.Logger,
	backoff func(attempt int) time.Duration,
	source, label string,
) error {
	if backoff == nil {
		backoff = retryBackoff
	}
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		attempt := 0
		for {
			err := handle(ctx, msg)
			if err == nil {
				break
			}
			attempt++
			logger.WarnContext(ctx, label+" handling failed; retrying same message",
				"error", err, "attempt", attempt, "partition", msg.Partition, "offset", msg.Offset)
			select {
			case <-ctx.Done():
				// Shutdown landed mid-handle: a clean stop. The offset was not
				// committed, so the message is redelivered and deduplicated.
				return nil
			case <-time.After(backoff(attempt)):
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil // shutdown: the uncommitted message is redelivered and deduplicated
			}
			return fmt.Errorf("commit %s message: %w", source, err)
		}
	}
}
