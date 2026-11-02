package facilitycache

import (
	"context"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// dlqTopicSuffix names the dead-letter topic this consumer publishes an
// unparseable/malformed message to, relative to its OWN source topic
// (never a fixed constant): NewConsumerForTopic's isolated test topics
// each get their own matching "<topic>.dlq".
//
// Unlike order-management's RepromiseConsumer (ADR-0025 §DLQ over
// there), this consumer does NOT retry apply() before dead-lettering:
// apply()'s only failure mode is a permanently-malformed JSON payload
// (bad envelope/data shape, an empty required field) — a deterministic
// parse failure that would fail identically on every retry attempt,
// never a transient infrastructure error. Retrying it would just burn
// CPU for zero benefit. The value this DLQ still adds (ADR-0020) is
// pure visibility/replay: today a malformed message is logged and
// silently skipped forever; now it is ALSO preserved on
// Topic+dlqTopicSuffix (raw payload plus x-dlq-* headers) so an operator
// can inspect and, if facility-layout ever fixes its own publish bug,
// replay it. Readiness still advances past it either way (observe()
// runs unconditionally) — this consumer was already fail-open/
// non-blocking on a bad message before this change, unlike
// RepromiseConsumer's redelivery-blocking risk that originally
// motivated retry-then-DLQ there.
const dlqTopicSuffix = ".dlq"

// dlqPublish writes the raw, unmodified message payload plus error
// context (as headers, so the raw body stays byte-identical for a
// manual replay tool) to the dead-letter topic. A nil dlqWriter (the
// zero-value Consumer every existing unit test constructs directly via
// newTestConsumer, which never exercises this path) is a documented
// no-op rather than a nil-pointer panic.
func (c *Consumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return writeDLQ(ctx, c.dlqWriter, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// newDLQWriter builds the dead-letter writer for topic. The fleet
// auto-creates every topic on first write (warehouse-infra kafka.tf);
// without AllowAutoTopicCreation a missing "<topic>.dlq" fails the DLQ
// publish with "Unknown Topic Or Partition" and stops the consumer,
// freezing the facility cache.
func newDLQWriter(brokers []string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic + dlqTopicSuffix,
		AllowAutoTopicCreation: true,
		// BatchTimeout: a DLQ write is a synchronous single message; with
		// kafka-go's 1s default the writer holds every write for a full second
		// waiting to fill a batch, capping dead-lettering at ~1 msg/s/partition
		// (observed live: a backlog of legacy messages took hours to drain while
		// the consumer processed nothing else).
		BatchTimeout: dlqBatchTimeout,
		// Same synchronous-writer contract as the producer adapters in
		// internal/adapters/outbound/kafka/writer_config.go (ADR-0020/0021):
		// RequireAll so a DLQ write that returns nil really was stored (the
		// consumer commits the source offset on success -- a RequireNone
		// "success" the broker dropped would be silent message loss), and
		// Hash so the source message Key keeps routing to one DLQ partition
		// (kafka-go's default balancer ignores Key).
		RequiredAcks: kafkago.RequireAll,
		Balancer:     &kafkago.Hash{},
	}
}

// dlqTopicReadyAttempts / dlqTopicReadyBackoff bound how long a DLQ publish
// waits for an auto-created "<topic>.dlq" to become writable.
const (
	dlqTopicReadyAttempts = 40
	dlqTopicReadyBackoff  = 250 * time.Millisecond
)

// dlqMessageWriter is the slice of *kafkago.Writer writeDLQ needs.
type dlqMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// writeDLQ publishes msg to the dead-letter topic, retrying (bounded) while
// the topic is still being auto-created. AllowAutoTopicCreation alone is not
// enough: the first write races partition leader election and the broker
// answers UnknownTopicOrPartition / LeaderNotAvailable for a few hundred
// milliseconds. Any other error -- or exhausting the budget -- is returned,
// so the caller still refuses to commit the offset (no message loss).
func writeDLQ(ctx context.Context, w dlqMessageWriter, msg kafkago.Message) error {
	var err error
	for attempt := 0; attempt < dlqTopicReadyAttempts; attempt++ {
		if err = w.WriteMessages(ctx, msg); err == nil || !isTopicNotReady(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(dlqTopicReadyBackoff):
		}
	}
	return err
}

// isTopicNotReady reports whether err only means the (auto-created) topic
// has no leader yet.
func isTopicNotReady(err error) bool {
	var werrs kafkago.WriteErrors
	if errors.As(err, &werrs) {
		for _, e := range werrs {
			if e != nil && !isTopicNotReady(e) {
				return false
			}
		}
		return werrs.Count() > 0
	}
	return errors.Is(err, kafkago.UnknownTopicOrPartition) || errors.Is(err, kafkago.LeaderNotAvailable)
}

// dlqBatchTimeout flushes a dead-letter write almost immediately.
const dlqBatchTimeout = 10 * time.Millisecond
