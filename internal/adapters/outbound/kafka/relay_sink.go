package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

// RelaySink is the outbox relay's Sink (postgres.OutboxRelay): it wraps a
// *kafkago.Writer with NO fixed Topic, so a single relay instance can drain
// outbox rows destined for either the integration topic or the analytics
// topic in the same pass — the topic travels per-message via Encoded.Topic
// (set on kafkago.Message.Topic here), never on the writer itself.
type RelaySink struct {
	writer *kafkago.Writer
}

// NewRelaySink builds a RelaySink writing to brokers with auto topic
// creation enabled (both topics already exist in a real cluster, but a
// throwaway integration-test broker starts empty). Balancer is
// kafkago.Hash, not LeastBytes, for the same reason as the direct
// Publisher/AnalyticsPublisher writers (see kafka.NewWriter's doc comment,
// ADR-0021): LeastBytes ignores Message.Key when routing, so it cannot
// honor the aggregate-id Key every Encoder on this path already sets.
func NewRelaySink(brokers []string) *RelaySink {
	return &RelaySink{
		writer: &kafkago.Writer{
			BatchTimeout:           syncWriterBatchTimeout,
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Send writes every message in msgs to Kafka, each addressed at its own
// Encoded.Topic, in one WriteMessages call.
func (s *RelaySink) Send(ctx context.Context, msgs ...Encoded) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		out[i] = kafkago.Message{Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers}
	}
	return s.writer.WriteMessages(ctx, out...)
}

// Close releases the underlying Kafka writer.
func (s *RelaySink) Close() error {
	return s.writer.Close()
}
