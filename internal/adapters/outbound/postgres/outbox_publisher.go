package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// OutboxPublisher implements ports.EventPublisher by writing each event's
// already-encoded Kafka message(s) into outbox_events instead of sending
// them to Kafka directly. Publish always runs through querierFrom, so
// when called from inside a UnitOfWork.Execute closure the insert joins
// the same transaction as the aggregate Save it accompanies: both commit
// together, or neither does (ADR 0017). Called with no active
// transaction (e.g. a one-off script), it commits its own outbox rows
// immediately, same as any other single-statement write in this package.
//
// It fans every event out to every encoder it is given — one per Kafka
// topic this service publishes to (today: the integration topic via
// kafka.Publisher, and the analytics topic via kafka.AnalyticsPublisher)
// — preserving the same dual-topic fan-out the pre-outbox code performed
// with two direct ports.EventPublisher calls.
type OutboxPublisher struct {
	pool     *pgxpool.Pool
	encoders []kafka.Encoder
}

// NewOutboxPublisher builds an OutboxPublisher that fans every event out
// to encoders (typically a kafka.Publisher and a kafka.AnalyticsPublisher).
func NewOutboxPublisher(pool *pgxpool.Pool, encoders ...kafka.Encoder) *OutboxPublisher {
	return &OutboxPublisher{pool: pool, encoders: encoders}
}

// outboxHeader is the JSON shape headers are stored as in outbox_events,
// so W3C trace headers survive the round trip through the table.
type outboxHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// Publish encodes event via every configured encoder and inserts one
// outbox_events row per resulting message. An event with no wire form for
// a given encoder (e.g. LocationRecorded against the integration
// publisher) contributes no row for that encoder, exactly as a direct
// Publish call would have silently skipped sending it.
func (p *OutboxPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	q := querierFrom(ctx, p.pool)

	for _, enc := range p.encoders {
		messages, err := enc.Encode(ctx, event)
		if err != nil {
			return fmt.Errorf("postgres: encode %s for outbox: %w", event.EventName(), err)
		}
		for _, msg := range messages {
			headers := make([]outboxHeader, 0, len(msg.Headers))
			for _, h := range msg.Headers {
				headers = append(headers, outboxHeader{Key: h.Key, Value: h.Value})
			}
			headersJSON, err := json.Marshal(headers)
			if err != nil {
				return fmt.Errorf("postgres: marshal outbox headers: %w", err)
			}

			if _, err := q.Exec(ctx, `
				INSERT INTO outbox_events (topic, event_type, key, value, headers)
				VALUES ($1, $2, $3, $4, $5)
			`, msg.Topic, msg.EventType, msg.Key, msg.Value, headersJSON); err != nil {
				return fmt.Errorf("postgres: insert outbox row for %s: %w", event.EventName(), err)
			}
		}
	}
	return nil
}

// decodeOutboxHeaders is the inverse of the marshal step in Publish, used
// by the relay to rebuild kafka-go headers from a stored row.
func decodeOutboxHeaders(raw []byte) ([]kafkago.Header, error) {
	var headers []outboxHeader
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &headers); err != nil {
			return nil, err
		}
	}
	out := make([]kafkago.Header, len(headers))
	for i, h := range headers {
		out[i] = kafkago.Header{Key: h.Key, Value: h.Value}
	}
	return out, nil
}

// Compile-time assertion that OutboxPublisher satisfies ports.EventPublisher.
var _ ports.EventPublisher = (*OutboxPublisher)(nil)
