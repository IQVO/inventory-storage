package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Encoded is one already-encoded, wire-ready Kafka message. Topic is
// carried explicitly (rather than relying on the Writer's own fixed
// Topic) so a single relay Sink with no fixed topic of its own can route
// a batch of heterogeneous messages to the right topic (ADR 0017,
// transactional outbox).
//
// Value is a CloudEvents 1.0 structured-mode event (ADR-0024) whose `id` was
// minted once at Encode time, so the outbox row persists it and every relay
// redelivery carries the same id. EventType is that event's full CloudEvents
// `type` (com.warehouse.wms.inventory-storage.<entity>.<EventName>), stored
// in outbox_events.event_type for operator visibility only — consumers read
// the `type` attribute from Value, never this field.
type Encoded struct {
	Topic     string
	EventType string
	Key       []byte
	Value     []byte
	Headers   []kafkago.Header
}

// Encoder turns one domain event into its Kafka wire form(s) for one
// topic, without sending it. Both Publisher (the integration topic) and
// AnalyticsPublisher (the analytics topic) implement it, so
// postgres.OutboxPublisher can fan a single event out to both topics
// inside one transaction. The result is a slice because an event outside
// a given topic's contract (e.g. LocationRecorded for the analytics
// topic, or anything other than StockReserved/ReservationRevoked for the
// integration topic) legitimately encodes to zero messages — the caller
// skips it rather than erroring.
type Encoder interface {
	Encode(ctx context.Context, event shared.DomainEvent) ([]Encoded, error)
}
