// Package kafka publishes cross-service integration events to the shared
// warehouse-systems Kafka broker. It implements ports.EventPublisher, so it
// drops in wherever the log or Postgres outbox publisher is used today.
//
// Only StockReserved and ReservationRevoked are part of the published
// integration contract (see CLAUDE.md's Cross-service integration section);
// every other domain event is a local concern and is not forwarded here.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Topic is the integration events topic this service publishes to.
const Topic = "warehouse.inventory.events"

// Source identifies this service in the event envelope.
const Source = "inventory-storage"

// tracerName scopes the publish spans this adapter emits.
const tracerName = "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"

// spanName follows the fleet-wide convention for messaging spans:
// "kafka.publish <topic>" on the producer side, "kafka.consume <topic>" on
// the consumer side. This service only publishes.
const spanName = "kafka.publish " + Topic

// ErrReservationNotFound is returned when a ReservationRevoked event
// references a reservation the repo no longer has (should not happen: the
// use case saves the reservation before publishing).
var ErrReservationNotFound = errors.New("kafka publisher: reservation not found for revoked event")

// Writer is the subset of *kafkago.Writer the Publisher depends on, so unit
// tests can substitute a fake without a real broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// envelope is the integration event wrapper shared across all
// warehouse-systems services.
type envelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Source     string          `json:"source"`
	Data       json.RawMessage `json:"data"`
}

// reservationData is the `data` payload shape for both StockReserved and
// ReservationRevoked, per CLAUDE.md.
type reservationData struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demand_ref"`
}

// Publisher publishes StockReserved and ReservationRevoked domain events as
// integration events on Topic. It satisfies both ports.EventPublisher
// (direct publish) and kafka.Encoder (used by postgres.OutboxPublisher to
// enqueue the wire-ready message inside a transaction — ADR 0017).
type Publisher struct {
	writer       Writer
	reservations ports.ReservationRepo
}

// NewPublisher builds a Publisher. reservations is used to look up the SKU,
// quantity, and demand reference for a ReservationRevoked event, since the
// domain event itself carries only the reservation id.
func NewPublisher(writer Writer, reservations ports.ReservationRepo) *Publisher {
	return &Publisher{writer: writer, reservations: reservations}
}

// NewWriter builds a *kafkago.Writer addressed at Topic on the given broker
// addresses. Balancer is kafkago.Hash (FNV-1a of the message Key), not
// LeastBytes: LeastBytes ignores Message.Key entirely when choosing a
// partition (it only uses len(Key) to track a byte counter), so it cannot
// route same-key messages to the same partition no matter what Key Encode
// sets — confirmed against a real broker (see
// TestPublisher_RealBroker_SameReservationLandsOnSamePartition). Hash is
// what actually makes the per-reservation ordering guarantee in Encode's
// doc comment true (ADR-0021).
func NewWriter(brokers ...string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  Topic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}
}

// isIntegrationEvent reports whether event is one of the two types this
// publisher forwards, without doing any repo lookup — a cheap pre-check
// so Publish never opens a producer span for an event it will not send.
func isIntegrationEvent(event shared.DomainEvent) bool {
	switch event.(type) {
	case shared.StockReserved, shared.ReservationRevoked:
		return true
	default:
		return false
	}
}

// Encode maps event onto its integration wire form: the envelope, the
// event's data payload, and W3C trace headers injected from whatever span
// is active on ctx. The Kafka message Key is always the reservation
// aggregate id (ReservationID) — StockReserved and ReservationRevoked for
// the SAME reservation must land on the same partition so a consumer never
// observes them out of order (a real bug exposed when the shared broker's
// business topics grew from 1 to 8 partitions — see ADR-0021). It returns
// an empty slice (never an error) for an event outside this publisher's
// contract, so a caller — the outbox publisher included — can hand it the
// full event stream indiscriminately.
//
// The trace headers carry whatever span is active on ctx at the moment
// Encode runs: for a direct Publish call that is the "kafka.publish
// <topic>" producer span Publish itself opens before calling Encode; for
// the transactional-outbox path (postgres.OutboxPublisher) it is whatever
// span is wrapping the use case's Save+Publish call, since the eventual
// Kafka write happens later, asynchronously, via the relay.
func (p *Publisher) Encode(ctx context.Context, event shared.DomainEvent) ([]Encoded, error) {
	var data reservationData
	var key string

	switch e := event.(type) {
	case shared.StockReserved:
		data = reservationData{SKU: e.SKU.String(), Quantity: e.Quantity.Int(), DemandRef: e.DemandRef}
		key = e.ReservationID
	case shared.ReservationRevoked:
		res, err := p.reservations.FindByID(ctx, e.ReservationID)
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, ErrReservationNotFound
		}
		data = reservationData{SKU: res.SKU().String(), Quantity: res.Quantity().Int(), DemandRef: res.DemandRef()}
		key = e.ReservationID
	default:
		return nil, nil
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	env := envelope{
		EventID:    uuid.NewString(),
		EventType:  event.EventName(),
		OccurredAt: event.OccurredAt(),
		Source:     Source,
		Data:       payload,
	}

	msg, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}

	// Inject whatever span is active on ctx: for a direct publish that is
	// the just-started publish span (see Publish below), so a downstream
	// consumer's Extract parents onto it.
	headers := []kafkago.Header{}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	return []Encoded{{Topic: Topic, EventType: env.EventType, Key: []byte(key), Value: msg, Headers: headers}}, nil
}

// Compile-time assertion that Publisher satisfies the outbox's Encoder port.
var _ Encoder = (*Publisher)(nil)

// Publish forwards event onto Kafka directly (no outbox). This is the
// EVENT_PUBLISHER=kafka path used when the service runs without Postgres;
// with a database configured the composition root wires the transactional
// outbox instead (postgres.OutboxPublisher, using Encode above), and
// outbox rows are drained onto Kafka by a separate RelaySink (ADR 0017) —
// this Publisher's own writer keeps its fixed Topic and is never reused
// as the relay's sink.
func (p *Publisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	if !isIntegrationEvent(event) {
		return nil
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(Topic),
			semconv.MessagingOperationName("publish"),
		),
	)
	defer span.End()

	encoded, err := p.Encode(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(encoded) == 0 {
		return nil
	}

	for _, enc := range encoded {
		span.SetAttributes(attribute.String("messaging.message.event_type", enc.EventType))
		// The writer carries this publisher's own fixed Topic (see
		// NewWriter), so the message itself must NOT set Topic — kafka-go
		// rejects a message with Topic set when the Writer also fixes one.
		msg := kafkago.Message{Key: enc.Key, Value: enc.Value, Headers: enc.Headers}
		if err := p.writer.WriteMessages(ctx, msg); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}
	return nil
}

// Close releases the underlying Kafka writer.
func (p *Publisher) Close() error {
	if w, ok := p.writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}
