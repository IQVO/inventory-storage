package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// AnalyticsTopic is the dedicated topic the Inventory Flow & Accuracy data
// product consumes. It is separate from the integration topic (Topic) so the
// OLTP integration contract and the analytical read-model stream evolve
// independently (ADR-0011).
const AnalyticsTopic = "warehouse.inventory.analytics"

// analyticsSchemaVersion is the schema version stamped onto every analytics
// envelope this publisher emits.
const analyticsSchemaVersion = 1

// analyticsTracerName scopes the analytics publish spans this adapter emits.
const analyticsTracerName = "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"

// AnalyticsEnvelope is the shared Envelope v1 wrapper for the analytics
// stream. Unlike the integration envelope it carries the payload as a
// json.RawMessage so a single publisher can emit the event_type-specific data
// object for every projecting domain event without a bespoke struct per type.
type AnalyticsEnvelope struct {
	EventId       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Source        string          `json:"source"`
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
}

// AnalyticsPublisher publishes every inventory-storage domain event onto
// AnalyticsTopic as an AnalyticsEnvelope. It satisfies ports.EventPublisher
// (direct publish) and kafka.Encoder (used by postgres.OutboxPublisher to
// enqueue the wire-ready message inside a transaction — ADR 0017), and is a
// SEPARATE adapter from Publisher: the integration publisher (publisher.go)
// forwards only StockReserved and ReservationRevoked and is left untouched.
//
// Reservation-lifecycle events (ReservationExpired, ReservationRevoked) carry
// only a reservation id in the domain event, so they are enriched with the
// reservation's SKU via a ReservationRepo lookup — the same repo-lookup
// enrichment the integration publisher already uses for ReservationRevoked.
// The report is keyed by SKU, so this enrichment is what populates that
// dimension for reservation events.
type AnalyticsPublisher struct {
	Writer       Writer
	Reservations ports.ReservationRepo
	NewId        func() string
}

// NewAnalyticsPublisher constructs an AnalyticsPublisher writing to
// AnalyticsTopic on brokers. newId mints the envelope event_id; reservations
// is used to enrich reservation-lifecycle events with their SKU. Balancer
// is kafkago.Hash, not LeastBytes: LeastBytes ignores Message.Key entirely
// when routing (it only tracks per-partition byte counts), so it cannot
// honor the aggregate-id Key marshalData already sets on every message —
// confirmed against a real broker in the integration publisher's own
// TestPublisher_RealBroker_SameReservationLandsOnSamePartition (ADR-0021).
// Hash is what actually makes same-key messages land on the same partition.
func NewAnalyticsPublisher(brokers []string, reservations ports.ReservationRepo, newId func() string) *AnalyticsPublisher {
	return &AnalyticsPublisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  AnalyticsTopic,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		Reservations: reservations,
		NewId:        newId,
	}
}

// Encode maps event onto its analytics wire form. It returns an empty
// slice (never an error) for an event outside the analytics contract
// (e.g. LocationRecorded), so a caller — the outbox publisher included —
// can hand it the full event stream indiscriminately. Trace headers are
// injected from whatever span is active on ctx, same convention as the
// integration Publisher's Encode.
func (p *AnalyticsPublisher) Encode(ctx context.Context, event shared.DomainEvent) ([]Encoded, error) {
	eventType, key, data, ok := p.marshalData(ctx, event)
	if !ok {
		return nil, nil
	}
	env := AnalyticsEnvelope{
		EventId:       p.newID(),
		EventType:     eventType,
		OccurredAt:    event.OccurredAt().UTC(),
		Source:        Source,
		SchemaVersion: analyticsSchemaVersion,
		Data:          data,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("kafka: marshal analytics envelope: %w", err)
	}

	headers := []kafkago.Header{}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	return []Encoded{{
		Topic:     AnalyticsTopic,
		EventType: eventType,
		Key:       []byte(key),
		Value:     payload,
		Headers:   headers,
	}}, nil
}

// Compile-time assertion that AnalyticsPublisher satisfies the outbox's
// Encoder port.
var _ Encoder = (*AnalyticsPublisher)(nil)

// Publish emits event onto AnalyticsTopic directly (no outbox). An event
// with no analytics payload is skipped rather than erroring (see Encode).
func (p *AnalyticsPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	ctx, span := otel.Tracer(analyticsTracerName).Start(ctx,
		"kafka.publish "+AnalyticsTopic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(AnalyticsTopic),
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
		msg := kafkago.Message{Key: enc.Key, Value: enc.Value, Headers: enc.Headers}
		if err := p.Writer.WriteMessages(ctx, msg); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("kafka: publish %s analytics event: %w", enc.EventType, err)
		}
	}
	return nil
}

// newID mints an envelope event id, defaulting to a random UUID when NewId is
// not injected.
func (p *AnalyticsPublisher) newID() string {
	if p.NewId != nil {
		return p.NewId()
	}
	return uuid.NewString()
}

// reservationSKU looks up the SKU of the reservation with id, returning "" when
// the reservation cannot be found (a best-effort enrichment: a missing SKU
// leaves the report's SKU dimension unspecified rather than failing the
// publish).
func (p *AnalyticsPublisher) reservationSKU(ctx context.Context, reservationID string) string {
	if p.Reservations == nil {
		return ""
	}
	r, err := p.Reservations.FindByID(ctx, reservationID)
	if err != nil || r == nil {
		return ""
	}
	return r.SKU().String()
}

// marshalData maps a domain event to its analytics event_type, aggregate-id
// message key, and snake_case JSON payload. The bool return is false for an
// event type outside the analytics contract, so callers can skip it.
func (p *AnalyticsPublisher) marshalData(ctx context.Context, e shared.DomainEvent) (eventType, key string, data json.RawMessage, ok bool) {
	switch ev := e.(type) {
	case shared.StockReceived:
		return "StockReceived", ev.SKU.String(), mustMarshal(map[string]any{
			"sku":      ev.SKU.String(),
			"quantity": ev.Quantity.Int(),
		}), true
	case shared.ItemStowed:
		return "ItemStowed", ev.SKU.String(), mustMarshal(map[string]any{
			"sku":      ev.SKU.String(),
			"bin_id":   ev.BinID.String(),
			"quantity": ev.Quantity.Int(),
		}), true
	case shared.StockPicked:
		return "StockPicked", ev.SKU.String(), mustMarshal(map[string]any{
			"sku":            ev.SKU.String(),
			"reservation_id": ev.ReservationID,
			"quantity":       ev.Quantity.Int(),
		}), true
	case shared.StockReserved:
		return "StockReserved", ev.SKU.String(), mustMarshal(map[string]any{
			"sku":            ev.SKU.String(),
			"reservation_id": ev.ReservationID,
			"quantity":       ev.Quantity.Int(),
		}), true
	case shared.ReservationExpired:
		return "ReservationExpired", ev.ReservationID, mustMarshal(map[string]any{
			"reservation_id": ev.ReservationID,
			"sku":            p.reservationSKU(ctx, ev.ReservationID),
		}), true
	case shared.ReservationRevoked:
		return "ReservationRevoked", ev.ReservationID, mustMarshal(map[string]any{
			"reservation_id": ev.ReservationID,
			"sku":            p.reservationSKU(ctx, ev.ReservationID),
		}), true
	case shared.CycleCountCompleted:
		return "CycleCountCompleted", ev.BinID.String(), mustMarshal(map[string]any{
			"bin_id":      ev.BinID.String(),
			"counted":     ev.CountedQty.Int(),
			"system":      ev.SystemQty.Int(),
			"discrepancy": ev.Discrepancy,
		}), true
	case shared.DiscrepancyDetected:
		return "DiscrepancyDetected", ev.BinID.String(), mustMarshal(map[string]any{
			"bin_id":  ev.BinID.String(),
			"counted": ev.CountedQty.Int(),
			"system":  ev.SystemQty.Int(),
		}), true
	case shared.ItemUnlocated:
		return "ItemUnlocated", ev.SKU.String(), mustMarshal(map[string]any{
			"sku":           ev.SKU.String(),
			"bin_id":        ev.BinID.String(),
			"stock_unit_id": ev.StockUnitID,
			"quantity":      ev.Quantity.Int(),
		}), true
	default:
		// LocationRecorded and any future event outside the analytics
		// contract are acknowledged by the caller but not published.
		return "", "", nil, false
	}
}

// mustMarshal marshals a map whose shape is fully controlled by marshalData,
// so an error here is a programming mistake rather than a runtime condition.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("kafka: marshal analytics data: %v", err))
	}
	return b
}

// Close releases the underlying Kafka writer.
func (p *AnalyticsPublisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

// Compile-time assertion that AnalyticsPublisher satisfies the outbound
// event-publishing port.
var _ ports.EventPublisher = (*AnalyticsPublisher)(nil)
