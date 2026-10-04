package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// AnalyticsTopic is the dedicated topic the Inventory Flow & Accuracy data
// product consumes. It is separate from the integration topic (Topic) so the
// OLTP integration contract and the analytical read-model stream evolve
// independently (ADR-0011).
const AnalyticsTopic = "warehouse.inventory.analytics"

// analyticsSchemaVersion is the `v<N>` of every analytics `dataschema`
// (urn:warehouse:inventory-storage:analytics:<EventName>:v1). It replaces the
// retired envelope-level schema_version field (ADR-0024).
const analyticsSchemaVersion = 1

// analyticsTracerName scopes the analytics publish spans this adapter emits.
const analyticsTracerName = "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"

// analyticsEvent is one analytics occurrence ready to wrap in a CloudEvent:
// the CloudEvents `type` entity segment, the aggregate-id `subject`, the
// unchanged Kafka partition key, and the payload object.
type analyticsEvent struct {
	entity  string
	subject string
	key     string
	data    map[string]any
}

// AnalyticsPublisher publishes every inventory-storage domain event onto
// AnalyticsTopic as a CloudEvents 1.0 event (ADR-0024). It satisfies ports.EventPublisher
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
// AnalyticsTopic on brokers. newId mints the CloudEvents `id`; reservations
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
			BatchTimeout:           syncWriterBatchTimeout,
			RequiredAcks:           syncWriterRequiredAcks,
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
	ae, ok := p.analyticsEventFor(ctx, event)
	if !ok {
		return nil, nil
	}
	subject := ae.subject
	if subject == "" {
		subject = ae.key
	}
	payload, err := cloudevents.New(cloudevents.Spec{
		ID:        newEventID(p.NewId),
		Entity:    ae.entity,
		EventName: event.EventName(),
		Subject:   subject,
		Time:      event.OccurredAt(),
		Stream:    cloudevents.StreamAnalytics,
		Version:   analyticsSchemaVersion,
		Data:      ae.data,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka: encode analytics cloudevent: %w", err)
	}

	headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	return []Encoded{{
		Topic:     AnalyticsTopic,
		EventType: cloudevents.Type(ae.entity, event.EventName()),
		Key:       []byte(ae.key),
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

// analyticsEventFor maps a domain event to its analytics CloudEvent parts:
// the `type` entity segment (stock | reservation | bin, per
// apis/asyncapi.yaml), the aggregate-id `subject`, the Kafka partition key
// and the snake_case payload. The partition key follows ADR-0021: every event
// of one reservation's lifecycle (StockReserved, StockPicked,
// ReservationRevoked, ReservationExpired) is keyed by the RESERVATION id so
// they all land on one partition and a consumer sees them in order; only
// events with no reservation (StockReceived, ItemStowed, ItemUnlocated: SKU;
// cycle-count events: bin id) are keyed by their own aggregate.
// The bool return is false for an event outside the analytics contract, so
// callers can skip it.
func (p *AnalyticsPublisher) analyticsEventFor(ctx context.Context, e shared.DomainEvent) (analyticsEvent, bool) {
	switch ev := e.(type) {
	case shared.StockReceived:
		return analyticsEvent{entity: "stock", subject: ev.SKU.String(), key: ev.SKU.String(), data: map[string]any{
			"sku":      ev.SKU.String(),
			"quantity": ev.Quantity.Int(),
		}}, true
	case shared.ItemStowed:
		return analyticsEvent{entity: "stock", subject: ev.SKU.String(), key: ev.SKU.String(), data: map[string]any{
			"sku":      ev.SKU.String(),
			"bin_id":   ev.BinID.String(),
			"quantity": ev.Quantity.Int(),
		}}, true
	case shared.StockPicked:
		return analyticsEvent{entity: "reservation", subject: ev.ReservationID, key: ev.ReservationID, data: map[string]any{
			"sku":            ev.SKU.String(),
			"reservation_id": ev.ReservationID,
			"quantity":       ev.Quantity.Int(),
		}}, true
	case shared.StockReserved:
		return analyticsEvent{entity: "reservation", subject: ev.ReservationID, key: ev.ReservationID, data: map[string]any{
			"sku":            ev.SKU.String(),
			"reservation_id": ev.ReservationID,
			"quantity":       ev.Quantity.Int(),
		}}, true
	case shared.ReservationExpired:
		return analyticsEvent{entity: "reservation", subject: ev.ReservationID, key: ev.ReservationID, data: map[string]any{
			"reservation_id": ev.ReservationID,
			"sku":            p.reservationSKU(ctx, ev.ReservationID),
		}}, true
	case shared.ReservationRevoked:
		return analyticsEvent{entity: "reservation", subject: ev.ReservationID, key: ev.ReservationID, data: map[string]any{
			"reservation_id": ev.ReservationID,
			"sku":            p.reservationSKU(ctx, ev.ReservationID),
		}}, true
	default:
		return p.binOrUnlocatedEvent(e)
	}
}

// binOrUnlocatedEvent covers the cycle-count events (Bin aggregate) and
// ItemUnlocated, split out of analyticsEventFor to keep each switch small.
func (p *AnalyticsPublisher) binOrUnlocatedEvent(e shared.DomainEvent) (analyticsEvent, bool) {
	switch ev := e.(type) {
	case shared.CycleCountCompleted:
		return analyticsEvent{entity: "bin", subject: ev.BinID.String(), key: ev.BinID.String(), data: map[string]any{
			"bin_id":      ev.BinID.String(),
			"counted":     ev.CountedQty.Int(),
			"system":      ev.SystemQty.Int(),
			"discrepancy": ev.Discrepancy,
		}}, true
	case shared.DiscrepancyDetected:
		return analyticsEvent{entity: "bin", subject: ev.BinID.String(), key: ev.BinID.String(), data: map[string]any{
			"bin_id":  ev.BinID.String(),
			"counted": ev.CountedQty.Int(),
			"system":  ev.SystemQty.Int(),
		}}, true
	case shared.ItemUnlocated:
		return analyticsEvent{entity: "stock", subject: ev.StockUnitID, key: ev.SKU.String(), data: map[string]any{
			"sku":           ev.SKU.String(),
			"bin_id":        ev.BinID.String(),
			"stock_unit_id": ev.StockUnitID,
			"quantity":      ev.Quantity.Int(),
		}}, true
	default:
		// LocationRecorded and any future event outside the analytics
		// contract are acknowledged by the caller but not published.
		return analyticsEvent{}, false
	}
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
