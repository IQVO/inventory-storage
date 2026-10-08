// Package kafka publishes cross-service integration events to the shared
// warehouse-systems Kafka broker. It implements ports.EventPublisher, so it
// drops in wherever the log or Postgres outbox publisher is used today.
//
// Only the reservation-lifecycle events (StockReserved, ReservationRevoked),
// the transfer replies, and the receipt/stow facts (ADR 0030, ADR 0033) are
// part of the published integration contract; the legacy ProductClassified
// (ADR 0031) is emitted only by the one-shot republish-product-classifications
// backfill since ADR 0034 handed SKU master data to product-master (see CLAUDE.md's
// Cross-service integration section and apis/asyncapi.yaml); every other
// domain event is a local concern and is not forwarded here.
package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Topic is the integration events topic this service publishes to.
const Topic = "warehouse.inventory.events"

// reservationEntity is the `<entity>` segment of the CloudEvents `type` for
// the reservation-lifecycle and transfer-allocation-reply events (the
// Reservation aggregate raises StockReserved and ReservationRevoked, and
// AllocateTransferStock replies on its stock-holding path — see
// apis/asyncapi.yaml).
const reservationEntity = "reservation"

// productEntity is the `<entity>` segment for ProductClassified, raised by
// the ProductClassification master-data aggregate (ADR 0033, ADR 0024's
// reserved name).
const productEntity = "product"

// stockEntity is the `<entity>` segment for the destination receipt/stow
// custody events (ADR 0033): TransferReceiptStaged and TransferStockStowed
// are facts about stock becoming (or preparing to become) usable at the
// destination site.
const stockEntity = "stock"

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

// reservationData is the `data` payload shape for StockReserved and
// ReservationRevoked, per CLAUDE.md.
type reservationData struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demand_ref"`
}

// transferAllocatedData is the `data` payload for
// TransferStockAllocated (v1, frozen).
type transferAllocatedData struct {
	TransferID     string                     `json:"transfer_id"`
	TransferLineID string                     `json:"transfer_line_id"`
	OriginSiteID   string                     `json:"origin_site_id"`
	ReservationID  string                     `json:"reservation_id"`
	SKU            string                     `json:"sku"`
	Quantity       int                        `json:"quantity"`
	Allocations    []transferAllocationLegOut `json:"allocations"`
	ExpiresAt      time.Time                  `json:"expires_at"`
}

type transferAllocationLegOut struct {
	StockUnitID string `json:"stock_unit_id"`
	BinID       string `json:"bin_id"`
	Quantity    int    `json:"quantity"`
}

// transferRejectedData is the `data` payload for
// TransferStockAllocationRejected (v1, frozen).
type transferRejectedData struct {
	TransferID        string `json:"transfer_id"`
	TransferLineID    string `json:"transfer_line_id"`
	OriginSiteID      string `json:"origin_site_id"`
	SKU               string `json:"sku"`
	RequestedQuantity int    `json:"requested_quantity"`
	Reason            string `json:"reason"`
}

// productClassifiedData is the `data` payload for ProductClassified on BOTH
// topics (v1; ADR 0033). It is the SKU master-data classification as a
// full-state replacement: a consumer keeping a local copy overwrites its
// row with this, and an absent optional field means "none". No PII — SKU
// handling attributes only.
type productClassifiedData struct {
	SKU              string   `json:"sku"`
	HandlingTags     []string `json:"handling_tags"`
	TemperatureClass string   `json:"temperature_class,omitempty"`
	DOTHazardClass   int      `json:"dot_hazard_class,omitempty"`
}

// newProductClassifiedData maps the domain event to its wire payload.
// HandlingTags arrive in the aggregate's stable enum order.
func newProductClassifiedData(e product.ProductClassified) productClassifiedData {
	tags := make([]string, 0, len(e.HandlingTags))
	for _, tag := range e.HandlingTags {
		tags = append(tags, string(tag))
	}
	return productClassifiedData{
		SKU:              e.SKU.String(),
		HandlingTags:     tags,
		TemperatureClass: string(e.TemperatureClass),
		DOTHazardClass:   int(e.DOTHazardClass),
	}
}

// receiptStagedData is the `data` payload for TransferReceiptStaged
// (v1, frozen — ADR 0033).
type receiptStagedData struct {
	TransferID        string `json:"transfer_id"`
	TransferLineID    string `json:"transfer_line_id"`
	DestinationSiteID string `json:"destination_site_id"`
	SKU               string `json:"sku"`
	ExpectedQuantity  int    `json:"expected_quantity"`
	ReceivedQuantity  int    `json:"received_quantity"`
	Variance          int    `json:"variance"`
}

// stockStowedData is the `data` payload for TransferStockStowed
// (v1, frozen — ADR 0033).
type stockStowedData struct {
	TransferID        string                     `json:"transfer_id"`
	TransferLineID    string                     `json:"transfer_line_id"`
	DestinationSiteID string                     `json:"destination_site_id"`
	SKU               string                     `json:"sku"`
	ReceivedQuantity  int                        `json:"received_quantity"`
	StowedQuantity    int                        `json:"stowed_quantity"`
	Allocations       []transferAllocationLegOut `json:"allocations"`
}

// Publisher publishes StockReserved and ReservationRevoked domain events as
// integration events on Topic. It satisfies both ports.EventPublisher
// (direct publish) and kafka.Encoder (used by postgres.OutboxPublisher to
// enqueue the wire-ready message inside a transaction — ADR 0017).
type Publisher struct {
	writer       Writer
	reservations ports.ReservationRepo
	// NewID mints the CloudEvents `id`. nil means a random UUID v4. The id
	// is minted ONCE in Encode and baked into the message value, so the
	// outbox row persists it and every relay redelivery carries the same id.
	NewID func() string
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
		BatchTimeout:           syncWriterBatchTimeout,
		RequiredAcks:           syncWriterRequiredAcks,
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  Topic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}
}

// isIntegrationEvent reports whether event is one of the types this
// publisher forwards, without doing any repo lookup — a cheap pre-check
// so Publish never opens a producer span for an event it will not send.
func isIntegrationEvent(event shared.DomainEvent) bool {
	switch event.(type) {
	case shared.StockReserved, shared.ReservationRevoked,
		shared.TransferStockAllocated, shared.TransferStockAllocationRejected,
		shared.TransferReceiptStaged, shared.TransferStockStowed,
		product.ProductClassified:
		return true
	default:
		return false
	}
}

// Encode maps event onto its integration wire form: a CloudEvents 1.0
// structured-mode event (ADR-0024) whose `data` is the event payload, the
// `content-type: application/cloudevents+json` header, and W3C trace headers injected from whatever span
// is active on ctx. The Kafka message Key is always the reservation
// aggregate id (ReservationID) — StockReserved and ReservationRevoked for the
// SAME reservation must land on the same partition so a consumer never
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
	data, key, entity, err := encodeEvent(ctx, p, event)
	if err != nil {
		return nil, err
	}
	if data == nil {
		// Not part of this publisher's contract: no messages, no error.
		return nil, nil
	}

	msg, err := cloudevents.New(cloudevents.Spec{
		ID:        newEventID(p.NewID),
		Entity:    entity,
		EventName: event.EventName(),
		Subject:   key,
		Time:      event.OccurredAt(),
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		return nil, err
	}

	// Inject whatever span is active on ctx: for a direct publish that is
	// the just-started publish span (see Publish below), so a downstream
	// consumer's Extract parents onto it.
	headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	return []Encoded{{Topic: Topic, EventType: cloudevents.Type(entity, event.EventName()), Key: []byte(key), Value: msg, Headers: headers}}, nil
}

// encodeEvent maps one domain event onto its wire data payload, Kafka
// key, and CloudEvents entity segment. A nil data return (with nil
// error) means the event is not part of this publisher's contract — the
// caller returns no messages for it. ReservationRevoked is the one
// event that needs a repo lookup to fill its payload, so it takes the
// publisher and can fail with ErrReservationNotFound.
func encodeEvent(ctx context.Context, p *Publisher, event shared.DomainEvent) (data any, key, entity string, err error) {
	entity = reservationEntity

	switch e := event.(type) {
	case shared.StockReserved:
		data = reservationData{SKU: e.SKU.String(), Quantity: e.Quantity.Int(), DemandRef: e.DemandRef}
		key = e.ReservationID
	case shared.ReservationRevoked:
		res, err := p.reservations.FindByID(ctx, e.ReservationID)
		if err != nil {
			return nil, "", "", err
		}
		if res == nil {
			return nil, "", "", ErrReservationNotFound
		}
		data = reservationData{SKU: res.SKU().String(), Quantity: res.Quantity().Int(), DemandRef: res.DemandRef()}
		key = e.ReservationID
	case shared.TransferStockAllocated:
		legs := make([]transferAllocationLegOut, 0, len(e.Allocations))
		for _, leg := range e.Allocations {
			legs = append(legs, transferAllocationLegOut{StockUnitID: leg.StockUnitID, BinID: leg.BinID.String(), Quantity: leg.Quantity.Int()})
		}
		data = transferAllocatedData{
			TransferID:     e.TransferID,
			TransferLineID: e.TransferLineID,
			OriginSiteID:   e.OriginSiteID.String(),
			ReservationID:  e.ReservationID,
			SKU:            e.SKU.String(),
			Quantity:       e.Quantity.Int(),
			Allocations:    legs,
			ExpiresAt:      e.ExpiresAt.UTC(),
		}
		// Key = subject = reservation_id: the transfer saga's replies for
		// one reservation stay ordered on one partition (ADR-0021).
		key = e.ReservationID
	case shared.TransferStockAllocationRejected:
		data = transferRejectedData{
			TransferID:        e.TransferID,
			TransferLineID:    e.TransferLineID,
			OriginSiteID:      e.OriginSiteID.String(),
			SKU:               e.SKU.String(),
			RequestedQuantity: e.RequestedQuantity.Int(),
			Reason:            e.Reason,
		}
		// Key = subject = transfer_line_id: a rejection has no
		// reservation, so the line is the aggregate the reply is about.
		key = e.TransferLineID
	case product.ProductClassified:
		data = newProductClassifiedData(e)
		// Key = subject = SKU: reclassifications of one SKU stay ordered
		// on one partition, so a consumer's local copy converges on the
		// last write (ADR-0021).
		entity = productEntity
		key = e.SKU.String()
	case shared.TransferReceiptStaged:
		data = receiptStagedData{
			TransferID:        e.TransferID,
			TransferLineID:    e.TransferLineID,
			DestinationSiteID: e.DestinationSiteID.String(),
			SKU:               e.SKU.String(),
			ExpectedQuantity:  e.ExpectedQuantity.Int(),
			ReceivedQuantity:  e.ReceivedQuantity.Int(),
			Variance:          e.Variance,
		}
		// Key = subject = transfer_line_id: the receipt (and every later
		// fact about this line, including its stow) stays ordered on one
		// partition (ADR 0021).
		key = e.TransferLineID
		entity = stockEntity
	case shared.TransferStockStowed:
		data = stockStowedData{
			TransferID:        e.TransferID,
			TransferLineID:    e.TransferLineID,
			DestinationSiteID: e.DestinationSiteID.String(),
			SKU:               e.SKU.String(),
			ReceivedQuantity:  e.ReceivedQuantity.Int(),
			StowedQuantity:    e.StowedQuantity.Int(),
			Allocations:       transferLegsOut(e.Allocations),
		}
		// Key = subject = transfer_line_id, same as the staged receipt:
		// stage-then-stow for one line lands in order on one partition.
		key = e.TransferLineID
		entity = stockEntity
	default:
		return nil, "", "", nil
	}
	return data, key, entity, nil
}

// transferLegsOut maps domain allocation legs onto their wire shape.
func transferLegsOut(legs []shared.TransferAllocationLeg) []transferAllocationLegOut {
	out := make([]transferAllocationLegOut, 0, len(legs))
	for _, leg := range legs {
		out = append(out, transferAllocationLegOut{StockUnitID: leg.StockUnitID, BinID: leg.BinID.String(), Quantity: leg.Quantity.Int()})
	}
	return out
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

// newEventID mints a CloudEvents `id` with gen, defaulting to a random
// UUID v4 when gen is nil.
func newEventID(gen func() string) string {
	if gen != nil {
		return gen()
	}
	return uuid.NewString()
}

// Close releases the underlying Kafka writer.
func (p *Publisher) Close() error {
	if w, ok := p.writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}
