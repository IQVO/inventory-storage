// Package kafka is the inbound Kafka adapter for the inventory-storage
// analytics data product: it consumes the dedicated analytics topic and
// applies each event to the Inventory Flow & Accuracy projection exactly once.
// It depends inward on the analytics read-model region (internal/analytics/report)
// only — never on an OLTP domain or application package (ADR-0011).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/analytics/report"
)

// AnalyticsConsumerGroup is the Kafka consumer group the analytics projector
// reads under. It is distinct from any OLTP consumer group so the analytics
// pipeline tracks its offsets independently.
const AnalyticsConsumerGroup = "inventory-analytics"

// consumerTracerName scopes the consume spans this adapter emits.
const consumerTracerName = "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"

// Full CloudEvents `type` strings this projector dispatches on (ADR-0024).
// The analytics topic carries the SAME type as the integration topic for an
// occurrence; `dataschema` (urn:warehouse:inventory-storage:analytics:...)
// is what names the analytics payload shape.
var (
	typeStockReceived       = cloudevents.Type("stock", "StockReceived")
	typeItemStowed          = cloudevents.Type("stock", "ItemStowed")
	typeItemUnlocated       = cloudevents.Type("stock", "ItemUnlocated")
	typeStockPicked         = cloudevents.Type("reservation", "StockPicked")
	typeStockReserved       = cloudevents.Type("reservation", "StockReserved")
	typeReservationExpired  = cloudevents.Type("reservation", "ReservationExpired")
	typeReservationRevoked  = cloudevents.Type("reservation", "ReservationRevoked")
	typeCycleCountCompleted = cloudevents.Type("bin", "CycleCountCompleted")
	typeDiscrepancyDetected = cloudevents.Type("bin", "DiscrepancyDetected")
)

// analyticsEvent is the decoded view of one analytics CloudEvent the
// projection needs: the `id` (dedupe key), the full `type`, the `time`
// attribute (domain occurred-at) and the payload.
type analyticsEvent struct {
	ID         string
	Type       string
	OccurredAt time.Time
}

// analyticsData is the union of fields the projecting event payloads carry.
// Each event type populates the subset it needs. SKU is enriched onto
// reservation-lifecycle events by the publisher (via a ReservationRepo lookup)
// since those domain events carry only a reservation id.
type analyticsData struct {
	SKU      string `json:"sku"`
	BinId    string `json:"bin_id"`
	Quantity int    `json:"quantity"`
}

// AnalyticsConsumer reads analytics events off the analytics topic and applies
// each to the Inventory Flow & Accuracy ProjectionStore, exactly once per
// CloudEvents id despite Kafka's at-least-once delivery.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  report.ProcessedEvents
	Logger     *slog.Logger
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under AnalyticsConsumerGroup.
func NewAnalyticsConsumer(brokers []string, topic string, projection report.ProjectionStore, processed report.ProcessedEvents, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: AnalyticsConsumerGroup,
		// Start a brand-new consumer group at the EARLIEST offset. The
		// analytics projection must see the full history of the topic (it is a
		// replayable read model, not a live integration reaction), so a fresh
		// projector — or a backfill into a new group — reads from the beginning
		// rather than kafka-go's default of the latest offset, which would
		// silently drop every event produced before the group first committed
		// an offset. Once the group has committed offsets, those take
		// precedence and this only affects the first join.
		StartOffset: kafkago.FirstOffset,
	})
	return &AnalyticsConsumer{Reader: reader, Projection: projection, Processed: processed, Logger: logger}
}

// Run reads and handles messages until ctx is cancelled or the reader returns
// a fatal error. A handling error is logged and the loop continues so one bad
// message cannot wedge the projector.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.Handle(ctx, msg); err != nil {
			c.Logger.ErrorContext(ctx, "analytics message handling failed", "error", err)
		}
	}
}

// Close releases the underlying Kafka reader.
func (c *AnalyticsConsumer) Close() error {
	return c.Reader.Close()
}

// Handle processes one consumed message inside a "kafka.consume <topic>" span
// whose parent is the producer's span, read from the message headers. It is
// exported separately from Run so the propagation can be tested without a live
// broker.
func (c *AnalyticsConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: msg.Headers})

	ctx, span := otel.Tracer(consumerTracerName).Start(ctx,
		"kafka.consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationName("process"),
		),
	)
	defer span.End()

	if err := c.HandleMessage(ctx, msg.Value); err != nil {
		if errors.Is(err, cloudevents.ErrNotCloudEvent) {
			// Deterministic poison message (legacy flat envelope, bad JSON,
			// missing required attribute): this consumer has no DLQ, so it
			// is logged at WARN and skipped — the group offset still
			// advances, the partition is never blocked (ADR-0024 §5).
			c.Logger.WarnContext(ctx, "skipping analytics message that is not a valid CloudEvent",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
			return nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// HandleMessage decodes raw as a CloudEvents 1.0 event (cloudevents.Decode)
// and applies the matching projection method for its full `type`. Types
// outside the projection contract are ignored (and not marked processed).
// For a projecting event it dedupes on the CloudEvents `id` via
// ProcessedEvents before applying, so a redelivery is a no-op. A message
// that is not a valid CloudEvent returns an error wrapping
// cloudevents.ErrNotCloudEvent, which Handle logs and skips. It is exported
// separately from Run so tests can feed raw events without a live broker.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return fmt.Errorf("analytics: %w", err)
	}

	// Only the flow/accuracy-moving events project; everything else (e.g.
	// LocationRecorded, or a type added upstream later) is acknowledged
	// without touching the read model or the processed set.
	if !projects(e.Type()) {
		return nil
	}

	isNew, err := c.Processed.MarkProcessed(ctx, e.ID())
	if err != nil {
		return fmt.Errorf("analytics: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	var data analyticsData
	if err := e.DataAs(&data); err != nil {
		return fmt.Errorf("analytics: decode data: %w", err)
	}

	return c.applyEvent(ctx, analyticsEvent{ID: e.ID(), Type: e.Type(), OccurredAt: e.Time().UTC()}, data)
}

// projects reports whether eventType (a full CloudEvents `type`) moves the
// Inventory Flow & Accuracy projection. Everything else on the analytics
// topic is acknowledged without touching the read model or the processed set.
func projects(eventType string) bool {
	switch eventType {
	case typeStockReceived, typeItemStowed, typeStockPicked, typeStockReserved,
		typeReservationExpired, typeReservationRevoked,
		typeCycleCountCompleted, typeDiscrepancyDetected, typeItemUnlocated:
		return true
	default:
		return false
	}
}

// applyEvent routes one already-deduped, already-decoded projecting event to
// its projection method by its full CloudEvents `type`.
func (c *AnalyticsConsumer) applyEvent(ctx context.Context, ev analyticsEvent, data analyticsData) error {
	switch ev.Type {
	case typeStockReceived:
		return c.Projection.ApplyStockReceived(ctx, ev.ID, data.SKU, data.Quantity, ev.OccurredAt)
	case typeItemStowed:
		return c.Projection.ApplyItemStowed(ctx, ev.ID, data.SKU, data.BinId, ev.OccurredAt)
	case typeStockPicked:
		return c.Projection.ApplyStockPicked(ctx, ev.ID, data.SKU, data.Quantity, ev.OccurredAt)
	case typeStockReserved:
		return c.Projection.ApplyStockReserved(ctx, ev.ID, data.SKU, ev.OccurredAt)
	case typeReservationExpired:
		return c.Projection.ApplyReservationExpired(ctx, ev.ID, data.SKU, ev.OccurredAt)
	case typeReservationRevoked:
		return c.Projection.ApplyReservationRevoked(ctx, ev.ID, data.SKU, ev.OccurredAt)
	case typeCycleCountCompleted:
		return c.Projection.ApplyCycleCountCompleted(ctx, ev.ID, data.BinId, ev.OccurredAt)
	case typeDiscrepancyDetected:
		return c.Projection.ApplyDiscrepancyDetected(ctx, ev.ID, data.BinId, ev.OccurredAt)
	case typeItemUnlocated:
		return c.Projection.ApplyItemUnlocated(ctx, ev.ID, data.SKU, data.BinId, ev.OccurredAt)
	default:
		return nil
	}
}

// headerCarrier adapts a kafka-go header slice to propagation.TextMapCarrier
// for reading the W3C traceparent/tracestate on the consumer side, so the
// consume span becomes a child of the producer's publish span. It is
// read-only: Set is a no-op because the consumer never re-injects.
type headerCarrier struct {
	headers []kafkago.Header
}

var _ propagation.TextMapCarrier = headerCarrier{}

func (c headerCarrier) Get(key string) string {
	for _, h := range c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(string, string) {}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.headers))
	for _, h := range c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}
