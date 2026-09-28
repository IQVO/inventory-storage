package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// fakeWriter captures every message it's asked to write, so tests can assert
// on the exact envelope shape without a real broker.
type fakeWriter struct {
	messages []kafkago.Message
	err      error
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if w.err != nil {
		return w.err
	}
	w.messages = append(w.messages, msgs...)
	return nil
}

type envelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Source     string          `json:"source"`
	Data       json.RawMessage `json:"data"`
}

type reservationData struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demand_ref"`
}

func TestPublisher_StockReserved_EnvelopeShape(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer, memory.NewReservationRepo())

	sku, _ := shared.NewSKU("SKU-1")
	qty, _ := shared.NewPositiveQuantity(5)
	occurredAt := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)
	event := shared.NewStockReserved(occurredAt, "res-1", sku, qty, "order-42")

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}

	var env envelope
	if err := json.Unmarshal(writer.messages[0].Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}

	if env.EventType != "StockReserved" {
		t.Errorf("EventType = %q, want StockReserved", env.EventType)
	}
	if env.Source != kafka.Source {
		t.Errorf("Source = %q, want %q", env.Source, kafka.Source)
	}
	if !env.OccurredAt.Equal(occurredAt) {
		t.Errorf("OccurredAt = %v, want %v", env.OccurredAt, occurredAt)
	}
	if env.EventID == "" {
		t.Error("EventID must not be empty")
	}

	var data reservationData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	if data.SKU != "SKU-1" || data.Quantity != 5 || data.DemandRef != "order-42" {
		t.Errorf("data = %+v, want {SKU-1 5 order-42}", data)
	}
	if string(writer.messages[0].Key) != "res-1" {
		t.Errorf("Key = %q, want %q (the reservation id)", string(writer.messages[0].Key), "res-1")
	}
}

func TestPublisher_ReservationRevoked_EnvelopeShape(t *testing.T) {
	writer := &fakeWriter{}
	repo := memory.NewReservationRepo()

	sku, _ := shared.NewSKU("SKU-2")
	qty, _ := shared.NewPositiveQuantity(3)
	res, err := reservation.New("res-2", sku, qty, "order-99",
		[]reservation.Allocation{{StockUnitID: "unit-1", Quantity: qty}},
		time.Date(2026, 8, 21, 21, 0, 0, 0, time.UTC), time.Hour)
	if err != nil {
		t.Fatalf("failed to build reservation: %v", err)
	}
	if err := res.Revoke(); err != nil {
		t.Fatalf("failed to revoke reservation: %v", err)
	}
	if err := repo.Save(context.Background(), res); err != nil {
		t.Fatalf("failed to save reservation: %v", err)
	}

	pub := kafka.NewPublisher(writer, repo)

	occurredAt := time.Date(2026, 8, 21, 22, 5, 0, 0, time.UTC)
	event := shared.NewReservationRevoked(occurredAt, "res-2")

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}

	var env envelope
	if err := json.Unmarshal(writer.messages[0].Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
	if env.EventType != "ReservationRevoked" {
		t.Errorf("EventType = %q, want ReservationRevoked", env.EventType)
	}

	var data reservationData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}
	if data.SKU != "SKU-2" || data.Quantity != 3 || data.DemandRef != "order-99" {
		t.Errorf("data = %+v, want {SKU-2 3 order-99}", data)
	}
	if string(writer.messages[0].Key) != "res-2" {
		t.Errorf("Key = %q, want %q (the reservation id)", string(writer.messages[0].Key), "res-2")
	}
}

// TestPublisher_SameReservation_AllEventsShareTheSameKey proves every
// integration event for the SAME reservation carries an identical Kafka
// message Key, regardless of event type. With the shared broker's business
// topics now at 8 partitions (up from 1 — the partition-scaleup that
// exposed this gap, see ADR-0021), a stable per-aggregate key is what
// guarantees StockReserved and a later ReservationRevoked for that exact
// reservation land on the same partition, and so are never observed out of
// order by a consumer. A round-robin/unkeyed publish would scatter them
// across up to 8 partitions in publish order instead.
func TestPublisher_SameReservation_AllEventsShareTheSameKey(t *testing.T) {
	writer := &fakeWriter{}
	repo := memory.NewReservationRepo()

	sku, _ := shared.NewSKU("SKU-9")
	qty, _ := shared.NewPositiveQuantity(7)
	res, err := reservation.New("res-shared", sku, qty, "order-shared",
		[]reservation.Allocation{{StockUnitID: "unit-9", Quantity: qty}},
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Hour)
	if err != nil {
		t.Fatalf("failed to build reservation: %v", err)
	}
	if err := repo.Save(context.Background(), res); err != nil {
		t.Fatalf("failed to save reservation: %v", err)
	}

	pub := kafka.NewPublisher(writer, repo)

	// StockReserved first, for the same reservation id.
	stockReserved := shared.NewStockReserved(time.Now(), "res-shared", sku, qty, "order-shared")
	if err := pub.Publish(context.Background(), stockReserved); err != nil {
		t.Fatalf("Publish StockReserved returned error: %v", err)
	}

	// Then the reservation is revoked; the repo must reflect that for the
	// revoked-lookup enrichment to succeed.
	if err := res.Revoke(); err != nil {
		t.Fatalf("failed to revoke reservation: %v", err)
	}
	if err := repo.Save(context.Background(), res); err != nil {
		t.Fatalf("failed to re-save revoked reservation: %v", err)
	}
	revoked := shared.NewReservationRevoked(time.Now(), "res-shared")
	if err := pub.Publish(context.Background(), revoked); err != nil {
		t.Fatalf("Publish ReservationRevoked returned error: %v", err)
	}

	if len(writer.messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(writer.messages))
	}

	firstKey := string(writer.messages[0].Key)
	secondKey := string(writer.messages[1].Key)
	if firstKey == "" {
		t.Fatal("expected a non-empty partition key on the StockReserved message")
	}
	if firstKey != secondKey {
		t.Errorf("Key mismatch across events for the same reservation: StockReserved Key = %q, ReservationRevoked Key = %q", firstKey, secondKey)
	}
	if firstKey != "res-shared" {
		t.Errorf("Key = %q, want %q (the reservation id)", firstKey, "res-shared")
	}
}

func TestPublisher_ReservationRevoked_UnknownReservation(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer, memory.NewReservationRepo())

	event := shared.NewReservationRevoked(time.Now(), "does-not-exist")
	if err := pub.Publish(context.Background(), event); err != kafka.ErrReservationNotFound {
		t.Errorf("Publish error = %v, want ErrReservationNotFound", err)
	}
	if len(writer.messages) != 0 {
		t.Errorf("expected no message written, got %d", len(writer.messages))
	}
}

func TestPublisher_IgnoresOtherDomainEvents(t *testing.T) {
	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer, memory.NewReservationRepo())

	sku, _ := shared.NewSKU("SKU-3")
	qty, _ := shared.NewPositiveQuantity(1)
	event := shared.NewStockReceived(time.Now(), sku, qty)

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(writer.messages) != 0 {
		t.Errorf("expected no message written for a non-integration event, got %d", len(writer.messages))
	}
}

// TestPublisher_InjectsTraceContextIntoHeaders proves the published message
// carries the W3C traceparent for the publish span, which is what lets
// wes-work-planning's consumer parent its span onto this one. It also pins
// the span's name and messaging attributes, since those are the fleet-wide
// convention the other four services follow.
func TestPublisher_InjectsTraceContextIntoHeaders(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	installTracing(t, sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))

	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer, memory.NewReservationRepo())

	sku, _ := shared.NewSKU("SKU-1")
	qty, _ := shared.NewPositiveQuantity(5)
	event := shared.NewStockReserved(time.Now(), "res-1", sku, qty, "order-42")

	ctx, traceID, spanID := publishParentContext(t)

	if err := pub.Publish(ctx, event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}

	traceparent := headerValue(writer.messages[0].Headers, "traceparent")
	if traceparent == "" {
		t.Fatalf("no traceparent header on the published message: %+v", writer.messages[0].Headers)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	published := spans[0]

	assertPublishSpan(t, published, spanID)

	// The traceparent must carry the shared trace id and the publish span's
	// own id — a consumer extracting it lands on this trace, as a child of
	// this span rather than a sibling.
	want := "00-" + traceID.String() + "-" + published.SpanContext().SpanID().String() + "-01"
	if traceparent != want {
		t.Errorf("traceparent = %q, want %q", traceparent, want)
	}

	assertMessagingAttributes(t, published)
}

// publishParentContext builds a context carrying a fixed, sampled span
// context, so the publish span's parent linkage can be asserted without an
// SDK exporter. It returns the context plus the parent's trace and span ids,
// for comparison against what the publisher stamped.
func publishParentContext(t *testing.T) (context.Context, trace.TraceID, trace.SpanID) {
	t.Helper()

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))
	return ctx, traceID, spanID
}

// headerValue returns the value of the last header named key, or "" when no
// such header is present.
func headerValue(headers []kafkago.Header, key string) string {
	value := ""
	for _, h := range headers {
		if h.Key == key {
			value = string(h.Value)
		}
	}
	return value
}

// assertPublishSpan pins the publish span's name, kind and parent — the
// fleet-wide convention the other four services follow.
func assertPublishSpan(t *testing.T, span sdktrace.ReadOnlySpan, parentSpanID trace.SpanID) {
	t.Helper()

	if span.Name() != "kafka.publish "+kafka.Topic {
		t.Errorf("span name = %q, want %q", span.Name(), "kafka.publish "+kafka.Topic)
	}
	if span.SpanKind() != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want producer", span.SpanKind())
	}
	if span.Parent().SpanID() != parentSpanID {
		t.Errorf("publish span parent = %s, want the caller's span %s", span.Parent().SpanID(), parentSpanID)
	}
}

// assertMessagingAttributes pins the publish span's messaging attributes.
func assertMessagingAttributes(t *testing.T, span sdktrace.ReadOnlySpan) {
	t.Helper()

	attrs := map[string]string{}
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}
	if attrs["messaging.system"] != "kafka" {
		t.Errorf("messaging.system = %q, want kafka", attrs["messaging.system"])
	}
	if attrs["messaging.destination.name"] != kafka.Topic {
		t.Errorf("messaging.destination.name = %q, want %q", attrs["messaging.destination.name"], kafka.Topic)
	}
	if attrs["messaging.message.event_type"] != "StockReserved" {
		t.Errorf("messaging.message.event_type = %q, want StockReserved", attrs["messaging.message.event_type"])
	}
}

// installTracing points the globals at tp and the W3C propagator for the
// duration of one test, restoring whatever was there before.
func installTracing(t *testing.T, tp trace.TracerProvider) {
	t.Helper()

	previousTracer := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Cleanup(func() {
		otel.SetTracerProvider(previousTracer)
		otel.SetTextMapPropagator(previousPropagator)
	})
}

// TestPublisher_NoTraceContextWithoutASpan covers the un-instrumented case —
// no Setup, so the global provider is the no-op one. Publishing must still
// work and must leave the headers clean rather than stamping on an all-zero
// traceparent that a consumer would try to parent onto.
func TestPublisher_NoTraceContextWithoutASpan(t *testing.T) {
	installTracing(t, noop.NewTracerProvider())

	writer := &fakeWriter{}
	pub := kafka.NewPublisher(writer, memory.NewReservationRepo())

	sku, _ := shared.NewSKU("SKU-1")
	qty, _ := shared.NewPositiveQuantity(5)
	event := shared.NewStockReserved(time.Now(), "res-1", sku, qty, "order-42")

	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	for _, h := range writer.messages[0].Headers {
		if h.Key == "traceparent" {
			t.Errorf("unexpected traceparent header with no active span: %q", h.Value)
		}
	}
}
