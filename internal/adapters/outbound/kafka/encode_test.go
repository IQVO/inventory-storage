package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// TestPublisher_Encode_StockReserved proves Encode alone (no Send/Writer
// involved) produces the same integration envelope shape Publish always
// has — this is what postgres.OutboxPublisher stores in outbox_events.
func TestPublisher_Encode_StockReserved(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())

	sku, _ := shared.NewSKU("SKU-1")
	qty, _ := shared.NewPositiveQuantity(5)
	occurredAt := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)
	event := shared.NewStockReserved(occurredAt, "res-1", sku, qty, "order-42")

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 encoded message, got %d", len(encoded))
	}
	enc := encoded[0]
	if enc.Topic != kafka.Topic {
		t.Errorf("Topic = %q, want %q", enc.Topic, kafka.Topic)
	}
	const wantType = "com.warehouse.wms.inventory-storage.reservation.StockReserved"
	if enc.EventType != wantType {
		t.Errorf("EventType = %q, want %q", enc.EventType, wantType)
	}
	if string(enc.Key) != "res-1" {
		t.Errorf("Key = %q, want %q (the reservation id)", string(enc.Key), "res-1")
	}

	e, err := cloudevents.Decode(enc.Value)
	if err != nil {
		t.Fatalf("encoded value is not a valid CloudEvent: %v", err)
	}
	if e.Type() != wantType {
		t.Errorf("type = %q, want %q", e.Type(), wantType)
	}
	if e.Source() != cloudevents.Source {
		t.Errorf("source = %q, want %q", e.Source(), cloudevents.Source)
	}
	if headerValue(enc.Headers, "content-type") != cloudevents.MediaType {
		t.Errorf("missing content-type header: %+v", enc.Headers)
	}
}

// TestPublisher_Encode_IgnoresOtherDomainEvents mirrors
// TestPublisher_IgnoresOtherDomainEvents but at the Encode layer: an event
// outside the integration contract encodes to zero messages rather than
// erroring, so the outbox publisher can hand it every event indiscriminately.
func TestPublisher_Encode_IgnoresOtherDomainEvents(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())

	sku, _ := shared.NewSKU("SKU-3")
	qty, _ := shared.NewPositiveQuantity(1)
	event := shared.NewStockReceived(time.Now(), sku, qty)

	encoded, err := pub.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	if len(encoded) != 0 {
		t.Errorf("expected 0 encoded messages for a non-integration event, got %d", len(encoded))
	}
}

// TestPublisher_Encode_ReservationRevoked_UnknownReservation mirrors the
// existing Publish-level test at the Encode layer.
func TestPublisher_Encode_ReservationRevoked_UnknownReservation(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())

	event := shared.NewReservationRevoked(time.Now(), "does-not-exist")
	if _, err := pub.Encode(context.Background(), event); err != kafka.ErrReservationNotFound {
		t.Errorf("Encode error = %v, want ErrReservationNotFound", err)
	}
}

// TestAnalyticsPublisher_Encode_MatchesPublishCloudEvent proves Encode alone
// produces the same analytics CloudEvent Publish sends.
func TestAnalyticsPublisher_Encode_MatchesPublishCloudEvent(t *testing.T) {
	p := kafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, func() string { return "evt-fixed" })

	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sku, _ := shared.NewSKU("SKU-1")
	qty, _ := shared.NewPositiveQuantity(10)
	event := shared.NewStockReceived(at, sku, qty)

	encoded, err := p.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 encoded message, got %d", len(encoded))
	}
	enc := encoded[0]
	if enc.Topic != kafka.AnalyticsTopic {
		t.Errorf("Topic = %q, want %q", enc.Topic, kafka.AnalyticsTopic)
	}
	if string(enc.Key) != "SKU-1" {
		t.Errorf("Key = %q, want SKU-1", string(enc.Key))
	}

	e, err := cloudevents.Decode(enc.Value)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.ID() != "evt-fixed" {
		t.Errorf("id = %q, want evt-fixed", e.ID())
	}
	if e.Type() != "com.warehouse.wms.inventory-storage.stock.StockReceived" {
		t.Errorf("type = %q", e.Type())
	}
}

// TestAnalyticsPublisher_Encode_SkipsUnpublishedEvents mirrors the
// Publish-level skip test at the Encode layer.
func TestAnalyticsPublisher_Encode_SkipsUnpublishedEvents(t *testing.T) {
	p := kafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, func() string { return "evt" })

	binID, _ := shared.NewBinId("BIN-A")
	encoded, err := p.Encode(context.Background(), shared.NewLocationRecorded(time.Now(), "su-1", binID))
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	if len(encoded) != 0 {
		t.Errorf("expected 0 encoded messages for LocationRecorded, got %d", len(encoded))
	}
}

// TestRelaySink_Send_EmptyBatch_NoOp proves the relay sink safely no-ops
// on an empty batch rather than issuing a zero-message write.
func TestRelaySink_Send_EmptyBatch_NoOp(t *testing.T) {
	sink := kafka.NewRelaySink([]string{"127.0.0.1:0"})
	if err := sink.Send(context.Background()); err != nil {
		t.Fatalf("expected no error on an empty batch, got %v", err)
	}
}
