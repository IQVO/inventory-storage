package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
	if enc.EventType != "StockReserved" {
		t.Errorf("EventType = %q, want StockReserved", enc.EventType)
	}
	if string(enc.Key) != "res-1" {
		t.Errorf("Key = %q, want %q (the reservation id)", string(enc.Key), "res-1")
	}

	var env envelope
	if err := json.Unmarshal(enc.Value, &env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
	if env.EventType != "StockReserved" {
		t.Errorf("env.EventType = %q, want StockReserved", env.EventType)
	}
	if env.Source != kafka.Source {
		t.Errorf("env.Source = %q, want %q", env.Source, kafka.Source)
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

// TestAnalyticsPublisher_Encode_MatchesPublishEnvelope proves Encode alone
// produces the exact same AnalyticsEnvelope shape Publish always has.
func TestAnalyticsPublisher_Encode_MatchesPublishEnvelope(t *testing.T) {
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

	var env kafka.AnalyticsEnvelope
	if err := json.Unmarshal(enc.Value, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.EventId != "evt-fixed" {
		t.Errorf("event_id = %q, want evt-fixed", env.EventId)
	}
	if env.EventType != "StockReceived" {
		t.Errorf("event_type = %q, want StockReceived", env.EventType)
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
