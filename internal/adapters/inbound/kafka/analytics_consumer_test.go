package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
)

// call captures one projection-store method invocation.
type call struct {
	method  string
	eventId string
	sku     string
	binId   string
	qty     int
	at      time.Time
}

// fakeProjection records the calls the consumer makes so a test can assert the
// envelope was routed to the right method with the right fields.
type fakeProjection struct {
	calls []call
}

func (f *fakeProjection) ApplyStockReceived(_ context.Context, eventId, sku string, qty int, at time.Time) error {
	f.calls = append(f.calls, call{"received", eventId, sku, "", qty, at})
	return nil
}
func (f *fakeProjection) ApplyItemStowed(_ context.Context, eventId, sku, binId string, at time.Time) error {
	f.calls = append(f.calls, call{"stowed", eventId, sku, binId, 0, at})
	return nil
}
func (f *fakeProjection) ApplyStockPicked(_ context.Context, eventId, sku string, qty int, at time.Time) error {
	f.calls = append(f.calls, call{"picked", eventId, sku, "", qty, at})
	return nil
}
func (f *fakeProjection) ApplyStockReserved(_ context.Context, eventId, sku string, at time.Time) error {
	f.calls = append(f.calls, call{"reserved", eventId, sku, "", 0, at})
	return nil
}
func (f *fakeProjection) ApplyReservationExpired(_ context.Context, eventId, sku string, at time.Time) error {
	f.calls = append(f.calls, call{"expired", eventId, sku, "", 0, at})
	return nil
}
func (f *fakeProjection) ApplyReservationRevoked(_ context.Context, eventId, sku string, at time.Time) error {
	f.calls = append(f.calls, call{"revoked", eventId, sku, "", 0, at})
	return nil
}
func (f *fakeProjection) ApplyCycleCountCompleted(_ context.Context, eventId, binId string, at time.Time) error {
	f.calls = append(f.calls, call{"cycle", eventId, "", binId, 0, at})
	return nil
}
func (f *fakeProjection) ApplyDiscrepancyDetected(_ context.Context, eventId, binId string, at time.Time) error {
	f.calls = append(f.calls, call{"discrepancy", eventId, "", binId, 0, at})
	return nil
}
func (f *fakeProjection) ApplyItemUnlocated(_ context.Context, eventId, sku, binId string, at time.Time) error {
	f.calls = append(f.calls, call{"unlocated", eventId, sku, binId, 0, at})
	return nil
}

// fakeProcessed is an in-memory report.ProcessedEvents.
type fakeProcessed struct {
	seen map[string]bool
}

func newFakeProcessed() *fakeProcessed { return &fakeProcessed{seen: map[string]bool{}} }

func (p *fakeProcessed) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if p.seen[eventId] {
		return false, nil
	}
	p.seen[eventId] = true
	return true, nil
}

// cloudEvent builds a CloudEvents 1.0 analytics message exactly as the
// outbound AnalyticsPublisher does, via the shared helper.
func cloudEvent(t *testing.T, id, entity, eventName string, at time.Time, data map[string]any) []byte {
	t.Helper()
	b, err := cloudevents.New(cloudevents.Spec{
		ID:        id,
		Entity:    entity,
		EventName: eventName,
		Subject:   "subject-1",
		Time:      at,
		Stream:    cloudevents.StreamAnalytics,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		t.Fatalf("cloudevents.New: %v", err)
	}
	return b
}

func TestAnalyticsConsumer_RoutesEachEventType(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		entity     string
		eventType  string
		data       map[string]any
		wantMethod string
		wantSKU    string
		wantBin    string
		wantQty    int
	}{
		{"received", "stock", "StockReceived", map[string]any{"sku": "SKU-1", "quantity": 10}, "received", "SKU-1", "", 10},
		{"stowed", "stock", "ItemStowed", map[string]any{"sku": "SKU-1", "bin_id": "BIN-A"}, "stowed", "SKU-1", "BIN-A", 0},
		{"picked", "reservation", "StockPicked", map[string]any{"sku": "SKU-1", "quantity": 4}, "picked", "SKU-1", "", 4},
		{"reserved", "reservation", "StockReserved", map[string]any{"sku": "SKU-1"}, "reserved", "SKU-1", "", 0},
		{"expired", "reservation", "ReservationExpired", map[string]any{"sku": "SKU-1"}, "expired", "SKU-1", "", 0},
		{"revoked", "reservation", "ReservationRevoked", map[string]any{"sku": "SKU-1"}, "revoked", "SKU-1", "", 0},
		{"cycle", "bin", "CycleCountCompleted", map[string]any{"bin_id": "BIN-A"}, "cycle", "", "BIN-A", 0},
		{"discrepancy", "bin", "DiscrepancyDetected", map[string]any{"bin_id": "BIN-A"}, "discrepancy", "", "BIN-A", 0},
		{"unlocated", "stock", "ItemUnlocated", map[string]any{"sku": "SKU-1", "bin_id": "BIN-A"}, "unlocated", "SKU-1", "BIN-A", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proj := &fakeProjection{}
			processed := newFakeProcessed()
			c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

			raw := cloudEvent(t, "e-"+tt.name, tt.entity, tt.eventType, at, tt.data)
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(proj.calls) != 1 {
				t.Fatalf("calls = %d, want 1", len(proj.calls))
			}
			got := proj.calls[0]
			if got.method != tt.wantMethod {
				t.Errorf("method = %q, want %q", got.method, tt.wantMethod)
			}
			if got.sku != tt.wantSKU || got.binId != tt.wantBin || got.qty != tt.wantQty {
				t.Errorf("fields = %+v, want sku=%q bin=%q qty=%d", got, tt.wantSKU, tt.wantBin, tt.wantQty)
			}
			if got.eventId != "e-"+tt.name {
				t.Errorf("eventId = %q, want the CloudEvents id %q", got.eventId, "e-"+tt.name)
			}
			if !got.at.Equal(at) {
				t.Errorf("at = %v, want %v", got.at, at)
			}
		})
	}
}

func TestAnalyticsConsumer_Idempotent(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	raw := cloudEvent(t, "dup", "stock", "StockReceived", at, map[string]any{"sku": "SKU-1", "quantity": 5})
	for range 2 {
		if err := c.HandleMessage(context.Background(), raw); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
	if len(proj.calls) != 1 {
		t.Fatalf("expected 1 apply for duplicate delivery, got %d", len(proj.calls))
	}
}

func TestAnalyticsConsumer_IgnoresUnknownEventType(t *testing.T) {
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	// LocationRecorded is on the topic but does not move the report.
	raw := cloudEvent(t, "e1", "stock", "LocationRecorded", time.Now(), map[string]any{"bin_id": "BIN-A"})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 0 {
		t.Fatalf("expected non-projecting event to make no call, got %d", len(proj.calls))
	}
	// An event with no projection method must NOT be marked processed, so a
	// later contract change could reprocess it.
	if processed.seen["e1"] {
		t.Error("non-projecting event should not be marked processed")
	}
}

// TestAnalyticsConsumer_DispatchesOnFullTypeOnly proves a bare short name or
// another context's type with the same trailing event name is NOT applied.
func TestAnalyticsConsumer_DispatchesOnFullTypeOnly(t *testing.T) {
	proj := &fakeProjection{}
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: newFakeProcessed(), Logger: slog.Default()}

	foreign := `{"specversion":"1.0","id":"f1","source":"/warehouse/other","type":"com.warehouse.wes.other.stock.StockReceived","time":"2026-05-01T08:00:00Z","datacontenttype":"application/json","data":{"sku":"SKU-1","quantity":1}}`
	if err := c.HandleMessage(context.Background(), []byte(foreign)); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 0 {
		t.Fatalf("expected a foreign type to be ignored, got %d calls", len(proj.calls))
	}
}

// TestAnalyticsConsumer_RejectsLegacyFlatEnvelope proves the retired flat
// envelope (event_id/event_type/occurred_at/schema_version) is rejected as
// not-a-CloudEvent — never parsed — and that Handle skips it (WARN + commit
// past, this consumer has no DLQ) without touching the projection.
func TestAnalyticsConsumer_RejectsLegacyFlatEnvelope(t *testing.T) {
	legacy := []byte(`{"event_id":"legacy-1","event_type":"StockReceived","occurred_at":"2026-05-01T08:00:00Z","source":"inventory-storage","schema_version":1,"data":{"sku":"SKU-1","quantity":5}}`)

	proj := &fakeProjection{}
	processed := newFakeProcessed()
	var logs bytes.Buffer
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.New(slog.NewTextHandler(&logs, nil))}

	err := c.HandleMessage(context.Background(), legacy)
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("HandleMessage err = %v, want ErrNotCloudEvent", err)
	}

	if err := c.Handle(context.Background(), kafkago.Message{Topic: "warehouse.inventory.analytics", Partition: 2, Offset: 7, Value: legacy}); err != nil {
		t.Fatalf("Handle must skip (nil error) a non-CloudEvent message, got %v", err)
	}
	if len(proj.calls) != 0 || len(processed.seen) != 0 {
		t.Fatalf("legacy message must not be applied or marked processed: calls=%d seen=%v", len(proj.calls), processed.seen)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "offset=7") {
		t.Errorf("expected a WARN log with the offset, got %q", logs.String())
	}
}
