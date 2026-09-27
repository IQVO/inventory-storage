package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// fakeAnalyticsWriter captures the messages handed to WriteMessages so a test
// can assert on the published envelope without a live broker.
type fakeAnalyticsWriter struct {
	msgs []kafkago.Message
}

func (w *fakeAnalyticsWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	w.msgs = append(w.msgs, msgs...)
	return nil
}

// fakeReservationRepo is a minimal ports.ReservationRepo whose FindByID
// returns a reservation with a fixed SKU, so the publisher's SKU enrichment of
// reservation-lifecycle events can be asserted without a real repository.
type fakeReservationRepo struct {
	sku   string
	found bool
}

func (r fakeReservationRepo) FindByID(_ context.Context, id string) (*reservation.Reservation, error) {
	if !r.found {
		return nil, nil
	}
	sku, _ := shared.NewSKU(r.sku)
	qty, _ := shared.NewPositiveQuantity(1)
	return reservation.Rehydrate(id, sku, qty, "demand-1",
		[]reservation.Allocation{{StockUnitID: "su1", Quantity: qty}},
		reservation.StatusRevoked, time.Now(), time.Now().Add(time.Hour), 1), nil
}
func (fakeReservationRepo) Save(context.Context, *reservation.Reservation) error { return nil }
func (fakeReservationRepo) NextID(context.Context) (string, error)               { return "res-next", nil }
func (fakeReservationRepo) FindByDemandRef(context.Context, string) ([]*reservation.Reservation, error) {
	return nil, nil
}

func newQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("NewQuantity: %v", err)
	}
	return q
}

func TestAnalyticsPublisher_PublishesEachEventType(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name          string
		event         shared.DomainEvent
		wantType      string
		wantKey       string
		wantDataField string
		wantDataValue any
	}{
		{
			name:          "StockReceived",
			event:         shared.NewStockReceived(at, mustSKU(t, "SKU-1"), newQty(t, 10)),
			wantType:      "StockReceived",
			wantKey:       "SKU-1",
			wantDataField: "quantity",
			wantDataValue: float64(10),
		},
		{
			name:          "ItemStowed",
			event:         shared.NewItemStowed(at, mustSKU(t, "SKU-2"), mustBin(t, "BIN-A"), newQty(t, 3)),
			wantType:      "ItemStowed",
			wantKey:       "SKU-2",
			wantDataField: "bin_id",
			wantDataValue: "BIN-A",
		},
		{
			name:          "StockPicked",
			event:         shared.NewStockPicked(at, "res-1", mustSKU(t, "SKU-3"), newQty(t, 4)),
			wantType:      "StockPicked",
			wantKey:       "SKU-3",
			wantDataField: "quantity",
			wantDataValue: float64(4),
		},
		{
			name:          "StockReserved",
			event:         shared.NewStockReserved(at, "res-2", mustSKU(t, "SKU-4"), newQty(t, 2), "demand-x"),
			wantType:      "StockReserved",
			wantKey:       "SKU-4",
			wantDataField: "reservation_id",
			wantDataValue: "res-2",
		},
		{
			name:          "CycleCountCompleted",
			event:         shared.NewCycleCountCompleted(at, mustBin(t, "BIN-B"), newQty(t, 5), newQty(t, 6), true),
			wantType:      "CycleCountCompleted",
			wantKey:       "BIN-B",
			wantDataField: "discrepancy",
			wantDataValue: true,
		},
		{
			name:          "DiscrepancyDetected",
			event:         shared.NewDiscrepancyDetected(at, mustBin(t, "BIN-C"), newQty(t, 5), newQty(t, 7)),
			wantType:      "DiscrepancyDetected",
			wantKey:       "BIN-C",
			wantDataField: "counted",
			wantDataValue: float64(5),
		},
		{
			name:          "ItemUnlocated",
			event:         shared.NewItemUnlocated(at, "su-1", mustSKU(t, "SKU-5"), mustBin(t, "BIN-D"), newQty(t, 1)),
			wantType:      "ItemUnlocated",
			wantKey:       "SKU-5",
			wantDataField: "bin_id",
			wantDataValue: "BIN-D",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &fakeAnalyticsWriter{}
			p := outboundkafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, func() string { return "evt-fixed" })
			p.Writer = w

			if err := p.Publish(context.Background(), tt.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(w.msgs) != 1 {
				t.Fatalf("expected 1 message, got %d", len(w.msgs))
			}
			msg := w.msgs[0]
			if string(msg.Key) != tt.wantKey {
				t.Errorf("key = %q, want %q", string(msg.Key), tt.wantKey)
			}

			var env outboundkafka.AnalyticsEnvelope
			if err := json.Unmarshal(msg.Value, &env); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if env.EventType != tt.wantType {
				t.Errorf("event_type = %q, want %q", env.EventType, tt.wantType)
			}
			if env.EventId != "evt-fixed" {
				t.Errorf("event_id = %q, want evt-fixed", env.EventId)
			}
			if env.Source != "inventory-storage" {
				t.Errorf("source = %q, want inventory-storage", env.Source)
			}
			if env.SchemaVersion != 1 {
				t.Errorf("schema_version = %d, want 1", env.SchemaVersion)
			}
			if !env.OccurredAt.Equal(at) {
				t.Errorf("occurred_at = %v, want %v", env.OccurredAt, at)
			}

			var data map[string]any
			if err := json.Unmarshal(env.Data, &data); err != nil {
				t.Fatalf("unmarshal data: %v", err)
			}
			if got := data[tt.wantDataField]; got != tt.wantDataValue {
				t.Errorf("data[%q] = %v (%T), want %v (%T)", tt.wantDataField, got, got, tt.wantDataValue, tt.wantDataValue)
			}
		})
	}
}

func TestAnalyticsPublisher_SkipsUnpublishedEvents(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, func() string { return "evt" })
	p.Writer = w

	// LocationRecorded is acknowledged but not part of the analytics contract.
	if err := p.Publish(context.Background(), shared.NewLocationRecorded(time.Now(), "su-1", mustBin(t, "BIN-A"))); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected LocationRecorded to be skipped, got %d messages", len(w.msgs))
	}
}

// TestAnalyticsPublisher_EnrichesReservationSKU asserts a reservation-lifecycle
// event is stamped with the reservation's SKU, looked up via the
// ReservationRepo — the enrichment that populates the report's SKU dimension
// for reservation events (which carry only a reservation id).
func TestAnalyticsPublisher_EnrichesReservationSKU(t *testing.T) {
	for _, ev := range []shared.DomainEvent{
		shared.NewReservationExpired(time.Now(), "res-9"),
		shared.NewReservationRevoked(time.Now(), "res-9"),
	} {
		w := &fakeAnalyticsWriter{}
		p := outboundkafka.NewAnalyticsPublisher(nil, fakeReservationRepo{sku: "SKU-ENRICHED", found: true}, func() string { return "evt" })
		p.Writer = w

		if err := p.Publish(context.Background(), ev); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		var env outboundkafka.AnalyticsEnvelope
		if err := json.Unmarshal(w.msgs[0].Value, &env); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if string(w.msgs[0].Key) != "res-9" {
			t.Errorf("key = %q, want res-9 (reservation id)", string(w.msgs[0].Key))
		}
		var data map[string]any
		if err := json.Unmarshal(env.Data, &data); err != nil {
			t.Fatalf("unmarshal data: %v", err)
		}
		if data["sku"] != "SKU-ENRICHED" {
			t.Errorf("%s sku = %v, want SKU-ENRICHED", env.EventType, data["sku"])
		}
	}
}

// TestAnalyticsPublisher_ReservationSKUAbsentWhenNotFound asserts the
// enrichment is best-effort: a missing reservation leaves the SKU dimension
// empty rather than failing the publish.
func TestAnalyticsPublisher_ReservationSKUAbsentWhenNotFound(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, fakeReservationRepo{found: false}, func() string { return "evt" })
	p.Writer = w

	if err := p.Publish(context.Background(), shared.NewReservationExpired(time.Now(), "res-x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var env outboundkafka.AnalyticsEnvelope
	if err := json.Unmarshal(w.msgs[0].Value, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if data["sku"] != "" {
		t.Errorf("sku = %v, want empty (reservation not found)", data["sku"])
	}
}

func mustSKU(t *testing.T, v string) shared.SKU {
	t.Helper()
	s, err := shared.NewSKU(v)
	if err != nil {
		t.Fatalf("NewSKU: %v", err)
	}
	return s
}

func mustBin(t *testing.T, v string) shared.BinId {
	t.Helper()
	b, err := shared.NewBinId(v)
	if err != nil {
		t.Fatalf("NewBinId: %v", err)
	}
	return b
}
