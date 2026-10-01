package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
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
			wantType:      "com.warehouse.wms.inventory-storage.stock.StockReceived",
			wantKey:       "SKU-1",
			wantDataField: "quantity",
			wantDataValue: float64(10),
		},
		{
			name:          "ItemStowed",
			event:         shared.NewItemStowed(at, mustSKU(t, "SKU-2"), mustBin(t, "BIN-A"), newQty(t, 3)),
			wantType:      "com.warehouse.wms.inventory-storage.stock.ItemStowed",
			wantKey:       "SKU-2",
			wantDataField: "bin_id",
			wantDataValue: "BIN-A",
		},
		{
			name:          "StockPicked",
			event:         shared.NewStockPicked(at, "res-1", mustSKU(t, "SKU-3"), newQty(t, 4)),
			wantType:      "com.warehouse.wms.inventory-storage.reservation.StockPicked",
			wantKey:       "SKU-3",
			wantDataField: "quantity",
			wantDataValue: float64(4),
		},
		{
			name:          "StockReserved",
			event:         shared.NewStockReserved(at, "res-2", mustSKU(t, "SKU-4"), newQty(t, 2), "demand-x"),
			wantType:      "com.warehouse.wms.inventory-storage.reservation.StockReserved",
			wantKey:       "SKU-4",
			wantDataField: "reservation_id",
			wantDataValue: "res-2",
		},
		{
			name:          "CycleCountCompleted",
			event:         shared.NewCycleCountCompleted(at, mustBin(t, "BIN-B"), newQty(t, 5), newQty(t, 6), true),
			wantType:      "com.warehouse.wms.inventory-storage.bin.CycleCountCompleted",
			wantKey:       "BIN-B",
			wantDataField: "discrepancy",
			wantDataValue: true,
		},
		{
			name:          "DiscrepancyDetected",
			event:         shared.NewDiscrepancyDetected(at, mustBin(t, "BIN-C"), newQty(t, 5), newQty(t, 7)),
			wantType:      "com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected",
			wantKey:       "BIN-C",
			wantDataField: "counted",
			wantDataValue: float64(5),
		},
		{
			name:          "ItemUnlocated",
			event:         shared.NewItemUnlocated(at, "su-1", mustSKU(t, "SKU-5"), mustBin(t, "BIN-D"), newQty(t, 1)),
			wantType:      "com.warehouse.wms.inventory-storage.stock.ItemUnlocated",
			wantKey:       "SKU-5",
			wantDataField: "bin_id",
			wantDataValue: "BIN-D",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertAnalyticsEventPublished(t, tt.event, tt.wantType, tt.wantKey, tt.wantDataField, tt.wantDataValue, at)
		})
	}
}

// assertAnalyticsEventPublished publishes one event through a capturing
// writer and pins the resulting analytics CloudEvent: message key, full
// `type`, minted `id`, `source`, analytics `dataschema`, `time`, the
// content-type header, and the case's one asserted data field. The retired
// envelope-level schema_version must be gone.
func assertAnalyticsEventPublished(t *testing.T, event shared.DomainEvent, wantType, wantKey, wantDataField string, wantDataValue any, at time.Time) {
	t.Helper()
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, func() string { return "evt-fixed" })
	p.Writer = w

	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	msg := w.msgs[0]
	if string(msg.Key) != wantKey {
		t.Errorf("key = %q, want %q", string(msg.Key), wantKey)
	}
	if got := headerValue(msg.Headers, "content-type"); got != cloudevents.MediaType {
		t.Errorf("content-type header = %q, want %q", got, cloudevents.MediaType)
	}

	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.Type() != wantType {
		t.Errorf("type = %q, want %q", e.Type(), wantType)
	}
	if e.ID() != "evt-fixed" {
		t.Errorf("id = %q, want evt-fixed", e.ID())
	}
	if e.Source() != "/warehouse/inventory-storage" {
		t.Errorf("source = %q, want /warehouse/inventory-storage", e.Source())
	}
	if want := "urn:warehouse:inventory-storage:analytics:" + event.EventName() + ":v1"; e.DataSchema() != want {
		t.Errorf("dataschema = %q, want %q", e.DataSchema(), want)
	}
	if !e.Time().Equal(at) {
		t.Errorf("time = %v, want %v", e.Time(), at)
	}
	assertNoSchemaVersion(t, msg.Value)

	var data map[string]any
	if err := e.DataAs(&data); err != nil {
		t.Fatalf("DataAs: %v", err)
	}
	if got := data[wantDataField]; got != wantDataValue {
		t.Errorf("data[%q] = %v (%T), want %v (%T)", wantDataField, got, got, wantDataValue, wantDataValue)
	}
}

// assertNoSchemaVersion pins that the retired Envelope v1 schema_version
// field is gone (replaced by `dataschema`, ADR-0024).
func assertNoSchemaVersion(t *testing.T, value []byte) {
	t.Helper()
	var raw map[string]any
	if err := json.Unmarshal(value, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := raw["schema_version"]; ok {
		t.Error("schema_version must not be present on a CloudEvent")
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
		e, err := cloudevents.Decode(w.msgs[0].Value)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if string(w.msgs[0].Key) != "res-9" {
			t.Errorf("key = %q, want res-9 (reservation id)", string(w.msgs[0].Key))
		}
		if e.Subject() != "res-9" {
			t.Errorf("subject = %q, want res-9", e.Subject())
		}
		var data map[string]any
		if err := e.DataAs(&data); err != nil {
			t.Fatalf("DataAs: %v", err)
		}
		if data["sku"] != "SKU-ENRICHED" {
			t.Errorf("%s sku = %v, want SKU-ENRICHED", e.Type(), data["sku"])
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
	e, err := cloudevents.Decode(w.msgs[0].Value)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	var data map[string]any
	if err := e.DataAs(&data); err != nil {
		t.Fatalf("DataAs: %v", err)
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
