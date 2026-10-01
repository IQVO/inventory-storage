package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON tests: one per published event type, per topic
// (ADR-0024 §Tests). Each pins every CloudEvents context attribute, the full
// `type`, the `dataschema`, the byte-for-byte `data` payload, and the
// `content-type` Kafka header. If one of these breaks, the wire contract
// changed — that needs a new `.v2` type + dataschema, not a golden update.

const goldenID = "0b7c4a1e-2f3d-4e5a-8b6c-7d8e9f0a1b2c"

var goldenAt = time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)

func fixedID() string { return goldenID }

func TestGolden_Integration_StockReserved(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID
	event := shared.NewStockReserved(goldenAt, "res-1", mustSKU(t, "SKU-1"), newQty(t, 5), "order-42")

	assertGolden(t, encodeOne(t, pub, event), "res-1", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.reservation.StockReserved",
		"subject": "res-1",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:StockReserved:v1",
		"data": {"sku": "SKU-1", "quantity": 5, "demand_ref": "order-42"}
	}`)
}

func TestGolden_Integration_ReservationRevoked(t *testing.T) {
	repo := memory.NewReservationRepo()
	qty := newQty(t, 3)
	res, err := reservation.New("res-2", mustSKU(t, "SKU-2"), qty, "order-99",
		[]reservation.Allocation{{StockUnitID: "unit-1", Quantity: qty}}, goldenAt, time.Hour)
	if err != nil {
		t.Fatalf("reservation.New: %v", err)
	}
	if err := repo.Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	pub := kafka.NewPublisher(&fakeWriter{}, repo)
	pub.NewID = fixedID

	assertGolden(t, encodeOne(t, pub, shared.NewReservationRevoked(goldenAt, "res-2")), "res-2", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.reservation.ReservationRevoked",
		"subject": "res-2",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:ReservationRevoked:v1",
		"data": {"sku": "SKU-2", "quantity": 3, "demand_ref": "order-99"}
	}`)
}

func TestGolden_Analytics(t *testing.T) {
	tests := []struct {
		name    string
		event   shared.DomainEvent
		key     string
		entity  string
		subject string
		data    string
	}{
		{"StockReceived", shared.NewStockReceived(goldenAt, mustSKU(t, "SKU-1"), newQty(t, 10)),
			"SKU-1", "stock", "SKU-1", `{"sku":"SKU-1","quantity":10}`},
		{"ItemStowed", shared.NewItemStowed(goldenAt, mustSKU(t, "SKU-2"), mustBin(t, "BIN-A"), newQty(t, 3)),
			"SKU-2", "stock", "SKU-2", `{"sku":"SKU-2","bin_id":"BIN-A","quantity":3}`},
		{"ItemUnlocated", shared.NewItemUnlocated(goldenAt, "su-1", mustSKU(t, "SKU-5"), mustBin(t, "BIN-D"), newQty(t, 1)),
			"SKU-5", "stock", "su-1", `{"sku":"SKU-5","bin_id":"BIN-D","stock_unit_id":"su-1","quantity":1}`},
		{"StockPicked", shared.NewStockPicked(goldenAt, "res-1", mustSKU(t, "SKU-3"), newQty(t, 4)),
			"SKU-3", "reservation", "res-1", `{"sku":"SKU-3","reservation_id":"res-1","quantity":4}`},
		{"StockReserved", shared.NewStockReserved(goldenAt, "res-2", mustSKU(t, "SKU-4"), newQty(t, 2), "demand-x"),
			"SKU-4", "reservation", "res-2", `{"sku":"SKU-4","reservation_id":"res-2","quantity":2}`},
		{"ReservationExpired", shared.NewReservationExpired(goldenAt, "res-9"),
			"res-9", "reservation", "res-9", `{"reservation_id":"res-9","sku":"SKU-R"}`},
		{"ReservationRevoked", shared.NewReservationRevoked(goldenAt, "res-9"),
			"res-9", "reservation", "res-9", `{"reservation_id":"res-9","sku":"SKU-R"}`},
		{"CycleCountCompleted", shared.NewCycleCountCompleted(goldenAt, mustBin(t, "BIN-B"), newQty(t, 5), newQty(t, 6), true),
			"BIN-B", "bin", "BIN-B", `{"bin_id":"BIN-B","counted":5,"system":6,"discrepancy":true}`},
		{"DiscrepancyDetected", shared.NewDiscrepancyDetected(goldenAt, mustBin(t, "BIN-C"), newQty(t, 5), newQty(t, 7)),
			"BIN-C", "bin", "BIN-C", `{"bin_id":"BIN-C","counted":5,"system":7}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := kafka.NewAnalyticsPublisher(nil, fakeReservationRepo{sku: "SKU-R", found: true}, fixedID)
			want := `{
				"specversion": "1.0",
				"id": "` + goldenID + `",
				"source": "/warehouse/inventory-storage",
				"type": "com.warehouse.wms.inventory-storage.` + tt.entity + `.` + tt.name + `",
				"subject": "` + tt.subject + `",
				"time": "2026-08-21T22:00:00Z",
				"datacontenttype": "application/json",
				"dataschema": "urn:warehouse:inventory-storage:analytics:` + tt.name + `:v1",
				"data": ` + tt.data + `
			}`
			assertGolden(t, encodeOne(t, p, tt.event), tt.key, want)
		})
	}
}

func encodeOne(t *testing.T, enc kafka.Encoder, event shared.DomainEvent) kafka.Encoded {
	t.Helper()
	out, err := enc.Encode(context.Background(), event)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 encoded message, got %d", len(out))
	}
	return out[0]
}

func assertGolden(t *testing.T, enc kafka.Encoded, wantKey, wantJSON string) {
	t.Helper()
	if string(enc.Key) != wantKey {
		t.Errorf("Kafka key = %q, want %q", enc.Key, wantKey)
	}
	if got := headerValue(enc.Headers, "content-type"); got != "application/cloudevents+json; charset=UTF-8" {
		t.Errorf("content-type header = %q", got)
	}
	if _, err := cloudevents.Decode(enc.Value); err != nil {
		t.Errorf("value does not round-trip through cloudevents.Decode: %v", err)
	}
	if got, want := canonicalJSON(t, enc.Value), canonicalJSON(t, []byte(wantJSON)); got != want {
		t.Errorf("CloudEvent JSON mismatch\n got: %s\nwant: %s", got, want)
	}
}

func canonicalJSON(t *testing.T, b []byte) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(out)
}
