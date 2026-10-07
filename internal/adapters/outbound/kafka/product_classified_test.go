package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ProductClassified is published on BOTH topics (ADR 0031, which supersedes
// only the "in-process only" clause of ADR 0024 for this event). The wire
// contract is pinned here exactly like every other published event: if a
// golden below breaks, the contract changed — that needs a new `.v2` type
// and dataschema, not a golden update.

func classifiedEvent(t *testing.T, tags []product.HandlingTag, temp product.TemperatureClass, dot product.DOTHazardClass) product.ProductClassified {
	t.Helper()
	c, err := product.New(mustSKU(t, "SKU-9"), tags, temp, dot)
	if err != nil {
		t.Fatalf("product.New: %v", err)
	}
	return product.NewProductClassified(c, goldenAt)
}

func TestGolden_ProductClassified_BothTopics(t *testing.T) {
	tests := []struct {
		name   string
		event  product.ProductClassified
		wantEv string // data payload
	}{
		{
			name: "full classification",
			event: classifiedEvent(t,
				[]product.HandlingTag{product.TemperatureSensitive, product.Hazmat},
				product.Frozen, product.DOTHazardClass(3)),
			// handling_tags is the aggregate's stable enum order, not input order.
			wantEv: `{"sku":"SKU-9","handling_tags":["Hazmat","TemperatureSensitive"],"temperature_class":"Frozen","dot_hazard_class":3}`,
		},
		{
			name:   "minimal classification omits the optional fields",
			event:  classifiedEvent(t, []product.HandlingTag{product.Fragile}, "", product.DOTHazardClassUnspecified),
			wantEv: `{"sku":"SKU-9","handling_tags":["Fragile"]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const typ = "com.warehouse.wms.inventory-storage.product.ProductClassified"
			envelope := func(stream string) string {
				return `{
					"specversion": "1.0",
					"id": "` + goldenID + `",
					"source": "/warehouse/inventory-storage",
					"type": "` + typ + `",
					"subject": "SKU-9",
					"time": "2026-08-21T22:00:00Z",
					"datacontenttype": "application/json",
					"dataschema": "urn:warehouse:inventory-storage:` + stream + `:ProductClassified:v1",
					"data": ` + tt.wantEv + `
				}`
			}

			integration := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
			integration.NewID = fixedID
			got := encodeOne(t, integration, tt.event)
			assertGolden(t, got, "SKU-9", envelope("events"))
			if got.Topic != kafka.Topic || got.EventType != typ {
				t.Errorf("integration topic/type = %q/%q, want %q/%q", got.Topic, got.EventType, kafka.Topic, typ)
			}

			analytics := kafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, fixedID)
			got = encodeOne(t, analytics, tt.event)
			assertGolden(t, got, "SKU-9", envelope("analytics"))
			if got.Topic != kafka.AnalyticsTopic || got.EventType != typ {
				t.Errorf("analytics topic/type = %q/%q, want %q/%q", got.Topic, got.EventType, kafka.AnalyticsTopic, typ)
			}
		})
	}
}

// The direct (non-outbox) EVENT_PUBLISHER=kafka path must forward it too,
// otherwise a deployment without Postgres would silently diverge from the
// outbox path.
func TestPublisher_Publish_ForwardsProductClassified(t *testing.T) {
	w := &fakeWriter{}
	pub := kafka.NewPublisher(w, memory.NewReservationRepo())
	ev := classifiedEvent(t, []product.HandlingTag{product.Fragile}, "", product.DOTHazardClassUnspecified)

	if err := pub.Publish(context.Background(), ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.messages) != 1 {
		t.Fatalf("messages written = %d, want 1", len(w.messages))
	}
	if string(w.messages[0].Key) != "SKU-9" {
		t.Errorf("key = %q, want SKU-9 (reclassifications of one SKU stay ordered)", w.messages[0].Key)
	}
}

func TestAnalyticsPublisher_Publish_ForwardsProductClassified(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	p := kafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, fixedID)
	p.Writer = w
	ev := classifiedEvent(t, []product.HandlingTag{product.Fragile}, "", product.DOTHazardClassUnspecified)

	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("messages written = %d, want 1", len(w.msgs))
	}
}

// LocationRecorded stays in-process (decision 9b: no consumer). Pin that so
// publishing ProductClassified is never mistaken for "publish everything".
func TestLocationRecorded_StaysInProcess(t *testing.T) {
	ev := shared.NewLocationRecorded(time.Now(), "su-1", mustBin(t, "BIN-A"))

	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	if out, err := pub.Encode(context.Background(), ev); err != nil || len(out) != 0 {
		t.Errorf("integration Encode(LocationRecorded) = %d msgs, err %v; want none", len(out), err)
	}
	analytics := kafka.NewAnalyticsPublisher(nil, fakeReservationRepo{}, fixedID)
	if out, err := analytics.Encode(context.Background(), ev); err != nil || len(out) != 0 {
		t.Errorf("analytics Encode(LocationRecorded) = %d msgs, err %v; want none", len(out), err)
	}
}
