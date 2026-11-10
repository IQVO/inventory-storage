package kafka_test

import (
	"encoding/json"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON tests for the held-stock events (ADR 0052). A change here
// is a wire-contract change: a new .v2 type, never an edit. Key = subject =
// the held unit id, so every fact about one unit stays ordered on one
// partition (ADR 0021).

func heldEvents(t *testing.T) []struct {
	name, file string
	event      shared.DomainEvent
} {
	t.Helper()
	sku := mustSKU(t, "SKU-1")
	return []struct {
		name, file string
		event      shared.DomainEvent
	}{
		{"StockHeld", "stock_held.v1.golden.json",
			shared.NewStockHeld(goldenAt, "hu-1", sku, newQty(t, 4), "DAMAGED", "R-1", "ASN-1", 2, "seller-a", testLPN)},
		{"StockReleased", "stock_released.v1.golden.json",
			shared.NewStockReleased(goldenAt, "hu-1", sku, newQty(t, 4), "R-1", "ASN-1", 2)},
		{"StockDispositioned", "stock_dispositioned.v1.golden.json",
			shared.NewStockDispositioned(goldenAt, "hu-1", sku, newQty(t, 4), "REWORK", "SUSPECT", "R-1", "ASN-1", 2)},
	}
}

func TestGolden_Integration_HeldStockEvents(t *testing.T) {
	for _, tc := range heldEvents(t) {
		t.Run(tc.name, func(t *testing.T) {
			pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
			pub.NewID = fixedID
			enc := encodeOne(t, pub, tc.event)
			assertGolden(t, enc, "hu-1", goldenFile(t, tc.file))
			if enc.Topic != "warehouse.inventory.events" || enc.EventType != "com.warehouse.wms.inventory-storage.stock."+tc.name {
				t.Errorf("topic %q type %q", enc.Topic, enc.EventType)
			}
		})
	}
}

// The analytics topic carries the same type, key and a byte-identical data
// payload, with the analytics dataschema; the projector ignores the type.
func TestGolden_Analytics_HeldStockEventsMirrorTheIntegrationData(t *testing.T) {
	for _, tc := range heldEvents(t) {
		t.Run(tc.name, func(t *testing.T) {
			integ := encodeOne(t, containerEncoder(), tc.event)
			an := encodeOne(t, kafka.NewAnalyticsPublisher(nil, memory.NewReservationRepo(), fixedID), tc.event)
			if an.Topic != "warehouse.inventory.analytics" || an.EventType != integ.EventType || string(an.Key) != "hu-1" {
				t.Fatalf("analytics topic %q type %q key %q", an.Topic, an.EventType, an.Key)
			}
			var a, i struct {
				Subject    string          `json:"subject"`
				DataSchema string          `json:"dataschema"`
				Data       json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(an.Value, &a); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(integ.Value, &i); err != nil {
				t.Fatal(err)
			}
			if canonicalJSON(t, a.Data) != canonicalJSON(t, i.Data) || a.Subject != "hu-1" {
				t.Errorf("data differs across topics:\n analytics   %s\n integration %s", a.Data, i.Data)
			}
			if a.DataSchema != "urn:warehouse:inventory-storage:analytics:"+tc.name+":v1" {
				t.Errorf("analytics dataschema = %q", a.DataSchema)
			}
		})
	}
}

// Optional fields are omitted when empty: a fleet-owned hold carries no
// owner_ref and no sscc.
func TestGolden_HeldStock_OmitsEmptyOptionalFields(t *testing.T) {
	ev := shared.NewStockHeld(goldenAt, "hu-1", mustSKU(t, "SKU-1"), newQty(t, 4), "DAMAGED", "R-1", "ASN-1", 2, "", "")
	var e struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(encodeOne(t, containerEncoder(), ev).Value, &e); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"owner_ref", "sscc"} {
		if _, present := e.Data[k]; present {
			t.Errorf("%s must be omitted when empty: %v", k, e.Data)
		}
	}
}
