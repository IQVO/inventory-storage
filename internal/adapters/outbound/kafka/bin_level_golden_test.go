package kafka_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON test for BinStockLevelChanged (ADR 0040), against a file in
// testdata/ so the consumer-compat test below decodes the very bytes the
// producer is pinned to. A change here is a wire-contract change (new .v2, not
// an edit); pick-face-replenishment consumes this shape.

const binLevelGoldenPath = "testdata/bin_stock_level_changed.v1.golden.json"

func binLevelGoldenBytes(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(binLevelGoldenPath) // #nosec G304 -- fixed testdata path
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	return raw
}

func binLevelEvent(t *testing.T) shared.BinStockLevelChanged {
	t.Helper()
	return shared.NewBinStockLevelChanged(goldenAt, shared.SiteID("WH1"), mustBin(t, "WH1-STOR-AMB-A01-01-01-A"),
		mustSKU(t, "SKU-1"), newQty(t, 12), newQty(t, 5), 7, shared.BinLevelPick)
}

func TestGolden_Integration_BinStockLevelChanged(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	enc := encodeOne(t, pub, binLevelEvent(t))

	assertGolden(t, enc, "WH1-STOR-AMB-A01-01-01-A:SKU-1", string(binLevelGoldenBytes(t)))
	if enc.Topic != "warehouse.inventory.events" {
		t.Errorf("topic = %q, want the integration topic", enc.Topic)
	}
	if want := "com.warehouse.wms.inventory-storage.stock.BinStockLevelChanged"; enc.EventType != want {
		t.Errorf("event type = %q, want %q", enc.EventType, want)
	}
}

// Every reason on the wire keeps its exact spelling, and the absolute levels
// are carried as plain integers (zero is a real level, never omitted).
func TestGolden_Integration_BinStockLevelChanged_ZeroLevelsAndEveryReason(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	for _, reason := range []shared.BinLevelReason{
		shared.BinLevelStow, shared.BinLevelPick, shared.BinLevelRelocate, shared.BinLevelReplenishment,
		shared.BinLevelCount, shared.BinLevelReserve, shared.BinLevelRevoke, shared.BinLevelUnlocated,
	} {
		ev := shared.NewBinStockLevelChanged(goldenAt, "WH1", mustBin(t, "BIN-A"), mustSKU(t, "SKU-9"), 0, 0, 1, reason)
		var e struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(encodeOne(t, pub, ev).Value, &e); err != nil {
			t.Fatalf("decode %s: %v", reason, err)
		}
		if e.Data["reason"] != string(reason) || e.Data["on_hand"] != float64(0) || e.Data["reserved"] != float64(0) {
			t.Errorf("reason %s: data = %v", reason, e.Data)
		}
	}
}

// consumerBinLevel mirrors, field for field, the payload struct the
// pick-face-replenishment consumer decodes (internal/adapters/inbound/kafka/
// inventory_consumer.go, binStockLevelChangedData). It is restated here, never
// imported: if this test needs an edit, the consumer's contract changed.
type consumerBinLevel struct {
	SiteCode string `json:"site_code"`
	Bin      string `json:"bin"`
	SKU      string `json:"sku"`
	OnHand   int64  `json:"on_hand"`
	Reserved int64  `json:"reserved"`
	Version  int64  `json:"version"`
}

func TestGolden_BinStockLevelChanged_DecodesAsTheReplenishmentConsumerExpects(t *testing.T) {
	e, err := cloudevents.Decode(binLevelGoldenBytes(t))
	if err != nil {
		t.Fatalf("golden is not a valid CloudEvent: %v", err)
	}
	if e.Type() != "com.warehouse.wms.inventory-storage.stock.BinStockLevelChanged" {
		t.Fatalf("type = %q", e.Type())
	}

	var got consumerBinLevel
	if err := e.DataAs(&got); err != nil {
		t.Fatalf("consumer decode: %v", err)
	}
	want := consumerBinLevel{SiteCode: "WH1", Bin: "WH1-STOR-AMB-A01-01-01-A", SKU: "SKU-1", OnHand: 12, Reserved: 5, Version: 7}
	if got != want {
		t.Fatalf("decoded = %+v, want %+v", got, want)
	}
	// The consumer also ignores reason and occurred_at; the contract still
	// carries them.
	var raw map[string]any
	if err := e.DataAs(&raw); err != nil {
		t.Fatalf("raw decode: %v", err)
	}
	if raw["reason"] != "PICK" || raw["occurred_at"] != "2026-08-21T22:00:00Z" || len(raw) != 8 {
		t.Fatalf("data fields = %v, want exactly site_code, bin, sku, on_hand, reserved, version, reason, occurred_at", raw)
	}
}

// BinStockLevelChanged is an integration-topic fact only; the analytics data
// product does not carry it.
func TestAnalytics_BinStockLevelChangedIsNotPublished(t *testing.T) {
	pub := kafka.NewAnalyticsPublisher(nil, memory.NewReservationRepo(), nil)

	msgs, err := pub.Encode(t.Context(), binLevelEvent(t))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("analytics encoded %d messages for BinStockLevelChanged, want 0", len(msgs))
	}
}
