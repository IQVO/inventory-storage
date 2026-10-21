package kafka_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON tests for the promoted exception facts (ADR 0043):
// DiscrepancyDetected, ItemUnlocated and ItemStowed now ALSO reach
// warehouse.inventory.events. Same `type`, `subject` and Kafka key as the
// analytics copy; only the `dataschema` stream differs
// (...:events:<Event>:v1). A change to any pinned byte is a wire-contract
// change (new .v2 type + dataschema, never an edit).

func TestGolden_Integration_ItemStowed(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewItemStowed(goldenAt, mustSKU(t, "SKU-1001"), mustBin(t, "A-12-3"), newQty(t, 50))

	assertGolden(t, encodeOne(t, pub, event), "SKU-1001", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.stock.ItemStowed",
		"subject": "SKU-1001",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:ItemStowed:v1",
		"data": {"sku":"SKU-1001","bin_id":"A-12-3","quantity":50}
	}`)
}

func TestGolden_Integration_ItemUnlocated(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewItemUnlocated(goldenAt, "su-7781", mustSKU(t, "SKU-1001"), mustBin(t, "A-12-3"), newQty(t, 3))

	// Key = SKU (all facts about one SKU's units stay on one partition),
	// subject = the stock unit that was lost — the analytics publisher's
	// split, kept byte-identical by ADR 0043.
	assertGolden(t, encodeOne(t, pub, event), "SKU-1001", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.stock.ItemUnlocated",
		"subject": "su-7781",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:ItemUnlocated:v1",
		"data": {"sku":"SKU-1001","bin_id":"A-12-3","stock_unit_id":"su-7781","quantity":3}
	}`)
}

func TestGolden_Integration_DiscrepancyDetected(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewDiscrepancyDetected(goldenAt, mustBin(t, "A-12-3"), newQty(t, 47), newQty(t, 50))

	assertGolden(t, encodeOne(t, pub, event), "A-12-3", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected",
		"subject": "A-12-3",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:DiscrepancyDetected:v1",
		"data": {"bin_id":"A-12-3","counted":47,"system":50}
	}`)
}

// TestPromoted_DataIsByteIdenticalAcrossTopics pins the ADR 0043 guarantee:
// for each promoted fact, the `data` JSON bytes the integration encoder
// emits are IDENTICAL to the analytics encoder's — not merely equal after
// re-marshalling. A drift here means the two copies of the payload mapping
// diverged (promoted.go vs analytics_publisher.go) and one of them is a
// wire-contract change.
func TestPromoted_DataIsByteIdenticalAcrossTopics(t *testing.T) {
	events := map[string]shared.DomainEvent{
		"ItemStowed":          shared.NewItemStowed(goldenAt, mustSKU(t, "SKU-1001"), mustBin(t, "A-12-3"), newQty(t, 50)),
		"ItemUnlocated":       shared.NewItemUnlocated(goldenAt, "su-7781", mustSKU(t, "SKU-1001"), mustBin(t, "A-12-3"), newQty(t, 3)),
		"DiscrepancyDetected": shared.NewDiscrepancyDetected(goldenAt, mustBin(t, "A-12-3"), newQty(t, 47), newQty(t, 50)),
	}

	integration := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	integration.NewID = fixedID
	analytics := kafka.NewAnalyticsPublisher(nil, fakeReservationRepo{sku: "SKU-R", found: true}, fixedID)

	for name, event := range events {
		t.Run(name, func(t *testing.T) {
			integEnc := encodeOne(t, integration, event)
			analEnc := encodeOne(t, analytics, event)

			if integEnc.EventType != analEnc.EventType {
				t.Errorf("type = %q on the integration topic but %q on the analytics topic; promoted types must be byte-identical", integEnc.EventType, analEnc.EventType)
			}
			if !bytes.Equal(integEnc.Key, analEnc.Key) {
				t.Errorf("Kafka key = %q on the integration topic but %q on the analytics topic", integEnc.Key, analEnc.Key)
			}

			integData := rawData(t, integEnc.Value)
			analData := rawData(t, analEnc.Value)
			if !bytes.Equal(integData, analData) {
				t.Errorf("data differs across topics\n integration: %s\n analytics:    %s", integData, analData)
			}

			// The CloudEvents `subject` is the one attribute allowed to
			// differ from the Kafka key (ItemUnlocated: key = SKU,
			// subject = stock unit id) but NOT across topics.
			if subjectOf(t, integEnc.Value) != subjectOf(t, analEnc.Value) {
				t.Errorf("subject = %q on the integration topic but %q on the analytics topic",
					subjectOf(t, integEnc.Value), subjectOf(t, analEnc.Value))
			}
		})
	}
}

// rawData extracts the raw `data` member bytes of a CloudEvent JSON value,
// without re-marshalling, so byte equality is real byte equality.
func rawData(t *testing.T, value []byte) json.RawMessage {
	t.Helper()
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(value, &envelope); err != nil {
		t.Fatalf("unmarshal CloudEvent %s: %v", value, err)
	}
	return envelope.Data
}

// subjectOf extracts the CloudEvents `subject` attribute.
func subjectOf(t *testing.T, value []byte) string {
	t.Helper()
	var envelope struct {
		Subject string `json:"subject"`
	}
	if err := json.Unmarshal(value, &envelope); err != nil {
		t.Fatalf("unmarshal CloudEvent %s: %v", value, err)
	}
	return envelope.Subject
}
