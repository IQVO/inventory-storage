package kafka_test

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON test for StockRelocated (ADR 0039). It pins the
// CloudEvents attributes, the key/subject (relocation id), the dataschema
// and the byte-for-byte snake_case data; a change here is a wire-contract
// change (new .v2, not an edit). slotting-optimization consumes this shape.

func TestGolden_Integration_StockRelocated(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewStockRelocated(goldenAt, "rel-42", shared.SiteID("WH1"), mustSKU(t, "SKU-R1"),
		mustBin(t, "WH1-STOR-AMB-A01-01-01-A"), mustBin(t, "WH1-STOR-AMB-A02-01-01-A"), newQty(t, 7),
		"SLOTTING", "plan-9/move-3")

	assertGolden(t, encodeOne(t, pub, event), "rel-42", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.stock.StockRelocated",
		"subject": "rel-42",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:StockRelocated:v1",
		"data": {
			"relocation_id": "rel-42",
			"site_code": "WH1",
			"sku": "SKU-R1",
			"from_bin": "WH1-STOR-AMB-A01-01-01-A",
			"to_bin": "WH1-STOR-AMB-A02-01-01-A",
			"quantity": 7,
			"reason": "SLOTTING",
			"source_ref": "plan-9/move-3",
			"occurred_at": "2026-08-21T22:00:00Z"
		}
	}`)
}

// An empty source ref is sent as "" (the field is always present, so a
// consumer never has to distinguish absent from empty).
func TestGolden_Integration_StockRelocated_EmptySourceRef(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewStockRelocated(goldenAt, "rel-43", shared.SiteID("WH1"), mustSKU(t, "SKU-R1"),
		mustBin(t, "WH1-STOR-AMB-A01-01-01-A"), mustBin(t, "WH1-STOR-AMB-A02-01-01-A"), newQty(t, 1),
		"MANUAL", "")

	assertGolden(t, encodeOne(t, pub, event), "rel-43", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.stock.StockRelocated",
		"subject": "rel-43",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:StockRelocated:v1",
		"data": {
			"relocation_id": "rel-43",
			"site_code": "WH1",
			"sku": "SKU-R1",
			"from_bin": "WH1-STOR-AMB-A01-01-01-A",
			"to_bin": "WH1-STOR-AMB-A02-01-01-A",
			"quantity": 1,
			"reason": "MANUAL",
			"source_ref": "",
			"occurred_at": "2026-08-21T22:00:00Z"
		}
	}`)
}

// A REPLENISHMENT relocation (ADR 0041) is the same wire shape with reason
// REPLENISHMENT and source_ref = the replenishment request id, which is what
// pick-face-replenishment matches on to complete its request.
func TestGolden_Integration_StockRelocated_Replenishment(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewStockRelocated(goldenAt, "rel-45", shared.SiteID("WH1"), mustSKU(t, "SKU-R1"),
		mustBin(t, "WH1-STOR-AMB-A01-01-01-A"), mustBin(t, "WH1-PICK-AMB-A02-01-01-A"), newQty(t, 12),
		"REPLENISHMENT", "repl-3b0e8d2a-7c41-4f6a-9d15-2a8b6c4e1f07")

	assertGolden(t, encodeOne(t, pub, event), "rel-45", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.stock.StockRelocated",
		"subject": "rel-45",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:StockRelocated:v1",
		"data": {
			"relocation_id": "rel-45",
			"site_code": "WH1",
			"sku": "SKU-R1",
			"from_bin": "WH1-STOR-AMB-A01-01-01-A",
			"to_bin": "WH1-PICK-AMB-A02-01-01-A",
			"quantity": 12,
			"reason": "REPLENISHMENT",
			"source_ref": "repl-3b0e8d2a-7c41-4f6a-9d15-2a8b6c4e1f07",
			"occurred_at": "2026-08-21T22:00:00Z"
		}
	}`)
}

// StockRelocated is an integration-topic fact only; the analytics data
// product does not carry it.
func TestAnalytics_StockRelocatedIsNotPublished(t *testing.T) {
	pub := kafka.NewAnalyticsPublisher(nil, memory.NewReservationRepo(), nil)
	event := shared.NewStockRelocated(goldenAt, "rel-44", "WH1", "SKU-R1", "A", "B", 1, "MANUAL", "")

	msgs, err := pub.Encode(t.Context(), event)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("analytics encoded %d messages for StockRelocated, want 0", len(msgs))
	}
}
