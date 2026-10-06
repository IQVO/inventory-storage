package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON tests for the transfer allocation reply events
// (Phase 2 command/reply leg). These pin the exact CloudEvents
// attributes, keys/subjects, dataschemas and byte-for-byte data shapes;
// a change here is a wire-contract change (new .v2, not an edit).

func TestGolden_Integration_TransferStockAllocated(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	expiresAt := time.Date(2026, 10, 6, 12, 30, 0, 0, time.UTC)
	site := shared.SiteID("SITE-A")
	sku := mustSKU(t, "SKU-T1")
	event := shared.NewTransferStockAllocated(
		goldenAt, "tr-77", "tl-77-1", site, "res-tr-1", sku, newQty(t, 6),
		[]shared.TransferAllocationLeg{
			{StockUnitID: "su-1", BinID: mustBin(t, "BIN-1"), Quantity: newQty(t, 4)},
			{StockUnitID: "su-2", BinID: mustBin(t, "BIN-2"), Quantity: newQty(t, 2)},
		},
		expiresAt,
	)

	assertGolden(t, encodeOne(t, pub, event), "res-tr-1", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated",
		"subject": "res-tr-1",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:TransferStockAllocated:v1",
		"data": {
			"transfer_id": "tr-77",
			"transfer_line_id": "tl-77-1",
			"origin_site_id": "SITE-A",
			"reservation_id": "res-tr-1",
			"sku": "SKU-T1",
			"quantity": 6,
			"allocations": [
				{"stock_unit_id": "su-1", "bin_id": "BIN-1", "quantity": 4},
				{"stock_unit_id": "su-2", "bin_id": "BIN-2", "quantity": 2}
			],
			"expires_at": "2026-10-06T12:30:00Z"
		}
	}`)
}

func TestGolden_Integration_TransferStockAllocationRejected(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewTransferStockAllocationRejected(
		goldenAt, "tr-77", "tl-77-2", shared.SiteID("SITE-B"), mustSKU(t, "SKU-T2"), newQty(t, 9),
		"INSUFFICIENT_USABLE",
	)

	assertGolden(t, encodeOne(t, pub, event), "tl-77-2", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected",
		"subject": "tl-77-2",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:TransferStockAllocationRejected:v1",
		"data": {
			"transfer_id": "tr-77",
			"transfer_line_id": "tl-77-2",
			"origin_site_id": "SITE-B",
			"sku": "SKU-T2",
			"requested_quantity": 9,
			"reason": "INSUFFICIENT_USABLE"
		}
	}`)
}

// The analytics publisher must NOT forward the transfer events: they are
// integration replies, not analytics facts (the Inventory Flow &
// Accuracy projection has no transfer dimension). A regression here
// would silently add an undocumented analytics type.
func TestTransferEvents_NotOnAnalyticsTopic(t *testing.T) {
	p := kafka.NewAnalyticsPublisher(nil, memory.NewReservationRepo(), fixedID)

	allocated := shared.NewTransferStockAllocated(goldenAt, "tr", "tl", "SITE-A", "res-1", mustSKU(t, "SKU-T"), newQty(t, 1), nil, goldenAt)
	if out, err := p.Encode(context.Background(), allocated); err != nil || len(out) != 0 {
		t.Fatalf("analytics Encode of TransferStockAllocated = %v, %v; want no messages", out, err)
	}

	rejected := shared.NewTransferStockAllocationRejected(goldenAt, "tr", "tl", "SITE-A", mustSKU(t, "SKU-T"), newQty(t, 1), "ORIGIN_SITE_UNKNOWN")
	if out, err := p.Encode(context.Background(), rejected); err != nil || len(out) != 0 {
		t.Fatalf("analytics Encode of TransferStockAllocationRejected = %v, %v; want no messages", out, err)
	}
}
