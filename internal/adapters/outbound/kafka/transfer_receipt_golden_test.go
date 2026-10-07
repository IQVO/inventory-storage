package kafka_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Golden exact-JSON tests for the destination receipt/stow events (ADR
// 0031, Phase 3). These pin the exact CloudEvents attributes,
// keys/subjects, dataschemas and byte-for-byte data shapes; a change
// here is a wire-contract change (new .v2, not an edit).

func TestGolden_Integration_TransferReceiptStaged(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewTransferReceiptStaged(
		goldenAt, "tr-77", "tl-77-1", shared.SiteID("SITE-DEST"), mustSKU(t, "SKU-T1"),
		newQty(t, 6), newQty(t, 5), -1,
	)

	assertGolden(t, encodeOne(t, pub, event), "tl-77-1", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged",
		"subject": "tl-77-1",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:TransferReceiptStaged:v1",
		"data": {
			"transfer_id": "tr-77",
			"transfer_line_id": "tl-77-1",
			"destination_site_id": "SITE-DEST",
			"sku": "SKU-T1",
			"expected_quantity": 6,
			"received_quantity": 5,
			"variance": -1
		}
	}`)
}

func TestGolden_Integration_TransferStockStowed(t *testing.T) {
	pub := kafka.NewPublisher(&fakeWriter{}, memory.NewReservationRepo())
	pub.NewID = fixedID

	event := shared.NewTransferStockStowed(
		goldenAt, "tr-77", "tl-77-1", shared.SiteID("SITE-DEST"), mustSKU(t, "SKU-T1"),
		newQty(t, 5), newQty(t, 5),
		[]shared.TransferAllocationLeg{
			{StockUnitID: "su-dest-1", BinID: mustBin(t, "BIN-DEST-1"), Quantity: newQty(t, 3)},
			{StockUnitID: "su-dest-2", BinID: mustBin(t, "BIN-DEST-2"), Quantity: newQty(t, 2)},
		},
	)

	assertGolden(t, encodeOne(t, pub, event), "tl-77-1", `{
		"specversion": "1.0",
		"id": "`+goldenID+`",
		"source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.stock.TransferStockStowed",
		"subject": "tl-77-1",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:TransferStockStowed:v1",
		"data": {
			"transfer_id": "tr-77",
			"transfer_line_id": "tl-77-1",
			"destination_site_id": "SITE-DEST",
			"sku": "SKU-T1",
			"received_quantity": 5,
			"stowed_quantity": 5,
			"allocations": [
				{"stock_unit_id": "su-dest-1", "bin_id": "BIN-DEST-1", "quantity": 3},
				{"stock_unit_id": "su-dest-2", "bin_id": "BIN-DEST-2", "quantity": 2}
			]
		}
	}`)
}

// The analytics publisher must NOT forward the receipt/stow events:
// they are integration facts, not analytics facts (the Inventory Flow &
// Accuracy projection has no transfer dimension). A regression here
// would silently add an undocumented analytics type.
func TestReceiptStowEvents_NotOnAnalyticsTopic(t *testing.T) {
	p := kafka.NewAnalyticsPublisher(nil, memory.NewReservationRepo(), fixedID)

	staged := shared.NewTransferReceiptStaged(goldenAt, "tr", "tl", "SITE-DEST", mustSKU(t, "SKU-T"), newQty(t, 5), newQty(t, 5), 0)
	if out, err := p.Encode(context.Background(), staged); err != nil || len(out) != 0 {
		t.Fatalf("analytics Encode of TransferReceiptStaged = %v, %v; want no messages", out, err)
	}

	stowed := shared.NewTransferStockStowed(goldenAt, "tr", "tl", "SITE-DEST", mustSKU(t, "SKU-T"), newQty(t, 5), newQty(t, 5), nil)
	if out, err := p.Encode(context.Background(), stowed); err != nil || len(out) != 0 {
		t.Fatalf("analytics Encode of TransferStockStowed = %v, %v; want no messages", out, err)
	}
}
