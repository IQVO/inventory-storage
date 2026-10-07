package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// transferServer wires the two ADR 0031 endpoints over in-memory repos
// with a seeded ALLOCATED ledger.
type transferServer struct {
	handler    http.Handler
	stock      *memory.StockRepo
	transfers  *memory.TransferAllocationRepo
	receipts   *memory.TransferReceiptRepo
	exceptions *memory.InventoryExceptionRepo
	locations  *memory.LocationRepo
}

func newTransferServer() transferServer {
	stockRepo := memory.NewStockRepo()
	locationRepo := memory.NewLocationRepo()
	transfers := memory.NewTransferAllocationRepo()
	receipts := memory.NewTransferReceiptRepo()
	exceptions := memory.NewInventoryExceptionRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))

	s := &inboundhttp.Server{
		StageTransferReceipt: &usecases.StageTransferReceipt{
			Transfers: transfers, Receipts: receipts, Exceptions: exceptions, Events: publisher, Clock: clock,
		},
		StowTransferStock: &usecases.StowTransferStock{
			Receipts: receipts, Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock,
		},
	}
	return transferServer{
		handler:    inboundhttp.NewRouter(s, nil, ""),
		stock:      stockRepo,
		transfers:  transfers,
		receipts:   receipts,
		exceptions: exceptions,
		locations:  locationRepo,
	}
}

func (ts transferServer) seedAllocated(t *testing.T, transferID, lineID, originSite, sku string, qty int) {
	t.Helper()
	allocated, err := transfer.NewAllocated(transferID, lineID, mustSite(t, originSite), mustSKU(t, sku), mustQ(t, qty), "res-"+lineID, time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	if err := ts.transfers.Save(context.Background(), allocated); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
}

func (ts transferServer) seedSiteBin(t *testing.T, id, site string, capacity int) {
	t.Helper()
	binID, _ := shared.NewBinId(id)
	cap, _ := shared.NewQuantity(capacity)
	base, err := location.NewBin(binID, cap)
	if err != nil {
		t.Fatalf("seed bin: %v", err)
	}
	atSite := location.RehydrateBinAtSite(binID, base.Capacity(), base.Occupied(), mustSite(t, site), base.Version())
	if err := ts.locations.Save(context.Background(), atSite); err != nil {
		t.Fatalf("seed bin: %v", err)
	}
}

func mustSKU(t *testing.T, raw string) shared.SKU {
	t.Helper()
	sku, err := shared.NewSKU(raw)
	if err != nil {
		t.Fatalf("NewSKU: %v", err)
	}
	return sku
}

func mustSite(t *testing.T, raw string) shared.SiteID {
	t.Helper()
	site, err := shared.NewSiteID(raw)
	if err != nil {
		t.Fatalf("NewSiteID: %v", err)
	}
	return site
}

func mustQ(t *testing.T, v int) shared.Quantity {
	t.Helper()
	qty, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("NewQuantity: %v", err)
	}
	return qty
}

func postJSON(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestTransferReceiptEndpoint_StageThenStow(t *testing.T) {
	ts := newTransferServer()
	ts.seedAllocated(t, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	ts.seedSiteBin(t, "BIN-D1", "SITE-DEST", 10)

	// Stage.
	rec := postJSON(t, ts.handler, "/transfers/tl-1/receipt", map[string]any{
		"transferId": "tr-1", "destinationSiteId": "SITE-DEST", "sku": "SKU-T", "receivedQuantity": 5,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("stage status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var staged map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &staged); err != nil {
		t.Fatalf("stage body: %v", err)
	}
	if staged["state"] != "STAGED" || staged["variance"].(float64) != 0 {
		t.Fatalf("stage body = %v", staged)
	}

	// Stow.
	rec = postJSON(t, ts.handler, "/transfers/tl-1/stow", map[string]any{
		"bins": []map[string]any{{"binId": "BIN-D1", "quantity": 5}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("stow status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var stowed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &stowed); err != nil {
		t.Fatalf("stow body: %v", err)
	}
	if stowed["state"] != "STOWED" {
		t.Fatalf("stow body = %v", stowed)
	}
	allocs, _ := stowed["allocations"].([]any)
	if len(allocs) != 1 {
		t.Fatalf("allocations = %v", stowed["allocations"])
	}
}

func TestTransferReceiptEndpoint_ReplayStageAnswersOriginal(t *testing.T) {
	ts := newTransferServer()
	ts.seedAllocated(t, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)

	body := map[string]any{"transferId": "tr-1", "destinationSiteId": "SITE-DEST", "sku": "SKU-T", "receivedQuantity": 5}
	first := postJSON(t, ts.handler, "/transfers/tl-1/receipt", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first stage = %d: %s", first.Code, first.Body.String())
	}
	second := postJSON(t, ts.handler, "/transfers/tl-1/receipt", body)
	if second.Code != http.StatusOK {
		t.Fatalf("replay stage = %d, want 200", second.Code)
	}
}

func TestTransferReceiptEndpoint_UnknownLineIsQuarantined422(t *testing.T) {
	ts := newTransferServer()

	rec := postJSON(t, ts.handler, "/transfers/tl-ghost/receipt", map[string]any{
		"transferId": "tr-ghost", "destinationSiteId": "SITE-DEST", "sku": "SKU-T", "receivedQuantity": 5,
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422: %s", rec.Code, rec.Body.String())
	}
	var problem map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("problem body: %v", err)
	}
	if problem["type"].(string) != "https://errors.inventory-storage.warehouse-systems.dev/transfer-scan-quarantined" {
		t.Fatalf("problem type = %v", problem["type"])
	}
	exc, _ := problem["exception"].(map[string]any)
	if exc == nil || exc["kind"] != "UNKNOWN_TRANSFER" {
		t.Fatalf("problem exception = %v", problem["exception"])
	}

	// The quarantine row exists and no stock changed.
	found, _ := ts.exceptions.FindByScan(context.Background(), "tl-ghost", mustSite(t, "SITE-DEST"), mustSKU(t, "SKU-T"), mustQ(t, 5))
	if found == nil {
		t.Fatal("the quarantine row must be persisted")
	}
	units, _ := ts.stock.FindBySKU(context.Background(), mustSKU(t, "SKU-T"))
	if len(units) != 0 {
		t.Fatalf("quarantine must raise no stock, found %d", len(units))
	}
}

func TestTransferStowEndpoint_WrongSiteBinIs409(t *testing.T) {
	ts := newTransferServer()
	ts.seedAllocated(t, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	ts.seedSiteBin(t, "BIN-OTHER", "SITE-OTHER", 10)

	if rec := postJSON(t, ts.handler, "/transfers/tl-1/receipt", map[string]any{
		"transferId": "tr-1", "destinationSiteId": "SITE-DEST", "sku": "SKU-T", "receivedQuantity": 5,
	}); rec.Code != http.StatusCreated {
		t.Fatalf("stage = %d: %s", rec.Code, rec.Body.String())
	}

	rec := postJSON(t, ts.handler, "/transfers/tl-1/stow", map[string]any{
		"bins": []map[string]any{{"binId": "BIN-OTHER", "quantity": 5}},
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("stow status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}

func TestTransferStowEndpoint_StowBeforeStageIs409(t *testing.T) {
	ts := newTransferServer()
	ts.seedSiteBin(t, "BIN-D1", "SITE-DEST", 10)

	rec := postJSON(t, ts.handler, "/transfers/tl-none/stow", map[string]any{
		"bins": []map[string]any{{"binId": "BIN-D1", "quantity": 5}},
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
}
