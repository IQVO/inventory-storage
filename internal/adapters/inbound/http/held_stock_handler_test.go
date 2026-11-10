package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// GET /held-stock, GET /held-stock/{id} and POST /held-stock/{id}/dispositions
// (ADR 0052) over in-memory repos. The Idempotency-Key behaviour needs Postgres
// and is covered by the route table in idempotency_routes_integration_test.go.

type heldServer struct {
	handler http.Handler
	held    *memory.HeldStockRepo
	events  *events.BufferedPublisher
	clock   *memory.FixedClock
}

func newHeldServer() heldServer {
	held := memory.NewHeldStockRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC))
	s := &inboundhttp.Server{
		ListHeldStock: &usecases.ListHeldStock{Held: held, Clock: clock},
		DecideHeldDisposition: &usecases.DecideHeldDisposition{
			Held: held, Receive: &usecases.ReceiveStock{Events: publisher, Clock: clock}, Events: publisher, Clock: clock,
		},
	}
	return heldServer{handler: inboundhttp.NewRouter(s, nil, ""), held: held, events: publisher, clock: clock}
}

// seed holds qty of sku on receipt line `line`, held `age` ago.
func (hs heldServer) seed(t *testing.T, line int, sku string, qty int, reason stock.HoldReason, age time.Duration) string {
	t.Helper()
	ctx := context.Background()
	id, _ := hs.held.NextID(ctx)
	h, err := stock.NewHeldUnit(id, stock.HeldSource{
		ReceiptID: "rcpt-1", ASNNumber: "ASN-1", LineNo: line, SKU: shared.SKU(sku), Quantity: shared.Quantity(qty),
		OwnerRef: "seller-a", SSCC: "003400000000000017",
	}, reason, hs.clock.Now().Add(-age))
	if err != nil {
		t.Fatal(err)
	}
	if err := hs.held.Save(ctx, h); err != nil {
		t.Fatal(err)
	}
	return id
}

func (hs heldServer) do(method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	hs.handler.ServeHTTP(rec, req)
	return rec
}

type heldJSON struct {
	ID          string   `json:"id"`
	SKU         string   `json:"sku"`
	Quantity    int      `json:"quantity"`
	Reason      string   `json:"reason"`
	State       string   `json:"state"`
	ReceiptID   string   `json:"receiptId"`
	ASNNumber   string   `json:"asnNumber"`
	LineNo      int      `json:"lineNo"`
	OwnerRef    string   `json:"ownerRef"`
	SSCC        string   `json:"sscc"`
	LPNs        []string `json:"lpns"`
	HeldAt      string   `json:"heldAt"`
	Disposition string   `json:"disposition"`
	DecidedAt   string   `json:"decidedAt"`
	ReworkCount int      `json:"reworkCount"`
}

// sameHeld compares every scalar field of two held-unit bodies (the LPNs are
// checked by the caller).
func sameHeld(a, b heldJSON) bool {
	a.LPNs, b.LPNs = nil, nil
	return reflect.DeepEqual(a, b)
}

func decodeHeld(t *testing.T, rec *httptest.ResponseRecorder) heldJSON {
	t.Helper()
	var h heldJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	return h
}

func problemSlug(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("not a problem body: %s", rec.Body.String())
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

func TestGetHeldStock_OneUnitWithItsSourceAndAge(t *testing.T) {
	hs := newHeldServer()
	id := hs.seed(t, 3, "SKU-1", 4, stock.ReasonDamaged, 5*time.Hour)
	_ = hs.held.MarkCarton(context.Background(), "rcpt-1", 3, "LPN-1", "")

	rec := hs.do(http.MethodGet, "/held-stock/"+id, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	h := decodeHeld(t, rec)
	want := heldJSON{
		ID: id, SKU: "SKU-1", Quantity: 4, Reason: "DAMAGED", State: "HELD", ReceiptID: "rcpt-1", ASNNumber: "ASN-1", LineNo: 3,
		OwnerRef: "seller-a", SSCC: "003400000000000017", HeldAt: "2026-11-09T07:00:00Z",
	}
	if got := h; !sameHeld(got, want) {
		t.Fatalf("unit = %+v, want %+v", h, want)
	}
	if len(h.LPNs) != 1 || h.LPNs[0] != "LPN-1" {
		t.Fatalf("lpns = %v", h.LPNs)
	}
	if rec := hs.do(http.MethodGet, "/held-stock/hu-nope", ""); rec.Code != http.StatusNotFound || problemSlug(t, rec) != "held-unit-not-found" {
		t.Fatalf("unknown id = %d %s", rec.Code, rec.Body.String())
	}
}

func TestListHeldStock_FiltersAndPages(t *testing.T) {
	hs := newHeldServer()
	oldest := hs.seed(t, 1, "SKU-A", 1, stock.ReasonDamaged, 50*time.Hour)
	middle := hs.seed(t, 2, "SKU-B", 2, stock.ReasonQCPending, 30*time.Hour)
	newest := hs.seed(t, 3, "SKU-A", 3, stock.ReasonDamaged, 2*time.Hour)

	ids := func(path string) []string {
		t.Helper()
		rec := hs.do(http.MethodGet, path, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d: %s", path, rec.Code, rec.Body.String())
		}
		var body struct {
			Items      []heldJSON `json:"items"`
			NextCursor string     `json:"nextCursor"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(body.Items))
		for _, h := range body.Items {
			out = append(out, h.ID)
		}
		return out
	}
	same := func(name string, got []string, want ...string) {
		t.Helper()
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	same("all, oldest first", ids("/held-stock"), oldest, middle, newest)
	same("by sku", ids("/held-stock?sku=SKU-A"), oldest, newest)
	same("by reason", ids("/held-stock?reason=QC_PENDING"), middle)
	same("older than 24h", ids("/held-stock?olderThanHours=24"), oldest, middle)

	// Walk the pages: every unit exactly once, in order.
	rec := hs.do(http.MethodGet, "/held-stock?limit=2", "")
	var first struct {
		Items      []heldJSON `json:"items"`
		NextCursor string     `json:"nextCursor"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &first)
	if len(first.Items) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %d items, cursor %q", len(first.Items), first.NextCursor)
	}
	same("second page", ids("/held-stock?limit=2&cursor="+first.NextCursor), newest)
}

func TestListHeldStock_RejectsBadQueries(t *testing.T) {
	hs := newHeldServer()
	for path, slug := range map[string]string{
		"/held-stock?reason=BROKEN":                 "invalid-hold-reason",
		"/held-stock?olderThanHours=-1":             "invalid-older-than-hours",
		"/held-stock?olderThanHours=abc":            "invalid-older-than-hours",
		"/held-stock?limit=0":                       "invalid-limit",
		"/held-stock?cursor=garbage":                "invalid-cursor",
		"/held-stock?color=red":                     "unknown-query-parameter",
		"/held-stock?sku=" + strings.Repeat("x", 0): "", // empty sku filter is ignored
	} {
		rec := hs.do(http.MethodGet, path, "")
		if slug == "" {
			if rec.Code != http.StatusOK {
				t.Errorf("%s = %d, want 200", path, rec.Code)
			}
			continue
		}
		if rec.Code != http.StatusBadRequest || problemSlug(t, rec) != slug {
			t.Errorf("%s = %d %s, want 400 %s", path, rec.Code, rec.Body.String(), slug)
		}
	}
}

func TestPostDisposition_ReturnToVendorRemovesAndAudits(t *testing.T) {
	hs := newHeldServer()
	id := hs.seed(t, 1, "SKU-1", 4, stock.ReasonDamaged, time.Hour)

	rec := hs.do(http.MethodPost, "/held-stock/"+id+"/dispositions", `{"disposition":"RETURN_TO_VENDOR"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	h := decodeHeld(t, rec)
	if h.State != "REMOVED" || h.Disposition != "RETURN_TO_VENDOR" || h.DecidedAt != "2026-11-09T12:00:00Z" || h.Quantity != 4 {
		t.Fatalf("unit = %+v", h)
	}
	// A removed unit is no longer a held one, but stays readable for audit.
	if rec := hs.do(http.MethodGet, "/held-stock", ""); !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Fatalf("list = %s, want empty items", rec.Body.String())
	}
	if got := hs.do(http.MethodGet, "/held-stock/"+id, ""); decodeHeld(t, got).Disposition != "RETURN_TO_VENDOR" {
		t.Fatalf("audit lost: %s", got.Body.String())
	}
}

func TestPostDisposition_ReworkKeepsItHeldWithNewReason(t *testing.T) {
	hs := newHeldServer()
	id := hs.seed(t, 1, "SKU-1", 4, stock.ReasonDamaged, time.Hour)
	rec := hs.do(http.MethodPost, "/held-stock/"+id+"/dispositions", `{"disposition":"REWORK","reason":"SUSPECT"}`)
	h := decodeHeld(t, rec)
	if rec.Code != http.StatusOK || h.State != "HELD" || h.Reason != "SUSPECT" || h.ReworkCount != 1 {
		t.Fatalf("rework = %d %+v", rec.Code, h)
	}
}

func TestPostDisposition_ReleaseStagesAFreshReceipt(t *testing.T) {
	hs := newHeldServer()
	id := hs.seed(t, 1, "SKU-1", 4, stock.ReasonDamaged, time.Hour)
	if rec := hs.do(http.MethodPost, "/held-stock/"+id+"/dispositions", `{"disposition":"RELEASE"}`); rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	names := map[string]int{}
	for _, e := range hs.events.Events() {
		names[e.EventName()]++
	}
	if names["StockReleased"] != 1 || names["StockDispositioned"] != 1 || names["StockReceived"] != 1 {
		t.Fatalf("events = %v, want one StockReleased, StockDispositioned and StockReceived", names)
	}
}

func TestPostDisposition_Problems(t *testing.T) {
	hs := newHeldServer()
	id := hs.seed(t, 1, "SKU-1", 4, stock.ReasonDamaged, time.Hour)
	post := func(id, body string) *httptest.ResponseRecorder {
		return hs.do(http.MethodPost, "/held-stock/"+id+"/dispositions", body)
	}
	for _, tc := range []struct {
		name, id, body string
		status         int
		slug           string
	}{
		{"unknown unit", "hu-nope", `{"disposition":"SCRAP"}`, http.StatusNotFound, "held-unit-not-found"},
		{"unknown disposition", id, `{"disposition":"DONATE"}`, http.StatusUnprocessableEntity, "invalid-disposition"},
		{"missing disposition", id, `{}`, http.StatusUnprocessableEntity, "invalid-disposition"},
		{"unknown rework reason", id, `{"disposition":"REWORK","reason":"BROKEN"}`, http.StatusUnprocessableEntity, "invalid-hold-reason"},
		{"malformed body", id, `{`, http.StatusBadRequest, ""},
	} {
		rec := post(tc.id, tc.body)
		if rec.Code != tc.status || (tc.slug != "" && problemSlug(t, rec) != tc.slug) {
			t.Errorf("%s = %d %s, want %d %s", tc.name, rec.Code, rec.Body.String(), tc.status, tc.slug)
		}
	}
	// A settled unit takes no further decision.
	if rec := post(id, `{"disposition":"SCRAP"}`); rec.Code != http.StatusOK {
		t.Fatalf("scrap = %d", rec.Code)
	}
	if rec := post(id, `{"disposition":"RELEASE"}`); rec.Code != http.StatusConflict || problemSlug(t, rec) != "held-unit-closed" {
		t.Fatalf("second decision = %d %s, want 409 held-unit-closed", rec.Code, rec.Body.String())
	}
}
