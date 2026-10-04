package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

type testServer struct {
	handler   http.Handler
	stock     *memory.StockRepo
	locations *memory.LocationRepo
}

func newTestServer() testServer {
	stockRepo := memory.NewStockRepo()
	locationRepo := memory.NewLocationRepo()
	reservationRepo := memory.NewReservationRepo()
	classificationRepo := memory.NewProductClassificationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	s := &inboundhttp.Server{
		ReceiveStock:               &usecases.ReceiveStock{Events: publisher, Clock: clock},
		StowStock:                  &usecases.StowStock{Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock},
		ReserveStock:               &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		RevokeReservation:          &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		ConfirmPick:                &usecases.ConfirmPick{Stock: stockRepo, Locations: locationRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		GetUsable:                  &usecases.GetUsable{Stock: stockRepo},
		GetReservationsByDemandRef: &usecases.GetReservationsByDemandRef{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		RunCycleCount:              &usecases.RunCycleCount{Stock: stockRepo, Events: publisher, Clock: clock},
		ClassifyProduct:            &usecases.ClassifyProduct{Classifications: classificationRepo, Events: publisher, Clock: clock},
		RegisterBin:                &usecases.RegisterBin{Locations: locationRepo},
		GetBin:                     &usecases.GetBin{Locations: locationRepo},
		Classifications:            classificationRepo,
	}

	return testServer{handler: inboundhttp.NewRouter(s, nil, ""), stock: stockRepo, locations: locationRepo}
}

func (ts testServer) seedBin(t *testing.T, id string, capacity int) {
	t.Helper()
	binID, _ := shared.NewBinId(id)
	cap, _ := shared.NewQuantity(capacity)
	bin, err := location.NewBin(binID, cap)
	if err != nil {
		t.Fatalf("unexpected error seeding bin: %v", err)
	}
	if err := ts.locations.Save(context.Background(), bin); err != nil {
		t.Fatalf("unexpected error saving bin: %v", err)
	}
}

func (ts testServer) do(t *testing.T, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("unexpected error marshaling body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	ts.handler.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodGet, "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// CORS is required for the browser SPAs that call this API directly (the
// warehouse-console shell and this service's own future MFE remote). The
// default allowed origins cover local dev; CORS_ALLOWED_ORIGINS overrides
// them for other environments. No credentials are needed (the API is
// unauthenticated by decision, ADR-0015 — no cookies, no bearer key).
func TestCORS_Preflight_AllowsDefaultOrigin(t *testing.T) {
	ts := newTestServer()
	req := httptest.NewRequest(http.MethodOptions, "/reservations", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	rec := httptest.NewRecorder()
	ts.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent && rec.Code != http.StatusOK {
		t.Fatalf("expected a successful preflight response, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("expected Access-Control-Allow-Origin=http://localhost:5173, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("expected no Access-Control-Allow-Credentials header (no cookie auth), got %q", got)
	}
}

// ADR-0018: a browser POST to /stock/receive or /reservations carries an
// Idempotency-Key header, so its CORS preflight must list it in
// Access-Control-Allow-Headers or the browser blocks the request.
func TestCORS_Preflight_AllowsIdempotencyKeyHeader(t *testing.T) {
	ts := newTestServer()
	for _, path := range []string{"/reservations", "/stock/receive"} {
		req := httptest.NewRequest(http.MethodOptions, path, nil)
		req.Header.Set("Origin", "http://localhost:5173")
		req.Header.Set("Access-Control-Request-Method", http.MethodPost)
		req.Header.Set("Access-Control-Request-Headers", "content-type, idempotency-key")
		rec := httptest.NewRecorder()
		ts.handler.ServeHTTP(rec, req)

		if got := strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers")); !strings.Contains(got, "idempotency-key") {
			t.Errorf("%s: Access-Control-Allow-Headers = %q, want it to include Idempotency-Key", path, got)
		}
	}
}

// A second default origin (this service's own future MFE remote dev
// origin) is allowed too, alongside the console shell.
func TestCORS_Preflight_AllowsSecondDefaultOrigin(t *testing.T) {
	ts := newTestServer()
	req := httptest.NewRequest(http.MethodOptions, "/reservations", nil)
	req.Header.Set("Origin", "http://localhost:5182")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	ts.handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5182" {
		t.Fatalf("expected Access-Control-Allow-Origin=http://localhost:5182, got %q", got)
	}
}

// An origin outside the allowed set gets no CORS headers, so the browser
// still blocks it — this proves the allowlist is enforced, not wide open.
func TestCORS_Preflight_RejectsUnknownOrigin(t *testing.T) {
	ts := newTestServer()
	req := httptest.NewRequest(http.MethodOptions, "/reservations", nil)
	req.Header.Set("Origin", "http://evil.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	ts.handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no Access-Control-Allow-Origin for an unknown origin, got %q", got)
	}
}

func TestReceiveStock_Endpoint(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPost, "/stock/receive", map[string]any{"sku": "SKU-1", "quantity": 10})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestReceiveStock_Endpoint_InvalidQuantity(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPost, "/stock/receive", map[string]any{"sku": "SKU-1", "quantity": 0})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestStowStock_Endpoint(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)

	rec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 5, "binId": "A-1-1"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc == "" || loc == "/stock/" {
		t.Fatalf("expected non-empty Location header pointing at the created stock unit, got %q", loc)
	}
}

func TestStowStock_Endpoint_CapacityExceeded(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 5)

	rec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 6, "binId": "A-1-1"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusConflict, "bin-full", "/stock/stow")
}

func TestStowStock_Endpoint_UnknownBin(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 6, "binId": "A-1-1"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestReservationLifecycle_Endpoints(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 10, "binId": "A-1-1"})

	reserveRec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 6, "demandRef": "order-1"})
	if reserveRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", reserveRec.Code, reserveRec.Body.String())
	}
	var reserved struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(reserveRec.Body.Bytes(), &reserved); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	if loc := reserveRec.Header().Get("Location"); loc != "/reservations/"+reserved.ID {
		t.Fatalf("expected Location header /reservations/%s, got %q", reserved.ID, loc)
	}

	usableRec := ts.do(t, http.MethodGet, "/inventory/SKU-1/usable", nil)
	if usableRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", usableRec.Code, usableRec.Body.String())
	}

	confirmRec := ts.do(t, http.MethodPost, "/reservations/"+reserved.ID+"/confirm-pick", nil)
	if confirmRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", confirmRec.Code, confirmRec.Body.String())
	}
}

// GET /reservations?demandRef= end to end: create a reservation, look it up
// by its demandRef, and confirm the response DTO round-trips id/sku/
// quantity/demandRef/status/allocations/createdAt/expiresAt.
func TestGetReservationsByDemandRef_Endpoint_Found(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 10, "binId": "A-1-1"})

	reserveRec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 6, "demandRef": "order-42-line-1"})
	if reserveRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", reserveRec.Code, reserveRec.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(reserveRec.Body.Bytes(), &created)

	rec := ts.do(t, http.MethodGet, "/reservations?demandRef=order-42-line-1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body []struct {
		ID          string `json:"id"`
		SKU         string `json:"sku"`
		Quantity    int    `json:"quantity"`
		DemandRef   string `json:"demandRef"`
		Status      string `json:"status"`
		Allocations []struct {
			StockUnitID string `json:"stockUnitId"`
			BinID       string `json:"binId"`
			Quantity    int    `json:"quantity"`
		} `json:"allocations"`
		CreatedAt string `json:"createdAt"`
		ExpiresAt string `json:"expiresAt"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("expected 1 reservation, got %d", len(body))
	}
	got := body[0]
	if got.ID != created.ID || got.SKU != "SKU-1" || got.Quantity != 6 || got.DemandRef != "order-42-line-1" || got.Status != "ACTIVE" {
		t.Fatalf("unexpected reservation in response: %+v", got)
	}
	if len(got.Allocations) != 1 || got.CreatedAt == "" || got.ExpiresAt == "" {
		t.Fatalf("expected allocations/createdAt/expiresAt populated, got %+v", got)
	}
	if got.Allocations[0].BinID != "A-1-1" {
		t.Fatalf("expected allocation pick location binId=A-1-1, got %q", got.Allocations[0].BinID)
	}
}

// POST /reservations exposes each allocation's pick location (binId), so
// a picker's RF gun can show where to go — across multiple bins when the
// reservation spans several StockUnits (ADR 0025).
func TestReserveStock_Endpoint_AllocationsCarryPickLocation(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 5)
	ts.seedBin(t, "A-1-2", 5)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 5, "binId": "A-1-1"})
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 5, "binId": "A-1-2"})

	rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 8, "demandRef": "order-pick-1"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Allocations []struct {
			StockUnitID string `json:"stockUnitId"`
			BinID       string `json:"binId"`
			Quantity    int    `json:"quantity"`
		} `json:"allocations"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	perBin := map[string]int{}
	for _, a := range body.Allocations {
		if a.StockUnitID == "" {
			t.Fatalf("expected stockUnitId on every allocation, got %+v", a)
		}
		perBin[a.BinID] += a.Quantity
	}
	if len(perBin) != 2 || perBin["A-1-1"]+perBin["A-1-2"] != 8 {
		t.Fatalf("expected 8 units allocated across bins A-1-1 and A-1-2, got %v", perBin)
	}
}

// An unknown demandRef is a 200 with an empty array, not a 404 — there is
// no single "resource" being looked up by id, just a filtered collection
// that may legitimately be empty.
func TestGetReservationsByDemandRef_Endpoint_NotFound_ReturnsEmptyArray(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodGet, "/reservations?demandRef=does-not-exist", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("expected empty array, got %d entries", len(body))
	}
}

// The demandRef query parameter is required: omitting it is a 400, matching
// this repo's RFC 7807 validation-error response shape.
func TestGetReservationsByDemandRef_Endpoint_MissingParam_Rejected(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodGet, "/reservations", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "missing-demand-ref", "/reservations")
}

// A demandRef with a revoked reservation and a successful retry returns
// BOTH — proving the "what did inventory-storage do for order X, line N"
// history use case end to end over HTTP, not just at the use-case layer.
func TestGetReservationsByDemandRef_Endpoint_MultipleReservations(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 20)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 20, "binId": "A-1-1"})

	firstRec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 5, "demandRef": "order-1"})
	var first struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(firstRec.Body.Bytes(), &first)

	revokeRec := ts.do(t, http.MethodDelete, "/reservations/"+first.ID, nil)
	if revokeRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 revoking, got %d: %s", revokeRec.Code, revokeRec.Body.String())
	}

	secondRec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 5, "demandRef": "order-1"})
	if secondRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 on retry, got %d: %s", secondRec.Code, secondRec.Body.String())
	}

	rec := ts.do(t, http.MethodGet, "/reservations?demandRef=order-1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body []struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	if len(body) != 2 {
		t.Fatalf("expected 2 reservations (revoked + retry), got %d: %s", len(body), rec.Body.String())
	}
}

func TestReserveStock_Endpoint_InsufficientUsable(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 5)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 5, "binId": "A-1-1"})

	rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 6, "demandRef": "order-1"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRevokeReservation_Endpoint(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 10, "binId": "A-1-1"})
	reserveRec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 6, "demandRef": "order-1"})
	var reserved struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(reserveRec.Body.Bytes(), &reserved)

	rec := ts.do(t, http.MethodDelete, "/reservations/"+reserved.ID, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRevokeReservation_Endpoint_UnknownID(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodDelete, "/reservations/does-not-exist", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusNotFound, "reservation-not-found", "/reservations/does-not-exist")
}

func TestReceiveStock_Endpoint_MalformedBody(t *testing.T) {
	ts := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/stock/receive", bytes.NewReader([]byte("{not-json")))
	rec := httptest.NewRecorder()
	ts.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "malformed-request-body", "/stock/receive")
}

// problemBody mirrors the RFC 7807 (application/problem+json) shape this
// service's error responses use.
type problemBody struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

func assertProblemDetails(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantSlug, wantInstance string) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("expected Content-Type application/problem+json, got %q", ct)
	}
	var p problemBody
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("unexpected error decoding problem details: %v (body: %s)", err, rec.Body.String())
	}
	wantType := "https://errors.inventory-storage.warehouse-systems.dev/" + wantSlug
	if p.Type != wantType {
		t.Fatalf("expected type %q, got %q", wantType, p.Type)
	}
	if p.Title == "" {
		t.Fatalf("expected non-empty title, got empty")
	}
	if p.Status != wantStatus {
		t.Fatalf("expected status %d in body, got %d", wantStatus, p.Status)
	}
	if p.Detail == "" {
		t.Fatalf("expected non-empty detail, got empty")
	}
	if p.Instance != wantInstance {
		t.Fatalf("expected instance %q, got %q", wantInstance, p.Instance)
	}
}

func TestGetUsable_Endpoint(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 7, "binId": "A-1-1"})

	rec := ts.do(t, http.MethodGet, "/inventory/SKU-1/usable", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Usable int `json:"usable"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Usable != 7 {
		t.Fatalf("expected usable=7, got %d", body.Usable)
	}
}

func TestRunCycleCount_Endpoint(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 10, "binId": "A-1-1"})

	rec := ts.do(t, http.MethodPost, "/bins/A-1-1/cycle-count", map[string]any{"countedQuantity": 10})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRunCycleCount_Endpoint_MissingCountedQuantity_Rejected(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)

	rec := ts.do(t, http.MethodPost, "/bins/A-1-1/cycle-count", map[string]any{})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for omitted countedQuantity, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "counted-quantity-required") {
		t.Fatalf("expected counted-quantity-required problem, got %s", rec.Body.String())
	}

	rec = ts.do(t, http.MethodPost, "/bins/A-1-1/cycle-count", map[string]any{"countedQuantity": nil})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for null countedQuantity, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRunCycleCount_Endpoint_ExplicitZeroCount_Accepted(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)

	rec := ts.do(t, http.MethodPost, "/bins/A-1-1/cycle-count", map[string]any{"countedQuantity": 0})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for explicit zero count, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestClassifyProduct_Endpoint_Create(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"Hazmat", "Fragile"},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		SKU          string   `json:"sku"`
		HandlingTags []string `json:"handlingTags"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	if body.SKU != "SKU-1" {
		t.Fatalf("expected sku=SKU-1, got %s", body.SKU)
	}
	if len(body.HandlingTags) != 2 {
		t.Fatalf("expected 2 handling tags, got %v", body.HandlingTags)
	}
}

func TestClassifyProduct_Endpoint_Replace_Returns200(t *testing.T) {
	ts := newTestServer()
	ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"Fragile"},
	})

	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"Hazmat"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on replace, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestClassifyProduct_Endpoint_TemperatureSensitiveWithClass(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags":     []string{"TemperatureSensitive"},
		"temperatureClass": "Frozen",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		TemperatureClass string `json:"temperatureClass"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.TemperatureClass != "Frozen" {
		t.Fatalf("expected temperatureClass=Frozen, got %s", body.TemperatureClass)
	}
}

func TestClassifyProduct_Endpoint_TemperatureSensitiveWithoutClass_Rejected(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"TemperatureSensitive"},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "temperature-class-required", "/products/SKU-1/classification")
}

func TestClassifyProduct_Endpoint_UnknownTag_Rejected(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"Explosive"},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "unknown-handling-tag", "/products/SKU-1/classification")
}

// DOTHazardClass round-trips through the classification endpoint: create
// with a Hazmat SKU carrying a DOT class, and the response echoes it back.
func TestClassifyProduct_Endpoint_DOTHazardClass_RoundTrip(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags":   []string{"Hazmat"},
		"dotHazardClass": 3,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		SKU            string   `json:"sku"`
		HandlingTags   []string `json:"handlingTags"`
		DOTHazardClass *int     `json:"dotHazardClass"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unexpected error decoding response: %v", err)
	}
	if body.DOTHazardClass == nil || *body.DOTHazardClass != 3 {
		t.Fatalf("expected dotHazardClass=3, got %v", body.DOTHazardClass)
	}

	getRec := ts.do(t, http.MethodGet, "/products/SKU-1/classification", nil)
	var getBody struct {
		DOTHazardClass *int `json:"dotHazardClass"`
	}
	_ = json.Unmarshal(getRec.Body.Bytes(), &getBody)
	if getBody.DOTHazardClass == nil || *getBody.DOTHazardClass != 3 {
		t.Fatalf("expected GET dotHazardClass=3, got %v", getBody.DOTHazardClass)
	}
}

// A Hazmat classification with no dotHazardClass field at all omits it
// from the response entirely (nil, not 0) — backward compatible with
// classifications registered before this field existed.
func TestClassifyProduct_Endpoint_DOTHazardClass_Omitted(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"Hazmat"},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var raw map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &raw)
	if _, present := raw["dotHazardClass"]; present {
		t.Fatalf("expected dotHazardClass to be omitted, got %v", raw["dotHazardClass"])
	}
}

// dotHazardClass supplied without the Hazmat tag is rejected 400.
func TestClassifyProduct_Endpoint_DOTHazardClassWithoutHazmat_Rejected(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags":   []string{"Fragile"},
		"dotHazardClass": 3,
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "dot-hazard-class-not-applicable", "/products/SKU-1/classification")
}

// dotHazardClass out of the valid 1-9 range is rejected 400.
func TestClassifyProduct_Endpoint_DOTHazardClassOutOfRange_Rejected(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags":   []string{"Hazmat"},
		"dotHazardClass": 10,
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "invalid-dot-hazard-class", "/products/SKU-1/classification")
}

func TestGetProductClassification_Endpoint_Found(t *testing.T) {
	ts := newTestServer()
	ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"HighValue"},
	})

	rec := ts.do(t, http.MethodGet, "/products/SKU-1/classification", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		SKU          string   `json:"sku"`
		HandlingTags []string `json:"handlingTags"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.SKU != "SKU-1" || len(body.HandlingTags) != 1 || body.HandlingTags[0] != "HighValue" {
		t.Fatalf("unexpected classification response: %+v", body)
	}
}

func TestGetProductClassification_Endpoint_NotFound(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodGet, "/products/SKU-UNKNOWN/classification", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusNotFound, "product-classification-not-found", "/products/SKU-UNKNOWN/classification")
}

// StowStock's placement enforcement, exercised end-to-end over HTTP: a
// Hazmat SKU stowed into a non-hazmat-rated bin is rejected 409 — proving
// the wiring, not just the use-case unit test.
func TestStowStock_Endpoint_HazmatPlacementRejected(t *testing.T) {
	stockRepo := memory.NewStockRepo()
	locationRepo := memory.NewLocationRepo()
	classificationRepo := memory.NewProductClassificationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	lookup := &stubLookup{known: true, hazmat: false}

	s := &inboundhttp.Server{
		StowStock: &usecases.StowStock{
			Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock,
			Classifications: classificationRepo, LocationLookup: lookup,
		},
		ClassifyProduct: &usecases.ClassifyProduct{Classifications: classificationRepo, Events: publisher, Clock: clock},
		Classifications: classificationRepo,
	}
	handler := inboundhttp.NewRouter(s, nil, "")

	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := location.NewBin(binID, shared.Quantity(10))
	_ = locationRepo.Save(context.Background(), bin)

	ts := testServer{handler: handler, stock: stockRepo, locations: locationRepo}
	classifyRec := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{"handlingTags": []string{"Hazmat"}})
	if classifyRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 classifying sku, got %d: %s", classifyRec.Code, classifyRec.Body.String())
	}

	rec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 5, "binId": "A-1-1"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusConflict, "hazmat-zone-required", "/stock/stow")
}

// stubLookup is a minimal ports.LocationClassificationLookup used only by
// the HTTP-level placement test above.
type stubLookup struct {
	known  bool
	hazmat bool
}

func (s *stubLookup) GetSlotAttributes(_ context.Context, _ shared.BinId) (product.SlotAttributes, error) {
	return product.SlotAttributes{Known: s.known, Hazmat: s.hazmat}, nil
}

// Same-bin DOT hazard-class segregation (ADR 0010), exercised end-to-end
// over HTTP: two incompatible-class Hazmat SKUs cannot share a bin.
func TestStowStock_Endpoint_SegregationRejected(t *testing.T) {
	stockRepo := memory.NewStockRepo()
	locationRepo := memory.NewLocationRepo()
	classificationRepo := memory.NewProductClassificationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	s := &inboundhttp.Server{
		StowStock: &usecases.StowStock{
			Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock,
			Classifications: classificationRepo,
		},
		ClassifyProduct: &usecases.ClassifyProduct{Classifications: classificationRepo, Events: publisher, Clock: clock},
		Classifications: classificationRepo,
	}
	handler := inboundhttp.NewRouter(s, nil, "")

	binID, _ := shared.NewBinId("A-1-1")
	bin, _ := location.NewBin(binID, shared.Quantity(100))
	_ = locationRepo.Save(context.Background(), bin)

	ts := testServer{handler: handler, stock: stockRepo, locations: locationRepo}

	// SKU-1: class 1 (explosives). SKU-2: class 8 (corrosives) — incompatible per the derived matrix.
	classifyRec1 := ts.do(t, http.MethodPut, "/products/SKU-1/classification", map[string]any{
		"handlingTags": []string{"Hazmat"}, "dotHazardClass": 1,
	})
	if classifyRec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 classifying SKU-1, got %d: %s", classifyRec1.Code, classifyRec1.Body.String())
	}
	classifyRec2 := ts.do(t, http.MethodPut, "/products/SKU-2/classification", map[string]any{
		"handlingTags": []string{"Hazmat"}, "dotHazardClass": 8,
	})
	if classifyRec2.Code != http.StatusCreated {
		t.Fatalf("expected 201 classifying SKU-2, got %d: %s", classifyRec2.Code, classifyRec2.Body.String())
	}

	stowRec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 5, "binId": "A-1-1"})
	if stowRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 stowing SKU-1, got %d: %s", stowRec.Code, stowRec.Body.String())
	}

	rec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-2", "quantity": 5, "binId": "A-1-1"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusConflict, "hazmat-class-incompatible", "/stock/stow")
}

// ---------------------------------------------------------------------------
// PUT /bins/{binId} and GET /bins/{binId} (ADR 0025)
// ---------------------------------------------------------------------------

type binBody struct {
	BinID     string `json:"binId"`
	Capacity  int    `json:"capacity"`
	Occupied  int    `json:"occupied"`
	Available int    `json:"available"`
}

func decodeBin(t *testing.T, rec *httptest.ResponseRecorder) binBody {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}
	var b binBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("unexpected error decoding bin response: %v (body: %s)", err, rec.Body.String())
	}
	return b
}

type registerBinEndpointCase struct {
	name      string
	seed      int // 0 = no bin seeded
	stow      int // units stowed into the seeded bin before the call
	body      any
	wantCode  int
	wantSlug  string // problem slug for error responses
	wantBin   binBody
	wantLocal bool // expect a Location header
}

func TestRegisterBin_Endpoint(t *testing.T) {
	tests := []registerBinEndpointCase{
		{name: "absent bin is created", body: map[string]any{"capacity": 10}, wantCode: http.StatusCreated,
			wantBin: binBody{BinID: "A-9-9", Capacity: 10, Occupied: 0, Available: 10}, wantLocal: true},
		{name: "same capacity is a no-op", seed: 10, stow: 3, body: map[string]any{"capacity": 10}, wantCode: http.StatusOK,
			wantBin: binBody{BinID: "A-9-9", Capacity: 10, Occupied: 3, Available: 7}},
		{name: "different capacity above occupancy resizes", seed: 10, stow: 3, body: map[string]any{"capacity": 4}, wantCode: http.StatusOK,
			wantBin: binBody{BinID: "A-9-9", Capacity: 4, Occupied: 3, Available: 1}},
		{name: "capacity below occupancy is rejected", seed: 10, stow: 3, body: map[string]any{"capacity": 2}, wantCode: http.StatusConflict,
			wantSlug: "capacity-below-occupancy"},
		{name: "zero capacity is rejected", body: map[string]any{"capacity": 0}, wantCode: http.StatusUnprocessableEntity,
			wantSlug: "invalid-bin-capacity"},
		{name: "negative capacity is rejected", body: map[string]any{"capacity": -1}, wantCode: http.StatusUnprocessableEntity,
			wantSlug: "negative-quantity"},
		{name: "missing capacity is rejected", body: map[string]any{}, wantCode: http.StatusBadRequest,
			wantSlug: "capacity-required"},
		{name: "capacity above int32 is rejected", body: map[string]any{"capacity": int64(2147483648)}, wantCode: http.StatusUnprocessableEntity,
			wantSlug: "capacity-out-of-range"},
		{name: "capacity at int32 max is accepted", body: map[string]any{"capacity": 2147483647}, wantCode: http.StatusCreated,
			wantBin: binBody{BinID: "A-9-9", Capacity: 2147483647, Occupied: 0, Available: 2147483647}, wantLocal: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runRegisterBinEndpointCase(t, tc) })
	}
}

// runRegisterBinEndpointCase seeds the case's starting bin/stock over the
// real router, issues the PUT, and checks status plus body or problem.
func runRegisterBinEndpointCase(t *testing.T, tc registerBinEndpointCase) {
	t.Helper()
	ts := newTestServer()
	if tc.seed > 0 {
		ts.seedBin(t, "A-9-9", tc.seed)
	}
	if tc.stow > 0 {
		stowRec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": tc.stow, "binId": "A-9-9"})
		if stowRec.Code != http.StatusCreated {
			t.Fatalf("expected 201 stowing, got %d: %s", stowRec.Code, stowRec.Body.String())
		}
	}

	rec := ts.do(t, http.MethodPut, "/bins/A-9-9", tc.body)
	if rec.Code != tc.wantCode {
		t.Fatalf("expected %d, got %d: %s", tc.wantCode, rec.Code, rec.Body.String())
	}
	if tc.wantSlug != "" {
		assertProblemDetails(t, rec, tc.wantCode, tc.wantSlug, "/bins/A-9-9")
		return
	}
	if got := decodeBin(t, rec); got != tc.wantBin {
		t.Fatalf("expected bin %+v, got %+v", tc.wantBin, got)
	}
	if loc := rec.Header().Get("Location"); (loc == "/bins/A-9-9") != tc.wantLocal {
		t.Fatalf("unexpected Location header %q (want present=%v)", loc, tc.wantLocal)
	}
}

func TestRegisterBin_Endpoint_MalformedBody_Rejected(t *testing.T) {
	ts := newTestServer()
	req := httptest.NewRequest(http.MethodPut, "/bins/A-9-9", strings.NewReader("{not json"))
	rec := httptest.NewRecorder()
	ts.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "malformed-request-body", "/bins/A-9-9")
}

// A registered bin is immediately usable as a stow target — the gap this
// endpoint closes (no more seeding bins straight into Postgres).
func TestRegisterBin_Endpoint_ThenStowSucceeds(t *testing.T) {
	ts := newTestServer()
	if rec := ts.do(t, http.MethodPut, "/bins/A-9-9", map[string]any{"capacity": 5}); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 registering bin, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 5, "binId": "A-9-9"}); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 stowing into registered bin, got %d: %s", rec.Code, rec.Body.String())
	}
	rec := ts.do(t, http.MethodGet, "/bins/A-9-9", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got, want := decodeBin(t, rec), (binBody{BinID: "A-9-9", Capacity: 5, Occupied: 5, Available: 0}); got != want {
		t.Fatalf("expected bin %+v, got %+v", want, got)
	}
}

func TestGetBin_Endpoint_Found(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 4, "binId": "A-1-1"})

	rec := ts.do(t, http.MethodGet, "/bins/A-1-1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got, want := decodeBin(t, rec), (binBody{BinID: "A-1-1", Capacity: 10, Occupied: 4, Available: 6}); got != want {
		t.Fatalf("expected bin %+v, got %+v", want, got)
	}
}

func TestGetBin_Endpoint_NotFound(t *testing.T) {
	ts := newTestServer()
	rec := ts.do(t, http.MethodGet, "/bins/NOPE-1", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusNotFound, "bin-not-found", "/bins/NOPE-1")
}

// demandRef is ReserveStock's idempotency key and the GET lookup key, so an
// empty one is a 400 (missing-demand-ref), never a 201 for an unreachable
// reservation.
func TestReserveStock_Endpoint_EmptyDemandRef_Rejected(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 10)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 10, "binId": "A-1-1"})

	rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 3, "demandRef": ""})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertProblemDetails(t, rec, http.StatusBadRequest, "missing-demand-ref", "/reservations")

	usable := ts.do(t, http.MethodGet, "/inventory/SKU-1/usable", nil)
	if !strings.Contains(usable.Body.String(), `"usable":10`) {
		t.Fatalf("a rejected reservation must not reserve stock, got %s", usable.Body.String())
	}
}
