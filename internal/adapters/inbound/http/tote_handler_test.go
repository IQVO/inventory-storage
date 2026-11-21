package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// The tote endpoints (ADR 0054) over in-memory repos. Internal test: it also
// pins toAllocationResponse.

func TestPutToteCreatesThenAnswersOK(t *testing.T) {
	srv := newToteTestServer(t)
	first := do(t, srv, http.MethodPut, "/totes/TOTE-1", "")
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first PUT = %d", first.StatusCode)
	}
	again := do(t, srv, http.MethodPut, "/totes/TOTE-1", "")
	if again.StatusCode != http.StatusOK {
		t.Fatalf("second PUT = %d", again.StatusCode)
	}
	var body struct {
		LPN      string `json:"lpn"`
		Contents []any  `json:"contents"`
	}
	decode(t, again, &body)
	if body.LPN != "TOTE-1" || body.Contents == nil || len(body.Contents) != 0 {
		t.Fatalf("body = %+v (contents must be [] not null)", body)
	}
}

func TestFillAndStowAToteThroughREST(t *testing.T) {
	srv := newToteTestServer(t)
	do(t, srv, http.MethodPut, "/totes/TOTE-1", "")
	mustStatus(t, doKeyed(t, srv, "/totes/TOTE-1/stowages", `{"bin":"WH1-RCV-DEC-D01-01-01-A"}`, "k1"), http.StatusOK)
	mustStatus(t, doKeyed(t, srv, "/totes/TOTE-1/fills", `{"sku":"SKU-1","quantity":6}`, "k2"), http.StatusCreated)
	mustStatus(t, doKeyed(t, srv, "/totes/TOTE-1/stowages", `{"bin":"WH1-AUTO-ASRS-ASRS1-00-00-S"}`, "k3"), http.StatusOK)
	got := do(t, srv, http.MethodGet, "/totes/TOTE-1", "")
	var body struct {
		BinID    string `json:"binId"`
		SiteID   string `json:"siteId"`
		Contents []struct {
			SKU         string `json:"sku"`
			Quantity    int    `json:"quantity"`
			StockUnitID string `json:"stockUnitId"`
		} `json:"contents"`
	}
	decode(t, got, &body)
	if body.BinID != "WH1-AUTO-ASRS-ASRS1-00-00-S" || body.SiteID != "WH1" || len(body.Contents) != 1 ||
		body.Contents[0].SKU != "SKU-1" || body.Contents[0].Quantity != 6 || body.Contents[0].StockUnitID == "" {
		t.Fatalf("tote = %+v", body)
	}
}

func TestToteProblems(t *testing.T) {
	srv := newToteTestServer(t)
	assertProblem(t, do(t, srv, http.MethodGet, "/totes/NOPE", ""), http.StatusNotFound, "tote-not-found")
	do(t, srv, http.MethodPut, "/totes/TOTE-1", "")
	assertProblem(t, doKeyed(t, srv, "/totes/TOTE-1/fills", `{"sku":"SKU-1","quantity":1}`, "k1"), http.StatusConflict, "tote-not-placed")
	assertProblem(t, doKeyed(t, srv, "/totes/TOTE-1/fills", `{"sku":"","quantity":1}`, "k2"), http.StatusBadRequest, "empty-sku")
	assertProblem(t, doKeyed(t, srv, "/totes/TOTE-1/stowages", `{"bin":""}`, "k3"), http.StatusBadRequest, "empty-bin-id")
	assertProblem(t, do(t, srv, http.MethodPut, "/totes/TOTE%2F1", ""), http.StatusNotFound, "")
}

func TestAllocationCarriesContainerIDOnlyWhenThereIsOne(t *testing.T) {
	a := toAllocationResponse(reservation.Allocation{StockUnitID: "su-1", BinID: "B-1", Quantity: 2, ContainerLPN: "TOTE-1"})
	raw, _ := json.Marshal(a)
	if !strings.Contains(string(raw), `"containerId":"TOTE-1"`) {
		t.Fatalf("allocation = %s", raw)
	}
	raw, _ = json.Marshal(toAllocationResponse(reservation.Allocation{StockUnitID: "su-1", BinID: "B-1", Quantity: 2}))
	if strings.Contains(string(raw), "containerId") {
		t.Fatalf("an allocation in no container must omit containerId: %s", raw)
	}
}

// newToteTestServer builds the router with the five tote use cases over
// in-memory repos and two WH1 bins: a decant station bin and the AS/RS
// storage location.
func newToteTestServer(t *testing.T) http.Handler {
	t.Helper()
	stockRepo, locations, containers := memory.NewStockRepo(), memory.NewLocationRepo(), memory.NewContainerRepo()
	totes := memory.NewToteRepo(containers)
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC))
	for _, id := range []string{"WH1-RCV-DEC-D01-01-01-A", "WH1-AUTO-ASRS-ASRS1-00-00-S"} {
		if err := locations.Save(context.Background(), location.RehydrateBinAtSite(shared.BinId(id), 100, 0, "WH1", 1)); err != nil {
			t.Fatal(err)
		}
	}
	s := &Server{
		RegisterTote: &usecases.RegisterTote{Totes: totes, Containers: containers, Events: publisher, Clock: clock},
		GetTote:      &usecases.GetTote{Totes: totes, Stock: stockRepo},
		FillTote:     &usecases.FillTote{Totes: totes, Stock: stockRepo, Locations: locations, Events: publisher, Clock: clock},
		StowTote: &usecases.StowTote{Totes: totes, Contents: stockRepo, Stock: stockRepo, Locations: locations,
			Events: publisher, Clock: clock},
		ListBinTotes: &usecases.ListBinTotes{Totes: totes, Locations: locations},
	}
	return NewRouter(s, nil, "")
}

// recorded is one answer of the router: the status and the body bytes (a
// recorder, not a live *http.Response, so there is no body to close).
type recorded struct {
	StatusCode int
	body       []byte
}

func serve(srv http.Handler, req *http.Request) *recorded {
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return &recorded{StatusCode: rec.Code, body: rec.Body.Bytes()}
}

// do performs one request with an optional JSON body.
func do(t *testing.T, srv http.Handler, method, path, body string) *recorded {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return serve(srv, req)
}

// doKeyed POSTs body to path with an Idempotency-Key.
func doKeyed(t *testing.T, srv http.Handler, path, body, key string) *recorded {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(IdempotencyKeyHeader, key)
	return serve(srv, req)
}

func decode(t *testing.T, resp *recorded, dest any) {
	t.Helper()
	if err := json.Unmarshal(resp.body, dest); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, resp.body)
	}
}

func mustStatus(t *testing.T, resp *recorded, want int) {
	t.Helper()
	if resp.StatusCode != want {
		var body map[string]any
		decode(t, resp, &body)
		t.Fatalf("status = %d, want %d: %v", resp.StatusCode, want, body)
	}
}

// assertProblem checks the status and, when slug is not empty, that the
// problem type ends in /slug.
func assertProblem(t *testing.T, resp *recorded, status int, slug string) {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	decode(t, resp, &p)
	if resp.StatusCode != status || (slug != "" && !strings.HasSuffix(p.Type, "/"+slug)) {
		t.Fatalf("status %d type %q, want %d %q", resp.StatusCode, p.Type, status, slug)
	}
}
