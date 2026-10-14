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
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// POST /relocations and GET /relocations/{id} (ADR 0039) over in-memory
// repos. The Idempotency-Key behaviour needs Postgres and is covered by
// idempotency_relocation_integration_test.go.

const (
	httpRelFrom = "WH1-STOR-AMB-A01-01-01-A"
	httpRelTo   = "WH1-STOR-AMB-A02-01-01-A"
	httpRelWH2  = "WH2-STOR-AMB-A02-01-01-A"
)

type relocationServer struct {
	handler   http.Handler
	stock     *memory.StockRepo
	locations *memory.LocationRepo
	events    *events.BufferedPublisher
}

func newRelocationServer(t *testing.T) relocationServer {
	t.Helper()
	stockRepo := memory.NewStockRepo()
	locationRepo := memory.NewLocationRepo()
	relocations := memory.NewRelocationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	for _, spec := range []struct {
		id       string
		capacity int
		occupied int
	}{{httpRelFrom, 50, 10}, {httpRelTo, 5, 0}, {httpRelWH2, 50, 0}} {
		bin, _ := location.NewBin(shared.BinId(spec.id), shared.Quantity(spec.capacity))
		if spec.occupied > 0 {
			_ = bin.Occupy(shared.Quantity(spec.occupied))
		}
		_ = locationRepo.Save(ctx, bin)
	}
	unit, _ := stock.NewStockUnit("su-src", "SKU-1", httpRelFrom, 10)
	_ = stockRepo.Save(ctx, unit)

	s := &inboundhttp.Server{
		RelocateStock: &usecases.RelocateStock{Stock: stockRepo, Locations: locationRepo, Relocations: relocations, Events: publisher, Clock: clock},
		GetRelocation: &usecases.GetRelocation{Relocations: relocations},
	}
	return relocationServer{handler: inboundhttp.NewRouter(s, nil, ""), stock: stockRepo, locations: locationRepo, events: publisher}
}

func (rs relocationServer) post(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/relocations", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	rs.handler.ServeHTTP(rec, req)
	return rec
}

func (rs relocationServer) get(path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	rs.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

const relocateBody = `{"siteCode":"WH1","sku":"SKU-1","fromBin":"` + httpRelFrom + `","toBin":"` + httpRelTo + `","quantity":4,"reason":"SLOTTING","sourceRef":"plan-1/move-2"}`

type relocationJSON struct {
	ID                     string `json:"id"`
	SiteCode               string `json:"siteCode"`
	SKU                    string `json:"sku"`
	FromBin                string `json:"fromBin"`
	ToBin                  string `json:"toBin"`
	Quantity               int    `json:"quantity"`
	Reason                 string `json:"reason"`
	SourceRef              string `json:"sourceRef"`
	DestinationStockUnitID string `json:"destinationStockUnitId"`
	Legs                   []struct {
		StockUnitID string `json:"stockUnitId"`
		Quantity    int    `json:"quantity"`
	} `json:"legs"`
	OccurredAt string `json:"occurredAt"`
}

func decodeRelocation(t *testing.T, body []byte) relocationJSON {
	t.Helper()
	var got relocationJSON
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode relocation: %v (%s)", err, body)
	}
	return got
}

func TestPostRelocations_Created(t *testing.T) {
	rs := newRelocationServer(t)

	rec := rs.post(relocateBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	got := decodeRelocation(t, rec.Body.Bytes())
	if rec.Header().Get("Location") != "/relocations/"+got.ID {
		t.Fatalf("Location = %q, want /relocations/%s", rec.Header().Get("Location"), got.ID)
	}
	assertCreatedRelocation(t, got)
	if len(rs.events.Events()) != 1 {
		t.Fatalf("published %d events, want 1 StockRelocated", len(rs.events.Events()))
	}
}

func assertCreatedRelocation(t *testing.T, got relocationJSON) {
	t.Helper()
	if got.SiteCode != "WH1" || got.SKU != "SKU-1" || got.FromBin != httpRelFrom || got.ToBin != httpRelTo {
		t.Fatalf("response = %+v", got)
	}
	if got.Quantity != 4 || got.Reason != "SLOTTING" || got.SourceRef != "plan-1/move-2" || got.OccurredAt != "2026-10-13T12:00:00Z" {
		t.Fatalf("response = %+v", got)
	}
	if got.DestinationStockUnitID == "" || len(got.Legs) != 1 || got.Legs[0].StockUnitID != "su-src" || got.Legs[0].Quantity != 4 {
		t.Fatalf("outcome = dest %q legs %+v", got.DestinationStockUnitID, got.Legs)
	}
}

func TestPostRelocations_ReasonDefaultsToManualAndSourceRefToEmpty(t *testing.T) {
	rs := newRelocationServer(t)

	rec := rs.post(`{"siteCode":"WH1","sku":"SKU-1","fromBin":"` + httpRelFrom + `","toBin":"` + httpRelTo + `","quantity":1}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d (body: %s)", rec.Code, rec.Body.String())
	}
	got := decodeRelocation(t, rec.Body.Bytes())
	if got.Reason != "MANUAL" || got.SourceRef != "" {
		t.Fatalf("reason %q sourceRef %q, want MANUAL and empty", got.Reason, got.SourceRef)
	}
}

func TestPostRelocations_Rejections(t *testing.T) {
	base := map[string]any{"siteCode": "WH1", "sku": "SKU-1", "fromBin": httpRelFrom, "toBin": httpRelTo, "quantity": 4}
	cases := []struct {
		name   string
		patch  map[string]any
		raw    string
		status int
		slug   string
	}{
		{name: "malformed json", raw: `{`, status: 400, slug: "malformed-request-body"},
		{name: "empty site", patch: map[string]any{"siteCode": " "}, status: 400, slug: "empty-site-id"},
		{name: "empty sku", patch: map[string]any{"sku": ""}, status: 400, slug: "empty-sku"},
		{name: "empty from bin", patch: map[string]any{"fromBin": ""}, status: 400, slug: "empty-bin-id"},
		{name: "empty to bin", patch: map[string]any{"toBin": ""}, status: 400, slug: "empty-bin-id"},
		{name: "zero quantity", patch: map[string]any{"quantity": 0}, status: 422, slug: "zero-quantity"},
		{name: "unknown reason", patch: map[string]any{"reason": "WHIM"}, status: 422, slug: "invalid-relocation-reason"},
		{name: "source ref too long", patch: map[string]any{"sourceRef": strings.Repeat("x", 201)}, status: 422, slug: "source-ref-too-long"},
		{name: "same bin", patch: map[string]any{"toBin": httpRelFrom}, status: 422, slug: "same-bin"},
		{name: "unknown bin", patch: map[string]any{"toBin": "WH1-STOR-AMB-Z99-01-01-A"}, status: 404, slug: "bin-not-found"},
		{name: "bin in another site", patch: map[string]any{"toBin": httpRelWH2}, status: 409, slug: "bin-not-at-site"},
		{name: "more than usable", patch: map[string]any{"quantity": 11}, status: 409, slug: "insufficient-usable"},
		{name: "destination full", patch: map[string]any{"quantity": 6}, status: 409, slug: "bin-full"},
	}
	for _, tc := range cases {
		rs := newRelocationServer(t)
		body := tc.raw
		if body == "" {
			m := map[string]any{}
			for k, v := range base {
				m[k] = v
			}
			for k, v := range tc.patch {
				m[k] = v
			}
			raw, _ := json.Marshal(m)
			body = string(raw)
		}
		rec := rs.post(body)
		if rec.Code != tc.status {
			t.Fatalf("%s: status = %d, want %d (body: %s)", tc.name, rec.Code, tc.status, rec.Body.String())
		}
		if slug := problemSlugOf(t, rec.Body.Bytes()); slug != tc.slug {
			t.Fatalf("%s: problem slug = %q, want %q", tc.name, slug, tc.slug)
		}
		if len(rs.events.Events()) != 0 {
			t.Fatalf("%s: a rejected relocation published an event", tc.name)
		}
	}
}

func problemSlugOf(t *testing.T, body []byte) string {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode problem: %v (%s)", err, body)
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

func TestGetRelocation(t *testing.T) {
	rs := newRelocationServer(t)
	created := decodeRelocation(t, rs.post(relocateBody).Body.Bytes())

	rec := rs.get("/relocations/" + created.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %s)", rec.Code, rec.Body.String())
	}
	if got := decodeRelocation(t, rec.Body.Bytes()); got.ID != created.ID || got.Quantity != 4 || got.DestinationStockUnitID != created.DestinationStockUnitID {
		t.Fatalf("GET = %+v, want the created relocation %+v", got, created)
	}

	missing := rs.get("/relocations/rel-missing")
	if missing.Code != http.StatusNotFound || problemSlugOf(t, missing.Body.Bytes()) != "relocation-not-found" {
		t.Fatalf("missing: status %d body %s, want 404 relocation-not-found", missing.Code, missing.Body.String())
	}
}
