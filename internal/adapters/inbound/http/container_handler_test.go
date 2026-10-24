package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// The container endpoints (ADR 0045) over in-memory repos.

const (
	cCaseBin  = "S1-A-Z-1-1-1-1"
	cEachBin  = "S1-A-Z-1-1-1-2"
	cOtherBin = "S2-A-Z-1-1-1-1"
	cLPN      = "003400000000000017"
)

type containerServer struct {
	testServer
	containers *memory.ContainerRepo
	handling   *memory.BinHandlingUnits
	events     *events.BufferedPublisher
}

func newContainerServer(t *testing.T) containerServer {
	t.Helper()
	stockRepo := memory.NewStockRepo()
	locations := memory.NewLocationRepo()
	reservations := memory.NewReservationRepo()
	containers := memory.NewContainerRepo()
	handling := memory.NewBinHandlingUnits()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	reserve := &usecases.ReserveStock{Stock: stockRepo, Reservations: reservations, Events: publisher, Clock: clock}
	s := &inboundhttp.Server{
		ReserveStock:      reserve,
		GetUsable:         &usecases.GetUsable{Stock: stockRepo},
		StowContainer:     &usecases.StowContainer{Containers: containers, Stock: stockRepo, Locations: locations, HandlingUnits: handling, Events: publisher, Clock: clock},
		BreakCase:         &usecases.BreakCase{Containers: containers, Stock: stockRepo, Locations: locations, HandlingUnits: handling, Events: publisher, Clock: clock},
		GetContainer:      &usecases.GetContainer{Containers: containers},
		ListBinContainers: &usecases.ListBinContainers{Containers: containers, Locations: locations},
		ReserveFullCases:  &usecases.ReserveFullCases{Reserve: reserve, Containers: containers},
	}
	cs := containerServer{
		testServer: testServer{handler: inboundhttp.NewRouter(s, nil, ""), stock: stockRepo, locations: locations},
		containers: containers, handling: handling, events: publisher,
	}
	for _, b := range []struct {
		id, site string
		hu       container.HandlingUnit
	}{{cCaseBin, "S1", container.HandlingCase}, {cEachBin, "S1", container.HandlingEach}, {cOtherBin, "S2", container.HandlingCase}} {
		bin := location.RehydrateBinAtSite(shared.BinId(b.id), 100, 0, shared.SiteID(b.site), 1)
		if err := locations.Save(context.Background(), bin); err != nil {
			t.Fatal(err)
		}
		handling.Set(shared.BinId(b.id), b.hu)
	}
	c, err := container.Register(container.RegisterSpec{
		LPN: cLPN, SKU: "SKU-1", GTIN: "24006381333935", UnitsPerPack: 12, Quantity: 12, OwnerRef: "seller-a", Lot: "L1",
		Expiry: "2027-03-01", ReceiptID: "R-1", LineNo: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := containers.Add(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	return cs
}

func decodeMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, rec.Body.String())
	}
	return m
}

// step performs one request and returns the status and decoded body.
func (cs containerServer) step(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	rec := cs.do(t, method, path, body)
	return rec.Code, decodeMap(t, rec)
}

func TestContainersREST_GetStowBreakLifecycle(t *testing.T) {
	cs := newContainerServer(t)
	facts := map[string]any{}
	record := func(name string, status int, body map[string]any, keys ...string) {
		facts[name+".status"] = status
		for _, k := range keys {
			facts[name+"."+k] = body[k]
		}
	}

	status, body := cs.step(t, http.MethodGet, "/containers/"+cLPN, nil)
	record("get", status, body, "state", "qtyEaches", "ownerRef", "gtin", "binId")
	status, body = cs.step(t, http.MethodPost, "/containers/"+cLPN+":stow", map[string]any{"bin": cCaseBin})
	record("stow", status, body, "binId", "siteId", "state")
	facts["stow.hasStockUnit"] = body["stockUnitId"] != "" && body["stockUnitId"] != nil
	status, body = cs.step(t, http.MethodGet, "/bins/"+cCaseBin+"/containers", nil)
	record("list", status, body, "binId")
	facts["list.count"] = len(body["containers"].([]any))
	status, body = cs.step(t, http.MethodPost, "/containers/"+cLPN+":break", map[string]any{"looseBin": cEachBin})
	record("break", status, body, "state")
	_, body = cs.step(t, http.MethodGet, "/bins/"+cCaseBin+"/containers", nil)
	facts["afterBreak.count"] = len(body["containers"].([]any))

	want := map[string]any{
		"get.status": 200, "get.state": "SEALED", "get.qtyEaches": float64(12), "get.ownerRef": "seller-a", "get.gtin": "24006381333935", "get.binId": nil,
		"stow.status": 200, "stow.binId": cCaseBin, "stow.siteId": "S1", "stow.state": "SEALED", "stow.hasStockUnit": true,
		"list.status": 200, "list.binId": cCaseBin, "list.count": 1,
		"break.status": 200, "break.state": "EMPTY", "afterBreak.count": 0,
	}
	if !reflect.DeepEqual(facts, want) {
		t.Fatalf("lifecycle facts differ:\n got  %v\n want %v", facts, want)
	}
}

func TestContainersREST_ErrorsAreProblemDetails(t *testing.T) {
	cs := newContainerServer(t)
	cases := []struct {
		name, method, path string
		body               any
		status             int
		slug               string
	}{
		{"unknown container", http.MethodGet, "/containers/NOPE", nil, 404, "container-not-found"},
		{"stow unknown container", http.MethodPost, "/containers/NOPE:stow", map[string]any{"bin": cCaseBin}, 404, "container-not-found"},
		{"stow to an each-only bin", http.MethodPost, "/containers/" + cLPN + ":stow", map[string]any{"bin": cEachBin}, 409, "bin-handling-unit-mismatch"},
		{"stow to a bin at another site is fine until the site is set; unknown bin", http.MethodPost, "/containers/" + cLPN + ":stow", map[string]any{"bin": "S1-NO-BIN"}, 404, "bin-not-found"},
		{"stow without a bin", http.MethodPost, "/containers/" + cLPN + ":stow", map[string]any{}, 400, "empty-bin-id"},
		{"break an unstowed container", http.MethodPost, "/containers/" + cLPN + ":break", map[string]any{"looseBin": cEachBin}, 409, "container-not-stowed"},
		{"break without a bin", http.MethodPost, "/containers/" + cLPN + ":break", map[string]any{}, 400, "empty-bin-id"},
		{"list an unknown bin", http.MethodGet, "/bins/S1-NO-BIN/containers", nil, 404, "bin-not-found"},
	}
	for _, tc := range cases {
		rec := cs.do(t, tc.method, tc.path, tc.body)
		got := decodeMap(t, rec)
		if rec.Code != tc.status || got["type"] != "https://errors.inventory-storage.warehouse-systems.dev/"+tc.slug {
			t.Errorf("%s: %d %v, want %d %s", tc.name, rec.Code, got, tc.status, tc.slug)
		}
	}
	for _, path := range []string{"/containers/" + cLPN + ":stow", "/containers/" + cLPN + ":break"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.Body = http.NoBody
		rec := httptest.NewRecorder()
		cs.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s with no body = %d, want 400", path, rec.Code)
		}
	}
	// a stowed container cannot be stowed at another site, nor broken twice
	if rec := cs.do(t, http.MethodPost, "/containers/"+cLPN+":stow", map[string]any{"bin": cCaseBin}); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	if rec := cs.do(t, http.MethodPost, "/containers/"+cLPN+":stow", map[string]any{"bin": cOtherBin}); rec.Code != http.StatusConflict ||
		decodeMap(t, rec)["type"] != "https://errors.inventory-storage.warehouse-systems.dev/container-not-at-site" {
		t.Fatalf("restow at another site = %d %s", rec.Code, rec.Body.String())
	}
	if rec := cs.do(t, http.MethodPost, "/containers/"+cLPN+":break", map[string]any{"looseBin": cEachBin}); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	if rec := cs.do(t, http.MethodPost, "/containers/"+cLPN+":break", map[string]any{"looseBin": cEachBin}); rec.Code != http.StatusConflict ||
		decodeMap(t, rec)["type"] != "https://errors.inventory-storage.warehouse-systems.dev/container-not-sealed" {
		t.Fatalf("second break = %d %s", rec.Code, rec.Body.String())
	}
}

func TestReservationsREST_FullCasesAndOwners(t *testing.T) {
	cs := newContainerServer(t)
	if status, body := cs.step(t, http.MethodPost, "/containers/"+cLPN+":stow", map[string]any{"bin": cCaseBin}); status != http.StatusOK {
		t.Fatal(body)
	}
	const insufficient = "https://errors.inventory-storage.warehouse-systems.dev/insufficient-usable"
	const reserved = "https://errors.inventory-storage.warehouse-systems.dev/container-reserved"
	facts := map[string]any{}

	// owner B cannot get owner A's case, and neither can a caller naming no owner
	status, body := cs.step(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "cases": 1, "demandRef": "D-b", "ownerRef": "seller-b"})
	facts["ownerB"] = []any{status, body["type"]}
	status, body = cs.step(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "cases": 1, "demandRef": "D-f"})
	facts["fleet"] = []any{status, body["type"]}

	_, usable := cs.step(t, http.MethodGet, "/inventory/SKU-1/usable?ownerRef=seller-a", nil)
	facts["usable"] = []any{usable["usable"], usable["looseUsable"], usable["reservableCases"], usable["ownerRef"]}
	_, fleetUsable := cs.step(t, http.MethodGet, "/inventory/SKU-1/usable", nil)
	facts["fleetUsable"] = fleetUsable["usable"]

	status, res := cs.step(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "cases": 1, "demandRef": "D-a", "ownerRef": "seller-a"})
	allocs := res["allocations"].([]any)
	facts["reserve"] = []any{status, res["quantity"], res["ownerRef"], len(allocs), allocs[0].(map[string]any)["binId"]}
	status, body = cs.step(t, http.MethodPost, "/containers/"+cLPN+":break", map[string]any{"looseBin": cEachBin})
	facts["breakReserved"] = []any{status, body["type"]}

	want := map[string]any{
		"ownerB": []any{409, insufficient}, "fleet": []any{409, insufficient},
		"usable": []any{float64(12), float64(0), float64(1), "seller-a"}, "fleetUsable": float64(0),
		"reserve": []any{201, float64(12), "seller-a", 1, cCaseBin}, "breakReserved": []any{409, reserved},
	}
	if !reflect.DeepEqual(facts, want) {
		t.Fatalf("facts differ:\n got  %v\n want %v", facts, want)
	}
}

func TestReservationsREST_CasesValidation(t *testing.T) {
	cs := newContainerServer(t)
	cases := []struct {
		name   string
		body   map[string]any
		status int
		slug   string
	}{
		{"quantity and cases", map[string]any{"sku": "SKU-1", "cases": 1, "quantity": 5, "demandRef": "D"}, 400, "quantity-and-cases-exclusive"},
		{"zero cases", map[string]any{"sku": "SKU-1", "cases": 0, "demandRef": "D"}, 422, "zero-quantity"},
		{"bad owner", map[string]any{"sku": "SKU-1", "quantity": 1, "demandRef": "D", "ownerRef": "no good"}, 400, "invalid-owner-ref"},
	}
	for _, tc := range cases {
		rec := cs.do(t, http.MethodPost, "/reservations", tc.body)
		if rec.Code != tc.status || decodeMap(t, rec)["type"] != "https://errors.inventory-storage.warehouse-systems.dev/"+tc.slug {
			t.Errorf("%s = %d %s", tc.name, rec.Code, rec.Body.String())
		}
	}
	if rec := cs.do(t, http.MethodGet, "/inventory/SKU-1/usable?ownerRef=no%20good", nil); rec.Code != http.StatusBadRequest {
		t.Errorf("bad ownerRef on usable = %d", rec.Code)
	}
}
