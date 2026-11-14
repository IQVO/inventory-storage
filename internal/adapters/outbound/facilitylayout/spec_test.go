package facilitylayout

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Contract tests against the PINNED COPY of facility-layout's OpenAPI spec
// (testdata/facility-layout.openapi.yaml, byte-equal to IQVO/facility-layout
// develop per contracts/pinned.json; refreshed by copying, never edited).
//
// Archetypes (REST R1-R4): R1 the route(s) the production client actually
// calls are declared in the pinned spec; R2 the spec's own example validates
// against its schema AND round-trips through the real client; R3 the real
// client against an httptest gateway, including problem+json error mapping
// to this package's sentinels; R4 the response DTO's json tags stay a subset
// of the pinned schema's properties (absence-tolerant: the spec may grow
// fields the DTO does not map yet — that must not fail the pin).

const pinnedSpecPath = "testdata/facility-layout.openapi.yaml"

// classificationRoute is the ONE route Client.GetSlotAttributes calls
// (client.go builds "<base>/locations/<code>/classification", GET).
const classificationRoute = "/locations/{locationCode}/classification"

func loadPinnedSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(pinnedSpecPath)
	if err != nil {
		t.Fatalf("load pinned facility-layout spec: %v", err)
	}
	return doc
}

func classificationOperation(t *testing.T) *openapi3.Operation {
	t.Helper()
	item := loadPinnedSpec(t).Paths.Find(classificationRoute)
	if item == nil || item.Get == nil {
		t.Fatalf("pinned spec does not declare GET %s", classificationRoute)
	}
	return item.Get
}

// R1: the route the client calls must stay in the producer's contract. If
// facility-layout renames or drops it, this fails before the fleet notices
// in production.
func TestPinnedSpecDeclaresTheRouteTheClientCalls(t *testing.T) {
	if op := classificationOperation(t); op.OperationID != "getLocationClassification" {
		t.Fatalf("operationId = %q, want getLocationClassification", op.OperationID)
	}
}

// R2 part 1: the spec's own 200 example validates against its own
// LocationClassification schema (a broken producer example is a contract
// bug worth surfacing here, not at the first consumer).
func TestPinnedSpecExampleSatisfiesItsSchema(t *testing.T) {
	media := classificationOperation(t).Responses.Status(200).Value.Content["application/json"]
	if media == nil || media.Schema == nil {
		t.Fatal("200 response lacks an application/json schema")
	}
	if err := media.Schema.Value.VisitJSON(media.Example); err != nil {
		t.Fatalf("spec example violates LocationClassification: %v", err)
	}
}

// R2 part 2 + R3 happy path: the spec's example round-trips through the
// REAL client over a real httptest gateway — decode, temperature-class
// parsing and all.
func TestClientDecodesThePinnedSpecExample(t *testing.T) {
	example := classificationOperation(t).Responses.Status(200).Value.Content["application/json"].Example
	body, err := json.Marshal(example)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	got, err := NewClient(srv.URL, srv.Client()).GetSlotAttributes(t.Context(), shared.BinId("WH1-STOR-FRZ-A07-03-02-B"))
	if err != nil {
		t.Fatal(err)
	}
	want := example.(map[string]any)
	if !got.Known || got.Hazmat != want["hazmat"] {
		t.Fatalf("attributes = %+v (example %v)", got, want)
	}
}

// R3: the spec's documented 404 maps to the client's fail-open sentinel
// (Known=false, no error) — the contract and the client must agree that an
// unmodeled location is not a failure.
func TestClientMapsSpecNotFoundToFailOpen(t *testing.T) {
	media := classificationOperation(t).Responses.Status(404).Value.Content["application/problem+json"]
	body, err := json.Marshal(media.Example)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	got, err := NewClient(srv.URL, srv.Client()).GetSlotAttributes(t.Context(), shared.BinId("WH1-STOR-AMB-A07-03-02-B"))
	if err != nil {
		t.Fatalf("404 must not be an error: %v", err)
	}
	if got.Known {
		t.Fatal("404 must report Known=false")
	}
}

// R3: an undocumented-for-this-client status carrying the spec's own
// problem+json InternalError example surfaces as ErrUnexpectedStatus, the
// sentinel StowStock's caller normalizes — never a silent misdecode.
func TestClientMapsProblemJSONErrorToSentinel(t *testing.T) {
	doc := loadPinnedSpec(t)
	problem := doc.Components.Responses["InternalError"].Value.Content["application/problem+json"]
	body, err := json.Marshal(problem.Example)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	_, err = NewClient(srv.URL, srv.Client()).GetSlotAttributes(t.Context(), shared.BinId("WH1-STOR-AMB-A07-03-02-B"))
	if !errors.Is(err, ErrUnexpectedStatus) {
		t.Fatalf("error = %v, want ErrUnexpectedStatus", err)
	}
}

// R4: every json tag the response DTO decodes must stay a declared property
// of the pinned LocationClassification schema. Absence-tolerant in the other
// direction: fields the spec adds but the DTO does not map yet do not fail —
// they are invisible to this client by design.
func TestResponseDTOTagsStayInThePinnedSchema(t *testing.T) {
	props := loadPinnedSpec(t).Components.Schemas["LocationClassification"].Value.Properties
	dto := reflect.TypeOf(classificationResponse{})
	for _, f := range reflect.VisibleFields(dto) {
		tag := f.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		if _, declared := props[tag]; !declared {
			t.Errorf("DTO field %s decodes json %q, which LocationClassification no longer declares", f.Name, tag)
		}
	}
}
