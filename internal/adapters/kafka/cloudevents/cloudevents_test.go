package cloudevents_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
)

func TestType_BuildsFullyQualifiedType(t *testing.T) {
	got := cloudevents.Type("reservation", "StockReserved")
	want := "com.warehouse.wms.inventory-storage.reservation.StockReserved"
	if got != want {
		t.Fatalf("Type = %q, want %q", got, want)
	}
}

func TestDataSchema_BuildsURN(t *testing.T) {
	if got, want := cloudevents.DataSchema(cloudevents.StreamEvents, "StockReserved", 1),
		"urn:warehouse:inventory-storage:events:StockReserved:v1"; got != want {
		t.Fatalf("DataSchema = %q, want %q", got, want)
	}
	if got, want := cloudevents.DataSchema(cloudevents.StreamAnalytics, "ItemStowed", 2),
		"urn:warehouse:inventory-storage:analytics:ItemStowed:v2"; got != want {
		t.Fatalf("DataSchema = %q, want %q", got, want)
	}
}

func TestNew_ExactJSON(t *testing.T) {
	b, err := cloudevents.New(cloudevents.Spec{
		ID:        "11111111-1111-4111-8111-111111111111",
		Entity:    "reservation",
		EventName: "StockReserved",
		Subject:   "res-1",
		Time:      time.Date(2026, 8, 21, 22, 0, 0, 0, time.FixedZone("BRT", -3*3600)),
		Stream:    cloudevents.StreamEvents,
		Data:      map[string]any{"sku": "SKU-1"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/inventory-storage","type":"com.warehouse.wms.inventory-storage.reservation.StockReserved","subject":"res-1","datacontenttype":"application/json","dataschema":"urn:warehouse:inventory-storage:events:StockReserved:v1","time":"2026-08-22T01:00:00Z","data":{"sku":"SKU-1"}}`
	assertJSONEqual(t, string(b), want)
}

func TestNew_RejectsEmptySubject(t *testing.T) {
	_, err := cloudevents.New(cloudevents.Spec{ID: "x", Entity: "stock", EventName: "E", Time: time.Now(), Stream: cloudevents.StreamEvents, Version: 1})
	if err == nil {
		t.Fatal("expected an error for an empty subject")
	}
}

func TestNew_RejectsEmptyID(t *testing.T) {
	_, err := cloudevents.New(cloudevents.Spec{Entity: "stock", EventName: "E", Subject: "s", Time: time.Now(), Stream: cloudevents.StreamEvents, Version: 1})
	if err == nil {
		t.Fatal("expected an error for an empty id")
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := cloudevents.ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("header = %s: %s", h.Key, h.Value)
	}
}

func TestDecode_RoundTrip(t *testing.T) {
	at := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)
	b, err := cloudevents.New(cloudevents.Spec{
		ID: "22222222-2222-4222-8222-222222222222", Entity: "bin", EventName: "CycleCountCompleted",
		Subject: "A-1", Time: at, Stream: cloudevents.StreamAnalytics, Version: 1,
		Data: map[string]any{"bin_id": "A-1"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := cloudevents.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.ID() != "22222222-2222-4222-8222-222222222222" || e.Subject() != "A-1" || !e.Time().Equal(at) ||
		e.Type() != "com.warehouse.wms.inventory-storage.bin.CycleCountCompleted" {
		t.Fatalf("decoded event attributes wrong: %s", e.String())
	}
	var data struct {
		BinID string `json:"bin_id"`
	}
	if err := e.DataAs(&data); err != nil || data.BinID != "A-1" {
		t.Fatalf("DataAs = %+v, %v", data, err)
	}
}

func TestDecode_RejectsLegacyFlatEnvelope(t *testing.T) {
	legacy := `{"event_id":"e1","event_type":"StockReserved","occurred_at":"2026-08-21T22:00:00Z","source":"inventory-storage","data":{"sku":"SKU-1"}}`
	if _, err := cloudevents.Decode([]byte(legacy)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("Decode(legacy) err = %v, want ErrNotCloudEvent", err)
	}
}

func TestDecode_RejectsGarbageAndWrongSpecVersion(t *testing.T) {
	for name, raw := range map[string]string{
		"not json":     `not-json`,
		"spec 0.3":     `{"specversion":"0.3","id":"1","source":"/x","type":"t"}`,
		"missing type": `{"specversion":"1.0","id":"1","source":"/x"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := cloudevents.Decode([]byte(raw)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
				t.Fatalf("err = %v, want ErrNotCloudEvent", err)
			}
		})
	}
}

func assertJSONEqual(t *testing.T, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("unmarshal got: %v (%s)", err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", gb, wb)
	}
}
