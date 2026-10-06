package events_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/log_publisher_golden.json from current output")

const goldenPath = "testdata/log_publisher_golden.json"

func mustSKU(t *testing.T, v string) shared.SKU {
	t.Helper()
	s, err := shared.NewSKU(v)
	if err != nil {
		t.Fatalf("NewSKU: %v", err)
	}
	return s
}

func mustBin(t *testing.T, v string) shared.BinId {
	t.Helper()
	b, err := shared.NewBinId(v)
	if err != nil {
		t.Fatalf("NewBinId: %v", err)
	}
	return b
}

func mustQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("NewQuantity: %v", err)
	}
	return q
}

// goldenEvents is one instance of every domain event type, with fixed
// values, keyed by event name.
func goldenEvents(t *testing.T) map[string]shared.DomainEvent {
	t.Helper()
	at := time.Date(2026, 8, 21, 22, 0, 0, 0, time.UTC)
	sku := mustSKU(t, "SKU-1")
	bin := mustBin(t, "A-1-1")
	qty := mustQty(t, 5)
	system := mustQty(t, 7)

	cls, err := product.New(sku, []product.HandlingTag{product.Hazmat, product.TemperatureSensitive}, product.Chilled, 3)
	if err != nil {
		t.Fatalf("product.New: %v", err)
	}

	evs := []shared.DomainEvent{
		shared.NewStockReceived(at, sku, qty),
		shared.NewItemStowed(at, sku, bin, qty),
		shared.NewLocationRecorded(at, "su-1", bin),
		shared.NewStockReserved(at, "res-1", sku, qty, "order-42"),
		shared.NewReservationExpired(at, "res-1"),
		shared.NewReservationRevoked(at, "res-1"),
		shared.NewStockPicked(at, "res-1", sku, qty),
		shared.NewItemUnlocated(at, "su-1", sku, bin, qty),
		shared.NewCycleCountCompleted(at, bin, qty, system, true),
		shared.NewDiscrepancyDetected(at, bin, qty, system),
		product.NewProductClassified(cls, at),
	}
	out := make(map[string]shared.DomainEvent, len(evs))
	for _, e := range evs {
		out[e.EventName()] = e
	}
	return out
}

// logPayload publishes event through the LogPublisher and returns the
// compact JSON of the "payload" attribute it logged.
func logPayload(t *testing.T, event shared.DomainEvent) []byte {
	t.Helper()
	var buf bytes.Buffer
	pub := events.NewLogPublisher(slog.New(slog.NewJSONHandler(&buf, nil)))
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var rec struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode log line %q: %v", buf.String(), err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, rec.Payload); err != nil {
		t.Fatalf("compact payload: %v", err)
	}
	return compact.Bytes()
}

// TestLogPublisher_PayloadMatchesGolden pins the JSON the LogPublisher emits
// for every domain event type byte-for-byte. The golden file was captured
// BEFORE the json tags were moved out of internal/domain (tier-2 item 1a),
// so it proves the adapter-owned DTOs reproduce the original wire shape.
func TestLogPublisher_PayloadMatchesGolden(t *testing.T) {
	got := map[string]json.RawMessage{}
	for name, e := range goldenEvents(t) {
		got[name] = logPayload(t, e)
	}

	if *updateGolden {
		b, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatalf("marshal golden: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(goldenPath, append(b, '\n'), 0o600); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}

	raw, err := os.ReadFile(goldenPath) // #nosec G304 -- fixed testdata path
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var want map[string]json.RawMessage
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(want) != len(got) {
		t.Fatalf("golden has %d events, test produced %d", len(want), len(got))
	}
	for name, g := range got {
		w, ok := want[name]
		if !ok {
			t.Errorf("%s: missing from golden file", name)
			continue
		}
		var wc bytes.Buffer
		if err := json.Compact(&wc, w); err != nil {
			t.Fatalf("%s: compact golden: %v", name, err)
		}
		if !bytes.Equal(g, wc.Bytes()) {
			t.Errorf("%s: log payload changed\n got: %s\nwant: %s", name, g, wc.Bytes())
		}
	}
}
