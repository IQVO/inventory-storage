package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// productMasterCE builds a CloudEvent exactly as product-master (the
// producer) emits it: its own source and the full type string, so the
// envelope is hand-assembled rather than built with this repo's helper
// (which hardcodes inventory-storage's identity).
func productMasterCE(t *testing.T, id, eventType string, data any) []byte {
	t.Helper()
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	value, err := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/product-master",
		"type":            eventType,
		"subject":         "SKU-1",
		"time":            "2026-10-06T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:product-master:events:ProductClassified:v1",
		"data":            json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

func classifiedPayload(version int64, tags ...string) map[string]any {
	return map[string]any{
		"sku":                   "SKU-1",
		"handling_tags":         tags,
		"classification_source": "native",
		"version":               version,
	}
}

// recordingApplier wraps a real ApplyProductClassification (or fails the
// first N calls) and records every update it received.
type recordingApplier struct {
	mu        sync.Mutex
	delegate  ProductClassificationApplier
	updates   []usecases.ProductClassificationUpdate
	failFirst int
}

func (a *recordingApplier) Execute(ctx context.Context, u usecases.ProductClassificationUpdate) (usecases.ApplyOutcome, error) {
	a.mu.Lock()
	a.updates = append(a.updates, u)
	n := len(a.updates)
	a.mu.Unlock()
	if n <= a.failFirst {
		return "", errors.New("db is down")
	}
	return a.delegate.Execute(ctx, u)
}

func (a *recordingApplier) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.updates)
}

// fakeLocalCopy is an in-test ports.ProductClassificationLocalCopy +
// ProcessedEventRepo, so this inbound-adapter test drives the REAL use case
// without importing an outbound adapter.
type fakeLocalCopy struct {
	mu       sync.Mutex
	rows     map[shared.SKU]*product.ProductClassification
	versions map[shared.SKU]int64
	sources  map[shared.SKU]string
	claimed  map[string]bool
}

func newFakeLocalCopy() *fakeLocalCopy {
	return &fakeLocalCopy{
		rows: map[shared.SKU]*product.ProductClassification{}, versions: map[shared.SKU]int64{},
		sources: map[shared.SKU]string{}, claimed: map[string]bool{},
	}
}

func (f *fakeLocalCopy) ApplyIfNewer(_ context.Context, c *product.ProductClassification, version int64, source string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.versions[c.SKU()]; ok && v >= version {
		return false, nil
	}
	f.rows[c.SKU()], f.versions[c.SKU()], f.sources[c.SKU()] = c, version, source
	return true, nil
}

func (f *fakeLocalCopy) Claim(_ context.Context, consumer, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimed[consumer+"/"+id] {
		return false, nil
	}
	f.claimed[consumer+"/"+id] = true
	return true, nil
}

func (f *fakeLocalCopy) find(sku shared.SKU) (*product.ProductClassification, int64, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rows[sku], f.versions[sku], f.sources[sku]
}

type productMasterFixture struct {
	repo     *fakeLocalCopy
	applier  *recordingApplier
	reader   *fakeReader
	consumer *ProductMasterConsumer
}

func newProductMasterFixture(failFirst int, msgs ...kafkago.Message) productMasterFixture {
	repo := newFakeLocalCopy()
	applier := &recordingApplier{
		delegate:  &usecases.ApplyProductClassification{Classifications: repo, ProcessedEvents: repo},
		failFirst: failFirst,
	}
	reader := &fakeReader{queue: msgs}
	consumer := &ProductMasterConsumer{
		Reader: reader, Apply: applier, Logger: quietLogger(),
		Backoff: func(int) time.Duration { return time.Millisecond },
	}
	return productMasterFixture{repo: repo, applier: applier, reader: reader, consumer: consumer}
}

// runUntilCommitted runs the consumer until want messages are committed
// (or a deadline), then stops it.
func (f productMasterFixture) runUntilCommitted(t *testing.T, want int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && f.reader.commitsLen() < want {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if got := f.reader.commitsLen(); got != want {
		t.Fatalf("commits = %d, want %d", got, want)
	}
}

func (f productMasterFixture) tags(t *testing.T) []product.HandlingTag {
	t.Helper()
	c, _, _ := f.repo.find(shared.SKU("SKU-1"))
	if c == nil {
		return nil
	}
	return c.HandlingTags()
}

func pmMsg(offset int64, value []byte) kafkago.Message {
	return kafkago.Message{Topic: ProductMasterTopic, Partition: 0, Offset: offset, Value: value}
}

func TestProductMasterConsumer_ValidEventUpsertsTheLocalCopy(t *testing.T) {
	data := classifiedPayload(3, "Hazmat", "TemperatureSensitive")
	data["temperature_class"] = "Frozen"
	data["dot_hazard_class"] = 3
	f := newProductMasterFixture(0, pmMsg(0, productMasterCE(t, "ce-1", typeProductMasterClassified, data)))

	f.runUntilCommitted(t, 1)

	c, v, src := f.repo.find(shared.SKU("SKU-1"))
	if c == nil || !c.IsHazmat() || c.TemperatureClass() != product.Frozen || c.DOTHazardClass() != 3 {
		t.Fatalf("local copy = %+v", c)
	}
	if v != 3 || src != "native" {
		t.Fatalf("version/source = %d/%q, want 3/native", v, src)
	}
	got := f.applier.updates[0]
	if got.EventID != "ce-1" || got.SKU != "SKU-1" || got.Version != 3 || got.ClassificationSource != "native" || got.DOTHazardClass != 3 {
		t.Fatalf("decoded update = %+v", got)
	}
}

func TestProductMasterConsumer_StaleVersionIsCommittedWithoutOverwriting(t *testing.T) {
	f := newProductMasterFixture(0,
		pmMsg(0, productMasterCE(t, "ce-1", typeProductMasterClassified, classifiedPayload(5, "Hazmat"))),
		pmMsg(1, productMasterCE(t, "ce-2", typeProductMasterClassified, classifiedPayload(4, "Fragile"))),
	)

	f.runUntilCommitted(t, 2)

	if tags := f.tags(t); len(tags) != 1 || tags[0] != product.Hazmat {
		t.Fatalf("tags = %v, want [Hazmat] (the stale v4 must not win)", tags)
	}
	if f.applier.calls() != 2 {
		t.Fatalf("applier calls = %d, want 2", f.applier.calls())
	}
}

func TestProductMasterConsumer_UnknownTypesAreIgnored(t *testing.T) {
	f := newProductMasterFixture(0,
		pmMsg(0, productMasterCE(t, "ce-r", "com.warehouse.wms.product-master.product.ProductRegistered", map[string]any{"sku": "SKU-1", "version": 1})),
		pmMsg(1, productMasterCE(t, "ce-m", "com.warehouse.wms.product-master.product.ProductMeasured", map[string]any{"sku": "SKU-1", "version": 2})),
		// Same entity/event name, other producer: the FULL type differs.
		pmMsg(2, productMasterCE(t, "ce-l", "com.warehouse.wms.inventory-storage.product.ProductClassified", classifiedPayload(9, "Fragile"))),
	)

	f.runUntilCommitted(t, 3)

	if f.applier.calls() != 0 {
		t.Fatalf("applier calls = %d, want 0", f.applier.calls())
	}
}

func TestProductMasterConsumer_InvalidCloudEventIsSkipped(t *testing.T) {
	f := newProductMasterFixture(0,
		pmMsg(0, []byte(`{"event_id":"old","event_type":"ProductClassified","sku":"SKU-1"}`)),
		pmMsg(1, []byte(`not json`)),
	)

	f.runUntilCommitted(t, 2)

	if f.applier.calls() != 0 {
		t.Fatalf("poison reached the use case %d times", f.applier.calls())
	}
}

func TestProductMasterConsumer_MalformedPayloadsAreCommittedPast(t *testing.T) {
	undecodable := productMasterCE(t, "ce-u", typeProductMasterClassified, map[string]any{"sku": 42, "version": "three"})
	invalid := productMasterCE(t, "ce-i", typeProductMasterClassified, classifiedPayload(1, "Radioactive"))
	f := newProductMasterFixture(0, pmMsg(0, undecodable), pmMsg(1, invalid))

	f.runUntilCommitted(t, 2)

	if f.tags(t) != nil {
		t.Fatal("a malformed payload wrote the local copy")
	}
	// The undecodable one never reaches the use case; the invalid one does
	// and is rejected deterministically — no retry.
	if f.applier.calls() != 1 {
		t.Fatalf("applier calls = %d, want 1 (no retry of a deterministic failure)", f.applier.calls())
	}
}

// A transient failure retries the SAME message and commits exactly once,
// after the success.
func TestProductMasterConsumer_TransientErrorIsRetriedThenCommittedOnce(t *testing.T) {
	f := newProductMasterFixture(2, pmMsg(7, productMasterCE(t, "ce-1", typeProductMasterClassified, classifiedPayload(1, "Fragile"))))

	f.runUntilCommitted(t, 1)

	if f.applier.calls() != 3 {
		t.Fatalf("applier calls = %d, want 3 (fail, fail, succeed)", f.applier.calls())
	}
	if f.reader.commitAt(0).Offset != 7 {
		t.Fatalf("committed offset %d, want 7", f.reader.commitAt(0).Offset)
	}
	if tags := f.tags(t); len(tags) != 1 || tags[0] != product.Fragile {
		t.Fatalf("tags = %v, want [Fragile]", tags)
	}
}

// Cancelling during a retry wait returns without committing the message.
func TestProductMasterConsumer_CancelDuringRetryDoesNotCommit(t *testing.T) {
	f := newProductMasterFixture(1_000_000, pmMsg(0, productMasterCE(t, "ce-1", typeProductMasterClassified, classifiedPayload(1, "Fragile"))))
	f.consumer.Backoff = func(int) time.Duration { return time.Hour }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && f.applier.calls() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if f.reader.commitsLen() != 0 {
		t.Fatal("a message whose handler never succeeded was committed")
	}
}

// The real constructor wires the configured group and topic.
func TestNewProductMasterConsumer_UsesConfiguredGroupAndTopic(t *testing.T) {
	c := NewProductMasterConsumer([]string{"broker:9092"}, "inventory-storage-product-master", nil, nil)
	defer func() { _ = c.Close() }()
	r, ok := c.Reader.(*kafkago.Reader)
	if !ok {
		t.Fatalf("reader is %T", c.Reader)
	}
	cfg := r.Config()
	if cfg.Topic != ProductMasterTopic || cfg.GroupID != "inventory-storage-product-master" {
		t.Fatalf("reader config topic=%q group=%q", cfg.Topic, cfg.GroupID)
	}
}
