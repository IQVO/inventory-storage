package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// returnCE builds a CloudEvent exactly as returns-processing (the
// producer) emits it: its own source, subject = the disposition id, key
// = the disposition id.
func returnCE(t *testing.T, id, eventType string, data any) []byte {
	t.Helper()
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	value, err := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/returns-processing",
		"type":            eventType,
		"subject":         "disp-1",
		"time":            "2026-10-22T15:10:02Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:returns-processing:events:ReturnLineStaged:v1",
		"data":            json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

func stagedLineData(sku string, qty any, condition string) map[string]any {
	return map[string]any{
		"receipt_id": "rcpt-1", "line_no": 1, "sku": sku, "quantity": qty,
		"condition": condition, "received_at": "2026-10-22T14:05:00Z",
		"rma_id": "rma-1", "disposition_id": "disp-1", "site_id": "WH1",
	}
}

type returnFixture struct {
	stock    *receivedStock
	reader   *fakeReader
	consumer *ReturnLineConsumer
}

func newReturnFixture(failFirst int, msgs ...kafkago.Message) returnFixture {
	stock := &receivedStock{failFirst: failFirst}
	processed := &claims{}
	uow := rollbackUoW{claims: processed}
	book := &usecases.BookReturnLine{
		Receive:         &usecases.ReceiveStock{Events: stock, Clock: fixedNow(time.Date(2026, 10, 22, 15, 10, 2, 0, time.UTC)), UnitOfWork: uow},
		ProcessedEvents: processed,
		UnitOfWork:      uow,
	}
	reader := &fakeReader{queue: msgs}
	consumer := &ReturnLineConsumer{
		Reader: reader, Book: book, Logger: quietLogger(),
		Backoff: func(int) time.Duration { return time.Millisecond },
	}
	return returnFixture{stock: stock, reader: reader, consumer: consumer}
}

func returnMsg(offset int64, value []byte) kafkago.Message {
	return kafkago.Message{Topic: ReturnsProcessingTopic, Partition: 0, Offset: offset, Key: []byte("disp-1"), Value: value}
}

func (f returnFixture) runUntilCommitted(t *testing.T, want int) {
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

func TestReturnLineConsumer_StagedLineIsBookedAsStagedStock(t *testing.T) {
	f := newReturnFixture(0, returnMsg(0, returnCE(t, "ce-1", typeReturnLineStaged, stagedLineData("SKU-1", 2, "Good"))))

	f.runUntilCommitted(t, 1)

	got := f.stock.received()
	if len(got) != 1 || got[0].SKU != "SKU-1" || got[0].Quantity.Int() != 2 {
		t.Fatalf("StockReceived events = %+v, want one for SKU-1 x 2", got)
	}
}

func TestReturnLineConsumer_UnknownTypesAreIgnored(t *testing.T) {
	f := newReturnFixture(0,
		returnMsg(0, returnCE(t, "ce-a", "com.warehouse.wms.returns-processing.returnauthorization.ReturnAuthorized", map[string]any{"rma_id": "rma-1"})),
		returnMsg(1, returnCE(t, "ce-d", "com.warehouse.wms.returns-processing.disposition.DispositionDecided", map[string]any{"disposition_id": "disp-1", "outcome": "LIQUIDATE"})),
		returnMsg(2, returnCE(t, "ce-i", "com.warehouse.wms.returns-processing.returnreceipt.ReturnLineInspected", map[string]any{"receipt_id": "rcpt-1", "grade": "B"})),
		// Same event name, other producer: the FULL type differs.
		returnMsg(3, returnCE(t, "ce-x", "com.warehouse.wms.inbound-receiving.disposition.ReturnLineStaged", stagedLineData("SKU-1", 2, "Good"))),
		// Same entity and name, other versioned type.
		returnMsg(4, returnCE(t, "ce-v2", typeReturnLineStaged+".v2", stagedLineData("SKU-1", 2, "Good"))),
	)

	f.runUntilCommitted(t, 5)

	if n := f.stock.attemptCount(); n != 0 {
		t.Fatalf("ReceiveStock attempts = %d, want 0", n)
	}
}

func TestReturnLineConsumer_InvalidCloudEventIsSkipped(t *testing.T) {
	f := newReturnFixture(0,
		returnMsg(0, []byte(`{"event_id":"old","event_type":"ReturnLineStaged","sku":"SKU-1","quantity":5}`)),
		returnMsg(1, []byte(`not json`)),
		returnMsg(2, nil),
	)

	f.runUntilCommitted(t, 3)

	if n := f.stock.attemptCount(); n != 0 {
		t.Fatalf("poison reached ReceiveStock %d times", n)
	}
}

func TestReturnLineConsumer_InvalidPayloadsAreCommittedPast(t *testing.T) {
	f := newReturnFixture(0,
		returnMsg(0, returnCE(t, "ce-1", typeReturnLineStaged, stagedLineData("", 2, "Good"))),
		returnMsg(1, returnCE(t, "ce-2", typeReturnLineStaged, stagedLineData("SKU-1", 0, "Good"))),
		returnMsg(2, returnCE(t, "ce-3", typeReturnLineStaged, stagedLineData("SKU-1", -2, "Good"))),
		returnMsg(3, returnCE(t, "ce-4", typeReturnLineStaged, stagedLineData("SKU-1", 2, "Damaged"))),
		returnMsg(4, returnCE(t, "ce-5", typeReturnLineStaged, stagedLineData("SKU-1", "two", "Good"))),
		// No disposition id: the line has no provenance anchor.
		returnMsg(5, returnCE(t, "ce-6", typeReturnLineStaged, map[string]any{
			"receipt_id": "rcpt-1", "line_no": 1, "sku": "SKU-1", "quantity": 2,
			"condition": "Good", "received_at": "2026-10-22T14:05:00Z",
			"rma_id": "rma-1", "site_id": "WH1",
		})),
		// A valid line after the poison still goes through.
		returnMsg(6, returnCE(t, "ce-ok", typeReturnLineStaged, stagedLineData("SKU-9", 7, "Good"))),
	)

	f.runUntilCommitted(t, 7)

	got := f.stock.received()
	if len(got) != 1 || got[0].SKU != "SKU-9" {
		t.Fatalf("StockReceived events = %+v, want only SKU-9", got)
	}
	// Deterministic failures never retry: the only ReceiveStock attempt
	// is the valid line.
	if n := f.stock.attemptCount(); n != 1 {
		t.Fatalf("ReceiveStock attempts = %d, want 1", n)
	}
}

func TestReturnLineConsumer_DuplicateEventIDIsAppliedOnce(t *testing.T) {
	value := returnCE(t, "ce-1", typeReturnLineStaged, stagedLineData("SKU-1", 2, "Good"))
	f := newReturnFixture(0, returnMsg(0, value), returnMsg(1, value))

	f.runUntilCommitted(t, 2)

	if got := f.stock.received(); len(got) != 1 {
		t.Fatalf("StockReceived events = %d, want 1 (the redelivered id must not book twice)", len(got))
	}
}

// A transient failure retries the SAME message and commits exactly once,
// after the success.
func TestReturnLineConsumer_TransientErrorIsRetriedThenCommittedOnce(t *testing.T) {
	f := newReturnFixture(2, returnMsg(7, returnCE(t, "ce-1", typeReturnLineStaged, stagedLineData("SKU-1", 2, "Good"))))

	f.runUntilCommitted(t, 1)

	if n := f.stock.attemptCount(); n != 3 {
		t.Fatalf("ReceiveStock attempts = %d, want 3 (fail, fail, succeed)", n)
	}
	if f.reader.commitAt(0).Offset != 7 {
		t.Fatalf("committed offset %d, want 7", f.reader.commitAt(0).Offset)
	}
	if got := f.stock.received(); len(got) != 1 {
		t.Fatalf("StockReceived events = %d, want 1", len(got))
	}
}

// Cancelling during a retry wait returns without committing the message.
func TestReturnLineConsumer_CancelDuringRetryDoesNotCommit(t *testing.T) {
	f := newReturnFixture(1_000_000, returnMsg(0, returnCE(t, "ce-1", typeReturnLineStaged, stagedLineData("SKU-1", 2, "Good"))))
	f.consumer.Backoff = func(int) time.Duration { return time.Hour }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && f.stock.attemptCount() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil (a shutdown mid-retry is a clean stop)", err)
	}
	if f.reader.commitsLen() != 0 {
		t.Fatal("a message whose handler never succeeded was committed")
	}
}

// The real constructor wires the configured group and topic.
func TestNewReturnLineConsumer_UsesConfiguredGroupAndTopic(t *testing.T) {
	c := NewReturnLineConsumer([]string{"broker:9092"}, "inventory-storage-return-lines", nil, nil)
	defer func() { _ = c.Close() }()
	r, ok := c.Reader.(*kafkago.Reader)
	if !ok {
		t.Fatalf("reader is %T", c.Reader)
	}
	cfg := r.Config()
	if cfg.Topic != "warehouse.returns-processing.events" || cfg.GroupID != "inventory-storage-return-lines" {
		t.Fatalf("reader config topic=%q group=%q", cfg.Topic, cfg.GroupID)
	}
	if cfg.StartOffset != kafkago.FirstOffset {
		t.Fatalf("start offset = %d, want FirstOffset", cfg.StartOffset)
	}
}

func TestReturnLineType_IsByteIdenticalToTheProducerContract(t *testing.T) {
	if typeReturnLineStaged != "com.warehouse.wms.returns-processing.disposition.ReturnLineStaged" {
		t.Fatalf("type = %q", typeReturnLineStaged)
	}
}
