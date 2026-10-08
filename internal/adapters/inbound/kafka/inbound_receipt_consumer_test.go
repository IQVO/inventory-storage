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
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// receiptCE builds a CloudEvent exactly as inbound-receiving (the producer)
// emits it: its own source and the full type string.
func receiptCE(t *testing.T, id, eventType string, data any) []byte {
	t.Helper()
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	value, err := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/inbound-receiving",
		"type":            eventType,
		"subject":         "rcpt-1",
		"time":            "2026-10-08T14:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:inbound-receiving:events:ReceiptLineReceived:v1",
		"data":            json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

func lineData(sku string, qty any, condition string) map[string]any {
	return map[string]any{
		"receipt_id": "rcpt-1", "asn_number": "ASN-1001", "line_no": 1,
		"sku": sku, "quantity": qty, "condition": condition, "received_at": "2026-10-08T14:00:00Z",
	}
}

// receivedStock records the StockReceived events ReceiveStock publishes and
// can fail the first N publishes (a transient outbox/DB error).
type receivedStock struct {
	mu        sync.Mutex
	got       []shared.StockReceived
	attempts  int
	failFirst int
}

func (r *receivedStock) Publish(_ context.Context, e shared.DomainEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts++
	if r.attempts <= r.failFirst {
		return errors.New("db is down")
	}
	if sr, ok := e.(shared.StockReceived); ok {
		r.got = append(r.got, sr)
	}
	return nil
}

func (r *receivedStock) received() []shared.StockReceived {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]shared.StockReceived(nil), r.got...)
}

func (r *receivedStock) attemptCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts
}

type fixedNow time.Time

func (f fixedNow) Now() time.Time { return time.Time(f) }

// claims is an in-test ProcessedEventRepo (an inbound adapter test must not
// import an outbound adapter).
type claims struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (c *claims) Claim(_ context.Context, consumer, id string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	if c.seen[consumer+"/"+id] {
		return false, nil
	}
	c.seen[consumer+"/"+id] = true
	return true, nil
}

// rollbackUoW stands in for the Postgres transaction: when fn fails, the
// claims made inside it are discarded, exactly as a rollback would.
type rollbackUoW struct{ claims *claims }

func (u rollbackUoW) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	u.claims.mu.Lock()
	before := make(map[string]bool, len(u.claims.seen))
	for k, v := range u.claims.seen {
		before[k] = v
	}
	u.claims.mu.Unlock()

	err := fn(ctx)
	if err != nil {
		u.claims.mu.Lock()
		u.claims.seen = before
		u.claims.mu.Unlock()
	}
	return err
}

type receiptFixture struct {
	stock    *receivedStock
	reader   *fakeReader
	consumer *InboundReceiptConsumer
}

func newReceiptFixture(failFirst int, msgs ...kafkago.Message) receiptFixture {
	stock := &receivedStock{failFirst: failFirst}
	processed := &claims{}
	uow := rollbackUoW{claims: processed}
	book := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: stock, Clock: fixedNow(time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)), UnitOfWork: uow},
		ProcessedEvents: processed,
		UnitOfWork:      uow,
	}
	reader := &fakeReader{queue: msgs}
	consumer := &InboundReceiptConsumer{
		Reader: reader, Book: book, Logger: quietLogger(),
		Backoff: func(int) time.Duration { return time.Millisecond },
	}
	return receiptFixture{stock: stock, reader: reader, consumer: consumer}
}

// runUntilCommitted runs the consumer until want messages are committed (or a
// deadline), then stops it.
func (f receiptFixture) runUntilCommitted(t *testing.T, want int) {
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

func receiptMsg(offset int64, value []byte) kafkago.Message {
	return kafkago.Message{Topic: InboundReceivingTopic, Partition: 0, Offset: offset, Key: []byte("ASN-1001"), Value: value}
}

func TestInboundReceiptConsumer_GoodLineIsBookedAsStagedStock(t *testing.T) {
	f := newReceiptFixture(0, receiptMsg(0, receiptCE(t, "ce-1", typeReceiptLineReceived, lineData("SKU-1", 40, "Good"))))

	f.runUntilCommitted(t, 1)

	got := f.stock.received()
	if len(got) != 1 || got[0].SKU != "SKU-1" || got[0].Quantity.Int() != 40 {
		t.Fatalf("StockReceived events = %+v, want one for SKU-1 x 40", got)
	}
}

func TestInboundReceiptConsumer_DamagedLineIsCommittedButNotBooked(t *testing.T) {
	f := newReceiptFixture(0, receiptMsg(0, receiptCE(t, "ce-1", typeReceiptLineReceived, lineData("SKU-1", 4, "Damaged"))))

	f.runUntilCommitted(t, 1)

	if n := f.stock.attemptCount(); n != 0 {
		t.Fatalf("a Damaged line reached ReceiveStock %d times", n)
	}
}

func TestInboundReceiptConsumer_UnknownTypesAreIgnored(t *testing.T) {
	f := newReceiptFixture(0,
		receiptMsg(0, receiptCE(t, "ce-o", "com.warehouse.wms.inbound-receiving.receipt.ReceiptOpened", map[string]any{"receipt_id": "rcpt-1"})),
		receiptMsg(1, receiptCE(t, "ce-c", "com.warehouse.wms.inbound-receiving.receipt.ReceiptClosed", map[string]any{"receipt_id": "rcpt-1", "discrepancies": []any{}})),
		receiptMsg(2, receiptCE(t, "ce-a", "com.warehouse.wms.inbound-receiving.asn.ASNRegistered", map[string]any{"asn_number": "ASN-1001"})),
		// Same event name, other producer: the FULL type differs.
		receiptMsg(3, receiptCE(t, "ce-x", "com.warehouse.wms.other-context.receipt.ReceiptLineReceived", lineData("SKU-1", 40, "Good"))),
		// Same entity and name, other versioned type.
		receiptMsg(4, receiptCE(t, "ce-v2", typeReceiptLineReceived+".v2", lineData("SKU-1", 40, "Good"))),
	)

	f.runUntilCommitted(t, 5)

	if n := f.stock.attemptCount(); n != 0 {
		t.Fatalf("ReceiveStock attempts = %d, want 0", n)
	}
}

func TestInboundReceiptConsumer_InvalidCloudEventIsSkipped(t *testing.T) {
	f := newReceiptFixture(0,
		receiptMsg(0, []byte(`{"event_id":"old","event_type":"ReceiptLineReceived","sku":"SKU-1","quantity":5}`)),
		receiptMsg(1, []byte(`not json`)),
		receiptMsg(2, nil),
	)

	f.runUntilCommitted(t, 3)

	if n := f.stock.attemptCount(); n != 0 {
		t.Fatalf("poison reached ReceiveStock %d times", n)
	}
}

func TestInboundReceiptConsumer_InvalidPayloadsAreCommittedPast(t *testing.T) {
	f := newReceiptFixture(0,
		receiptMsg(0, receiptCE(t, "ce-1", typeReceiptLineReceived, lineData("", 5, "Good"))),
		receiptMsg(1, receiptCE(t, "ce-2", typeReceiptLineReceived, lineData("SKU-1", 0, "Good"))),
		receiptMsg(2, receiptCE(t, "ce-3", typeReceiptLineReceived, lineData("SKU-1", -2, "Good"))),
		receiptMsg(3, receiptCE(t, "ce-4", typeReceiptLineReceived, lineData("SKU-1", 5, "Quarantined"))),
		receiptMsg(4, receiptCE(t, "ce-5", typeReceiptLineReceived, lineData("SKU-1", "five", "Good"))),
		receiptMsg(5, receiptCE(t, "ce-6", typeReceiptLineReceived, map[string]any{"receipt_id": "rcpt-1"})),
		// A valid line after the poison still goes through.
		receiptMsg(6, receiptCE(t, "ce-ok", typeReceiptLineReceived, lineData("SKU-9", 7, "Good"))),
	)

	f.runUntilCommitted(t, 7)

	got := f.stock.received()
	if len(got) != 1 || got[0].SKU != "SKU-9" {
		t.Fatalf("StockReceived events = %+v, want only SKU-9", got)
	}
	// Deterministic failures never retry: the only ReceiveStock attempt is the valid line.
	if n := f.stock.attemptCount(); n != 1 {
		t.Fatalf("ReceiveStock attempts = %d, want 1", n)
	}
}

func TestInboundReceiptConsumer_DuplicateEventIDIsAppliedOnce(t *testing.T) {
	value := receiptCE(t, "ce-1", typeReceiptLineReceived, lineData("SKU-1", 40, "Good"))
	f := newReceiptFixture(0, receiptMsg(0, value), receiptMsg(1, value))

	f.runUntilCommitted(t, 2)

	if got := f.stock.received(); len(got) != 1 {
		t.Fatalf("StockReceived events = %d, want 1 (the redelivered id must not book twice)", len(got))
	}
}

// A transient failure retries the SAME message and commits exactly once,
// after the success.
func TestInboundReceiptConsumer_TransientErrorIsRetriedThenCommittedOnce(t *testing.T) {
	f := newReceiptFixture(2, receiptMsg(7, receiptCE(t, "ce-1", typeReceiptLineReceived, lineData("SKU-1", 40, "Good"))))

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
func TestInboundReceiptConsumer_CancelDuringRetryDoesNotCommit(t *testing.T) {
	f := newReceiptFixture(1_000_000, receiptMsg(0, receiptCE(t, "ce-1", typeReceiptLineReceived, lineData("SKU-1", 40, "Good"))))
	f.consumer.Backoff = func(int) time.Duration { return time.Hour }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && f.stock.attemptCount() < 1 {
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
func TestNewInboundReceiptConsumer_UsesConfiguredGroupAndTopic(t *testing.T) {
	c := NewInboundReceiptConsumer([]string{"broker:9092"}, "inventory-storage-inbound-receipt", nil, nil)
	defer func() { _ = c.Close() }()
	r, ok := c.Reader.(*kafkago.Reader)
	if !ok {
		t.Fatalf("reader is %T", c.Reader)
	}
	cfg := r.Config()
	if cfg.Topic != "warehouse.inbound-receiving.events" || cfg.GroupID != "inventory-storage-inbound-receipt" {
		t.Fatalf("reader config topic=%q group=%q", cfg.Topic, cfg.GroupID)
	}
	if cfg.StartOffset != kafkago.FirstOffset {
		t.Fatalf("start offset = %d, want FirstOffset", cfg.StartOffset)
	}
}

func TestInboundReceiptType_IsByteIdenticalToTheProducerContract(t *testing.T) {
	if typeReceiptLineReceived != "com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived" {
		t.Fatalf("type = %q", typeReceiptLineReceived)
	}
}
