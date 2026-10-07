package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

const taskCompletedTypeForTest = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"

// fulfillmentCE builds a CloudEvent exactly as fulfillment-execution emits it
// (its own source and full type string).
func fulfillmentCE(t *testing.T, id, eventType string, data any) []byte {
	t.Helper()
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	value, err := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/fulfillment-execution",
		"type":            eventType,
		"subject":         "task-1",
		"time":            "2026-10-06T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":            json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

func taskCompletedCE(t *testing.T, id string, data map[string]any) []byte {
	t.Helper()
	return fulfillmentCE(t, id, taskCompletedTypeForTest, data)
}

func pickData(orderRef string) map[string]any {
	d := map[string]any{
		"task_id": "task-1", "station_id": "station-03", "work_unit_id": "wu-1",
		"associate_id": "worker-42", "duration_seconds": 245, "task_type": "PICK",
	}
	if orderRef != "" {
		d["order_ref"] = orderRef
	}
	return d
}

func fcMsg(offset int64, value []byte) kafkago.Message {
	return kafkago.Message{Topic: FulfillmentTopic, Partition: 0, Offset: offset, Key: []byte("task-1"), Value: value}
}

// fakeConfirmer records every completion it receives and can fail the first N
// calls (transient) or always return a fixed error.
type fakeConfirmer struct {
	mu        sync.Mutex
	got       []usecases.PickCompletion
	failFirst int
	failErr   error
	always    error
}

func (f *fakeConfirmer) Execute(_ context.Context, in usecases.PickCompletion) (usecases.ConfirmPicksResult, error) {
	f.mu.Lock()
	f.got = append(f.got, in)
	n := len(f.got)
	f.mu.Unlock()
	if f.always != nil {
		return usecases.ConfirmPicksResult{}, f.always
	}
	if n <= f.failFirst {
		return usecases.ConfirmPicksResult{}, f.failErr
	}
	return usecases.ConfirmPicksResult{Outcome: usecases.PicksProcessed, Confirmed: 1}, nil
}

func (f *fakeConfirmer) calls() []usecases.PickCompletion {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]usecases.PickCompletion(nil), f.got...)
}

// fakeDLQ records dead-lettered messages and can fail the first N writes.
type fakeDLQ struct {
	mu        sync.Mutex
	written   []kafkago.Message
	attempts  int
	failFirst int
}

func (d *fakeDLQ) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempts++
	if d.attempts <= d.failFirst {
		return errors.New("dlq broker unavailable")
	}
	d.written = append(d.written, msgs...)
	return nil
}

func (d *fakeDLQ) Close() error { return nil }

func (d *fakeDLQ) messages() []kafkago.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]kafkago.Message(nil), d.written...)
}

type taskCompletedFixture struct {
	reader   *fakeReader
	confirm  *fakeConfirmer
	dlq      *fakeDLQ
	consumer *TaskCompletedConsumer
}

func newTaskCompletedFixture(confirm *fakeConfirmer, msgs ...kafkago.Message) taskCompletedFixture {
	reader := &fakeReader{queue: msgs}
	dlq := &fakeDLQ{}
	return taskCompletedFixture{
		reader: reader, confirm: confirm, dlq: dlq,
		consumer: &TaskCompletedConsumer{
			Reader: reader, Confirm: confirm, DeadLetter: dlq, Logger: quietLogger(),
			Topic: FulfillmentTopic, MaxAttempts: 3,
			Backoff: func(int) time.Duration { return time.Millisecond },
		},
	}
}

func (f taskCompletedFixture) runUntilCommitted(t *testing.T, want int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && f.reader.commitsLen() < want {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if got := f.reader.commitsLen(); got != want {
		t.Fatalf("commits = %d, want %d", got, want)
	}
}

func TestTaskCompletedConsumer_PickWithOrderRefIsHandedToTheUseCase(t *testing.T) {
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, taskCompletedCE(t, "ce-1", pickData("order-7"))))
	f.runUntilCommitted(t, 1)

	calls := f.confirm.calls()
	want := usecases.PickCompletion{EventID: "ce-1", TaskID: "task-1", TaskType: "PICK", OrderRef: "order-7"}
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("use case calls = %+v, want exactly [%+v]", calls, want)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d messages, want 0", n)
	}
}

func TestTaskCompletedConsumer_MissingOrderRefReachesTheUseCaseAsNothingToDo(t *testing.T) {
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, taskCompletedCE(t, "ce-1", pickData(""))))
	f.runUntilCommitted(t, 1)

	calls := f.confirm.calls()
	if len(calls) != 1 || calls[0].OrderRef != "" || calls[0].TaskType != "PICK" {
		t.Fatalf("use case calls = %+v, want one PICK completion with empty order_ref", calls)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("a missing order_ref is not poison, dead-lettered %d", n)
	}
}

func TestTaskCompletedConsumer_OtherTypesAreIgnoredByTheFullTypeString(t *testing.T) {
	f := newTaskCompletedFixture(&fakeConfirmer{},
		fcMsg(0, fulfillmentCE(t, "ce-a", "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", map[string]any{"task_id": "t", "order_ref": "order-7", "task_type": "PICK"})),
		fcMsg(1, fulfillmentCE(t, "ce-b", "com.warehouse.wes.fulfillment-execution.package.PackageManifested", map[string]any{"package_id": "p", "order_ref": "order-7"})),
		// Same trailing name, different context: a suffix match would wrongly accept it.
		fcMsg(2, fulfillmentCE(t, "ce-c", "com.warehouse.wes.somewhere-else.task.TaskCompleted", pickData("order-7"))),
		fcMsg(3, fulfillmentCE(t, "ce-d", "com.warehouse.wes.fulfillment-execution.transfer.TransferPicked", map[string]any{"transfer_ref": "tr-1"})),
	)
	f.runUntilCommitted(t, 4)

	if calls := f.confirm.calls(); len(calls) != 0 {
		t.Fatalf("use case called for foreign types: %+v", calls)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d unrelated messages, want 0", n)
	}
}

func TestTaskCompletedConsumer_NonCloudEventIsSkippedAndCommittedNotRetried(t *testing.T) {
	f := newTaskCompletedFixture(&fakeConfirmer{},
		fcMsg(0, []byte(`{"event":"TaskCompleted","task_id":"t","order_ref":"order-7"}`)),
		fcMsg(1, []byte(`not json at all`)),
	)
	f.runUntilCommitted(t, 2)

	if calls := f.confirm.calls(); len(calls) != 0 {
		t.Fatalf("legacy/garbage message reached the use case: %+v", calls)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("non-CloudEvents are skipped, not dead-lettered (replay would flood the DLQ); got %d", n)
	}
}

func TestTaskCompletedConsumer_NotCloudEventWarnIsSampled(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	c := &TaskCompletedConsumer{Logger: logger}

	const total = 2500
	for i := 0; i < total; i++ {
		if err := c.Handle(context.Background(), fcMsg(int64(i), []byte(`{"legacy":true}`))); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	mu.Lock()
	lines := strings.Count(buf.String(), "level=WARN")
	mu.Unlock()
	// 5 first + the 1000th and 2000th.
	if lines != notCloudEventLogFirst+2 {
		t.Fatalf("WARN lines = %d for %d legacy messages, want %d (sampled)", lines, total, notCloudEventLogFirst+2)
	}
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func TestTaskCompletedConsumer_PoisonIsDeadLetteredImmediatelyAndCommitted(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
		fail  error
	}{
		{"payload with the wrong shape", fulfillmentCE(t, "ce-1", taskCompletedTypeForTest, map[string]any{"task_id": 42, "task_type": []string{"PICK"}}), nil},
		{"use case rejects the completion", taskCompletedCE(t, "ce-2", pickData("order-7")), usecases.ErrMalformedPickCompletion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			confirm := &fakeConfirmer{always: tt.fail}
			f := newTaskCompletedFixture(confirm, fcMsg(5, tt.value))
			f.runUntilCommitted(t, 1)

			dead := f.dlq.messages()
			if len(dead) != 1 {
				t.Fatalf("dead-lettered %d, want 1", len(dead))
			}
			if !bytes.Equal(dead[0].Value, tt.value) || string(dead[0].Key) != "task-1" {
				t.Fatalf("DLQ message must carry the raw key and value unchanged: %+v", dead[0])
			}
			headers := map[string]string{}
			for _, h := range dead[0].Headers {
				headers[h.Key] = string(h.Value)
			}
			if headers["x-dlq-source-topic"] != FulfillmentTopic || headers["x-dlq-error"] == "" || headers["x-dlq-failed-at"] == "" {
				t.Fatalf("DLQ headers incomplete: %v", headers)
			}
			if n := len(confirm.calls()); n > 1 {
				t.Fatalf("poison must not be retried, use case called %d times", n)
			}
		})
	}
}

func TestTaskCompletedConsumer_TransientFailureRetriesSameMessageThenCommitsOnce(t *testing.T) {
	confirm := &fakeConfirmer{failFirst: 2, failErr: errors.New("db is down")}
	f := newTaskCompletedFixture(confirm, fcMsg(7, taskCompletedCE(t, "ce-1", pickData("order-7"))))
	f.runUntilCommitted(t, 1)

	calls := confirm.calls()
	if len(calls) != 3 {
		t.Fatalf("use case called %d times, want 3 (fail, fail, succeed)", len(calls))
	}
	for _, c := range calls {
		if c.EventID != "ce-1" {
			t.Fatalf("retry handled a different message: %+v", c)
		}
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d, want 0 (the third attempt succeeded)", n)
	}
	if got := f.reader.commitAt(0).Offset; got != 7 {
		t.Fatalf("committed offset %d, want 7", got)
	}
}

func TestTaskCompletedConsumer_PersistentTransientFailureIsDeadLetteredAfterBoundedRetries(t *testing.T) {
	confirm := &fakeConfirmer{always: errors.New("db is down")}
	value := taskCompletedCE(t, "ce-1", pickData("order-7"))
	f := newTaskCompletedFixture(confirm, fcMsg(0, value), fcMsg(1, taskCompletedCE(t, "ce-2", pickData("order-8"))))
	f.consumer.MaxAttempts = 3
	f.runUntilCommitted(t, 2)

	// Two messages x 3 attempts each: the partition is not wedged on the first.
	if n := len(confirm.calls()); n != 6 {
		t.Fatalf("use case called %d times, want 6 (3 attempts per message)", n)
	}
	dead := f.dlq.messages()
	if len(dead) != 2 || !bytes.Equal(dead[0].Value, value) {
		t.Fatalf("dead-lettered %d messages, want both, first unchanged", len(dead))
	}
}

func TestTaskCompletedConsumer_OffsetIsNotCommittedUntilTheDeadLetterIsStored(t *testing.T) {
	f := newTaskCompletedFixture(&fakeConfirmer{always: usecases.ErrMalformedPickCompletion}, fcMsg(0, taskCompletedCE(t, "ce-1", pickData("order-7"))))
	f.dlq.failFirst = 2
	f.runUntilCommitted(t, 1)

	if f.dlq.attempts != 3 {
		t.Fatalf("DLQ write attempts = %d, want 3 (two failures, then stored)", f.dlq.attempts)
	}
	if n := len(f.dlq.messages()); n != 1 {
		t.Fatalf("DLQ holds %d messages, want 1", n)
	}
}

func TestTaskCompletedConsumer_ShutdownMidRetryDoesNotCommit(t *testing.T) {
	confirm := &fakeConfirmer{always: errors.New("db is down")}
	f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-1", pickData("order-7"))))
	f.consumer.MaxAttempts = 1000
	f.consumer.Backoff = func(int) time.Duration { return 20 * time.Millisecond }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.consumer.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(confirm.calls()) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	if f.reader.commitsLen() != 0 {
		t.Fatal("an unsettled message must not be committed on shutdown")
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d on shutdown, want 0", n)
	}
}

func TestTaskCompletedConsumer_RedeliveredEventReachesTheUseCaseEachTime(t *testing.T) {
	// Dedupe is the use case's job (claim in the same transaction); the
	// consumer must pass both deliveries through with the same id.
	value := taskCompletedCE(t, "ce-1", pickData("order-7"))
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, value), fcMsg(1, value))
	f.runUntilCommitted(t, 2)

	calls := f.confirm.calls()
	if len(calls) != 2 || calls[0].EventID != calls[1].EventID {
		t.Fatalf("calls = %+v, want two deliveries with the same CloudEvents id", calls)
	}
}
