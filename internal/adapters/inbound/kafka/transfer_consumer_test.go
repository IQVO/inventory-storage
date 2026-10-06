package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// fakeReader feeds a fixed queue of messages and records commits.
type fakeReader struct {
	mu      sync.Mutex
	queue   []kafkago.Message
	commits []kafkago.Message
	closed  bool
}

// commitsLen is the race-safe commits count for polling goroutines.
func (r *fakeReader) commitsLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.commits)
}

// commitAt is the race-safe accessor for asserting one committed message.
func (r *fakeReader) commitAt(i int) kafkago.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commits[i]
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if len(r.queue) > 0 {
		msg := r.queue[0]
		r.queue = r.queue[1:]
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()
	// Block like a real reader with no data — WITHOUT holding the lock,
	// so the test goroutine can keep polling the commits recording.
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, msgs...)
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

// fakeAllocator records the commands it was asked to decide.
type fakeAllocator struct {
	mu        sync.Mutex
	commands  []usecases.TransferCommand
	failFirst int   // fail the first N calls with a transient error
	transient error // what to fail with
	results   []*usecases.Result
}

// commandsLen is the race-safe commands count for polling goroutines.
func (f *fakeAllocator) commandsLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.commands)
}

// commandAt is the race-safe accessor for asserting one decoded command.
func (f *fakeAllocator) commandAt(i int) usecases.TransferCommand {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commands[i]
}

func (f *fakeAllocator) Execute(ctx context.Context, cmd usecases.TransferCommand) (*usecases.Result, error) {
	f.mu.Lock()
	f.commands = append(f.commands, cmd)
	n := len(f.commands)
	f.mu.Unlock()
	if f.failFirst > 0 && n <= f.failFirst {
		return nil, f.transient
	}
	site, _ := shared.NewSiteID("irrelevant")
	result := &usecases.Result{Replay: false}
	_ = site
	f.mu.Lock()
	f.results = append(f.results, result)
	f.mu.Unlock()
	return result, nil
}

// buildTransferCommandCE encodes one TransferAllocationRequested
// CloudEvent exactly as network-inventory-planning (the producer, a wes
// context) would: its own source and the full type string this consumer
// dispatches on. The local cloudevents helper hardcodes THIS repo's
// identity, so the envelope is hand-assembled per the CloudEvents
// structured JSON form the sdk-go validator accepts.
func buildTransferCommandCE(t *testing.T, id, transferID, lineID, site, sku string, qty int) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"transfer_id":      transferID,
		"transfer_line_id": lineID,
		"origin_site_id":   site,
		"sku":              sku,
		"quantity":         qty,
	})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	value, err := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/network-inventory-planning",
		"type":            typeTransferAllocationRequested,
		"subject":         lineID,
		"time":            "2026-10-06T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:network-inventory-planning:events:TransferAllocationRequested:v1",
		"data":            json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("marshal CE: %v", err)
	}
	return value
}

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// A transient failure must retry the SAME message and commit exactly
// once, after the success — never commit on failure (the atomicity
// checklist's fake-reader test).
func TestConsumer_RetriesTransientFailureThenCommitsOnce(t *testing.T) {
	transient := errors.New("db is down")
	alloc := &fakeAllocator{failFirst: 2, transient: transient}
	reader := &fakeReader{queue: []kafkago.Message{
		{Topic: Topic, Partition: 0, Offset: 0, Value: buildTransferCommandCE(t, "ce-1", "tr-1", "tl-1", "SITE-A", "SKU-1", 5)},
	}}
	c := &Consumer{Reader: reader, Allocate: alloc, Logger: quietLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if alloc.commandsLen() >= 3 && reader.commitsLen() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if alloc.commandsLen() != 3 {
		t.Fatalf("handler invoked %d times, want 3 (fail, fail, succeed)", alloc.commandsLen())
	}
	if reader.commitsLen() != 1 {
		t.Fatalf("CommitMessages called with %d messages, want exactly 1 (after success)", reader.commitsLen())
	}
	if reader.commitAt(0).Offset != 0 {
		t.Fatalf("committed offset = %d, want 0 (the SAME message)", reader.commitAt(0).Offset)
	}
}

// A deterministic poison message (invalid command payload) must be
// committed past on the FIRST attempt — no retry, no partition wedge.
func TestConsumer_MalformedCommandCommittedPastImmediately(t *testing.T) {
	alloc := &fakeAllocator{}
	reader := &fakeReader{queue: []kafkago.Message{
		{Topic: Topic, Partition: 0, Offset: 7, Value: buildTransferCommandCE(t, "ce-1", "", "tl-1", "SITE-A", "SKU-1", 0)},
	}}
	c := &Consumer{Reader: reader, Allocate: alloc, Logger: quietLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reader.commitsLen() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if reader.commitsLen() != 1 {
		t.Fatalf("commits = %d, want 1", reader.commitsLen())
	}
	// The malformed command never reached the handler (validation happens
	// in Handle before Execute — zero-quantity is rejected there).
	if alloc.commandsLen() != 0 {
		t.Fatalf("malformed command reached the use case %d times, want 0", alloc.commandsLen())
	}
}

// A message that is not CloudEvents at all: WARN + commit past, never an
// error return, never a retry.
func TestConsumer_LegacyFlatEnvelopeSkippedNotRetried(t *testing.T) {
	alloc := &fakeAllocator{}
	reader := &fakeReader{queue: []kafkago.Message{
		{Topic: Topic, Partition: 0, Offset: 3, Value: []byte(`{"event_id":"old","event_type":"flat"}`)},
	}}
	c := &Consumer{Reader: reader, Allocate: alloc, Logger: quietLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reader.commitsLen() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if reader.commitsLen() != 1 {
		t.Fatalf("commits = %d, want 1 (skipped past the poison)", reader.commitsLen())
	}
	if alloc.commandsLen() != 0 {
		t.Fatalf("poison reached the handler %d times", alloc.commandsLen())
	}
}

// Unknown CloudEvents types on the topic are acknowledged and ignored.
func TestConsumer_UnknownTypeIgnored(t *testing.T) {
	alloc := &fakeAllocator{}
	other, err := cloudevents.New(cloudevents.Spec{
		ID: "ce-x", Entity: "transfer", EventName: "TransferSomethingElse",
		Subject: "tl", Time: time.Now().UTC(), Stream: cloudevents.StreamEvents, Version: 1,
		Data: map[string]any{"foo": 1},
	})
	if err != nil {
		t.Fatalf("build CE: %v", err)
	}
	reader := &fakeReader{queue: []kafkago.Message{{Topic: Topic, Partition: 0, Offset: 1, Value: other}}}
	c := &Consumer{Reader: reader, Allocate: alloc, Logger: quietLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reader.commitsLen() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if reader.commitsLen() != 1 || alloc.commandsLen() != 0 {
		t.Fatalf("unknown type: commits=%d handler=%d, want 1/0", reader.commitsLen(), alloc.commandsLen())
	}
}

// A well-formed command reaches the use case with the decoded fields.
func TestConsumer_ValidCommandReachesUseCase(t *testing.T) {
	alloc := &fakeAllocator{}
	reader := &fakeReader{queue: []kafkago.Message{
		{Topic: Topic, Partition: 0, Offset: 0, Value: buildTransferCommandCE(t, "ce-1", "tr-9", "tl-9", "SITE-A", "SKU-9", 12)},
	}}
	c := &Consumer{Reader: reader, Allocate: alloc, Logger: quietLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if alloc.commandsLen() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if alloc.commandsLen() != 1 {
		t.Fatalf("handler calls = %d, want 1", alloc.commandsLen())
	}
	got := alloc.commandAt(0)
	if got.TransferID != "tr-9" || got.TransferLineID != "tl-9" || got.SKU != shared.SKU("SKU-9") || got.Quantity.Int() != 12 || got.OriginSiteID != shared.SiteID("SITE-A") {
		t.Fatalf("decoded command = %+v", got)
	}
}
