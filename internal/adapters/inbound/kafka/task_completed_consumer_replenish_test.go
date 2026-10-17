package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// replenishData is a REPLENISH TaskCompleted payload with the additive fields
// fulfillment-execution emits for REPLENISH tasks only (ADR 0041): the
// RELOCATE shape with request_ref in place of relocation_ref.
func replenishData() map[string]any {
	return map[string]any{
		"task_id": "task-p1", "station_id": "station-03", "work_unit_id": "wu-p1",
		"associate_id": "worker-42", "duration_seconds": 61, "task_type": "REPLENISH",
		"sku": "SKU-1", "from_bin": "WH1-STOR-AMB-A01-01-01-A", "to_bin": "WH1-STOR-AMB-A02-01-01-A",
		"quantity": 7, "request_ref": "repl-3b0e8d2a",
	}
}

// fakeReplenisher records every REPLENISH completion and can fail the first N
// calls (transient) or always return a fixed error.
type fakeReplenisher struct {
	mu        sync.Mutex
	got       []usecases.ReplenishTaskCompletion
	failFirst int
	failErr   error
	always    error
}

func (f *fakeReplenisher) Execute(_ context.Context, in usecases.ReplenishTaskCompletion) (usecases.ReplenishTaskResult, error) {
	f.mu.Lock()
	f.got = append(f.got, in)
	n := len(f.got)
	f.mu.Unlock()
	if f.always != nil {
		return usecases.ReplenishTaskResult{}, f.always
	}
	if n <= f.failFirst {
		return usecases.ReplenishTaskResult{}, f.failErr
	}
	return usecases.ReplenishTaskResult{Outcome: usecases.ReplenishApplied}, nil
}

func (f *fakeReplenisher) calls() []usecases.ReplenishTaskCompletion {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]usecases.ReplenishTaskCompletion(nil), f.got...)
}

func TestTaskCompletedConsumer_ReplenishIsHandedToTheReplenishUseCaseWithRequestRef(t *testing.T) {
	confirm := &fakeConfirmer{}
	reloc := &fakeRelocator{}
	repl := &fakeReplenisher{}
	f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-p1", replenishData())))
	f.consumer.Relocate = reloc
	f.consumer.Replenish = repl
	f.runUntilCommitted(t, 1)

	calls := repl.calls()
	if len(calls) != 1 {
		t.Fatalf("replenish use case called %d times, want 1", len(calls))
	}
	got := calls[0]
	if got.Quantity == nil || *got.Quantity != 7 {
		t.Fatalf("quantity = %v, want 7", got.Quantity)
	}
	got.Quantity = nil
	want := usecases.ReplenishTaskCompletion{
		EventID: "ce-p1", TaskID: "task-p1", TaskType: "REPLENISH", SKU: "SKU-1",
		FromBin: "WH1-STOR-AMB-A01-01-01-A", ToBin: "WH1-STOR-AMB-A02-01-01-A", RequestRef: "repl-3b0e8d2a",
	}
	if got != want {
		t.Fatalf("completion = %+v, want %+v", got, want)
	}
	if n := len(confirm.calls()) + len(reloc.calls()); n != 0 {
		t.Fatalf("a REPLENISH completion reached confirm-pick or relocate %d times, want 0", n)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d, want 0", n)
	}
}

func TestTaskCompletedConsumer_ReplenishWithoutRequestRefReachesTheUseCaseEmpty(t *testing.T) {
	data := replenishData()
	delete(data, "request_ref")
	delete(data, "quantity")
	repl := &fakeReplenisher{}
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, taskCompletedCE(t, "ce-p1", data)))
	f.consumer.Replenish = repl
	f.runUntilCommitted(t, 1)

	if calls := repl.calls(); len(calls) != 1 || calls[0].RequestRef != "" || calls[0].Quantity != nil {
		t.Fatalf("calls = %+v, want one completion with an empty request_ref and a nil quantity", calls)
	}
}

func TestTaskCompletedConsumer_RelocateAndPickDoNotReachTheReplenishUseCase(t *testing.T) {
	repl := &fakeReplenisher{}
	reloc := &fakeRelocator{}
	f := newTaskCompletedFixture(&fakeConfirmer{},
		fcMsg(0, taskCompletedCE(t, "ce-r1", relocateData())),
		fcMsg(1, taskCompletedCE(t, "ce-1", pickData("order-7"))))
	f.consumer.Relocate = reloc
	f.consumer.Replenish = repl
	f.runUntilCommitted(t, 2)

	if n := len(repl.calls()); n != 0 {
		t.Fatalf("replenish use case called %d times for RELOCATE/PICK completions, want 0", n)
	}
	if n := len(reloc.calls()); n != 1 {
		t.Fatalf("relocate use case called %d times, want 1 (the RELOCATE completion)", n)
	}
}

func TestTaskCompletedConsumer_ReplenishWithoutAWiredHandlerKeepsTheConfirmPickPath(t *testing.T) {
	confirm := &fakeConfirmer{}
	f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-p1", replenishData())))
	f.runUntilCommitted(t, 1)

	if calls := confirm.calls(); len(calls) != 1 || calls[0].TaskType != "REPLENISH" {
		t.Fatalf("confirm-pick calls = %+v, want the unchanged behaviour (one call it ignores)", calls)
	}
}

func TestTaskCompletedConsumer_MalformedReplenishIsDeadLetteredImmediately(t *testing.T) {
	repl := &fakeReplenisher{always: fmt.Errorf("%w: request_ref is missing", usecases.ErrMalformedRelocationTask)}
	value := taskCompletedCE(t, "ce-p1", replenishData())
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(3, value))
	f.consumer.Replenish = repl
	f.runUntilCommitted(t, 1)

	if n := len(repl.calls()); n != 1 {
		t.Fatalf("a deterministic rejection must not be retried, use case called %d times", n)
	}
	dead := f.dlq.messages()
	if len(dead) != 1 || string(dead[0].Value) != string(value) {
		t.Fatalf("dead-lettered %d messages, want the raw REPLENISH completion once", len(dead))
	}
}

func TestTaskCompletedConsumer_TransientReplenishFailureRetriesTheSameMessage(t *testing.T) {
	repl := &fakeReplenisher{failFirst: 2, failErr: usecases.ErrConcurrentModification}
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(4, taskCompletedCE(t, "ce-p1", replenishData())))
	f.consumer.Replenish = repl
	f.runUntilCommitted(t, 1)

	if n := len(repl.calls()); n != 3 {
		t.Fatalf("use case called %d times, want 3 (fail, fail, succeed)", n)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d, want 0", n)
	}
}

// cancellingReplenisher simulates a shutdown landing inside the replenish use
// case (its transaction interrupted).
type cancellingReplenisher struct{ cancel context.CancelFunc }

func (c cancellingReplenisher) Execute(ctx context.Context, _ usecases.ReplenishTaskCompletion) (usecases.ReplenishTaskResult, error) {
	c.cancel()
	return usecases.ReplenishTaskResult{}, ctx.Err()
}

func TestTaskCompletedConsumer_ShutdownMidReplenishDeadLettersNothing(t *testing.T) {
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, taskCompletedCE(t, "ce-p1", replenishData())))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.consumer.Replenish = cancellingReplenisher{cancel: cancel}
	f.consumer.MaxAttempts = 1
	f.consumer.Backoff = func(int) time.Duration { return time.Millisecond }

	if err := f.consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want a clean stop", err)
	}
	if f.reader.commitsLen() != 0 {
		t.Fatal("a REPLENISH completion only interrupted by shutdown must not be committed")
	}
	if f.dlq.attempts != 0 {
		t.Fatalf("DLQ write attempts = %d, want 0 (never dead-letter a cancelled message)", f.dlq.attempts)
	}
}
