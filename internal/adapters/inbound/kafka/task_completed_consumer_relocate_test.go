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

// relocateData is a RELOCATE TaskCompleted payload with the ADDITIVE fields
// fulfillment-execution emits for RELOCATE tasks only (decision E1, ADR 0039).
func relocateData() map[string]any {
	return map[string]any{
		"task_id": "task-r1", "station_id": "station-03", "work_unit_id": "wu-r1",
		"associate_id": "worker-42", "duration_seconds": 61, "task_type": "RELOCATE",
		"sku": "SKU-1", "from_bin": "WH1-STOR-AMB-A01-01-01-A", "to_bin": "WH1-STOR-AMB-A02-01-01-A",
		"quantity": 7, "relocation_ref": "plan-9#3",
	}
}

// fakeRelocator records every RELOCATE completion and can fail the first N
// calls (transient) or always return a fixed error.
type fakeRelocator struct {
	mu        sync.Mutex
	got       []usecases.RelocationTaskCompletion
	failFirst int
	failErr   error
	always    error
}

func (f *fakeRelocator) Execute(_ context.Context, in usecases.RelocationTaskCompletion) (usecases.RelocationTaskResult, error) {
	f.mu.Lock()
	f.got = append(f.got, in)
	n := len(f.got)
	f.mu.Unlock()
	if f.always != nil {
		return usecases.RelocationTaskResult{}, f.always
	}
	if n <= f.failFirst {
		return usecases.RelocationTaskResult{}, f.failErr
	}
	return usecases.RelocationTaskResult{Outcome: usecases.RelocationApplied}, nil
}

func (f *fakeRelocator) calls() []usecases.RelocationTaskCompletion {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]usecases.RelocationTaskCompletion(nil), f.got...)
}

func TestTaskCompletedConsumer_RelocateIsHandedToTheRelocationUseCaseWithTheAdditiveFields(t *testing.T) {
	confirm := &fakeConfirmer{}
	reloc := &fakeRelocator{}
	f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-r1", relocateData())))
	f.consumer.Relocate = reloc
	f.runUntilCommitted(t, 1)

	qty := 7
	want := usecases.RelocationTaskCompletion{
		EventID: "ce-r1", TaskID: "task-r1", TaskType: "RELOCATE", SKU: "SKU-1",
		FromBin: "WH1-STOR-AMB-A01-01-01-A", ToBin: "WH1-STOR-AMB-A02-01-01-A", Quantity: &qty, RelocationRef: "plan-9#3",
	}
	calls := reloc.calls()
	if len(calls) != 1 {
		t.Fatalf("relocation use case called %d times, want 1", len(calls))
	}
	assertRelocationCompletion(t, calls[0], want)
	if n := len(confirm.calls()); n != 0 {
		t.Fatalf("a RELOCATE completion reached confirm-pick %d times, want 0", n)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d, want 0", n)
	}
}

func assertRelocationCompletion(t *testing.T, got, want usecases.RelocationTaskCompletion) {
	t.Helper()
	if got.Quantity == nil || *got.Quantity != *want.Quantity {
		t.Fatalf("quantity = %v, want %d", got.Quantity, *want.Quantity)
	}
	got.Quantity, want.Quantity = nil, nil
	if got != want {
		t.Fatalf("completion = %+v, want %+v", got, want)
	}
}

func TestTaskCompletedConsumer_RelocateWithoutQuantityReachesTheUseCaseAsNil(t *testing.T) {
	data := relocateData()
	delete(data, "quantity")
	reloc := &fakeRelocator{}
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, taskCompletedCE(t, "ce-r1", data)))
	f.consumer.Relocate = reloc
	f.runUntilCommitted(t, 1)

	if calls := reloc.calls(); len(calls) != 1 || calls[0].Quantity != nil {
		t.Fatalf("calls = %+v, want one completion with a nil quantity", calls)
	}
}

func TestTaskCompletedConsumer_PickStillGoesToConfirmPickWhenRelocationIsWired(t *testing.T) {
	confirm := &fakeConfirmer{}
	reloc := &fakeRelocator{}
	f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-1", pickData("order-7"))))
	f.consumer.Relocate = reloc
	f.runUntilCommitted(t, 1)

	want := usecases.PickCompletion{EventID: "ce-1", TaskID: "task-1", TaskType: "PICK", OrderRef: "order-7"}
	if calls := confirm.calls(); len(calls) != 1 || calls[0] != want {
		t.Fatalf("confirm-pick calls = %+v, want exactly [%+v]", calls, want)
	}
	if n := len(reloc.calls()); n != 0 {
		t.Fatalf("a PICK completion reached the relocation use case %d times", n)
	}
}

func TestTaskCompletedConsumer_RelocateWithoutAWiredHandlerKeepsTheConfirmPickPath(t *testing.T) {
	confirm := &fakeConfirmer{}
	f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-r1", relocateData())))
	f.runUntilCommitted(t, 1)

	if calls := confirm.calls(); len(calls) != 1 || calls[0].TaskType != "RELOCATE" {
		t.Fatalf("confirm-pick calls = %+v, want the unchanged pre-RELOCATE behaviour (one call it ignores)", calls)
	}
}

func TestTaskCompletedConsumer_MalformedRelocateIsDeadLetteredImmediately(t *testing.T) {
	reloc := &fakeRelocator{always: fmt.Errorf("%w: from_bin: empty", usecases.ErrMalformedRelocationTask)}
	value := taskCompletedCE(t, "ce-r1", relocateData())
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(3, value))
	f.consumer.Relocate = reloc
	f.runUntilCommitted(t, 1)

	if n := len(reloc.calls()); n != 1 {
		t.Fatalf("a deterministic rejection must not be retried, use case called %d times", n)
	}
	dead := f.dlq.messages()
	if len(dead) != 1 || string(dead[0].Value) != string(value) {
		t.Fatalf("dead-lettered %d messages, want the raw RELOCATE completion once", len(dead))
	}
}

func TestTaskCompletedConsumer_TransientRelocateFailureRetriesTheSameMessage(t *testing.T) {
	reloc := &fakeRelocator{failFirst: 2, failErr: usecases.ErrConcurrentModification}
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(4, taskCompletedCE(t, "ce-r1", relocateData())))
	f.consumer.Relocate = reloc
	f.runUntilCommitted(t, 1)

	if n := len(reloc.calls()); n != 3 {
		t.Fatalf("use case called %d times, want 3 (fail, fail, succeed)", n)
	}
	if n := len(f.dlq.messages()); n != 0 {
		t.Fatalf("dead-lettered %d, want 0", n)
	}
}

// cancellingRelocator simulates a shutdown landing inside the relocation use
// case (its transaction interrupted).
type cancellingRelocator struct{ cancel context.CancelFunc }

func (c cancellingRelocator) Execute(ctx context.Context, _ usecases.RelocationTaskCompletion) (usecases.RelocationTaskResult, error) {
	c.cancel()
	return usecases.RelocationTaskResult{}, ctx.Err()
}

func TestTaskCompletedConsumer_ShutdownMidRelocateDeadLettersNothing(t *testing.T) {
	f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, taskCompletedCE(t, "ce-r1", relocateData())))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.consumer.Relocate = cancellingRelocator{cancel: cancel}
	f.consumer.MaxAttempts = 1
	f.consumer.Backoff = func(int) time.Duration { return time.Millisecond }

	if err := f.consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want a clean stop", err)
	}
	if f.reader.commitsLen() != 0 {
		t.Fatal("a RELOCATE completion only interrupted by shutdown must not be committed")
	}
	if f.dlq.attempts != 0 {
		t.Fatalf("DLQ write attempts = %d, want 0 (never dead-letter a cancelled message)", f.dlq.attempts)
	}
}
