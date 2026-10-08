package kafka

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

func pickDataLine(orderRef string, lineNo any) map[string]any {
	d := pickData(orderRef)
	d["line_no"] = lineNo
	return d
}

// The optional line_no of TaskCompleted v1 (decision 18, ADR 0036) reaches the
// use case; absent and explicit null both mean "unknown".
func TestTaskCompletedConsumer_LineNoIsHandedToTheUseCase(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
		want *int
	}{
		{"present", pickDataLine("order-7", 3), ptrInt(3)},
		{"absent", pickData("order-7"), nil},
		{"explicit null", pickDataLine("order-7", nil), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTaskCompletedFixture(&fakeConfirmer{}, fcMsg(0, taskCompletedCE(t, "ce-1", tt.data)))
			f.runUntilCommitted(t, 1)

			calls := f.confirm.calls()
			if len(calls) != 1 {
				t.Fatalf("use case calls = %+v, want exactly one", calls)
			}
			got := calls[0]
			if got.EventID != "ce-1" || got.OrderRef != "order-7" || got.TaskType != "PICK" {
				t.Fatalf("completion = %+v", got)
			}
			if (got.LineNo == nil) != (tt.want == nil) || (tt.want != nil && *got.LineNo != *tt.want) {
				t.Fatalf("LineNo = %v, want %v", got.LineNo, tt.want)
			}
			if n := len(f.dlq.messages()); n != 0 {
				t.Fatalf("dead-lettered %d messages, want 0", n)
			}
		})
	}
}

// A line_no that is not an integer is a payload that can never be handled:
// dead-lettered at once, never retried.
func TestTaskCompletedConsumer_NonIntegerLineNoIsPoison(t *testing.T) {
	for name, v := range map[string]any{"string": "three", "fraction": 1.5, "object": map[string]any{"n": 1}} {
		t.Run(name, func(t *testing.T) {
			confirm := &fakeConfirmer{}
			f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-1", pickDataLine("order-7", v))))
			f.runUntilCommitted(t, 1)

			if n := len(f.dlq.messages()); n != 1 {
				t.Fatalf("dead-lettered %d, want 1", n)
			}
			if n := len(confirm.calls()); n != 0 {
				t.Fatalf("a malformed payload must not reach the use case, got %d calls", n)
			}
		})
	}
}

// line_no below 1 decodes (it is an integer) and the use case rejects it as
// malformed, which the consumer dead-letters like any other poison.
func TestTaskCompletedConsumer_NonPositiveLineNoIsDeadLetteredViaTheUseCase(t *testing.T) {
	confirm := &fakeConfirmer{always: usecases.ErrMalformedPickCompletion}
	f := newTaskCompletedFixture(confirm, fcMsg(0, taskCompletedCE(t, "ce-1", pickDataLine("order-7", 0))))
	f.runUntilCommitted(t, 1)

	calls := confirm.calls()
	if len(calls) != 1 || calls[0].LineNo == nil || *calls[0].LineNo != 0 {
		t.Fatalf("calls = %+v, want one with LineNo 0", calls)
	}
	if n := len(f.dlq.messages()); n != 1 {
		t.Fatalf("dead-lettered %d, want 1", n)
	}
}

func ptrInt(n int) *int { return &n }
