package facilitylayout_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/facilitylayout"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// recordingRecorder implements resilience.StateRecorder, capturing every
// state transition in order.
type recordingRecorder struct {
	states []int64
}

func (r *recordingRecorder) SetState(_ string, state int64) {
	r.states = append(r.states, state)
}

func (r *recordingRecorder) last() int64 {
	if len(r.states) == 0 {
		return -1
	}
	return r.states[len(r.states)-1]
}

// The gobreaker.State values (0=closed,1=half-open,2=open), duplicated
// as untyped constants for the same reason as order-management's breaker
// tests: resilience.RecordStateChange's documented "no translation
// table" contract.
const (
	gobreakerClosed = 0
	gobreakerOpen   = 2
)

// countingFakeDoer wraps fakeDoer (see client_test.go) with a call
// counter, so a test can prove retry attempt counts and breaker
// short-circuiting precisely.
type countingFakeDoer struct {
	resp  func() *http.Response
	err   error
	calls int32
}

func (d *countingFakeDoer) Do(*http.Request) (*http.Response, error) {
	atomic.AddInt32(&d.calls, 1)
	if d.err != nil {
		return nil, d.err
	}
	return d.resp(), nil
}

// TestBreakerClient_RetriesTransportErrorsUpToMaxAttempts is the
// ADR-0020 retry acceptance test: a fake HTTPDoer that always fails is
// retried exactly maxRetryAttempts (3) times per GetSlotAttributes
// call — not fewer (leaving retry budget on the table) and not more (an
// unbounded retry storm). Unlike order-management's
// productclassification (whose public GetClassification method is
// itself fail-open on every error), facilitylayout.Client.GetSlotAttributes
// has RAW semantics — a transport error is a real error — so with the
// breaker still CLOSED (only 1 of the 5 consecutive failures ReadyToTrip
// needs), the real error propagates unchanged; the fail-open fallback
// is reserved for when the breaker has actually tripped OPEN (see the
// next test).
func TestBreakerClient_RetriesTransportErrorsUpToMaxAttempts(t *testing.T) {
	boom := errors.New("connection refused")
	doer := &countingFakeDoer{err: boom}
	inner := facilitylayout.NewClient("http://facility-layout.local", doer)
	client := facilitylayout.NewBreakerClient(inner, nil)

	_, err := client.GetSlotAttributes(context.Background(), shared.BinId("A-1-1"))
	if err == nil {
		t.Fatal("expected the real transport error to propagate with the breaker still closed")
	}
	if got := atomic.LoadInt32(&doer.calls); got != 3 {
		t.Fatalf("doer calls = %d, want exactly 3 (1 original + 2 retries)", got)
	}
}

// TestBreakerClient_SucceedsOnSecondAttempt proves a transient failure
// (1 failure then success) is transparently retried into a successful
// result, with no error and no fail-open fallback.
func TestBreakerClient_SucceedsOnSecondAttempt(t *testing.T) {
	var call int32
	doer := &countingFakeDoer{
		resp: func() *http.Response {
			if atomic.AddInt32(&call, 1) == 1 {
				return jsonResponse(http.StatusInternalServerError, "")
			}
			return jsonResponse(http.StatusOK, `{"hazmat": true, "temperatureClass": "Ambient"}`)
		},
	}
	inner := facilitylayout.NewClient("http://facility-layout.local", doer)
	client := facilitylayout.NewBreakerClient(inner, nil)

	attrs, err := client.GetSlotAttributes(context.Background(), shared.BinId("A-1-1"))
	if err != nil {
		t.Fatalf("GetSlotAttributes: %v", err)
	}
	if !attrs.Known || !attrs.Hazmat {
		t.Fatalf("expected a successful classified result after retrying past the first 500, got %+v", attrs)
	}
	if got := atomic.LoadInt32(&doer.calls); got != 2 {
		t.Fatalf("doer calls = %d, want exactly 2 (1 failure + 1 success)", got)
	}
}

// TestBreakerClient_404DoesNotRetry proves a 404 (a legitimate
// Known=false answer) returns on the FIRST attempt, exactly like a 200
// does — it must never consume retry budget or look like a failure.
func TestBreakerClient_404DoesNotRetry(t *testing.T) {
	doer := &countingFakeDoer{resp: func() *http.Response { return jsonResponse(http.StatusNotFound, "") }}
	inner := facilitylayout.NewClient("http://facility-layout.local", doer)
	client := facilitylayout.NewBreakerClient(inner, nil)

	attrs, err := client.GetSlotAttributes(context.Background(), shared.BinId("unknown"))
	if err != nil {
		t.Fatalf("GetSlotAttributes: %v", err)
	}
	if attrs.Known {
		t.Fatalf("expected Known=false on 404")
	}
	if got := atomic.LoadInt32(&doer.calls); got != 1 {
		t.Fatalf("doer calls = %d, want exactly 1 — a 404 must not be retried", got)
	}
}

// TestBreakerClient_OpensAfterConsecutiveFailures_ShortCircuitsToFallback
// proves the breaker's trip-then-fallback lifecycle: the first 5
// consecutive failed calls (each internally already retried 3x, so the
// breaker sees ONE failure per call, not three) each still propagate the
// real error — the breaker is CLOSED for all of them, ReadyToTrip only
// fires AFTER the 5th — and only once the breaker has actually tripped
// OPEN does a subsequent call short-circuit to PermissiveLookup's
// fail-open contract WITHOUT ever reaching the doer again.
func TestBreakerClient_OpensAfterConsecutiveFailures_ShortCircuitsToFallback(t *testing.T) {
	boom := errors.New("connection refused")
	doer := &countingFakeDoer{err: boom}
	inner := facilitylayout.NewClient("http://facility-layout.local", doer)
	recorder := &recordingRecorder{}
	client := facilitylayout.NewBreakerClient(inner, recorder)

	// 5 consecutive failed calls (each internally retried 3x) trips
	// the breaker — ReadyToTrip only sees breaker-level Execute
	// outcomes, i.e. 5, not 15. The breaker is CLOSED throughout, so
	// each one still surfaces the real transport error.
	for i := 0; i < 5; i++ {
		_, err := client.GetSlotAttributes(context.Background(), shared.BinId("A-1-1"))
		if err == nil {
			t.Fatalf("call %d: expected the real transport error while the breaker is still closed", i)
		}
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("last recorded breaker state = %d, want gobreakerOpen (%d)", recorder.last(), gobreakerOpen)
	}

	callsBeforeOpen := atomic.LoadInt32(&doer.calls)
	attrs, err := client.GetSlotAttributes(context.Background(), shared.BinId("A-1-1"))
	if err != nil || attrs.Known {
		t.Fatalf("got (%+v, %v) while open, want a fail-open Known=false/nil", attrs, err)
	}
	if got := atomic.LoadInt32(&doer.calls); got != callsBeforeOpen {
		t.Fatalf("doer calls after the breaker opened = %d, want unchanged from %d — the open breaker must short-circuit before ever reaching the doer", got, callsBeforeOpen)
	}
}

// TestBreakerClient_RecoversAfterCooldown proves the breaker's
// half-open → closed recovery path: once cooldown elapses, a
// successful probe call closes the breaker again and subsequent calls
// reach the real client normally.
func TestBreakerClient_RecoversAfterCooldown(t *testing.T) {
	boom := errors.New("connection refused")
	doer := &countingFakeDoer{err: boom}
	inner := facilitylayout.NewClient("http://facility-layout.local", doer)
	recorder := &recordingRecorder{}
	client := facilitylayout.NewBreakerClientWithTimeout(inner, recorder, 50*time.Millisecond)

	for i := 0; i < 5; i++ {
		_, _ = client.GetSlotAttributes(context.Background(), shared.BinId("A-1-1"))
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("last recorded breaker state = %d, want gobreakerOpen (%d)", recorder.last(), gobreakerOpen)
	}

	doer.err = nil
	doer.resp = func() *http.Response {
		return jsonResponse(http.StatusOK, `{"hazmat": false, "temperatureClass": "Ambient"}`)
	}
	time.Sleep(75 * time.Millisecond)

	attrs, err := client.GetSlotAttributes(context.Background(), shared.BinId("A-1-1"))
	if err != nil {
		t.Fatalf("GetSlotAttributes after cooldown: %v", err)
	}
	if !attrs.Known {
		t.Fatalf("expected a real successful result after recovery, got %+v", attrs)
	}
	if recorder.last() != gobreakerClosed {
		t.Fatalf("last recorded breaker state = %d, want gobreakerClosed (%d) after a successful probe", recorder.last(), gobreakerClosed)
	}
}
