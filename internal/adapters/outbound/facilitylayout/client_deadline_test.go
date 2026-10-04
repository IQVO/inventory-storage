package facilitylayout

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/resilience"
)

// deadlineDoer records the deadline of the context each request carries.
type deadlineDoer struct {
	deadline    time.Time
	hasDeadline bool
}

func (d *deadlineDoer) Do(req *http.Request) (*http.Response, error) {
	d.deadline, d.hasDeadline = req.Context().Deadline()
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"hazmat":false,"temperatureClass":""}`)),
	}, nil
}

// ADR-0020 §3: the default doer must NOT carry http.Client.Timeout — a
// fixed client-level wall clock overrides the caller's context deadline.
func TestNewClient_DefaultDoerHasNoClientTimeout(t *testing.T) {
	c := NewClient("http://facility-layout.local", nil)
	hc, ok := c.doer.(*http.Client)
	if !ok {
		t.Fatalf("default doer = %T, want *http.Client", c.doer)
	}
	if hc.Timeout != 0 {
		t.Fatalf("default http.Client.Timeout = %v, want 0 (deadline comes from ctx / resilience.CallTimeout)", hc.Timeout)
	}
}

// A caller deadline shorter than the cap is honored exactly, not replaced.
func TestClient_GetSlotAttributes_HonorsCallerDeadline(t *testing.T) {
	doer := &deadlineDoer{}
	c := NewClient("http://facility-layout.local", doer)

	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	want, _ := ctx.Deadline()

	if _, err := c.GetSlotAttributes(ctx, "A-1-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !doer.hasDeadline {
		t.Fatal("request context has no deadline")
	}
	if !doer.deadline.Equal(want) {
		t.Fatalf("request deadline = %v, want the caller's %v", doer.deadline, want)
	}
}

// With no caller deadline, one attempt is capped at resilience.DefaultTimeout
// (30s) — NOT a hardcoded 5s.
func TestClient_GetSlotAttributes_NoDeadlineIsCappedAtResilienceDefault(t *testing.T) {
	doer := &deadlineDoer{}
	c := NewClient("http://facility-layout.local", doer)

	before := time.Now()
	if _, err := c.GetSlotAttributes(context.Background(), "A-1-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !doer.hasDeadline {
		t.Fatal("request context has no deadline; an attempt must never be unbounded")
	}
	got := doer.deadline.Sub(before)
	if got < resilience.DefaultTimeout-time.Second || got > resilience.DefaultTimeout+time.Second {
		t.Fatalf("per-attempt bound = %v, want ~%v (resilience.CallTimeout cap)", got, resilience.DefaultTimeout)
	}
}

// A caller deadline LONGER than the cap is capped (the old 5s client timeout
// is gone, but a single attempt still cannot outlive the cap).
func TestClient_GetSlotAttributes_LongCallerDeadlineIsCapped(t *testing.T) {
	doer := &deadlineDoer{}
	c := NewClient("http://facility-layout.local", doer)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	before := time.Now()
	if _, err := c.GetSlotAttributes(ctx, "A-1-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := doer.deadline.Sub(before); got > resilience.DefaultTimeout+time.Second {
		t.Fatalf("per-attempt bound = %v, want <= ~%v", got, resilience.DefaultTimeout)
	}
}
