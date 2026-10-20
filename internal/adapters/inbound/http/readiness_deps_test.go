package http

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func okDep(name string) Dependency {
	return Dependency{Name: name, Check: func(context.Context) error { return nil }}
}

func failDep(name string) Dependency {
	return Dependency{Name: name, Check: func(context.Context) error { return errors.New("down") }}
}

func serveReadyz(g *Readiness) *httptest.ResponseRecorder {
	s := &Server{Readiness: g}
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest("GET", "/readyz", nil))
	return rec
}

func TestReadyz_AllDependenciesUp_Is200(t *testing.T) {
	rec := serveReadyz(NewReadiness(0, 0, okDep("postgres"), okDep("kafka")))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"ready"`) {
		t.Fatalf("readyz = %d %s, want 200 ready", rec.Code, rec.Body.String())
	}
}

func TestReadyz_DependencyDown_Is503ProblemNamingIt(t *testing.T) {
	cases := []struct {
		name string
		deps []Dependency
		want []string
	}{
		{"kafka down", []Dependency{okDep("postgres"), failDep("kafka")}, []string{"kafka"}},
		{"postgres down", []Dependency{failDep("postgres"), okDep("kafka")}, []string{"postgres"}},
		{"both down", []Dependency{failDep("postgres"), failDep("kafka")}, []string{"postgres", "kafka"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveReadyz(NewReadiness(0, 0, tc.deps...))
			if rec.Code != 503 {
				t.Fatalf("status = %d, want 503", rec.Code)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("Content-Type = %q, want application/problem+json", ct)
			}
			body := rec.Body.String()
			if !strings.Contains(body, "dependency-unavailable") {
				t.Fatalf("body %s does not carry the dependency-unavailable type", body)
			}
			for _, name := range tc.want {
				if !strings.Contains(body, name) {
					t.Fatalf("body %s does not name %s", body, name)
				}
			}
		})
	}
}

func TestReadyz_DrainGateWinsOverHealthyDependencies(t *testing.T) {
	g := NewReadiness(0, 0, okDep("postgres"), okDep("kafka"))
	g.SetNotReady()
	rec := serveReadyz(g)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "not_ready") {
		t.Fatalf("readyz = %d %s, want the 503 not_ready drain answer", rec.Code, rec.Body.String())
	}
}

func TestReadiness_VerdictIsCachedForTheTTL(t *testing.T) {
	var calls int32
	dep := Dependency{Name: "kafka", Check: func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}}
	g := NewReadiness(time.Minute, 0, dep)
	now := time.Unix(1_700_000_000, 0)
	g.now = func() time.Time { return now }

	for i := 0; i < 5; i++ {
		if f := g.Failing(context.Background()); len(f) != 0 {
			t.Fatalf("Failing = %v, want none", f)
		}
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("check ran %d times within the TTL, want 1", n)
	}
	now = now.Add(2 * time.Minute)
	g.Failing(context.Background())
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("check ran %d times after the TTL elapsed, want 2", n)
	}
}

func TestReadiness_ACancelledCallerDoesNotPoisonTheCache(t *testing.T) {
	dep := Dependency{Name: "kafka", Check: func(ctx context.Context) error { return ctx.Err() }}
	g := NewReadiness(time.Minute, 0, dep)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel() // the probe hung up before the check ran
	if f := g.Failing(cancelled); len(f) != 0 {
		t.Fatalf("Failing = %v, want none: the check must not inherit the caller's cancellation", f)
	}
}

func TestReadiness_ARoundOfChecksIsBoundedByTheTimeout(t *testing.T) {
	dep := Dependency{Name: "kafka", Check: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}}
	g := NewReadiness(time.Minute, 50*time.Millisecond, dep)
	start := time.Now()
	f := g.Failing(context.Background())
	if len(f) != 1 || f[0] != "kafka" {
		t.Fatalf("Failing = %v, want [kafka] after the timeout", f)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the round took %s, want it bounded by the 50ms timeout", d)
	}
}

func TestNewReadiness_NonPositiveValuesFallBackToTheDefaults(t *testing.T) {
	g := NewReadiness(0, -1)
	if g.ttl != DefaultReadinessTTL || g.timeout != DefaultReadinessTimeout {
		t.Fatalf("ttl=%s timeout=%s, want the defaults %s / %s", g.ttl, g.timeout, DefaultReadinessTTL, DefaultReadinessTimeout)
	}
}
