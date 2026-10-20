package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mapEnv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

func TestLoadConfig(t *testing.T) {
	cfg, err := loadConfig(mapEnv(map[string]string{
		"ANALYTICS_DATABASE_URL": "postgres://x",
		"KAFKA_BROKERS":          " a:9092, b:9092 ,",
	}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if len(cfg.kafkaBrokers) != 2 || cfg.kafkaBrokers[0] != "a:9092" || cfg.kafkaBrokers[1] != "b:9092" {
		t.Fatalf("kafkaBrokers = %v", cfg.kafkaBrokers)
	}
	if cfg.migrationsPath != "migrations/analytics" {
		t.Fatalf("migrationsPath = %q, want the default", cfg.migrationsPath)
	}
}

// Postgres and Kafka are the only runtime: no localhost default, and every
// refusal names the variable.
func TestLoadConfig_RefusesAMissingDependency(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"no analytics database", map[string]string{"KAFKA_BROKERS": "b:9092"}, "ANALYTICS_DATABASE_URL"},
		{"no brokers", map[string]string{"ANALYTICS_DATABASE_URL": "postgres://x"}, "KAFKA_BROKERS"},
		{"only commas in brokers", map[string]string{"ANALYTICS_DATABASE_URL": "postgres://x", "KAFKA_BROKERS": " , "}, "KAFKA_BROKERS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(mapEnv(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to name %s", err, tt.want)
			}
		})
	}
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

func TestAdminServer_HealthzIsLivenessOnly(t *testing.T) {
	failing := inboundhttp.NewReadiness(time.Millisecond, time.Second,
		inboundhttp.Dependency{Name: "kafka", Check: func(context.Context) error { return errors.New("down") }})
	if rr := get(t, newAdminServer(failing).Handler, "/healthz"); rr.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200 even when a dependency is down", rr.Code)
	}
}

// /readyz must go 503 when a dependency goes away: an empty gate would hide it.
func TestAdminServer_ReadyzReflectsTheDependencies(t *testing.T) {
	var kafkaErr error
	readiness := inboundhttp.NewReadiness(time.Millisecond, time.Second,
		inboundhttp.Dependency{Name: "postgres", Check: func(context.Context) error { return nil }},
		inboundhttp.Dependency{Name: "kafka", Check: func(context.Context) error { return kafkaErr }},
	)
	h := newAdminServer(readiness).Handler

	if rr := get(t, h, "/readyz"); rr.Code != http.StatusOK {
		t.Fatalf("healthy /readyz = %d, want 200", rr.Code)
	}

	kafkaErr = errors.New("broker unreachable")
	time.Sleep(5 * time.Millisecond) // let the 1ms verdict expire
	rr := get(t, h, "/readyz")
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "dependency-unavailable") || !strings.Contains(rr.Body.String(), "kafka") {
		t.Fatalf("kafka down: /readyz = %d %s, want 503 dependency-unavailable naming kafka", rr.Code, rr.Body.String())
	}

	kafkaErr = nil
	readiness.SetNotReady()
	time.Sleep(5 * time.Millisecond)
	if rr := get(t, h, "/readyz"); rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "not_ready") {
		t.Fatalf("draining: /readyz = %d %s, want 503 not_ready", rr.Code, rr.Body.String())
	}
}

func newTestServer() *http.Server {
	return &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}
}

// A consumer that returns by itself ends the process with an error (so the pod
// restarts) after the drain; readiness flips.
func TestServeUntilDone_ADeadConsumerEndsTheProcessWithAnError(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	boom := errors.New("kafka: broker gone")
	err := serveUntilDone(context.Background(), quietLogger(),
		func(context.Context) error { return boom }, newTestServer(), readiness, []string{"b:9092"})
	if !errors.Is(err, boom) || !errors.Is(err, errConsumerStopped) {
		t.Fatalf("err = %v, want the consumer's death", err)
	}
	if readiness.Ready() {
		t.Error("readiness must flip to not-ready in the drain")
	}
}

// A nil return before any signal is a death too.
func TestServeUntilDone_ANilReturnWithoutASignalIsADeath(t *testing.T) {
	err := serveUntilDone(context.Background(), quietLogger(),
		func(context.Context) error { return nil }, newTestServer(), &inboundhttp.Readiness{}, nil)
	if !errors.Is(err, errConsumerStopped) {
		t.Fatalf("err = %v, want errConsumerStopped", err)
	}
}

// A signal is an orderly shutdown: the consumer's context is cancelled, its
// exit is awaited and the process ends cleanly.
func TestServeUntilDone_ASignalDrainsCleanly(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	stopped := false
	go func() {
		<-started
		cancel()
	}()
	err := serveUntilDone(ctx, quietLogger(), func(c context.Context) error {
		close(started)
		<-c.Done()
		stopped = true
		return c.Err()
	}, newTestServer(), readiness, nil)
	if err != nil {
		t.Fatalf("a signal is an orderly shutdown, got %v", err)
	}
	if !stopped {
		t.Error("the consumer's context must be cancelled and awaited")
	}
	if readiness.Ready() {
		t.Error("readiness must flip to not-ready in the drain")
	}
}
