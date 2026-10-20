package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
)

func TestWorkerDied_ReportsOnTheDeathsChannelAndNeverBlocks(t *testing.T) {
	ctx, deaths := withDeaths(context.Background())
	for i := 0; i < cap(deaths)+3; i++ { // more reports than the buffer holds
		workerDied(ctx, quietLogger(), "some consumer", errors.New("boom"))
	}
	err := <-deaths
	if err == nil || !strings.Contains(err.Error(), "some consumer stopped") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want it to name the consumer and wrap the cause", err)
	}
}

func TestWorkerDied_WithoutADeathsChannelOnlyLogs(t *testing.T) {
	workerDied(context.Background(), quietLogger(), "some consumer", errors.New("boom")) // must not panic
}

// A dead consumer ends the process with an error (so the pod restarts)
// instead of leaving it serving without the consumer, and the drain still
// runs: readiness flips first.
func TestServeHTTPUntilSignal_ADeadWorkerEndsTheProcessWithItsError(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	deaths := make(chan error, 1)
	cause := errors.New("task-completed confirm-pick consumer stopped: boom")
	deaths <- cause

	srv := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}
	stopped := false
	err := serveHTTPUntilSignal(context.Background(), quietLogger(), srv, readiness, deaths,
		func() { stopped = true }, func() {}, nil, noopTransferConsumer())

	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want the worker's death", err)
	}
	if !stopped {
		t.Error("the consumers' context must be cancelled in the drain")
	}
	if readiness.Ready() {
		t.Error("readiness must be flipped to not-ready by the drain")
	}
}

func TestServeHTTPUntilSignal_ACancelledContextDrainsCleanly(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	srv := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}
	err := serveHTTPUntilSignal(ctx, quietLogger(), srv, readiness, make(chan error), func() {}, func() {}, nil, noopTransferConsumer())
	if err != nil {
		t.Fatalf("a signal is an orderly shutdown, got %v", err)
	}
	if readiness.Ready() {
		t.Error("readiness must be flipped to not-ready by the drain")
	}
}
