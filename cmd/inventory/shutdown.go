package main

import (
	"context"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
)

// httpShutdownTimeout bounds the HTTP server's in-flight request drain.
const httpShutdownTimeout = 10 * time.Second

// serveHTTPUntilSignal runs the HTTP server until SIGINT/SIGTERM (or a
// listen error), then drains it together with the facility location
// cache consumer, bounded by a grace deadline.
func serveHTTPUntilSignal(logger *slog.Logger, httpServer *http.Server, readiness *inboundhttp.Readiness, stopLookup context.CancelFunc, closeLocationLookup func(), lookupRunDone <-chan struct{}) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		stopLookup()
		closeLocationLookup()
		return err
	case <-ctx.Done():
	}

	return gracefulShutdown(logger, httpServer, readiness, stopLookup, closeLocationLookup, lookupRunDone)
}

// gracefulShutdown (ADR-0020 §graceful shutdown), in order:
//
//  1. Flip readiness to not-ready FIRST, before anything else
//     stops — a Kubernetes readinessProbe polling /readyz needs a
//     window to observe this and stop routing NEW traffic to this
//     pod before step 2 below ever closes the listener, so a
//     request racing the SIGTERM is far less likely to be routed
//     here only to hit a closing connection.
//  2. Stop accepting new HTTP connections and drain in-flight
//     requests, bounded by httpShutdownTimeout.
//  3. Stop the facility location cache consumer's Run loop
//     cleanly: cancel lookupCtx (no new message is fetched/handled
//     after this) and wait, bounded by shutdownDrainTimeout, for
//     its goroutine to actually finish rather than merely asking
//     it to stop and moving on.
//  4. Only THEN does the deferred adapters.close (registered in
//     run(), so by defer's LIFO order it runs LAST of all, after
//     this function returns and every consumer/relay goroutine has
//     already stopped touching the pgx pool) close Postgres.
func gracefulShutdown(logger *slog.Logger, httpServer *http.Server, readiness *inboundhttp.Readiness, stopLookup context.CancelFunc, closeLocationLookup func(), lookupRunDone <-chan struct{}) error {
	readiness.SetNotReady()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	err := httpServer.Shutdown(shutdownCtx)

	stopFacilityCacheConsumer(logger, stopLookup, lookupRunDone)
	closeLocationLookup()

	return err
}

// stopFacilityCacheConsumer stops the facility location cache consumer's
// loop cleanly: cancel so no NEW message is fetched, then wait (bounded)
// for the Run goroutine to actually return before its Kafka reader is
// closed.
func stopFacilityCacheConsumer(logger *slog.Logger, stopLookup context.CancelFunc, lookupRunDone <-chan struct{}) {
	stopLookup()
	if lookupRunDone == nil {
		return
	}
	select {
	case <-lookupRunDone:
	case <-time.After(shutdownDrainTimeout):
		logger.Warn("facility location cache consumer did not stop before the shutdown drain deadline")
	}
}
