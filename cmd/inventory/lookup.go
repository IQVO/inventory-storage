package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/bootretry"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/facilitycache"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/facilitylayout"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/resilience"
)

// locationLookupHandle is a selected LocationClassificationLookup plus its
// lifecycle hooks.
//
// runDone closes once the kafka mode's consumer.Run goroutine has actually
// returned (nil in every other mode, since there is no such goroutine) —
// graceful shutdown waits on it, bounded, rather than firing the cancel and
// moving on immediately. close releases the Kafka reader when one was
// started, and is a no-op otherwise (nil only on a startup error).
type locationLookupHandle struct {
	lookup  ports.LocationClassificationLookup
	runDone chan struct{}
	close   func()
}

// buildLocationLookup selects the outbound LocationClassificationLookup
// adapter via LOCATION_LOOKUP_MODE (kafka|http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected — mirroring the EVENT_PUBLISHER=kafka|log
// pattern.
//
//   - "kafka"      maintains a local cache fed by facility-layout's
//     warehouse.facility.events topic. No per-stow HTTP call, so
//     facility-layout stops being a runtime dependency of StowStock.
//     Requires KAFKA_BROKERS. Blocks until the initial replay completes
//     (see the facilitycache package doc for why).
//   - "http"       calls facility-layout synchronously per stow, wrapped
//     in a per-dependency circuit breaker with jittered retry
//     (ADR-0020): on a trip, calls fall back to the SAME fail-open
//     behaviour this client already had, rather than a new fallback
//     path. recorder feeds the breaker's state transitions into the
//     circuit_breaker.state gauge; nil is fine (see
//     resilience.RecordStateChange's doc comment). Requires
//     FACILITY_LAYOUT_BASE_URL. Retained as the rollback for "kafka".
//   - "permissive" (default) answers Known=false for everything.
func buildLocationLookup(ctx context.Context, mode, facilityLayoutBaseURL string, brokers []string, recorder resilience.StateRecorder, logger *slog.Logger) (locationLookupHandle, error) {
	switch {
	case strings.EqualFold(mode, "kafka"):
		return kafkaLocationLookup(ctx, brokers, logger)

	case strings.EqualFold(mode, "http"):
		logger.Info("location classification lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL, "circuit_breaker", "enabled", "retry", "enabled")
		client := facilitylayout.NewBreakerClient(facilitylayout.NewClient(facilityLayoutBaseURL, nil), recorder)
		return locationLookupHandle{lookup: client, close: func() {}}, nil

	default:
		return locationLookupHandle{lookup: facilitylayout.NewPermissiveLookup(), close: func() {}}, nil
	}
}

// kafkaLocationLookup builds the Kafka-sourced facility location cache,
// starts its consumer loop and blocks until the initial replay completes.
func kafkaLocationLookup(ctx context.Context, brokers []string, logger *slog.Logger) (locationLookupHandle, error) {
	if len(brokers) == 0 {
		return locationLookupHandle{}, fmt.Errorf("LOCATION_LOOKUP_MODE=kafka requires KAFKA_BROKERS to be set")
	}
	consumer, err := dialFacilityCache(ctx, brokers, logger)
	if err != nil {
		return locationLookupHandle{}, err
	}
	logger.Info("location classification lookup configured",
		"mode", "kafka", "topic", facilitycache.Topic, "brokers", brokers)

	runDone := runFacilityCache(ctx, consumer, logger)
	if err := awaitFacilityCacheReady(ctx, consumer, logger); err != nil {
		_ = consumer.Close()
		return locationLookupHandle{runDone: runDone}, err
	}
	return locationLookupHandle{lookup: consumer, runDone: runDone, close: func() { _ = consumer.Close() }}, nil
}

// dialFacilityCache is retried for the same reason the Postgres dials are:
// facilitycache.NewConsumer's newTargetOffsets dials the broker
// synchronously (kafkago.DialContext) to capture the readiness watermark
// before any consuming begins, and that dial is exactly this fleet's known
// ~10s post-start first-outbound-dial reset. A single attempt turned that
// transient into CrashLoopBackOff here too.
func dialFacilityCache(ctx context.Context, brokers []string, logger *slog.Logger) (*facilitycache.Consumer, error) {
	var consumer *facilitycache.Consumer
	if err := bootretry.Retry(ctx, logger, "dial facility-layout kafka topic", func() error {
		c, err := facilitycache.NewConsumer(ctx, brokers, logger)
		if err != nil {
			return err
		}
		consumer = c
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to start the Kafka-sourced location cache: %w", err)
	}
	return consumer, nil
}

// runFacilityCache starts the consumer's Run loop and returns a channel that
// closes once it has returned.
func runFacilityCache(ctx context.Context, consumer *facilitycache.Consumer, logger *slog.Logger) chan struct{} {
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		logger.Info("facility location cache consumer running", "topic", facilitycache.Topic)
		// Run only ever returns on error (including the plain
		// context.Canceled of an orderly shutdown), never nil.
		if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
			logger.Error("facility location cache consumer stopped", "error", err)
		}
	}()
	return runDone
}

// awaitFacilityCacheReady preserves the retired HTTP client's "never answer
// against incomplete data" property: an empty cache reports Known=false,
// which is a FAIL-OPEN answer, so serving traffic mid-replay would silently
// wave through stows that should have been classified.
func awaitFacilityCacheReady(ctx context.Context, consumer *facilitycache.Consumer, logger *slog.Logger) error {
	logger.Info("waiting for the facility location cache to replay its initial history before accepting traffic")
	waitCtx, cancel := context.WithTimeout(ctx, facilitycache.WaitReadyTimeout)
	defer cancel()
	if err := consumer.WaitReady(waitCtx); err != nil {
		return fmt.Errorf("facility location cache did not become ready within %s: %w", facilitycache.WaitReadyTimeout, err)
	}
	if consumer.Slots() == 0 {
		// Not fatal — a genuinely empty facility-layout is a valid
		// state — but it means every lookup fails open, so say so
		// loudly rather than letting it look like a working cache.
		logger.Warn("facility location cache is ready but EMPTY; every location will report Known=false (fail-open) until facility-layout publishes",
			"topic", facilitycache.Topic)
	} else {
		logger.Info("facility location cache is ready", "slots", consumer.Slots(), "zones", consumer.Zones())
	}
	return nil
}
