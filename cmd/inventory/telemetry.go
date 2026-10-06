package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

// telemetryFlushTimeout bounds the final export attempt. Without a deadline
// the exporter would retry against an unreachable Collector and stretch a
// shutdown out well past what an orchestrator will wait for.
const telemetryFlushTimeout = 5 * time.Second

// startTelemetry brings OpenTelemetry up and returns the flush func to
// defer. Export is non-blocking: an unreachable Collector costs telemetry,
// never availability. A failed flush is logged, never returned — turning
// that into a non-zero exit would make every clean shutdown look like a
// crash.
func startTelemetry(logger *slog.Logger, cfg config) (func(), error) {
	shutdownTelemetry, err := telemetry.Setup(context.Background(), cfg.serviceName, cfg.serviceVersion, cfg.otlpEndpoint)
	if err != nil {
		return nil, err
	}
	logger.Info("telemetry configured", "service_name", cfg.serviceName, "otlp_endpoint", cfg.otlpEndpoint)
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("telemetry flush failed on shutdown", "error", err)
		}
	}, nil
}

// circuitBreakerMetrics wires the facility-layout breaker's OnStateChange
// into the circuit_breaker.state gauge (ADR-0020), reusing the SAME OTel
// MeterProvider telemetry.Setup already installed rather than standing up a
// second Prometheus registry. Errors mirror NewReservationMetrics' contract
// (invalid instrument name only, a programming error) — non-fatal: a nil
// recorder just means this process runs without the gauge, never without
// the breaker itself.
func circuitBreakerMetrics(logger *slog.Logger) *telemetry.CircuitBreakerMetrics {
	m, err := telemetry.NewCircuitBreakerMetrics()
	if err != nil {
		logger.Warn("circuit breaker metrics unavailable; the facility-layout breaker will run without the circuit_breaker.state gauge", "error", err)
	}
	return m
}
