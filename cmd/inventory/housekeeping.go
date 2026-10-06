package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
)

// housekeepingSettings is the sweeper's configuration (ADR-0026). A zero
// duration disables the corresponding behaviour: interval 0 disables the
// whole sweeper, TTL/retention 0 keep that table's rows forever.
type housekeepingSettings struct {
	interval        time.Duration
	idempotencyTTL  time.Duration
	outboxRetention time.Duration
}

// housekeepingSettingsFromEnv reads HOUSEKEEPING_INTERVAL (default 1h),
// IDEMPOTENCY_KEY_TTL (default 24h) and OUTBOX_RETENTION (default 168h = 7d).
func housekeepingSettingsFromEnv(logger *slog.Logger) housekeepingSettings {
	return housekeepingSettings{
		interval:        envDuration(logger, "HOUSEKEEPING_INTERVAL", postgres.DefaultSweepInterval),
		idempotencyTTL:  envDuration(logger, "IDEMPOTENCY_KEY_TTL", postgres.DefaultIdempotencyKeyTTL),
		outboxRetention: envDuration(logger, "OUTBOX_RETENTION", postgres.DefaultOutboxRetention),
	}
}

// closeWithSweeper starts the housekeeping sweeper (ADR-0026) for a
// Postgres-backed process — idempotency keys are written regardless of
// EVENT_PUBLISHER, so it is not tied to the outbox — and returns the closer
// that stops it BEFORE the pool closes.
func closeWithSweeper(pool *pgxpool.Pool, logger *slog.Logger) func() {
	stopSweeper := startSweeper(pool, housekeepingSettingsFromEnv(logger), logger)
	return func() {
		stopSweeper()
		pool.Close()
	}
}

// startSweeper runs the housekeeping Sweeper (ADR-0026) in a goroutine and
// returns a stop func that cancels it and waits, bounded by
// shutdownDrainTimeout, for the current pass to finish. With interval 0 it
// starts nothing and returns a no-op.
func startSweeper(pool *pgxpool.Pool, cfg housekeepingSettings, logger *slog.Logger) func() {
	if cfg.interval <= 0 {
		logger.Info("housekeeping sweeper disabled (HOUSEKEEPING_INTERVAL=0)")
		return func() {}
	}
	sweeper := postgres.NewSweeper(pool,
		postgres.WithSweeperLogger(logger),
		postgres.WithSweepInterval(cfg.interval),
		postgres.WithIdempotencyKeyTTL(cfg.idempotencyTTL),
		postgres.WithOutboxRetention(cfg.outboxRetention),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sweeper.Run(ctx) // only ever returns nil, on cancellation
	}()
	logger.Info("housekeeping sweeper started", "interval", cfg.interval,
		"idempotency_key_ttl", cfg.idempotencyTTL, "outbox_retention", cfg.outboxRetention)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(shutdownDrainTimeout):
			logger.Warn("housekeeping sweeper did not stop before the shutdown drain deadline")
		}
	}
}
