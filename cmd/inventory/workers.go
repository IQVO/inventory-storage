package main

import (
	"context"
	"fmt"
	"log/slog"
)

// deathsKey carries the channel on which a background worker reports that it
// stopped for any reason other than shutdown.
type deathsKey struct{}

// withDeaths returns ctx carrying deaths and the channel itself.
func withDeaths(ctx context.Context) (context.Context, chan error) {
	deaths := make(chan error, 8)
	return context.WithValue(ctx, deathsKey{}, deaths), deaths
}

// workerDied is called by a consumer goroutine whose Run returned something
// other than context.Canceled. Postgres and Kafka are the only runtime and the
// consumers are always on (the real-wiring ADR), so a dead consumer is a
// broken process: it is reported to run(), which drains and exits non-zero so
// the orchestrator restarts the pod instead of leaving it serving without the
// consumer. Never blocks: a full channel already holds enough reports.
func workerDied(ctx context.Context, logger *slog.Logger, name string, err error) {
	logger.Error(name+" stopped", "error", err)
	deaths, ok := ctx.Value(deathsKey{}).(chan error)
	if !ok {
		return
	}
	select {
	case deaths <- fmt.Errorf("%s stopped: %w", name, err):
	default:
	}
}
