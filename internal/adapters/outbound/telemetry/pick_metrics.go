package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/inventory-storage/internal/application/ports"
)

// pickConfirmationCounterName counts, per reservation, what the TaskCompleted
// consumer did when a PICK task completed (ADR 0035). outcome=expired is the
// one to alert on: the physical pick happened but the reservation had already
// timed out (ADR 0003), so its stock was returned to usable before the
// decrement could be recorded.
const pickConfirmationCounterName = "inventory.pick_confirmations"

// PickConfirmationMetrics implements ports.PickConfirmationMetrics against the
// global MeterProvider (a no-op until Setup installs a real one).
type PickConfirmationMetrics struct {
	counter metric.Int64Counter
}

var _ ports.PickConfirmationMetrics = (*PickConfirmationMetrics)(nil)

// NewPickConfirmationMetrics registers the counter. It only fails if the
// instrument name is invalid, a programming error.
func NewPickConfirmationMetrics() (*PickConfirmationMetrics, error) {
	counter, err := otel.Meter(meterName).Int64Counter(
		pickConfirmationCounterName,
		metric.WithDescription("Reservations settled by a completed PICK task, by outcome (confirmed, expired, already_picked, revoked)."),
		metric.WithUnit("{reservation}"),
	)
	if err != nil {
		return nil, err
	}
	return &PickConfirmationMetrics{counter: counter}, nil
}

// PickConfirmation adds n to outcome's series.
func (m *PickConfirmationMetrics) PickConfirmation(ctx context.Context, outcome string, n int) {
	m.counter.Add(ctx, int64(n), metric.WithAttributes(outcomeKey.String(outcome)))
}
