package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/inventory-storage/internal/application/ports"
)

// heldUnitsName is the number of units currently held out of usable stock,
// by hold reason (ADR 0052): +quantity when a unit is held, -quantity when it
// leaves the hold (RELEASE, RETURN_TO_VENDOR, SCRAP), and a move between
// reasons on REWORK. It is what the quarantine backlog dashboard reads.
const heldUnitsName = "inventory.held_units"

var reasonKey = attribute.Key("reason")

// HeldStockMetrics implements ports.HeldStockMetrics against the global
// MeterProvider (a no-op until Setup installs a real one).
type HeldStockMetrics struct {
	counter metric.Int64UpDownCounter
}

var _ ports.HeldStockMetrics = (*HeldStockMetrics)(nil)

// NewHeldStockMetrics registers the up-down counter. It only fails if the
// instrument name is invalid, a programming error.
func NewHeldStockMetrics() (*HeldStockMetrics, error) {
	counter, err := otel.Meter(meterName).Int64UpDownCounter(
		heldUnitsName,
		metric.WithDescription("Units currently held out of usable stock, by hold reason."),
		metric.WithUnit("{unit}"),
	)
	if err != nil {
		return nil, err
	}
	return &HeldStockMetrics{counter: counter}, nil
}

// HeldUnits adds delta to reason's series.
func (m *HeldStockMetrics) HeldUnits(ctx context.Context, reason string, delta int) {
	m.counter.Add(ctx, int64(delta), metric.WithAttributes(reasonKey.String(reason)))
}
