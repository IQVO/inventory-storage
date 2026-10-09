package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/inventory-storage/internal/application/ports"
)

// inboundReceiptCounterName counts, in units, what the inbound-receiving
// consumer did with each received line (ADR 0037). outcome=damaged_not_booked
// is the one to watch: those units were counted at the dock but are not stock.
const inboundReceiptCounterName = "inventory.inbound_receipt_units"

// InboundReceiptMetrics implements ports.InboundReceiptMetrics against the
// global MeterProvider (a no-op until Setup installs a real one).
type InboundReceiptMetrics struct {
	counter metric.Int64Counter
}

var _ ports.InboundReceiptMetrics = (*InboundReceiptMetrics)(nil)

// NewInboundReceiptMetrics registers the counter. It only fails if the
// instrument name is invalid, a programming error.
func NewInboundReceiptMetrics() (*InboundReceiptMetrics, error) {
	counter, err := otel.Meter(meterName).Int64Counter(
		inboundReceiptCounterName,
		metric.WithDescription("Units on inbound-receiving ReceiptLineReceived events, by outcome (booked, damaged_not_booked)."),
		metric.WithUnit("{unit}"),
	)
	if err != nil {
		return nil, err
	}
	return &InboundReceiptMetrics{counter: counter}, nil
}

// InboundReceiptUnits adds units to outcome's series.
func (m *InboundReceiptMetrics) InboundReceiptUnits(ctx context.Context, outcome string, units int) {
	m.counter.Add(ctx, int64(units), metric.WithAttributes(outcomeKey.String(outcome)))
}
