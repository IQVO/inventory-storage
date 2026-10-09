package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
	"github.com/claudioed/inventory-storage/internal/application/ports"
)

func TestInboundReceiptMetrics_CountsUnitsByOutcome(t *testing.T) {
	reader := metric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	metrics, err := telemetry.NewInboundReceiptMetrics()
	if err != nil {
		t.Fatalf("NewInboundReceiptMetrics: %v", err)
	}

	ctx := context.Background()
	metrics.InboundReceiptUnits(ctx, ports.InboundReceiptOutcomeBooked, 40)
	metrics.InboundReceiptUnits(ctx, ports.InboundReceiptOutcomeDamaged, 4)
	metrics.InboundReceiptUnits(ctx, ports.InboundReceiptOutcomeDamaged, 1)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "inventory.inbound_receipt_units" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("inventory.inbound_receipt_units is a %T, want an int64 Sum", m.Data)
			}
			for _, point := range sum.DataPoints {
				outcome, ok := point.Attributes.Value("outcome")
				if !ok {
					t.Fatal("data point has no outcome attribute")
				}
				counts[outcome.AsString()] = point.Value
			}
		}
	}
	if counts["booked"] != 40 || counts["damaged_not_booked"] != 5 {
		t.Fatalf("counts = %v, want booked=40 damaged_not_booked=5", counts)
	}
}
