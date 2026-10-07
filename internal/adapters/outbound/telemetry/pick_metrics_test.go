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

func TestPickConfirmationMetrics_CountsByOutcome(t *testing.T) {
	reader := metric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	metrics, err := telemetry.NewPickConfirmationMetrics()
	if err != nil {
		t.Fatalf("NewPickConfirmationMetrics: %v", err)
	}

	ctx := context.Background()
	metrics.PickConfirmation(ctx, ports.PickOutcomeConfirmed, 3)
	metrics.PickConfirmation(ctx, ports.PickOutcomeExpired, 1)
	metrics.PickConfirmation(ctx, ports.PickOutcomeExpired, 1)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "inventory.pick_confirmations" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("inventory.pick_confirmations is a %T, want an int64 Sum", m.Data)
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
	if counts["confirmed"] != 3 || counts["expired"] != 2 {
		t.Fatalf("counts = %v, want confirmed=3 expired=2", counts)
	}
}
