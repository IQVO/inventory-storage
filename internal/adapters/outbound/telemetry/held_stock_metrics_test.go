package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

// heldUnitsByReason collects inventory.held_units and returns its value per
// reason, failing the test when the instrument is not a non-monotonic Sum.
func heldUnitsByReason(t *testing.T, reader metric.Reader) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "inventory.held_units" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || sum.IsMonotonic {
				t.Fatalf("inventory.held_units is %T (monotonic=%v), want a non-monotonic int64 Sum: units leave the hold", m.Data, ok && sum.IsMonotonic)
			}
			for _, point := range sum.DataPoints {
				reason, _ := point.Attributes.Value("reason")
				counts[reason.AsString()] = point.Value
			}
		}
	}
	return counts
}

func TestHeldStockMetrics_TracksUnitsCurrentlyHeldByReason(t *testing.T) {
	reader := metric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	metrics, err := telemetry.NewHeldStockMetrics()
	if err != nil {
		t.Fatalf("NewHeldStockMetrics: %v", err)
	}

	ctx := context.Background()
	metrics.HeldUnits(ctx, "DAMAGED", 4)
	metrics.HeldUnits(ctx, "DAMAGED", 6)
	metrics.HeldUnits(ctx, "DAMAGED", -4)
	metrics.HeldUnits(ctx, "SUSPECT", 3)

	counts := heldUnitsByReason(t, reader)
	if counts["DAMAGED"] != 6 || counts["SUSPECT"] != 3 {
		t.Fatalf("counts = %v, want DAMAGED=6 SUSPECT=3", counts)
	}
}
