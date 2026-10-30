package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/system"
)

type nopUnitOfWork struct{}

func (nopUnitOfWork) Execute(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type nopMetrics struct{}

func (nopMetrics) ReservationCreated(context.Context) {}
func (nopMetrics) ReservationRevoked(context.Context) {}

// ADR-0016 Tier 1: cmd/a2a serves HTTP, so it emits
// http.server.request.duration like every other fleet HTTP server.
func TestNewRouter_EmitsStandardHTTPServerMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	router := newTestRouter(t) // built AFTER the provider is installed

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"SendMessage"}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(httptest.NewRecorder(), req)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = true
		}
	}
	for _, want := range []string{"http.server.request.duration", "http.server.active_requests"} {
		if !got[want] {
			t.Errorf("%s not emitted by cmd/a2a's router; got %v", want, got)
		}
	}
}

// The regression this guards (it bit cmd/mcp before): a write skill built with
// no UnitOfWork, no Metrics and a log-only publisher would never reach Kafka.
// A2A-initiated revocations must share the publisher, UnitOfWork and metrics
// the REST DELETE /reservations/{id} uses (ADR-0017).
func TestBuildDeps_RevokeReservationSharesPublisherUnitOfWorkAndMetrics(t *testing.T) {
	pub := events.NewLogPublisher(quietLogger())
	uow := nopUnitOfWork{}
	ad := adapters{stock: memory.NewStockRepo(), reservations: memory.NewReservationRepo(), publisher: pub, uow: uow, close: func() {}}

	deps := buildDeps(ad, system.Clock{}, nopMetrics{})

	rr := deps.RevokeReservation
	if rr == nil {
		t.Fatal("RevokeReservation not wired")
	}
	if rr.Events != pub {
		t.Errorf("RevokeReservation.Events = %v, want the shared publisher", rr.Events)
	}
	if rr.UnitOfWork != uow {
		t.Errorf("RevokeReservation.UnitOfWork = %v, want the shared UnitOfWork", rr.UnitOfWork)
	}
	if rr.Metrics == nil {
		t.Error("RevokeReservation.Metrics is nil; the revocation counter would be lost")
	}
	if deps.GetUsable == nil {
		t.Error("read-side deps not wired")
	}
}
