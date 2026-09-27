package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/telemetry"
)

// staticSpanContext builds a valid, sampled SpanContext without needing an
// SDK: the handler only reads ids off the context, so this keeps the test
// hermetic and free of global-provider side effects.
func staticSpanContext(t *testing.T) trace.SpanContext {
	t.Helper()

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
}

func TestWithTraceContext_Handle(t *testing.T) {
	sc := staticSpanContext(t)

	tests := []struct {
		name        string
		ctx         func(*testing.T) context.Context
		wantTraceID string
		wantSpanID  string
	}{
		{
			name: "records written under a span carry both ids",
			ctx: func(t *testing.T) context.Context {
				t.Helper()
				return trace.ContextWithSpanContext(context.Background(), sc)
			},
			wantTraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			wantSpanID:  "00f067aa0ba902b7",
		},
		{
			name: "records written outside a span are left alone",
			ctx: func(*testing.T) context.Context {
				return context.Background()
			},
		},
		{
			name: "an invalid span context is not treated as a span",
			ctx: func(t *testing.T) context.Context {
				t.Helper()
				return trace.ContextWithSpanContext(context.Background(), trace.SpanContext{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertRecordCarriesTraceIDs(t, tt.ctx(t), tt.wantTraceID, tt.wantSpanID)
		})
	}
}

// assertRecordCarriesTraceIDs logs one record under ctx through the
// trace-aware handler and asserts which trace/span ids were stamped onto
// it. An empty wantTraceID means no ids may be present at all.
func assertRecordCarriesTraceIDs(t *testing.T, ctx context.Context, wantTraceID, wantSpanID string) {
	t.Helper()

	var buf bytes.Buffer
	logger := slog.New(telemetry.WithTraceContext(slog.NewJSONHandler(&buf, nil)))

	logger.InfoContext(ctx, "stock reserved", "sku", "SKU-1")

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("log line is not valid JSON: %v (%q)", err, buf.String())
	}

	// The wrapper must never swallow the record's own attributes.
	if record["sku"] != "SKU-1" {
		t.Errorf("sku = %v, want SKU-1", record["sku"])
	}

	if wantTraceID == "" {
		assertRecordHasNoTraceIDs(t, record)
		return
	}

	if record[telemetry.TraceIDKey] != wantTraceID {
		t.Errorf("%s = %v, want %s", telemetry.TraceIDKey, record[telemetry.TraceIDKey], wantTraceID)
	}
	if record[telemetry.SpanIDKey] != wantSpanID {
		t.Errorf("%s = %v, want %s", telemetry.SpanIDKey, record[telemetry.SpanIDKey], wantSpanID)
	}
}

// assertRecordHasNoTraceIDs fails when a record written outside a (valid)
// span still carries trace/span ids.
func assertRecordHasNoTraceIDs(t *testing.T, record map[string]any) {
	t.Helper()
	if _, ok := record[telemetry.TraceIDKey]; ok {
		t.Errorf("%s present with no active span: %v", telemetry.TraceIDKey, record[telemetry.TraceIDKey])
	}
	if _, ok := record[telemetry.SpanIDKey]; ok {
		t.Errorf("%s present with no active span: %v", telemetry.SpanIDKey, record[telemetry.SpanIDKey])
	}
}

func TestWithTraceContext_SurvivesWithAttrsAndWithGroup(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(telemetry.WithTraceContext(slog.NewJSONHandler(&buf, nil))).
		With("component", "http").
		WithGroup("request")

	ctx := trace.ContextWithSpanContext(context.Background(), staticSpanContext(t))
	logger.InfoContext(ctx, "http request", "status", 200)

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("log line is not valid JSON: %v (%q)", err, buf.String())
	}

	if record["component"] != "http" {
		t.Errorf("WithAttrs attribute lost: component = %v", record["component"])
	}

	// The ids are added by the outermost wrapper, so WithGroup nests them
	// alongside the record's own attributes rather than at the top level.
	group, ok := record["request"].(map[string]any)
	if !ok {
		t.Fatalf("WithGroup did not produce a nested object: %v", record)
	}
	if group[telemetry.TraceIDKey] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("%s = %v inside group, want the active trace id", telemetry.TraceIDKey, group[telemetry.TraceIDKey])
	}
	if group["status"] != float64(200) {
		t.Errorf("status = %v, want 200", group["status"])
	}
}

func TestWithTraceContext_DelegatesEnabled(t *testing.T) {
	var buf bytes.Buffer
	handler := telemetry.WithTraceContext(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if handler.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Enabled(Info) = true, want false: the wrapper must honour the inner handler's level")
	}
	if !handler.Enabled(context.Background(), slog.LevelError) {
		t.Error("Enabled(Error) = false, want true")
	}
}
