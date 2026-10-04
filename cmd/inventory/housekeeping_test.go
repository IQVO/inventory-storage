package main

import (
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
)

func TestHousekeepingSettingsFromEnv_Defaults(t *testing.T) {
	t.Setenv("HOUSEKEEPING_INTERVAL", "")
	t.Setenv("IDEMPOTENCY_KEY_TTL", "")
	t.Setenv("OUTBOX_RETENTION", "")

	got := housekeepingSettingsFromEnv(quietLogger())
	want := housekeepingSettings{
		interval:        postgres.DefaultSweepInterval,
		idempotencyTTL:  24 * time.Hour,
		outboxRetention: 7 * 24 * time.Hour,
	}
	if got != want {
		t.Fatalf("defaults = %+v, want %+v (ADR-0026: 1h sweep, 24h key TTL, 7d outbox retention)", got, want)
	}
}

func TestHousekeepingSettingsFromEnv_Overrides(t *testing.T) {
	t.Setenv("HOUSEKEEPING_INTERVAL", "5m")
	t.Setenv("IDEMPOTENCY_KEY_TTL", "36h")
	t.Setenv("OUTBOX_RETENTION", "72h")

	got := housekeepingSettingsFromEnv(quietLogger())
	want := housekeepingSettings{interval: 5 * time.Minute, idempotencyTTL: 36 * time.Hour, outboxRetention: 72 * time.Hour}
	if got != want {
		t.Fatalf("overrides = %+v, want %+v", got, want)
	}
}

func TestEnvDuration(t *testing.T) {
	const key = "ADRFIX_TEST_DURATION"
	def := 3 * time.Hour
	tests := []struct {
		name string
		set  bool
		raw  string
		want time.Duration
	}{
		{"unset uses default", false, "", def},
		{"valid", true, "90s", 90 * time.Second},
		{"zero means disabled and is honored", true, "0", 0},
		{"garbage falls back to default", true, "soon", def},
		{"negative falls back to default", true, "-5m", def},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.raw)
			} else {
				t.Setenv(key, "")
			}
			if got := envDuration(quietLogger(), key, def); got != tt.want {
				t.Fatalf("envDuration(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

// HOUSEKEEPING_INTERVAL=0 disables the sweeper: nothing is started (so a nil
// pool is never touched) and the returned stop func is a safe no-op.
func TestStartSweeper_ZeroIntervalDisablesIt(t *testing.T) {
	stop := startSweeper(nil, housekeepingSettings{interval: 0}, quietLogger())
	if stop == nil {
		t.Fatal("stop func must never be nil")
	}
	stop()
	stop() // idempotent
}
