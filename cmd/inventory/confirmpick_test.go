package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestBuildTaskCompletedConsumer_OffByDefaultDoesNotStart(t *testing.T) {
	for _, mode := range []string{"", "off", "OFF", "nonsense"} {
		h, err := buildTaskCompletedConsumer(context.Background(), quietLogger(), config{taskCompletedConsumerMode: mode}, adapterSet{}, nil)
		if err != nil {
			t.Fatalf("mode %q must not fail boot: %v", mode, err)
		}
		// The noop handle's hooks are all safe to call.
		h.stop()
		h.wait()
		h.close()
	}
}

func TestBuildTaskCompletedConsumer_KafkaModeRequiresDatabase(t *testing.T) {
	tests := []struct {
		name string
		cfg  config
	}{
		{"no database url", config{taskCompletedConsumerMode: "kafka", kafkaBrokers: []string{"broker:9092"}}},
		{"database url but no pool", config{taskCompletedConsumerMode: "kafka", databaseURL: "postgres://x", kafkaBrokers: []string{"broker:9092"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildTaskCompletedConsumer(context.Background(), quietLogger(), tt.cfg, adapterSet{}, nil)
			if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
				t.Fatalf("err = %v, want a DATABASE_URL requirement", err)
			}
		})
	}
}

// lazyPool returns a pgxpool that never dials until used: enough to pass the
// wiring guards without a database.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/inventory_storage?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestBuildTaskCompletedConsumer_KafkaModeRequiresBrokers(t *testing.T) {
	cfg := config{taskCompletedConsumerMode: "kafka", databaseURL: "postgres://x"}
	_, err := buildTaskCompletedConsumer(context.Background(), quietLogger(), cfg, adapterSet{pool: lazyPool(t)}, nil)
	if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("err = %v, want a KAFKA_BROKERS requirement", err)
	}
}

// The first broker dial runs under bootretry: one transient reset (the
// fleet's known first-dial failure) must not fail boot.
func TestBuildTaskCompletedConsumer_RetriesTheBootDial(t *testing.T) {
	orig := dialBroker
	t.Cleanup(func() { dialBroker = orig })
	calls := 0
	dialBroker = func(context.Context, []string) error {
		calls++
		if calls == 1 {
			return errors.New("read: connection reset by peer")
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cfg := config{taskCompletedConsumerMode: "kafka", databaseURL: "postgres://x", kafkaBrokers: []string{"127.0.0.1:1"}}
	h, err := buildTaskCompletedConsumer(ctx, quietLogger(), cfg, adapterSet{pool: lazyPool(t)}, nil)
	if err != nil {
		cancel()
		t.Fatalf("a transient first dial must not fail boot: %v", err)
	}
	if calls != 2 {
		t.Errorf("dial attempts = %d, want 2", calls)
	}
	cancel()
	h.wait()
	h.close()
}

func TestLoadConfig_ReadsTaskCompletedConsumerSettings(t *testing.T) {
	t.Run("defaults off with no group", func(t *testing.T) {
		t.Setenv("TASK_COMPLETED_CONSUMER_MODE", "")
		t.Setenv("TASK_COMPLETED_CONSUMER_GROUP", "")
		cfg := loadConfig()
		if cfg.taskCompletedConsumerMode != "off" || cfg.taskCompletedConsumerGroup != "" {
			t.Fatalf("mode=%q group=%q, want off and empty", cfg.taskCompletedConsumerMode, cfg.taskCompletedConsumerGroup)
		}
	})
	t.Run("reads the env", func(t *testing.T) {
		t.Setenv("TASK_COMPLETED_CONSUMER_MODE", "kafka")
		t.Setenv("TASK_COMPLETED_CONSUMER_GROUP", "inventory-storage-confirm-pick-dev")
		cfg := loadConfig()
		if cfg.taskCompletedConsumerMode != "kafka" || cfg.taskCompletedConsumerGroup != "inventory-storage-confirm-pick-dev" {
			t.Fatalf("mode=%q group=%q", cfg.taskCompletedConsumerMode, cfg.taskCompletedConsumerGroup)
		}
	})
}
