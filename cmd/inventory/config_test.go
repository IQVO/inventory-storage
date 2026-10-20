package main

import (
	"strings"
	"testing"
)

// validEnv is a complete, valid process environment for loadConfig.
func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":                       "postgres://u:p@db:5432/inventory_storage",
		"KAFKA_BROKERS":                      "broker:9092",
		"LOCATION_LOOKUP_MODE":               "kafka",
		"TRANSFER_ALLOCATION_CONSUMER_GROUP": "inventory-storage-transfer-allocation",
		"PRODUCT_MASTER_CONSUMER_GROUP":      "inventory-storage-product-master",
		"INBOUND_RECEIPT_CONSUMER_GROUP":     "inventory-storage-inbound-receipt",
		"TASK_COMPLETED_CONSUMER_GROUP":      "inventory-storage-confirm-pick",
	}
}

func mapEnv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

func TestLoadConfig_ValidEnvironment(t *testing.T) {
	cfg, err := loadConfig(mapEnv(validEnv()))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.databaseURL == "" || len(cfg.kafkaBrokers) != 1 || cfg.locationLookupMode != "kafka" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.migrationsDatabaseURL != cfg.databaseURL {
		t.Fatalf("migrationsDatabaseURL = %q, want the DATABASE_URL fallback", cfg.migrationsDatabaseURL)
	}
	if cfg.transferConsumerGroup != "inventory-storage-transfer-allocation" ||
		cfg.productMasterConsumerGroup != "inventory-storage-product-master" ||
		cfg.inboundReceiptConsumerGroup != "inventory-storage-inbound-receipt" ||
		cfg.taskCompletedConsumerGroup != "inventory-storage-confirm-pick" {
		t.Fatalf("consumer groups = %+v", cfg)
	}
}

// The legacy variables warehouse-infra still sets stay accepted, with the one
// value they can have now.
func TestLoadConfig_AcceptsTheLegacyKafkaModes(t *testing.T) {
	env := validEnv()
	env["EVENT_PUBLISHER"] = "kafka"
	env["TRANSFER_ALLOCATION_CONSUMER_MODE"] = "KAFKA"
	env["TASK_COMPLETED_CONSUMER_MODE"] = "kafka"
	if _, err := loadConfig(mapEnv(env)); err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
}

func TestLoadConfig_HttpLookupModeStaysAccepted(t *testing.T) {
	env := validEnv()
	env["LOCATION_LOOKUP_MODE"] = "http"
	cfg, err := loadConfig(mapEnv(env))
	if err != nil || cfg.locationLookupMode != "http" {
		t.Fatalf("mode = %q err = %v", cfg.locationLookupMode, err)
	}
}

// Every refusal names the variable, so the pod's crash log says what to fix.
func TestLoadConfig_RefusesAnythingButRealWiring(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(env map[string]string)
		want   string
	}{
		{"no database", func(e map[string]string) { delete(e, "DATABASE_URL") }, "DATABASE_URL"},
		{"blank database", func(e map[string]string) { e["DATABASE_URL"] = "  " }, "DATABASE_URL"},
		{"no brokers", func(e map[string]string) { delete(e, "KAFKA_BROKERS") }, "KAFKA_BROKERS"},
		{"only commas in brokers", func(e map[string]string) { e["KAFKA_BROKERS"] = " , ," }, "KAFKA_BROKERS"},
		{"log publisher", func(e map[string]string) { e["EVENT_PUBLISHER"] = "log" }, "EVENT_PUBLISHER"},
		{"transfer consumer off", func(e map[string]string) { e["TRANSFER_ALLOCATION_CONSUMER_MODE"] = "off" }, "TRANSFER_ALLOCATION_CONSUMER_MODE"},
		{"task consumer off", func(e map[string]string) { e["TASK_COMPLETED_CONSUMER_MODE"] = "off" }, "TASK_COMPLETED_CONSUMER_MODE"},
		{"permissive lookup", func(e map[string]string) { e["LOCATION_LOOKUP_MODE"] = "permissive" }, "LOCATION_LOOKUP_MODE"},
		{"unset lookup", func(e map[string]string) { delete(e, "LOCATION_LOOKUP_MODE") }, "LOCATION_LOOKUP_MODE"},
		{"no transfer group", func(e map[string]string) { delete(e, "TRANSFER_ALLOCATION_CONSUMER_GROUP") }, "TRANSFER_ALLOCATION_CONSUMER_GROUP"},
		{"no product-master group", func(e map[string]string) { delete(e, "PRODUCT_MASTER_CONSUMER_GROUP") }, "PRODUCT_MASTER_CONSUMER_GROUP"},
		{"no receipt group", func(e map[string]string) { delete(e, "INBOUND_RECEIPT_CONSUMER_GROUP") }, "INBOUND_RECEIPT_CONSUMER_GROUP"},
		{"no task group", func(e map[string]string) { delete(e, "TASK_COMPLETED_CONSUMER_GROUP") }, "TASK_COMPLETED_CONSUMER_GROUP"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := validEnv()
			tt.mutate(env)
			_, err := loadConfig(mapEnv(env))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to name %s", err, tt.want)
			}
		})
	}
}

func TestBuildLocationLookup_RefusesWhatLoadConfigRefuses(t *testing.T) {
	for _, mode := range []string{"", "permissive", "nonsense"} {
		if _, err := buildLocationLookup(t.Context(), mode, "", []string{"b:9092"}, nil, quietLogger()); err == nil {
			t.Errorf("mode %q must fail, it has no real adapter", mode)
		}
	}
	if _, err := buildLocationLookup(t.Context(), "http", "  ", nil, nil, quietLogger()); err == nil || !strings.Contains(err.Error(), "FACILITY_LAYOUT_BASE_URL") {
		t.Errorf("http without a base URL: err = %v, want FACILITY_LAYOUT_BASE_URL", err)
	}
}
