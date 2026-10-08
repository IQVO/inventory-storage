package main

import (
	"context"
	"strings"
	"testing"
)

func TestBuildInboundReceiptConsumer_UnsetGroupDoesNotStart(t *testing.T) {
	h, err := buildInboundReceiptConsumer(context.Background(), quietLogger(), config{}, adapterSet{})
	if err != nil {
		t.Fatalf("unset INBOUND_RECEIPT_CONSUMER_GROUP must not fail boot: %v", err)
	}
	// The noop handle's hooks are all safe to call.
	h.stop()
	h.wait()
	h.close()
}

func TestBuildInboundReceiptConsumer_RequiresDatabase(t *testing.T) {
	cfg := config{inboundReceiptConsumerGroup: "inventory-storage-inbound-receipt", kafkaBrokers: []string{"broker:9092"}}
	_, err := buildInboundReceiptConsumer(context.Background(), quietLogger(), cfg, adapterSet{})
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("err = %v, want a DATABASE_URL requirement", err)
	}
}

func TestLoadConfig_ReadsInboundReceiptConsumerGroup(t *testing.T) {
	t.Setenv("INBOUND_RECEIPT_CONSUMER_GROUP", "inventory-storage-inbound-receipt")
	if got := loadConfig().inboundReceiptConsumerGroup; got != "inventory-storage-inbound-receipt" {
		t.Fatalf("inboundReceiptConsumerGroup = %q", got)
	}
}

func TestLoadConfig_InboundReceiptConsumerGroupDefaultsToUnset(t *testing.T) {
	t.Setenv("INBOUND_RECEIPT_CONSUMER_GROUP", "")
	if got := loadConfig().inboundReceiptConsumerGroup; got != "" {
		t.Fatalf("inboundReceiptConsumerGroup = %q, want empty (consumer not started)", got)
	}
}
