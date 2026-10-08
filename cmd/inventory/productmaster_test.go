package main

import (
	"context"
	"strings"
	"testing"
)

func TestBuildProductMasterConsumer_UnsetGroupDoesNotStart(t *testing.T) {
	h, err := buildProductMasterConsumer(context.Background(), quietLogger(), config{}, adapterSet{})
	if err != nil {
		t.Fatalf("unset PRODUCT_MASTER_CONSUMER_GROUP must not fail boot: %v", err)
	}
	// The noop handle's hooks are all safe to call.
	h.stop()
	h.wait()
	h.close()
}

func TestBuildProductMasterConsumer_RequiresDatabase(t *testing.T) {
	cfg := config{productMasterConsumerGroup: "inventory-storage-product-master", kafkaBrokers: []string{"broker:9092"}}
	_, err := buildProductMasterConsumer(context.Background(), quietLogger(), cfg, adapterSet{})
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("err = %v, want a DATABASE_URL requirement", err)
	}
}

func TestLoadConfig_ReadsProductMasterConsumerGroup(t *testing.T) {
	t.Setenv("PRODUCT_MASTER_CONSUMER_GROUP", "inventory-storage-product-master")
	if got := loadConfig().productMasterConsumerGroup; got != "inventory-storage-product-master" {
		t.Fatalf("productMasterConsumerGroup = %q", got)
	}
}

func TestCombineConsumerHandles_DrainsEveryHandle(t *testing.T) {
	var calls []string
	mk := func(name string) transferConsumerHandle {
		return transferConsumerHandle{
			stop:  func() { calls = append(calls, name+".stop") },
			wait:  func() { calls = append(calls, name+".wait") },
			close: func() { calls = append(calls, name+".close") },
		}
	}
	h := combineConsumerHandles(mk("a"), mk("b"))
	h.stop()
	h.wait()
	h.close()
	want := "a.stop b.stop a.wait b.wait a.close b.close"
	if got := strings.Join(calls, " "); got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}
