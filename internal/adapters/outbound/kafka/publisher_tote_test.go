package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// carry (Task 9 review): the direct Publish path (not only the outbox's
// Encode) forwards the three tote events: isIntegrationEvent must list them,
// or Publish returns nil and writes nothing.
func TestPublisher_ToteEventsArePublished(t *testing.T) {
	at := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	sku, _ := shared.NewSKU("SKU-1")
	qty, _ := shared.NewPositiveQuantity(6)
	decant, _ := shared.NewBinId("WH1-RCV-DEC-D01-01-01-A")
	asrs, _ := shared.NewBinId("WH1-AUTO-ASRS-ASRS1-00-00-S")
	cases := []struct {
		eventType string
		event     shared.DomainEvent
	}{
		{"com.warehouse.wms.inventory-storage.tote.ToteRegistered", shared.NewToteRegistered(at, "TOTE-1")},
		{"com.warehouse.wms.inventory-storage.tote.ToteFilled",
			shared.NewToteFilled(at, "TOTE-1", sku, qty, "", "WH1", decant, "su-1")},
		{"com.warehouse.wms.inventory-storage.tote.ToteStowed",
			shared.NewToteStowed(at, "TOTE-1", "WH1", decant, asrs, []shared.ToteLine{{SKU: sku, Quantity: qty}})},
	}
	for _, tc := range cases {
		t.Run(tc.eventType, func(t *testing.T) {
			writer := &fakeWriter{}
			pub := kafka.NewPublisher(writer, memory.NewReservationRepo())
			if err := pub.Publish(context.Background(), tc.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(writer.messages) != 1 {
				t.Fatalf("messages = %d, want 1", len(writer.messages))
			}
			e, err := cloudevents.Decode(writer.messages[0].Value)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if e.Type() != tc.eventType || string(writer.messages[0].Key) != "TOTE-1" {
				t.Errorf("type %q key %q", e.Type(), writer.messages[0].Key)
			}
		})
	}
}
