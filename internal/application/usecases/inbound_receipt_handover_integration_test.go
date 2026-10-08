//go:build integration

// inbound-receiving hand-over (ADR 0037) end-to-end proof on REAL
// infrastructure booted via testcontainers (its own Kafka broker plus the
// package's shared Postgres; never an external broker, never t.Skip):
//
//	inbound-receiving-shaped ReceiptLineReceived on a Kafka topic
//	  -> InboundReceiptConsumer -> BookInboundReceiptLine -> ReceiveStock
//	  -> a StockReceived row on the analytics topic in outbox_events
//	     (written in the same transaction as the processed_events claim).
//
// Damaged lines, other event types, invalid payloads and redelivered ids
// leave nothing behind.
package usecases_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

// publishReceiptEvent writes one CloudEvent exactly as inbound-receiving
// emits it (its source, the type given, key = ASN number, subject = receipt id).
func publishReceiptEvent(t *testing.T, brokers []string, topic, id, eventType string, data map[string]any) {
	t.Helper()
	payload, _ := json.Marshal(data)
	value, _ := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/inbound-receiving",
		"type":            eventType,
		"subject":         "rcpt-itest",
		"time":            "2026-10-08T14:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:inbound-receiving:events:ReceiptLineReceived:v1",
		"data":            json.RawMessage(payload),
	})
	w := kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}, BatchTimeout: 50 * time.Millisecond}
	defer func() { _ = w.Close() }()
	msg := kafkago.Message{
		Key:     []byte("ASN-1001"),
		Value:   value,
		Headers: []kafkago.Header{{Key: "content-type", Value: []byte("application/cloudevents+json; charset=UTF-8")}},
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := w.WriteMessages(context.Background(), msg)
		if err == nil {
			return
		}
		if !errors.Is(err, kafkago.UnknownTopicOrPartition) || time.Now().After(deadline) {
			t.Fatalf("publish %s: %v", id, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func receiptLinePayload(sku string, quantity int, condition string) map[string]any {
	return map[string]any{
		"receipt_id": "rcpt-itest", "asn_number": "ASN-1001", "line_no": 1,
		"sku": sku, "quantity": quantity, "condition": condition, "received_at": "2026-10-08T14:00:00Z",
	}
}

// stockReceivedAnalyticsRows counts the StockReceived messages for sku sitting
// in the outbox for the analytics topic, and returns the summed quantity.
func stockReceivedAnalyticsRows(t *testing.T, env *transferTestEnv, sku string) (rows, quantity int) {
	t.Helper()
	err := env.pool.QueryRow(context.Background(), `
		SELECT count(*), coalesce(sum((convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'quantity')::int), 0)
		FROM outbox_events
		WHERE topic = $1
		  AND event_type LIKE '%StockReceived'
		  AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'sku' = $2`,
		outboundkafka.AnalyticsTopic, sku).Scan(&rows, &quantity)
	if err != nil {
		t.Fatalf("read analytics outbox rows: %v", err)
	}
	return rows, quantity
}

func processedReceiptEvents(t *testing.T, env *transferTestEnv, ids ...string) int {
	t.Helper()
	var n int
	err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE consumer = $1 AND event_id = ANY($2)`,
		usecases.InboundReceiptConsumerName, ids).Scan(&n)
	if err != nil {
		t.Fatalf("count processed_events: %v", err)
	}
	return n
}

func TestIntegration_InboundReceipt_GoodLineBecomesStagedStock(t *testing.T) {
	ctx := context.Background()
	run := uniqueRun()
	topic := fmt.Sprintf("%s.itest-%s", inboundkafka.InboundReceivingTopic, run)
	env := inboundReceiptTestEnv(t, topic)

	goodSKU := "SKU-IR-GOOD-" + run
	damagedSKU := "SKU-IR-DMG-" + run
	invalidSKU := "SKU-IR-BAD-" + run
	goodID, damagedID, dupID := "ir-good-"+run, "ir-dmg-"+run, "ir-good-"+run

	receiptType := "com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived"
	// A mix a real topic carries: an unrelated type first, the Good line, a
	// Damaged line, an invalid quantity, a redelivery of the Good id, and a
	// second Good line for the same SKU under a NEW id (booked in addition).
	publishReceiptEvent(t, env.brokers, topic, "ir-open-"+run, "com.warehouse.wms.inbound-receiving.receipt.ReceiptOpened",
		map[string]any{"receipt_id": "rcpt-itest", "asn_number": "ASN-1001", "opened_at": "2026-10-08T13:00:00Z"})
	publishReceiptEvent(t, env.brokers, topic, goodID, receiptType, receiptLinePayload(goodSKU, 40, "Good"))
	publishReceiptEvent(t, env.brokers, topic, damagedID, receiptType, receiptLinePayload(damagedSKU, 4, "Damaged"))
	publishReceiptEvent(t, env.brokers, topic, "ir-bad-"+run, receiptType, receiptLinePayload(invalidSKU, 0, "Good"))
	publishReceiptEvent(t, env.brokers, topic, dupID, receiptType, receiptLinePayload(goodSKU, 40, "Good"))
	publishReceiptEvent(t, env.brokers, topic, "ir-good2-"+run, receiptType, receiptLinePayload(goodSKU, 2, "Good"))

	analytics := outboundkafka.NewAnalyticsPublisher(env.brokers, postgres.NewReservationRepo(env.pool), nil)
	t.Cleanup(func() { _ = analytics.Close() })
	uow := postgres.NewUnitOfWork(env.pool)
	book := &usecases.BookInboundReceiptLine{
		Receive: &usecases.ReceiveStock{
			Events:     postgres.NewOutboxPublisher(env.pool, analytics),
			Clock:      memory.SystemClock{},
			UnitOfWork: uow,
		},
		ProcessedEvents: postgres.NewProcessedEventRepo(env.pool),
		UnitOfWork:      uow,
	}
	group := "inbound-receipt-itest-" + run
	consumer := inboundkafka.NewInboundReceiptConsumerForTopic(topic, env.brokers, group, book, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = consumer.Close() })

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		rows, _ := stockReceivedAnalyticsRows(t, env, goodSKU)
		if rows == 2 && groupLag(env, group) == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("consumer did not settle: good rows=%d lag=%d", rows, groupLag(env, group))
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done

	// Good: 40 (once, despite the redelivered id) + 2 = two StockReceived rows, 42 units.
	if rows, qty := stockReceivedAnalyticsRows(t, env, goodSKU); rows != 2 || qty != 42 {
		t.Fatalf("Good SKU: %d StockReceived rows / %d units, want 2 / 42", rows, qty)
	}
	// Damaged and invalid lines leave no stock event behind.
	for _, sku := range []string{damagedSKU, invalidSKU} {
		if rows, _ := stockReceivedAnalyticsRows(t, env, sku); rows != 0 {
			t.Fatalf("%s: %d StockReceived rows, want 0", sku, rows)
		}
	}
	// The Good, Damaged and second Good ids are claimed once each; the invalid
	// line and the ReceiptOpened are never claimed.
	if n := processedReceiptEvents(t, env, goodID, damagedID, "ir-good2-"+run, "ir-bad-"+run, "ir-open-"+run); n != 3 {
		t.Fatalf("processed_events = %d, want 3", n)
	}
}

// inboundReceiptTestEnv boots this file's own Kafka broker (testcontainers,
// terminated when the test ends) next to the package's shared, fully-migrated
// Postgres pool.
func inboundReceiptTestEnv(t *testing.T, topic string) *transferTestEnv {
	t.Helper()
	ctx := context.Background()
	if transferSharedPool == nil {
		pool, err := bootSharedPostgresForTransfers(ctx, t)
		if err != nil {
			t.Fatalf("start postgres container: %v", err)
		}
		transferSharedPool = pool
	}
	broker, err := kafkatc.Start(ctx, "inbound-receipt-itest")
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() { _ = broker.Terminate() })
	createTransferTopic(t, broker.Addrs[0], topic, 2)
	return &transferTestEnv{pool: transferSharedPool, brokers: broker.Addrs, topic: topic}
}
