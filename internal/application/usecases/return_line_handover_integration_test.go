//go:build integration

// returns-processing hand-over (ADR 0044) end-to-end proof on REAL
// infrastructure booted via testcontainers (its own Kafka broker plus the
// package's shared Postgres; never an external broker, never t.Skip):
//
//	returns-processing-shaped ReturnLineStaged on a Kafka topic
//	  -> ReturnLineConsumer -> BookReturnLine -> ReceiveStock
//	  -> a StockReceived row on the analytics topic in outbox_events
//	     (written in the same transaction as the processed_events claim),
//	     staged and NOT usable
//	  -> StowStock -> GET usable (GetUsable) reflects the quantity.
//
// A redelivered CloudEvents id, other event types and invalid payloads leave
// nothing behind.
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
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

// publishReturnEvent writes one CloudEvent exactly as returns-processing
// emits it (its source, the type given, key = subject = the disposition id).
func publishReturnEvent(t *testing.T, brokers []string, topic, id, eventType string, data map[string]any) {
	t.Helper()
	payload, _ := json.Marshal(data)
	value, _ := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/returns-processing",
		"type":            eventType,
		"subject":         "disp-itest",
		"time":            "2026-10-22T15:10:02Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:returns-processing:events:ReturnLineStaged:v1",
		"data":            json.RawMessage(payload),
	})
	w := kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}, BatchTimeout: 50 * time.Millisecond}
	defer func() { _ = w.Close() }()
	msg := kafkago.Message{
		Key:     []byte("disp-itest"),
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

func returnLinePayload(sku string, quantity int) map[string]any {
	return map[string]any{
		"receipt_id": "rcpt-itest", "line_no": 1, "sku": sku, "quantity": quantity,
		"condition": "Good", "received_at": "2026-10-22T14:05:00Z",
		"rma_id": "rma-itest", "disposition_id": "disp-itest", "site_id": "WH1",
	}
}

func processedReturnEvents(t *testing.T, env *transferTestEnv, ids ...string) int {
	t.Helper()
	var n int
	err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE consumer = $1 AND event_id = ANY($2)`,
		usecases.ReturnLineConsumerName, ids).Scan(&n)
	if err != nil {
		t.Fatalf("count processed_events: %v", err)
	}
	return n
}

func usableOf(t *testing.T, env *transferTestEnv, sku shared.SKU) int {
	t.Helper()
	usable, err := (&usecases.GetUsable{Stock: postgres.NewStockRepo(env.pool)}).Execute(context.Background(), sku)
	if err != nil {
		t.Fatalf("get usable %s: %v", sku, err)
	}
	return usable.Usable.Int()
}

func TestIntegration_ReturnLine_StagedThenUsableAfterStow(t *testing.T) {
	ctx := context.Background()
	run := uniqueRun()
	topic := fmt.Sprintf("%s.itest-%s", inboundkafka.ReturnsProcessingTopic, run)
	env := returnLineTestEnv(t, topic)

	goodSKU := mustSKU(t, "SKU-RET-GOOD-"+run)
	invalidSKU := "SKU-RET-BAD-" + run
	goodID, invalidID := "ret-good-"+run, "ret-bad-"+run
	returnType := "com.warehouse.wms.returns-processing.disposition.ReturnLineStaged"

	// A mix a real topic carries: an unrelated disposition type first, the
	// staged return line, an invalid quantity, and a redelivery of the SAME
	// CloudEvents id (at-least-once).
	publishReturnEvent(t, env.brokers, topic, "ret-decided-"+run, "com.warehouse.wms.returns-processing.disposition.DispositionDecided",
		map[string]any{"disposition_id": "disp-itest", "outcome": "REFURBISH"})
	publishReturnEvent(t, env.brokers, topic, goodID, returnType, returnLinePayload(goodSKU.String(), 2))
	publishReturnEvent(t, env.brokers, topic, invalidID, returnType, returnLinePayload(invalidSKU, 0))
	publishReturnEvent(t, env.brokers, topic, goodID, returnType, returnLinePayload(goodSKU.String(), 2))

	analytics := outboundkafka.NewAnalyticsPublisher(env.brokers, postgres.NewReservationRepo(env.pool), nil)
	t.Cleanup(func() { _ = analytics.Close() })
	// The Postgres pool is shared by the whole package and other tests drain
	// the WHOLE outbox through their own broker, which has no analytics
	// topic: leave none of this test's rows behind, pass or fail.
	t.Cleanup(func() {
		for _, sku := range []string{goodSKU.String(), invalidSKU} {
			if _, err := env.pool.Exec(context.Background(), `
				DELETE FROM outbox_events
				WHERE topic = $1 AND convert_from(value, 'UTF8')::jsonb -> 'data' ->> 'sku' = $2`,
				outboundkafka.AnalyticsTopic, sku); err != nil {
				t.Errorf("clean outbox rows of %s: %v", sku, err)
			}
		}
	})
	uow := postgres.NewUnitOfWork(env.pool)
	book := &usecases.BookReturnLine{
		Receive: &usecases.ReceiveStock{
			Events:     postgres.NewOutboxPublisher(env.pool, analytics),
			Clock:      memory.SystemClock{},
			UnitOfWork: uow,
		},
		ProcessedEvents: postgres.NewProcessedEventRepo(env.pool),
		UnitOfWork:      uow,
	}
	group := "return-line-itest-" + run
	consumer := inboundkafka.NewReturnLineConsumerForTopic(topic, env.brokers, group, book, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = consumer.Close() })

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		rows, _ := stockReceivedAnalyticsRows(t, env, goodSKU.String())
		if rows == 1 && groupLag(env, group) == 0 {
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

	// Staged stock row: exactly one StockReceived for 2 units, despite the
	// redelivered id.
	if rows, qty := stockReceivedAnalyticsRows(t, env, goodSKU.String()); rows != 1 || qty != 2 {
		t.Fatalf("Good SKU: %d StockReceived rows / %d units, want 1 / 2", rows, qty)
	}
	// The invalid line and the unrelated type leave no stock event behind.
	if rows, _ := stockReceivedAnalyticsRows(t, env, invalidSKU); rows != 0 {
		t.Fatalf("%s: %d StockReceived rows, want 0", invalidSKU, rows)
	}
	// Only the staged line's id is claimed; the invalid line and the
	// DispositionDecided are never claimed.
	if n := processedReturnEvents(t, env, goodID, invalidID, "ret-decided-"+run); n != 1 {
		t.Fatalf("processed_events = %d, want 1", n)
	}

	// Staged is NOT usable: the quantity is invisible to GET usable until
	// the RF stow (ADR 0044 decision 2).
	if usable := usableOf(t, env, goodSKU); usable != 0 {
		t.Fatalf("usable before stow = %d, want 0 (staged is not usable)", usable)
	}

	// The RF stow makes it usable — the same path a dock receipt's staged
	// quantity takes.
	bin := saveBin(t, env, "BIN-RET-"+run)
	stow := &usecases.StowStock{
		Stock:      postgres.NewStockRepo(env.pool),
		Locations:  postgres.NewLocationRepo(env.pool),
		Events:     events.NewBufferedPublisher(),
		Clock:      memory.SystemClock{},
		UnitOfWork: postgres.NewUnitOfWork(env.pool),
	}
	if _, err := stow.Execute(ctx, goodSKU, mustQty(t, 2), bin); err != nil {
		t.Fatalf("stow staged return line: %v", err)
	}
	if usable := usableOf(t, env, goodSKU); usable != 2 {
		t.Fatalf("usable after stow = %d, want 2", usable)
	}
}

// returnLineTestEnv boots this file's own Kafka broker (testcontainers,
// terminated when the test ends) next to the package's shared, fully-migrated
// Postgres pool.
func returnLineTestEnv(t *testing.T, topic string) *transferTestEnv {
	t.Helper()
	ctx := context.Background()
	if transferSharedPool == nil {
		pool, err := bootSharedPostgresForTransfers(ctx, t)
		if err != nil {
			t.Fatalf("start postgres container: %v", err)
		}
		transferSharedPool = pool
	}
	broker, err := kafkatc.Start(ctx, "return-line-itest")
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() { _ = broker.Terminate() })
	createTransferTopic(t, broker.Addrs[0], topic, 2)
	return &transferTestEnv{pool: transferSharedPool, brokers: broker.Addrs, topic: topic}
}
