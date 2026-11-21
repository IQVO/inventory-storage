//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

// The real entry point of the analytics WRITER (ADR-0011) against real
// infrastructure, both booted by this test via testcontainers (never an
// external KAFKA_BROKERS/ANALYTICS_DATABASE_URL, never t.Skip):
//
//	the repo's own AnalyticsPublisher encoder building the CloudEvents
//	  -> a real Kafka broker (kafkatc, group-coordinator-ready)
//	  -> run(ctx): env -> OWN analytics migrations on an EMPTY Postgres ->
//	     /readyz (postgres + kafka answering) -> AnalyticsConsumer projects
//	  -> the Inventory Flow & Accuracy rollup, asserted on the fact table
//	  -> a cancelled context drains to a clean exit (nil error).
//
// The projector consumes exactly one input topic, warehouse.inventory.analytics
// (cmd/inventory-projector/main.go wires the consumer to
// outboundkafka.AnalyticsTopic only), so every event lands on that topic.
var (
	projItBroker *kafkatc.Broker
	projItPG     testcontainers.Container
)

func TestMain(m *testing.M) {
	code := m.Run()
	if projItBroker != nil {
		if err := projItBroker.Terminate(); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	if projItPG != nil {
		if err := testcontainers.TerminateContainer(projItPG); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

// projItFreeAddr reserves an ephemeral localhost port for the admin server.
func projItFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// projItWaitUntil polls cond until it holds or the 90s window closes.
func projItWaitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// projItMigrationsDir resolves the repo's own analytics migrations relative to
// this file, so run()'s ANALYTICS_MIGRATIONS_PATH works regardless of the
// directory go test was invoked from.
func projItMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve the test file's path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}

// projItCreateTopic creates topic and waits until its single partition has a
// leader: a group consumer that starts before its topic exists never picks the
// topic up (same finding as the e2e suite), so the topic must exist first.
func projItCreateTopic(t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial %s: %v", broker, err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	projItWaitUntil(t, "topic "+topic+" to have a leader", func() bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) == 1
	})
}

// projItPublish writes msgs onto topic, retrying briefly: a freshly created
// topic's metadata can lag on the broker the writer first dials.
func projItPublish(t *testing.T, brokers []string, topic string, msgs ...kafkago.Message) {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}, BatchTimeout: 50 * time.Millisecond}
	defer func() { _ = w.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		if err = w.WriteMessages(ctx, msgs...); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("publish to %s: %v", topic, err)
}

// TestProjectorBootsMigratesProjectsAndStopsCleanly proves the documented
// writer lifecycle end to end (ADR-0011, docs/docs/analytics):
// boot on an EMPTY analytical Postgres running its OWN migrations, answer
// /readyz only once Postgres and Kafka answer, project the repo's own encoder
// events into the flow-and-accuracy rollup, and drain cleanly on cancel.
func TestProjectorBootsMigratesProjectsAndStopsCleanly(t *testing.T) {
	ctx := context.Background()

	// An EMPTY analytical Postgres: no migrations are applied here — the
	// projector owns the schema and run() must apply it itself.
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_analytics"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	projItPG = pg
	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}

	broker, err := kafkatc.Start(ctx, "inv-projector-itest")
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	projItBroker = broker
	brokers := broker.Addrs
	projItCreateTopic(t, brokers[0], outboundkafka.AnalyticsTopic)

	// The repo's OWN encoder builds the analytics CloudEvents byte-for-byte
	// as cmd/inventory publishes them; the three fixtures cover all three row
	// key shapes of the documented grain (sku-only, sku+bin, bin-only).
	skuA, skuB, binA, binB := mustProjSKU(t, "SKU-IT-PROJ-A"), mustProjSKU(t, "SKU-IT-PROJ-B"),
		mustProjBin(t, "BIN-IT-PROJ-A"), mustProjBin(t, "BIN-IT-PROJ-B")
	qty7, qty3, counted, system := mustProjQty(t, 7), mustProjQty(t, 3), mustProjQty(t, 5), mustProjQty(t, 4)
	at := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	encoder := &outboundkafka.AnalyticsPublisher{}
	events := []shared.DomainEvent{
		shared.NewStockReceived(at, skuA, qty7),
		shared.NewItemStowed(at.Add(time.Hour), skuB, binA, qty3),
		shared.NewCycleCountCompleted(at.Add(2*time.Hour), binB, counted, system, true),
	}
	msgs := make([]kafkago.Message, 0, len(events))
	for _, ev := range events {
		encoded, err := encoder.Encode(ctx, ev)
		if err != nil {
			t.Fatalf("encode %s: %v", ev.EventName(), err)
		}
		if len(encoded) != 1 {
			t.Fatalf("expected one analytics message for %s, got %d", ev.EventName(), len(encoded))
		}
		msgs = append(msgs, kafkago.Message{Key: encoded[0].Key, Value: encoded[0].Value, Headers: encoded[0].Headers})
	}
	projItPublish(t, brokers, outboundkafka.AnalyticsTopic, msgs...)

	addr := projItFreeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("KAFKA_BROKERS", strings.Join(brokers, ","))
	t.Setenv("ADMIN_ADDR", addr)
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", projItMigrationsDir(t))
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a dial
	// at shutdown (same convention as the cmd/inventory e2e suite).
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- run(runCtx) }()

	projItWaitUntil(t, "/readyz = 200 on the migrated analytical Postgres and the real Kafka", func() bool {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// The documented projection: one rollup row per key shape, with the
	// summed quantity (received) and the event counts (stowed, cycle count).
	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	projItWaitUntil(t, "the StockReceived, ItemStowed and CycleCountCompleted events projected", func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM flow_accuracy_rollup WHERE sku = 'SKU-IT-PROJ-A' AND bin_id = '' AND hour_bucket = $1 AND received_quantity = 7)
			+ (SELECT count(*) FROM flow_accuracy_rollup WHERE sku = 'SKU-IT-PROJ-B' AND bin_id = 'BIN-IT-PROJ-A' AND hour_bucket = $2 AND stowed_count = 1)
			+ (SELECT count(*) FROM flow_accuracy_rollup WHERE sku = '' AND bin_id = 'BIN-IT-PROJ-B' AND hour_bucket = $3 AND cycle_counts_completed = 1)`,
			at, at.Add(time.Hour), at.Add(2*time.Hour)).Scan(&n)
		return err == nil && n == 3
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a cancelled projector must drain cleanly, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the projector did not stop")
	}
}

func mustProjSKU(t *testing.T, v string) shared.SKU {
	t.Helper()
	s, err := shared.NewSKU(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustProjBin(t *testing.T, v string) shared.BinId {
	t.Helper()
	b, err := shared.NewBinId(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustProjQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
