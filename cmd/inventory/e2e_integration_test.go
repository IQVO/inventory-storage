//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/facilitycache"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
)

// The end-to-end proof of the real-wiring rule: the REAL entry point,
// run(ctx), booted in-process with only the deployment's environment against
// a real Postgres and a real Kafka (testcontainers), driven ONLY through REST
// and Kafka. Assertions are on Postgres rows and on the CloudEvents the relay
// publishes. Every Kafka read is bounded by a deadline.
//
// Nothing is stubbed: LOCATION_LOOKUP_MODE=kafka (the deployment's setting)
// reads the facility topic from the same broker.

const (
	e2eSKU = "SKU-E2E"
	// e2eStockReceivedType is the CloudEvents type of StockReceived.
	e2eStockReceivedType = "com.warehouse.wms.inventory-storage.stock.StockReceived"
	// e2eTaskCompletedType is what fulfillment-execution publishes.
	e2eTaskCompletedType = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
)

type e2eEnv struct {
	dsn       string
	brokers   []string
	addr      string
	kafkaCont testcontainers.Container
}

// consumedTopics are the topics the always-on consumers read. A kafka-go
// group consumer that starts before its topic exists does not pick the topic
// up when it appears (same finding as inbound-receiving ADR 0011), so the
// test creates them like the cluster does. The OUTPUT topics are NOT
// created: the relay's writer auto-creates them, as in the cluster.
var consumedTopics = []string{
	facilitycache.Topic,
	inboundkafka.Topic,
	inboundkafka.ProductMasterTopic,
	inboundkafka.InboundReceivingTopic,
	inboundkafka.FulfillmentTopic,
}

func startE2EInfra(t *testing.T, ctx context.Context) e2eEnv {
	t.Helper()
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_storage"), tcpostgres.WithUsername("inv"), tcpostgres.WithPassword("inv"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID(fmt.Sprintf("inv-e2e-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kc) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	for _, topic := range consumedTopics {
		e2eCreateTopic(t, ctx, brokers[0], topic)
	}
	return e2eEnv{dsn: dsn, brokers: brokers, addr: e2eFreeAddr(t), kafkaCont: kc}
}

func e2eFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func e2eCreateTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	e2eWaitFor(t, ctx, 30*time.Second, "topic "+topic+" has a leader", func() bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) == 1 && parts[0].Leader.Host != ""
	})
}

func e2eWaitFor(t *testing.T, ctx context.Context, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

// startE2EService runs the REAL entry point in-process and returns a stop func
// that cancels it and returns run()'s result.
func startE2EService(t *testing.T, ctx context.Context, env e2eEnv) func() (error, error) {
	t.Helper()
	t.Setenv("DATABASE_URL", env.dsn)
	t.Setenv("MIGRATIONS_DATABASE_URL", "")
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	t.Setenv("KAFKA_BROKERS", strings.Join(env.brokers, ","))
	t.Setenv("HTTP_ADDR", env.addr)
	// The deployment's values (warehouse-infra helm-values/inventory-storage.yaml).
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("LOCATION_LOOKUP_MODE", "kafka")
	t.Setenv("TRANSFER_ALLOCATION_CONSUMER_MODE", "kafka")
	t.Setenv("TASK_COMPLETED_CONSUMER_MODE", "kafka")
	t.Setenv("TRANSFER_ALLOCATION_CONSUMER_GROUP", "inventory-storage-e2e-transfer")
	t.Setenv("PRODUCT_MASTER_CONSUMER_GROUP", "inventory-storage-e2e-product-master")
	t.Setenv("INBOUND_RECEIPT_CONSUMER_GROUP", "inventory-storage-e2e-receipts")
	t.Setenv("RETURN_LINE_CONSUMER_GROUP", "inventory-storage-e2e-return-lines")
	t.Setenv("TASK_COMPLETED_CONSUMER_GROUP", "inventory-storage-e2e-confirm-pick")
	t.Setenv("OUTBOX_RELAY_INTERVAL", "200ms")
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a dial
	// to localhost:4317 at shutdown.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- run(runCtx) }()
	stopped := false
	stop := func() (error, error) {
		if stopped {
			return nil, nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err, nil
		case <-time.After(40 * time.Second):
			return nil, errors.New("run() did not return within 40s of cancellation")
		}
	}
	t.Cleanup(func() { _, _ = stop() })

	e2eWaitFor(t, ctx, 120*time.Second, "GET /readyz = 200 on real Postgres + Kafka", func() bool {
		code, _ := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "", nil)
		return code == http.StatusOK
	})
	return stop
}

func (e e2eEnv) url(path string) string { return "http://" + e.addr + path }

// e2eDo issues a REST call and returns status and body. A transport error
// returns 0 (the service may still be starting).
func e2eDo(t *testing.T, ctx context.Context, method, url, body string, headers map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func e2eCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// e2eEvent is one message read from an output topic.
type e2eEvent struct {
	ce.Event
	Key string
}

// e2eEventsOn reads topic from the beginning, in a loop bounded by a deadline
// (the relay auto-creates the topic on its first write, so it may not exist
// yet), until every wanted "<type>|<key>" was seen.
func e2eEventsOn(t *testing.T, ctx context.Context, brokers []string, topic string, want ...string) map[string]e2eEvent {
	t.Helper()
	got := map[string]e2eEvent{}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MaxWait: 500 * time.Millisecond})
		if err := r.SetOffset(kafkago.FirstOffset); err == nil {
			readCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			for {
				m, err := r.ReadMessage(readCtx)
				if err != nil {
					break
				}
				var ev ce.Event
				if json.Unmarshal(m.Value, &ev) == nil {
					got[ev.Type()+"|"+string(m.Key)] = e2eEvent{Event: ev, Key: string(m.Key)}
				}
			}
			cancel()
		}
		_ = r.Close()
		missing := false
		for _, w := range want {
			if _, ok := got[w]; !ok {
				missing = true
			}
		}
		if !missing {
			return got
		}
		time.Sleep(500 * time.Millisecond)
	}
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	t.Fatalf("timed out reading %s: wanted %v, saw %v", topic, want, keys)
	return nil
}

func TestRunOnRealPostgresAndKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	env := startE2EInfra(t, ctx)
	stop := startE2EService(t, ctx, env)
	pool, err := pgxpool.New(ctx, env.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	t.Run("readyz is 200 only because Postgres and Kafka answer", func(t *testing.T) {
		code, body := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "", nil)
		if code != http.StatusOK || !strings.Contains(body, `"ready"`) {
			t.Fatalf("readyz = %d %s", code, body)
		}
		if code, _ := e2eDo(t, ctx, http.MethodGet, env.url("/healthz"), "", nil); code != http.StatusOK {
			t.Fatalf("healthz = %d", code)
		}
	})

	t.Run("a REST write is idempotent and its CloudEvent is published through the outbox", func(t *testing.T) {
		e2eReceiveFlow(t, ctx, env, pool)
	})

	t.Run("the task-completed consumer is running: a poison TaskCompleted is dead-lettered", func(t *testing.T) {
		e2ePoisonFlow(t, ctx, env)
	})

	t.Run("readyz goes 503 when the broker goes away, and shutdown still completes", func(t *testing.T) {
		timeout := 10 * time.Second
		if err := env.kafkaCont.Stop(ctx, &timeout); err != nil {
			t.Fatalf("stop kafka: %v", err)
		}
		e2eWaitFor(t, ctx, 30*time.Second, "GET /readyz = 503 dependency-unavailable (kafka)", func() bool {
			code, body := e2eDo(t, ctx, http.MethodGet, env.url("/readyz"), "", nil)
			return code == http.StatusServiceUnavailable && strings.Contains(body, "dependency-unavailable") && strings.Contains(body, "kafka")
		})
		// Liveness is unaffected by a dependency.
		if code, _ := e2eDo(t, ctx, http.MethodGet, env.url("/healthz"), "", nil); code != http.StatusOK {
			t.Fatalf("healthz with the broker down = %d, want 200", code)
		}
		start := time.Now()
		runErr, err := stop()
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 35*time.Second {
			t.Fatalf("shutdown with the broker down took %s", d)
		}
		t.Logf("run() returned %v after %s", runErr, time.Since(start))
	})
}

func e2eReceiveFlow(t *testing.T, ctx context.Context, env e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	receive := env.url("/stock/receive")
	body := fmt.Sprintf(`{"sku":%q,"quantity":3}`, e2eSKU)
	key := map[string]string{"Idempotency-Key": "e2e-receive-1"}

	// Postgres is the only runtime: the key is enforced, a NUL never reaches
	// Postgres, and neither refusal leaves an outbox row.
	if code, resp := e2eDo(t, ctx, http.MethodPost, receive, body, nil); code != http.StatusBadRequest || !strings.Contains(resp, "idempotency-key-required") {
		t.Fatalf("receive without a key = %d %s, want 400 idempotency-key-required", code, resp)
	}
	if code, resp := e2eDo(t, ctx, http.MethodPost, receive, "{\"sku\":\"A\x00B\",\"quantity\":1}", map[string]string{"Idempotency-Key": "e2e-nul"}); code != http.StatusBadRequest {
		t.Fatalf("receive with a NUL = %d %s, want 400", code, resp)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events`); n != 0 {
		t.Fatalf("outbox rows after two refusals = %d, want 0", n)
	}

	code, first := e2eDo(t, ctx, http.MethodPost, receive, body, key)
	if code != http.StatusAccepted {
		t.Fatalf("receive = %d %s, want 202", code, first)
	}
	// One StockReceived, one outbox row: StockReceived has a wire form on the
	// analytics topic only (the integration topic carries no StockReceived).
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE topic = $1`, outboundkafka.AnalyticsTopic); n != 1 {
		t.Fatalf("analytics outbox rows after one receive = %d, want 1", n)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events`); n != 1 {
		t.Fatalf("outbox rows after one receive = %d, want exactly 1", n)
	}

	// A replay returns the stored response and enqueues nothing; the same key
	// with another body is refused.
	if code, replay := e2eDo(t, ctx, http.MethodPost, receive, body, key); code != http.StatusAccepted || replay != first {
		t.Fatalf("replay = %d %s, want 202 and the original body %s", code, replay, first)
	}
	if code, resp := e2eDo(t, ctx, http.MethodPost, receive, fmt.Sprintf(`{"sku":%q,"quantity":4}`, e2eSKU), key); code != http.StatusUnprocessableEntity || !strings.Contains(resp, "idempotency-key-reused") {
		t.Fatalf("same key, other body = %d %s, want 422 idempotency-key-reused", code, resp)
	}
	if n := e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events`); n != 1 {
		t.Fatalf("outbox rows after a replay = %d, want still 1", n)
	}

	e2eWaitFor(t, ctx, 30*time.Second, "the outbox drains", func() bool {
		return e2eCount(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})

	want := e2eStockReceivedType + "|" + e2eSKU
	events := e2eEventsOn(t, ctx, env.brokers, outboundkafka.AnalyticsTopic, want)
	ev := events[want]
	if ev.SpecVersion() != "1.0" || ev.Type() != e2eStockReceivedType || ev.Key != e2eSKU || ev.ID() == "" {
		t.Fatalf("StockReceived envelope = specversion %q type %q key %q id %q", ev.SpecVersion(), ev.Type(), ev.Key, ev.ID())
	}
	var data struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	}
	if err := json.Unmarshal(ev.Data(), &data); err != nil || data.SKU != e2eSKU || data.Quantity != 3 {
		t.Fatalf("StockReceived data = %s (%v)", ev.Data(), err)
	}
}

// e2ePoisonFlow publishes a TaskCompleted CloudEvent whose payload has the
// wrong shape (it can never be handled) to the fulfillment topic and waits for
// the always-on consumer to dead-letter it. Reaching the DLQ proves the
// consumer is running against the real broker.
func e2ePoisonFlow(t *testing.T, ctx context.Context, env e2eEnv) {
	t.Helper()
	ev := ce.New()
	ev.SetSpecVersion("1.0")
	ev.SetID("e2e-poison-1")
	ev.SetType(e2eTaskCompletedType)
	ev.SetSource("/warehouse/fulfillment-execution")
	if err := ev.SetData("application/json", map[string]any{"task_id": 42, "task_type": []string{"PICK"}}); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	w := &kafkago.Writer{Addr: kafkago.TCP(env.brokers...), Topic: inboundkafka.FulfillmentTopic, RequiredAcks: kafkago.RequireAll}
	if err := w.WriteMessages(ctx, kafkago.Message{Key: []byte("t-e2e"), Value: raw}); err != nil {
		t.Fatalf("publish TaskCompleted: %v", err)
	}
	_ = w.Close()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: env.brokers, Topic: inboundkafka.FulfillmentTopic + ".dlq", Partition: 0, MaxWait: 500 * time.Millisecond})
		if err := r.SetOffset(kafkago.FirstOffset); err == nil {
			readCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			m, err := r.ReadMessage(readCtx)
			cancel()
			_ = r.Close()
			if err == nil {
				if string(m.Value) != string(raw) {
					t.Fatalf("DLQ value = %s, want the original message", m.Value)
				}
				return
			}
		} else {
			_ = r.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("the poison TaskCompleted never reached the dead-letter topic")
}
