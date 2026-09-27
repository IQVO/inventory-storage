//go:build integration

package facilitycache_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/facilitycache"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// These tests drive the real replay/readiness logic against a REAL Kafka
// broker that the test itself starts, via testcontainers. Nothing external
// is assumed: no KAFKA_BROKERS, no localhost:9092, no cluster. That matters
// twice over here — CI's integration job provisions Postgres only and never
// runs Kafka, and pointing tests at the shared cluster broker is what
// causes consumer-group collisions with live deployments elsewhere in this
// fleet.
//
// One broker is started per test binary and shared across tests (containers
// are slow to boot); isolation between tests comes from each one using its
// own unique topic instead.

var (
	sharedBrokers   []string
	sharedContainer testcontainers.Container
)

// TestMain owns the package-wide broker lifecycle. Per-test Cleanup would
// tear it down after the first caller and defeat the intended single broker
// shared by the unique-topic test cases.
func TestMain(m *testing.M) {
	code := m.Run()
	if sharedContainer != nil {
		if err := testcontainers.TerminateContainer(sharedContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

// startBroker boots one Kafka container for the whole package and returns its
// broker list. Subsequent calls reuse it.
func startBroker(t *testing.T) []string {
	t.Helper()
	if sharedBrokers != nil {
		return sharedBrokers
	}

	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("facilitycache-itest"),
	)
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	sharedContainer = container

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve kafka brokers: %v", err)
	}
	sharedBrokers = brokers
	return brokers
}

// uniqueTopic gives each test its own topic so tests never contaminate one
// another through the shared broker.
func uniqueTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("warehouse.facility.events.itest-%d", time.Now().UnixNano())
}

// createTopic creates topic explicitly rather than relying on
// auto-creation, so a consumer constructed immediately afterwards can
// always read its partitions.
func createTopic(t *testing.T, brokerList []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokerList[0])
	if err != nil {
		t.Fatalf("dial %s: %v", brokerList[0], err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.CreateTopics(kafkago.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	// Topic creation is asynchronous on the broker: wait until the
	// partition leader is actually resolvable before returning, otherwise
	// the first read/write races it.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never became readable", topic)
}

func publish(t *testing.T, brokerList []string, topic string, msgs ...kafkago.Message) {
	t.Helper()
	w := &kafkago.Writer{
		Addr:     kafkago.TCP(brokerList...),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}
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

func envelopeBytes(t *testing.T, entity, eventName string, data any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	env := map[string]any{
		"event_id":    fmt.Sprintf("itest-%d", time.Now().UnixNano()),
		"event_type":  "com.warehouse.wms.facility-layout." + entity + "." + eventName,
		"occurred_at": time.Now().UTC(),
		"source":      "facility-layout",
		"data":        json.RawMessage(raw),
	}
	value, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return value
}

func zoneMsg(t *testing.T, zoneID, temperatureClass string, hazmat bool) kafkago.Message {
	t.Helper()
	return kafkago.Message{
		Key: []byte(zoneID),
		Value: envelopeBytes(t, "zone", "ZoneRegistered", map[string]any{
			"zoneId": zoneID, "temperatureClass": temperatureClass, "hazmat": hazmat,
		}),
	}
}

func slotMsg(t *testing.T, locationCode, zoneID string) kafkago.Message {
	t.Helper()
	return kafkago.Message{
		Key: []byte(locationCode),
		Value: envelopeBytes(t, "locationslot", "LocationSlotRegistered", map[string]any{
			"locationCode": locationCode, "zoneId": zoneID,
		}),
	}
}

// The core guarantee: a consumer started AFTER events already exist must
// replay the topic's full history into its own cache and only then report
// ready. This is what preserves the retired HTTP client's "never answer
// against incomplete data" property.
func TestConsumerReplaysExistingHistoryBeforeBecomingReady(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)

	publish(t, brokerList, topic,
		zoneMsg(t, "WH1-STOR-HAZ", "Ambient", true),
		slotMsg(t, "WH1-STOR-HAZ-A01-01-01-A", "WH1-STOR-HAZ"),
	)

	c := newConsumerOnTopic(t, brokerList, topic)
	defer func() { _ = c.Close() }()

	runInBackground(t, c)
	waitReady(t, c, facilitycache.WaitReadyTimeout)

	got := lookup(t, c, "WH1-STOR-HAZ-A01-01-01-A")
	want := product.SlotAttributes{Hazmat: true, TemperatureClass: product.Ambient, Known: true}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// The regression test for the bug this whole pattern exists to avoid: a
// SECOND consumer constructed after an earlier one already consumed (as on
// every service restart) must perform its OWN full replay, never resume
// from the previous instance's committed group offset and report ready
// with an empty cache.
func TestSecondConsumerAlsoReplaysFullHistory(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)

	publish(t, brokerList, topic,
		zoneMsg(t, "WH1-STOR-AMB", "Chilled", false),
		slotMsg(t, "WH1-STOR-AMB-A07-03-02-B", "WH1-STOR-AMB"),
	)

	first := newConsumerOnTopic(t, brokerList, topic)
	runInBackground(t, first)
	waitReady(t, first, facilitycache.WaitReadyTimeout)
	if first.Slots() != 1 {
		t.Fatalf("first consumer cached %d slots, want 1", first.Slots())
	}
	_ = first.Close()

	// Nothing new is published between the two consumers. A fixed shared
	// group id would make this second one resume past everything and come
	// up ready with an empty cache.
	second := newConsumerOnTopic(t, brokerList, topic)
	defer func() { _ = second.Close() }()
	runInBackground(t, second)
	waitReady(t, second, facilitycache.WaitReadyTimeout)

	if second.Slots() != 1 {
		t.Fatalf("second consumer cached %d slots, want 1 — it resumed from another instance's offset instead of replaying", second.Slots())
	}
	if second.Zones() != 1 {
		t.Fatalf("second consumer cached %d zones, want 1", second.Zones())
	}
	if got := lookup(t, second, "WH1-STOR-AMB-A07-03-02-B"); !got.Known || got.TemperatureClass != product.Chilled {
		t.Fatalf("got %+v, want a known Chilled slot", got)
	}
}

// An empty topic must not hang the readiness gate: there is nothing to
// catch up to, so the consumer is trivially ready and fails open — exactly
// as the HTTP client's 404 path did.
func TestEmptyTopicIsImmediatelyReady(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)

	c := newConsumerOnTopic(t, brokerList, topic)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected an empty topic to be immediately ready, got %v", err)
	}

	if got := lookup(t, c, "WH1-STOR-AMB-A07-03-02-B"); got.Known {
		t.Fatalf("expected fail-open Known=false against an empty cache, got %+v", got)
	}
}

// Live propagation: an event published AFTER the consumer is already ready
// must still be applied, with no restart. This is the freshness property
// the whole migration exists for.
func TestEventsPublishedAfterReadyAreStillApplied(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)

	publish(t, brokerList, topic, zoneMsg(t, "WH1-STOR-AMB", "Ambient", false))

	c := newConsumerOnTopic(t, brokerList, topic)
	defer func() { _ = c.Close() }()
	runInBackground(t, c)
	waitReady(t, c, facilitycache.WaitReadyTimeout)

	if got := lookup(t, c, "WH1-STOR-AMB-A07-03-02-B"); got.Known {
		t.Fatalf("slot should not be known yet, got %+v", got)
	}

	publish(t, brokerList, topic, slotMsg(t, "WH1-STOR-AMB-A07-03-02-B", "WH1-STOR-AMB"))

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if got := lookup(t, c, "WH1-STOR-AMB-A07-03-02-B"); got.Known {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("slot registered after readiness was never applied to the cache")
}

// A decommission published live must remove the slot from the cache, so a
// retired location stops classifying — the inverse of the test above.
func TestDecommissionPublishedAfterReadyRemovesTheSlot(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)

	publish(t, brokerList, topic,
		zoneMsg(t, "WH1-STOR-AMB", "Ambient", false),
		slotMsg(t, "WH1-STOR-AMB-A07-03-02-B", "WH1-STOR-AMB"),
	)

	c := newConsumerOnTopic(t, brokerList, topic)
	defer func() { _ = c.Close() }()
	runInBackground(t, c)
	waitReady(t, c, facilitycache.WaitReadyTimeout)

	if got := lookup(t, c, "WH1-STOR-AMB-A07-03-02-B"); !got.Known {
		t.Fatalf("slot should be known after replay, got %+v", got)
	}

	publish(t, brokerList, topic, kafkago.Message{
		Key: []byte("WH1-STOR-AMB-A07-03-02-B"),
		Value: envelopeBytes(t, "locationslot", "LocationSlotDecommissioned", map[string]any{
			"locationCode": "WH1-STOR-AMB-A07-03-02-B",
		}),
	})

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if got := lookup(t, c, "WH1-STOR-AMB-A07-03-02-B"); !got.Known {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("decommission published after readiness was never applied to the cache")
}

// newConsumerOnTopic builds a Consumer against an arbitrary topic. The
// production constructor pins facilitycache.Topic; the replay/readiness
// logic under test is identical either way.
func newConsumerOnTopic(t *testing.T, brokerList []string, topic string) *facilitycache.Consumer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := facilitycache.NewConsumerForTopic(ctx, brokerList, topic, nil)
	if err != nil {
		t.Fatalf("NewConsumerForTopic: %v", err)
	}
	return c
}

func runInBackground(t *testing.T, c *facilitycache.Consumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
}

func waitReady(t *testing.T, c *facilitycache.Consumer, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func lookup(t *testing.T, c *facilitycache.Consumer, binID string) product.SlotAttributes {
	t.Helper()
	id, err := shared.NewBinId(binID)
	if err != nil {
		t.Fatalf("NewBinId(%q): %v", binID, err)
	}
	got, err := c.GetSlotAttributes(context.Background(), id)
	if err != nil {
		t.Fatalf("GetSlotAttributes: %v", err)
	}
	return got
}

// TestConsumerToleratesAMissingTopic is the regression test for the
// CrashLoopBackOff this adapter caused on its first real rollout:
// constructing a consumer against a topic that does NOT EXIST must
// succeed, not error. Reproduced here against a real broker, because the
// failure came from the broker's actual UnknownTopicOrPartition
// response -- no unit test with a fake reader ever reaches that code
// path.
func TestConsumerToleratesAMissingTopic(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t) // deliberately NEVER created

	c := newConsumerOnTopic(t, brokerList, topic)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected a missing topic to be tolerated and immediately ready, got %v", err)
	}

	if got := lookup(t, c, "WH1-STOR-AMB-A07-03-02-B"); got.Known {
		t.Fatalf("expected fail-open Known=false against a missing topic, got %+v", got)
	}
}

// TestConsumer_MalformedMessage_GoesToDeadLetterTopicWithoutBlockingPartition
// is the ADR-0020 §DLQ acceptance test for this consumer's simpler (no
// retry, see dlqTopicSuffix's doc comment for why) DLQ path: a message
// apply() cannot parse — here, a ZoneRegistered event with an empty
// zoneId — is dead-lettered to topic+".dlq" with the raw payload and
// error headers, offsets still advance past it (readiness/the cache keep
// moving), and a well-formed message published right after is applied
// normally, proving the malformed one never blocked the partition.
func TestConsumer_MalformedMessage_GoesToDeadLetterTopicWithoutBlockingPartition(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	dlqTopic := topic + ".dlq"
	createTopic(t, brokerList, topic)
	createTopic(t, brokerList, dlqTopic)

	c := newConsumerOnTopic(t, brokerList, topic)
	defer func() { _ = c.Close() }()
	runInBackground(t, c)
	waitReady(t, c, facilitycache.WaitReadyTimeout)

	// Start reading the DLQ topic BEFORE publishing, so the malformed
	// message's eventual dead-letter write is never missed to a race.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokerList,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	poisonKey := fmt.Sprintf("poison-zone-%d", time.Now().UnixNano())
	publish(t, brokerList, topic, kafkago.Message{
		Key: []byte(poisonKey),
		Value: envelopeBytes(t, "zone", "ZoneRegistered", map[string]any{
			"zoneId": "", "temperatureClass": "Ambient", "hazmat": false,
		}),
	})

	dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Key) != poisonKey {
		t.Errorf("DLQ message key = %q, want %q (raw key preserved)", string(dlqMsg.Key), poisonKey)
	}
	if headerValue(dlqMsg.Headers, "x-dlq-source-topic") != topic {
		t.Errorf("DLQ message missing/incorrect x-dlq-source-topic header")
	}
	if headerValue(dlqMsg.Headers, "x-dlq-error") == "" {
		t.Error("DLQ message missing x-dlq-error header with failure context")
	}
	if headerValue(dlqMsg.Headers, "x-dlq-failed-at") == "" {
		t.Error("DLQ message missing x-dlq-failed-at header")
	}

	// A well-formed message published right after must still be applied
	// without delay — proving the malformed message never blocked the
	// partition.
	publish(t, brokerList, topic,
		zoneMsg(t, "WH1-STOR-AMB", "Ambient", false),
		slotMsg(t, "WH1-STOR-AMB-A07-03-02-B", "WH1-STOR-AMB"),
	)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if got := lookup(t, c, "WH1-STOR-AMB-A07-03-02-B"); got.Known {
			if got.TemperatureClass != product.Ambient {
				t.Fatalf("got %+v, want TemperatureClass=Ambient", got)
			}
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("well-formed message published after the poison one was never applied — partition appears blocked")
}

// headerValue returns the string value of the first header named key,
// or "" if absent.
func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
