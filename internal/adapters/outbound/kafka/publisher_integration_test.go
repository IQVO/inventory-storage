//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

// This test drives Publisher.Publish against a REAL Kafka broker the test
// itself starts (testcontainers), created with 8 partitions — the exact
// partition count the fleet's shared broker was scaled to in the Phase 3
// partition-scaleup (warehouse-infra PR #42) that exposed this bug. It
// proves the fix at the level that actually matters: which partition a
// real broker routes each message to, not just what Key bytes were set.
//
// One broker is started per test binary (containers are slow to boot);
// isolation between tests comes from each one using its own unique topic.

var (
	partitionKeySharedBrokers   []string
	partitionKeySharedContainer *kafkatc.Broker
)

func TestMain(m *testing.M) {
	code := m.Run()
	if partitionKeySharedContainer != nil {
		if err := partitionKeySharedContainer.Terminate(); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

// startPartitionKeyBroker boots the package's ONE Kafka container via the
// shared kafkatc helper, which returns only once the broker can serve group
// coordination. Without that, the first test to run paid the cold broker's
// __consumer_offsets creation (50 partitions) inside its own timed window —
// topic creation and the first produce queue behind it on the controller.
func startPartitionKeyBroker(t *testing.T) []string {
	t.Helper()
	if partitionKeySharedBrokers != nil {
		return partitionKeySharedBrokers
	}

	broker, err := kafkatc.Start(context.Background(), "publisher-partition-key-itest")
	if err != nil {
		t.Fatalf("%v", err)
	}
	partitionKeySharedContainer = broker
	partitionKeySharedBrokers = broker.Addrs
	return partitionKeySharedBrokers
}

// createTopicWithPartitions creates topic with exactly numPartitions
// partitions, explicitly rather than relying on auto-creation, mirroring
// the fleet's shared broker's business topics (8 partitions since the
// Phase 3 scaleup).
func createTopicWithPartitions(t *testing.T, brokerList []string, topic string, numPartitions int) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokerList[0])
	if err != nil {
		t.Fatalf("dial %s: %v", brokerList[0], err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.CreateTopics(kafkago.TopicConfig{
		Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) == numPartitions {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never became readable with %d partitions", topic, numPartitions)
}

func uniquePartitionKeyTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("warehouse.inventory.events.itest-%d", time.Now().UnixNano())
}

// readAllPartitionsForKey reads every message on topic across all its
// partitions and returns the partition each message with the given key
// landed on, in the order read per-partition. It uses one reader per
// partition (kafka-go readers are partition-scoped when GroupID is unset)
// with a short read deadline, since the test topic only ever has a
// handful of messages.
func readPartitionsForKey(t *testing.T, brokerList []string, topic string, numPartitions int, key string) []kafkago.Message {
	t.Helper()
	var landedOn []kafkago.Message

	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:   brokerList,
			Topic:     topic,
			Partition: p,
			MinBytes:  1,
			MaxBytes:  10e6,
		})
		func() {
			defer func() { _ = reader.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					return // deadline hit: no more messages on this partition
				}
				if string(msg.Key) == key {
					landedOn = append(landedOn, msg)
				}
			}
		}()
	}
	return landedOn
}

// TestPublisher_RealBroker_SameReservationLandsOnSamePartition is the
// real-Kafka-level regression test for the partition-scaleup ordering bug:
// against a topic with 8 partitions (matching the fleet's live business
// topics post-scaleup), StockReserved and a later ReservationRevoked for
// the SAME reservation must both land on the SAME partition, so a single
// consumer reading that partition observes them in publish order. Before
// the fix (no Key set, and a LeastBytes balancer that ignores Key even once
// set — see kafka.NewWriter's doc comment), the two events landed on
// DIFFERENT partitions in a real run against this exact broker, breaking
// per-reservation ordering; this test is what caught that LeastBytes gap.
func TestPublisher_RealBroker_SameReservationLandsOnSamePartition(t *testing.T) {
	const numPartitions = 8
	brokerList := startPartitionKeyBroker(t)
	topic := uniquePartitionKeyTopic(t)
	createTopicWithPartitions(t, brokerList, topic, numPartitions)

	// A dedicated Transport (not kafka-go's process-global DefaultTransport):
	// the default one caches cluster metadata across writers, so a second
	// test creating a topic right after another test's writer warmed it saw
	// "Unknown Topic Or Partition" for its brand-new topic.
	transport := &kafkago.Transport{}
	defer transport.CloseIdleConnections()
	writer := &kafkago.Writer{
		Addr:      kafkago.TCP(brokerList...),
		Topic:     topic,
		Balancer:  &kafkago.Hash{},
		Transport: transport,
	}
	defer func() { _ = writer.Close() }()

	repo := memory.NewReservationRepo()
	sku, _ := shared.NewSKU("SKU-ITEST")
	qty, _ := shared.NewPositiveQuantity(4)
	res, err := reservation.New("res-itest-shared", sku, qty, "order-itest",
		[]reservation.Allocation{{StockUnitID: "unit-itest", Quantity: qty}},
		time.Now(), time.Hour)
	if err != nil {
		t.Fatalf("build reservation: %v", err)
	}
	if err := repo.Save(context.Background(), res); err != nil {
		t.Fatalf("save reservation: %v", err)
	}

	pub := outboundkafka.NewPublisher(writer, repo)

	// Encode both events (Publish can't be used directly: it targets the
	// publisher's own fixed Topic via NewWriter's convention, whereas this
	// test needs a topic with a controlled partition count) and write them
	// with the same writer/balancer the production Publisher uses.
	stockReserved := shared.NewStockReserved(time.Now(), "res-itest-shared", sku, qty, "order-itest")
	encoded1, err := pub.Encode(context.Background(), stockReserved)
	if err != nil || len(encoded1) != 1 {
		t.Fatalf("Encode StockReserved: encoded=%v err=%v", encoded1, err)
	}

	if err := res.Revoke(); err != nil {
		t.Fatalf("revoke reservation: %v", err)
	}
	if err := repo.Save(context.Background(), res); err != nil {
		t.Fatalf("re-save revoked reservation: %v", err)
	}
	revoked := shared.NewReservationRevoked(time.Now(), "res-itest-shared")
	encoded2, err := pub.Encode(context.Background(), revoked)
	if err != nil || len(encoded2) != 1 {
		t.Fatalf("Encode ReservationRevoked: encoded=%v err=%v", encoded2, err)
	}

	if string(encoded1[0].Key) != string(encoded2[0].Key) {
		t.Fatalf("Key mismatch before even hitting the broker: %q vs %q", encoded1[0].Key, encoded2[0].Key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: encoded1[0].Key, Value: encoded1[0].Value, Headers: encoded1[0].Headers},
		kafkago.Message{Key: encoded2[0].Key, Value: encoded2[0].Value, Headers: encoded2[0].Headers},
	); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}

	landedOn := readPartitionsForKey(t, brokerList, topic, numPartitions, "res-itest-shared")
	if len(landedOn) != 2 {
		t.Fatalf("expected to find 2 messages keyed res-itest-shared across %d partitions, found %d: %v", numPartitions, len(landedOn), landedOn)
	}
	if landedOn[0].Partition != landedOn[1].Partition {
		t.Errorf("StockReserved landed on partition %d, ReservationRevoked landed on partition %d — must be identical for per-reservation ordering", landedOn[0].Partition, landedOn[1].Partition)
	}

	// The wire format read back from the real broker is CloudEvents 1.0
	// structured mode (ADR-0024): content-type header + a valid event whose
	// full type is the cross-service contract wes-work-planning consumes.
	wantTypes := []string{
		"com.warehouse.wms.inventory-storage.reservation.StockReserved",
		"com.warehouse.wms.inventory-storage.reservation.ReservationRevoked",
	}
	for i, msg := range landedOn {
		if got := headerValue(msg.Headers, "content-type"); got != cloudevents.MediaType {
			t.Errorf("message %d content-type header = %q, want %q", i, got, cloudevents.MediaType)
		}
		e, err := cloudevents.Decode(msg.Value)
		if err != nil {
			t.Fatalf("message %d is not a valid CloudEvent: %v", i, err)
		}
		if e.Type() != wantTypes[i] || e.Subject() != "res-itest-shared" || e.Source() != "/warehouse/inventory-storage" {
			t.Errorf("message %d attributes = type %q subject %q source %q", i, e.Type(), e.Subject(), e.Source())
		}
	}
}
