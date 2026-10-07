//go:build integration

package kafkatc_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

// TestStart_ReturnsAGroupCoordinatorReadyBroker is the contract the helper
// exists for: the instant Start returns, a consumer-group request must
// already be answered with a coordinator (no [15]
// GroupCoordinatorNotAvailable), and a real group consumer must be able to
// join and read — so no test ever pays the cold broker's __consumer_offsets
// creation, or kafka-go's fixed 5 s JoinGroupBackoff, inside its own window.
func TestStart_ReturnsAGroupCoordinatorReadyBroker(t *testing.T) {
	broker, err := kafkatc.Start(context.Background(), "kafkatc-itest")
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() {
		if err := broker.Terminate(); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})
	if len(broker.Addrs) == 0 {
		t.Fatal("Start returned no broker addresses")
	}

	// A FRESH group key, one single un-retried request: it must succeed now.
	// (Dedicated transport: see WaitForGroupCoordinator.)
	transport := &kafkago.Transport{}
	defer transport.CloseIdleConnections()
	client := &kafkago.Client{Addr: kafkago.TCP(broker.Addrs[0]), Timeout: 10 * time.Second, Transport: transport}
	resp, err := client.FindCoordinator(context.Background(), &kafkago.FindCoordinatorRequest{
		Addr: kafkago.TCP(broker.Addrs[0]), Key: fmt.Sprintf("kafkatc-fresh-%d", time.Now().UnixNano()),
		KeyType: kafkago.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		t.Fatalf("FindCoordinator right after Start: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("FindCoordinator right after Start answered %v — the coordinator was not ready", resp.Error)
	}

	// And a real consumer group joins and reads end to end.
	topic := fmt.Sprintf("kafkatc.itest-%d", time.Now().UnixNano())
	createTopic(t, broker.Addrs[0], topic)
	w := &kafkago.Writer{Addr: kafkago.TCP(broker.Addrs...), Topic: topic}
	defer func() { _ = w.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kafkago.Message{Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("produce: %v", err)
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: broker.Addrs, Topic: topic, GroupID: fmt.Sprintf("kafkatc-itest-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = r.Close() }()
	msg, err := r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("group consumer read: %v", err)
	}
	if string(msg.Value) != "v" {
		t.Fatalf("read %q, want %q", msg.Value, "v")
	}
}

// createTopic creates a 1-partition topic explicitly (the fleet's tests never
// rely on auto-creation) and waits until its partition is readable.
func createTopic(t *testing.T, addr, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never became readable", topic)
}
