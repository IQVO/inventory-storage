//go:build integration

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

// parker parks a use-case call until the handler's context is cancelled, then
// reports that cancellation -- exactly what the Postgres-backed use cases do
// when the pod is told to stop mid-transaction.
type parker struct{ entered chan struct{} }

func newParker() parker { return parker{entered: make(chan struct{}, 1)} }

func (p parker) park(ctx context.Context) error {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

type parkingConfirmer struct{ parker }

func (p parkingConfirmer) Execute(ctx context.Context, _ usecases.PickCompletion) (usecases.ConfirmPicksResult, error) {
	return usecases.ConfirmPicksResult{}, p.park(ctx)
}

type parkingApplier struct{ parker }

func (p parkingApplier) Execute(ctx context.Context, _ usecases.ProductClassificationUpdate) (usecases.ApplyOutcome, error) {
	return "", p.park(ctx)
}

type parkingBooker struct{ parker }

func (p parkingBooker) Execute(ctx context.Context, _ usecases.InboundReceiptLine) (usecases.BookOutcome, error) {
	return "", p.park(ctx)
}

type parkingAllocator struct{ parker }

func (p parkingAllocator) Execute(ctx context.Context, _ usecases.TransferCommand) (*usecases.Result, error) {
	return nil, p.park(ctx)
}

// shutdownLogs is a goroutine-safe log sink.
type shutdownLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *shutdownLogs) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *shutdownLogs) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type runCloser interface {
	Run(ctx context.Context) error
	Close() error
}

// TestConsumers_ShutdownMidHandle_IsCleanAndDeadLettersNothing pins the
// shutdown contract for every retrying inbound consumer: cancelling Run's
// context while a healthy message is being handled is a clean stop (Run
// returns nil, not context.Canceled), and that message is NOT dead-lettered
// -- its offset stays uncommitted, so it is redelivered on the next start and
// the use case's processed-event claim absorbs the replay. Before the fix the
// retry loops returned ctx.Err() from Run, and TaskCompletedConsumer routed a
// cancellation that landed on its last attempt into the dead-letter path as
// if the message were poison.
func TestConsumers_ShutdownMidHandle_IsCleanAndDeadLettersNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	broker, err := kafkatc.Start(ctx, fmt.Sprintf("is-shutdown-itest-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("start Kafka: %v", err)
	}
	t.Cleanup(func() { _ = broker.Terminate() })
	brokers := broker.Addrs

	t.Run("TaskCompletedConsumer", func(t *testing.T) {
		p := newParker()
		topic := shutdownItTopic("warehouse.fulfillment.events")
		id := fmt.Sprintf("ce-task-%d", time.Now().UnixNano())
		assertShutdownMidHandleIsClean(t, ctx, brokers, topic, p,
			kafkago.Message{Key: []byte(id), Value: taskCompletedCE(t, id, pickData("ord-shutdown-1"))},
			func(logger *slog.Logger) runCloser {
				c := NewTaskCompletedConsumerForTopic(topic, brokers, shutdownItGroup(), parkingConfirmer{p}, logger)
				// One attempt: the cancellation lands on the LAST attempt, the
				// exact case that used to be dead-lettered as "gave up".
				c.MaxAttempts = 1
				return c
			})
	})

	t.Run("ProductMasterConsumer", func(t *testing.T) {
		p := newParker()
		topic := shutdownItTopic("warehouse.product-master.events")
		id := fmt.Sprintf("ce-pm-%d", time.Now().UnixNano())
		assertShutdownMidHandleIsClean(t, ctx, brokers, topic, p,
			kafkago.Message{Key: []byte(id), Value: productMasterCE(t, id, typeProductMasterClassified, classifiedPayload(1, "Fragile"))},
			func(logger *slog.Logger) runCloser {
				return NewProductMasterConsumerForTopic(topic, brokers, shutdownItGroup(), parkingApplier{p}, logger)
			})
	})

	t.Run("InboundReceiptConsumer", func(t *testing.T) {
		p := newParker()
		topic := shutdownItTopic("warehouse.inbound-receiving.events")
		id := fmt.Sprintf("ce-rcpt-%d", time.Now().UnixNano())
		assertShutdownMidHandleIsClean(t, ctx, brokers, topic, p,
			kafkago.Message{Key: []byte(id), Value: receiptCE(t, id, typeReceiptLineReceived, lineData("SKU-1", 4, "Good"))},
			func(logger *slog.Logger) runCloser {
				return NewInboundReceiptConsumerForTopic(topic, brokers, shutdownItGroup(), parkingBooker{p}, logger)
			})
	})

	t.Run("TransferConsumer", func(t *testing.T) {
		p := newParker()
		topic := shutdownItTopic("warehouse.network-inventory-planning.events")
		id := fmt.Sprintf("ce-tr-%d", time.Now().UnixNano())
		assertShutdownMidHandleIsClean(t, ctx, brokers, topic, p,
			kafkago.Message{Key: []byte(id), Value: buildTransferCommandCE(t, id, "tr-1", "tl-1", "SITE-A", "SKU-1", 5)},
			func(logger *slog.Logger) runCloser {
				return NewConsumerForTopic(topic, brokers, shutdownItGroup(), parkingAllocator{p}, logger)
			})
	})
}

// assertShutdownMidHandleIsClean creates topic and topic.dlq, publishes one
// healthy message, builds the consumer only then (a kafka-go group reader that
// joins before its topic exists is never assigned a partition), waits until
// the use case is parked on the message, cancels Run, and asserts the clean
// shutdown contract.
func assertShutdownMidHandleIsClean(t *testing.T, ctx context.Context, brokers []string, topic string, p parker,
	msg kafkago.Message, newConsumer func(*slog.Logger) runCloser) {
	t.Helper()
	dlqTopic := topic + dlqSuffix
	shutdownItCreateTopic(t, ctx, brokers[0], topic)
	shutdownItCreateTopic(t, ctx, brokers[0], dlqTopic)
	shutdownItPublish(t, ctx, brokers, topic, msg)

	logs := &shutdownLogs{}
	consumer := newConsumer(slog.New(slog.NewTextHandler(logs, nil)))
	defer func() { _ = consumer.Close() }()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	select {
	case <-p.entered:
	case <-time.After(60 * time.Second):
		t.Fatalf("the consumer never started handling the message; logs:\n%s", logs.String())
	}
	runCancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after its context was cancelled mid-handle = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	if out := logs.String(); strings.Contains(out, "dead-letter") {
		t.Errorf("a healthy message only interrupted by shutdown was routed to the dead-letter path:\n%s", out)
	}
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer func() { _ = dlqReader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	if dead, err := dlqReader.ReadMessage(readCtx); err == nil {
		t.Fatalf("a healthy message was dead-lettered on shutdown: key=%q headers=%v", string(dead.Key), dead.Headers)
	}
}

func shutdownItTopic(prefix string) string {
	return fmt.Sprintf("%s.shutdown-itest-%d", prefix, time.Now().UnixNano())
}

func shutdownItGroup() string {
	return fmt.Sprintf("inventory-storage-shutdown-itest-%d", time.Now().UnixNano())
}

// shutdownItCreateTopic creates a one-partition topic and waits for its leader,
// retrying the transient connection resets a freshly started broker produces.
func shutdownItCreateTopic(t *testing.T, ctx context.Context, addr, topic string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := kafkago.DialContext(ctx, "tcp", addr)
		if err == nil {
			err = conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
			if errors.Is(err, kafkago.TopicAlreadyExists) {
				err = nil
			}
			if err == nil {
				var parts []kafkago.Partition
				parts, err = conn.ReadPartitions(topic)
				if err == nil && (len(parts) != 1 || parts[0].Leader.ID < 0) {
					err = fmt.Errorf("topic %q has no partition leader yet", topic)
				}
			}
			_ = conn.Close()
		}
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic %q: %v", topic, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// shutdownItPublish writes msg, retrying while the writer's metadata cache
// still lags the just-created topic.
func shutdownItPublish(t *testing.T, ctx context.Context, brokers []string, topic string, msg kafkago.Message) {
	t.Helper()
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := writer.WriteMessages(ctx, msg)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("publish healthy message: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
