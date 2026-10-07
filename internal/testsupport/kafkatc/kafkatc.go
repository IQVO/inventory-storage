//go:build integration

// Package kafkatc is the ONE shared, test-only way this repository's
// `-tags=integration` tests obtain a real Kafka broker.
//
// It starts confluentinc/confluent-local via testcontainers-go (never
// localhost:9092, never an env-provided broker) and — the reason it exists —
// does not hand the brokers back until the broker can serve consumer-group
// coordination.
//
// # Why the coordinator wait
//
// A freshly booted broker accepts connections long before it is a usable group
// coordinator. The first group request makes it create the internal
// __consumer_offsets topic (50 partitions) and is answered with
// [15] GroupCoordinatorNotAvailable until that finishes. kafka-go then sleeps a
// FIXED 5 s JoinGroupBackoff per attempt, so on a loaded CI runner a consumer
// that races the warm-up can burn a test's whole wait window (the
// "timed out waiting for the transfer consumer to settle" and facilitycache
// replay flakes); the same burst of controller work also slows the first
// topic creation / produce of tests that use no consumer group at all.
// Waiting here, once, moves that cost out of every test's timed window.
//
// Test-only: only *_test.go files may import this package (it carries the
// integration build tag, so it is not even compiled otherwise). Nothing under
// internal/domain, internal/application (non-test) or cmd may depend on it.
package kafkatc

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// Image is the broker image every Kafka integration test runs against.
const Image = "confluentinc/confluent-local:7.6.1"

// coordinatorReadyBudget only bounds a broker that NEVER becomes ready; a
// healthy one returns on the first successful poll (it is not a sleep).
const coordinatorReadyBudget = 2 * time.Minute

// Broker is a running, group-coordinator-ready Kafka testcontainer.
type Broker struct {
	// Addrs are the bootstrap addresses to hand to kafka-go.
	Addrs []string

	container testcontainers.Container
}

// Start boots a Kafka container with the given cluster id and returns it only
// once the group coordinator is available. On any failure the container is
// terminated before the error is returned, so a caller never has to clean up
// a half-started broker.
func Start(ctx context.Context, clusterID string) (*Broker, error) {
	container, err := tckafka.Run(ctx, Image, tckafka.WithClusterID(clusterID))
	if err != nil {
		// Run may return a non-nil container alongside the error.
		if container != nil {
			_ = testcontainers.TerminateContainer(container)
		}
		return nil, fmt.Errorf("start kafka container: %w", err)
	}
	addrs, err := container.Brokers(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		return nil, fmt.Errorf("resolve kafka brokers: %w", err)
	}
	if err := WaitForGroupCoordinator(ctx, addrs[0]); err != nil {
		_ = testcontainers.TerminateContainer(container)
		return nil, err
	}
	return &Broker{Addrs: addrs, container: container}, nil
}

// Terminate stops and removes the container. It is safe on a nil Broker.
func (b *Broker) Terminate() error {
	if b == nil || b.container == nil {
		return nil
	}
	return testcontainers.TerminateContainer(b.container)
}

// WaitForGroupCoordinator blocks until the broker at addr can serve group
// coordination (the internal __consumer_offsets topic exists and its
// coordinator partition has a leader). It polls FindCoordinator for a
// throwaway group — the very request whose [15] answer a cold broker gives —
// and returns on the first success. Only ctx and coordinatorReadyBudget bound
// a broker that never becomes ready.
func WaitForGroupCoordinator(ctx context.Context, addr string) error {
	ctx, cancel := context.WithTimeout(ctx, coordinatorReadyBudget)
	defer cancel()

	client := &kafkago.Client{Addr: kafkago.TCP(addr), Timeout: 10 * time.Second}
	var last string
	for {
		resp, err := client.FindCoordinator(ctx, &kafkago.FindCoordinatorRequest{
			Addr: kafkago.TCP(addr), Key: "kafkatc-coordinator-warmup", KeyType: kafkago.CoordinatorKeyTypeConsumer,
		})
		switch {
		case err != nil:
			last = err.Error()
		case resp.Error != nil:
			last = resp.Error.Error()
		default:
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka group coordinator never became available on %s: %s", addr, last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
