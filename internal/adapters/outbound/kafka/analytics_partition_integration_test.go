//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// TestAnalyticsPublisher_RealBroker_ReservationLifecycleLandsOnSamePartition is
// the real-Kafka regression for ADR-0021's analytics amendment: the analytics
// publisher used to key StockReserved/StockPicked by SKU but
// ReservationRevoked/ReservationExpired by reservation id, so one
// reservation's lifecycle spanned partitions. Against an 8-partition topic
// (the fleet's live partition count), every lifecycle event of a reservation
// must now land on ONE partition, so a per-partition consumer sees
// StockReserved before its own ReservationRevoked/StockPicked.
//
// Several reservations, each over its own SKU, are used so a regression to SKU
// keying cannot pass by hash coincidence.
func TestAnalyticsPublisher_RealBroker_ReservationLifecycleLandsOnSamePartition(t *testing.T) {
	const numPartitions = 8
	brokerList := startPartitionKeyBroker(t)
	topic := uniquePartitionKeyTopic(t) + ".analytics"
	createTopicWithPartitions(t, brokerList, topic, numPartitions)

	repo := memory.NewReservationRepo()
	pub := outboundkafka.NewAnalyticsPublisher(brokerList, repo, nil)
	// Same balancer/acks as production (NewAnalyticsPublisher), but aimed at
	// the controlled 8-partition test topic instead of AnalyticsTopic.
	pub.Writer = &kafkago.Writer{
		Addr:         kafkago.TCP(brokerList...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	t.Cleanup(func() { _ = pub.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const reservations = 6
	qty, _ := shared.NewPositiveQuantity(2)
	for i := 0; i < reservations; i++ {
		id := fmt.Sprintf("res-affinity-%d", i)
		sku, _ := shared.NewSKU(fmt.Sprintf("SKU-AFFINITY-%d", i))
		res, err := reservation.New(id, sku, qty, "order-affinity",
			[]reservation.Allocation{{StockUnitID: "unit-affinity", Quantity: qty}},
			time.Now(), time.Hour)
		if err != nil {
			t.Fatalf("build reservation: %v", err)
		}
		if err := repo.Save(ctx, res); err != nil {
			t.Fatalf("save reservation: %v", err)
		}
		now := time.Now()
		for _, ev := range []shared.DomainEvent{
			shared.NewStockReserved(now, id, sku, qty, "order-affinity"),
			shared.NewStockPicked(now, id, sku, qty),
			shared.NewReservationRevoked(now, id),
			shared.NewReservationExpired(now, id),
		} {
			if err := pub.Publish(ctx, ev); err != nil {
				t.Fatalf("publish %s for %s: %v", ev.EventName(), id, err)
			}
		}
	}

	// Drain every partition ONCE, in parallel (the topic only holds this
	// test's 6x4 messages), and group what landed by message key.
	byKey := readAllByKey(brokerList, topic, numPartitions)
	for i := 0; i < reservations; i++ {
		id := fmt.Sprintf("res-affinity-%d", i)
		landed := byKey[id]
		if len(landed) != 4 {
			t.Fatalf("expected 4 lifecycle messages keyed %s, found %d", id, len(landed))
		}
		for _, m := range landed[1:] {
			if m.Partition != landed[0].Partition {
				t.Errorf("%s: lifecycle events spread over partitions %d and %d; all must share one",
					id, landed[0].Partition, m.Partition)
			}
		}
	}
}

// readAllByKey reads every partition of topic concurrently until each has
// been idle for ~2s (one pass, rather than one 5s-per-partition scan per
// key) and returns the messages grouped by Key.
func readAllByKey(brokerList []string, topic string, numPartitions int) map[string][]kafkago.Message {
	var mu sync.Mutex
	out := map[string][]kafkago.Message{}
	var wg sync.WaitGroup
	for p := 0; p < numPartitions; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			reader := kafkago.NewReader(kafkago.ReaderConfig{
				Brokers: brokerList, Topic: topic, Partition: p, MinBytes: 1, MaxBytes: 10e6,
			})
			defer func() { _ = reader.Close() }()
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				msg, err := reader.ReadMessage(ctx)
				cancel()
				if err != nil {
					return // idle for 2s: this partition is drained
				}
				mu.Lock()
				out[string(msg.Key)] = append(out[string(msg.Key)], msg)
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	return out
}
