//go:build integration

// Integration tests for the ANALYTICS inbound Kafka consumer against a REAL
// broker and a REAL analytical Postgres, both booted by the test itself via
// testcontainers (never an external KAFKA_BROKERS/ANALYTICS_DATABASE_URL,
// never t.Skip, never localhost):
//
//	analytics CloudEvents on a unique topic (built with this repo's own
//	cloudevents helper, exactly as the AnalyticsPublisher emits them)
//	  -> AnalyticsConsumer.Run (real kafka-go reader, real consumer group)
//	  -> analyticsstore PostgresProjection + ConsumedEventsRepo
//	  -> the Inventory Flow & Accuracy read model, asserted through
//	     PostgresReport.Query — the same reader cmd/inventory-reports
//	     serves.
//
// This is the one inbound Kafka adapter whose tests never touched a real
// broker (analytics_consumer_test.go drives HandleMessage with fakes); the
// four command consumers already have real-Kafka suites in
// internal/application/usecases.
//
// One Kafka broker and one Postgres serve the whole package run (TestMain
// owns their lifecycle, mirroring the facilitycache consumer's proven
// pattern); per-test isolation comes from a unique timestamped topic and
// unique SKUs/bins, never from separate infrastructure.
package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/analytics/report"
	"github.com/claudioed/inventory-storage/internal/testsupport/kafkatc"
)

var (
	analyticsItBrokers   []string
	analyticsItBroker    *kafkatc.Broker
	analyticsItDB        *pgxpool.Pool
	analyticsItContainer testcontainers.Container
)

// TestMain owns the package-wide broker + analytical Postgres lifecycle.
// Infrastructure is booted lazily by the first test that needs it and
// outlives every test (per-test Cleanup would tear it down after the first
// caller and defeat the shared-container point).
func TestMain(m *testing.M) {
	code := m.Run()
	if analyticsItDB != nil {
		analyticsItDB.Close()
	}
	if analyticsItBroker != nil {
		if err := analyticsItBroker.Terminate(); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	if analyticsItContainer != nil {
		if err := testcontainers.TerminateContainer(analyticsItContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

// analyticsItEnv lazily boots the shared broker and analytical database and
// hands back their handles. kafkatc.Start returns only once the broker is a
// usable group coordinator (a cold broker answers [15]
// GroupCoordinatorNotAvailable and kafka-go then sleeps a fixed 5 s
// JoinGroupBackoff per attempt — the settled-suite flake this helper
// exists to prevent).
func analyticsItEnv(t *testing.T) ([]string, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	if analyticsItBrokers == nil {
		broker, err := kafkatc.Start(ctx, "analytics-itest")
		if err != nil {
			t.Fatalf("start kafka container: %v", err)
		}
		analyticsItBroker = broker
		analyticsItBrokers = broker.Addrs
	}
	if analyticsItDB == nil {
		// One retry: the very first container boot of a test binary has
		// hit Docker Desktop's ryuk reaper startup race — transient, and a
		// fresh attempt clears it (same belt-and-braces as the transfer
		// suite's bootSharedPostgresForTransfers).
		_, pool, err := bootAnalyticsItPostgres(ctx)
		if err != nil {
			t.Logf("first postgres boot failed (%v); retrying once", err)
			_, pool, err = bootAnalyticsItPostgres(ctx)
			if err != nil {
				t.Fatalf("start postgres container (after retry): %v", err)
			}
		}
		analyticsItDB = pool
	}
	return analyticsItBrokers, analyticsItDB
}

// bootAnalyticsItPostgres starts the throwaway analytical Postgres, applies
// the analytics migrations once, and opens the writer pool. Its lifetime is
// owned by TestMain, not t.Cleanup.
func bootAnalyticsItPostgres(ctx context.Context) (string, *pgxpool.Pool, error) {
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_analytics_it"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return "", nil, err
	}

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		return "", nil, err
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		_ = testcontainers.TerminateContainer(container)
		return "", nil, fmt.Errorf("unable to resolve test file path")
	}
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		_ = testcontainers.TerminateContainer(container)
		return "", nil, fmt.Errorf("migrate analytics: %w", err)
	}

	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		return "", nil, err
	}
	analyticsItContainer = container
	return url, pool, nil
}

// analyticsItTopic creates a unique timestamped topic (1 partition) and
// waits until its partition leader is resolvable, so the consumer built
// immediately afterwards can always read it.
func analyticsItTopic(t *testing.T, brokers []string) string {
	t.Helper()
	topic := fmt.Sprintf("warehouse.inventory.analytics.itest-%d", time.Now().UnixNano())
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial %s: %v", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}); err != nil && !isTopicExistsErr(err) {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	// Topic creation is asynchronous on the broker: wait until the
	// partition leader is actually resolvable before returning, otherwise
	// the first read/write races it.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) == 1 {
			return topic
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never became readable", topic)
	return ""
}

// isTopicExistsErr reports whether err is Kafka's TopicAlreadyExists /
// UnknownTopicOrPartition-metadata-race shape (a broker racing
// auto-creation), which createTopic tolerates.
func isTopicExistsErr(err error) bool {
	return strings.Contains(err.Error(), "Topic with this name already exists") ||
		errors.Is(err, kafkago.UnknownTopicOrPartition)
}

// analyticsItPublish writes msgs onto topic, retrying briefly: a freshly
// created topic's metadata can lag on the broker the writer first dials
// (UnknownTopicOrPartition).
func analyticsItPublish(t *testing.T, brokers []string, topic string, msgs ...kafkago.Message) {
	t.Helper()
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		BatchTimeout: 50 * time.Millisecond,
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

// analyticsItEvent builds one analytics CloudEvent with this repo's own
// envelope helper — the same builder the AnalyticsPublisher encodes through
// (entity segment, StreamAnalytics dataschema, structured JSON) — so the
// consumer is proven against byte-identical producer output.
func analyticsItEvent(t *testing.T, id, entity, eventName, subject string, at time.Time, data any) kafkago.Message {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID: id, Entity: entity, EventName: eventName, Subject: subject,
		Time: at, Stream: cloudevents.StreamAnalytics, Data: data,
	})
	if err != nil {
		t.Fatalf("build %s cloudevent: %v", eventName, err)
	}
	return kafkago.Message{
		Key:     []byte(subject),
		Value:   value,
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
	}
}

// startAnalyticsItConsumer runs consumer.Run in the background for the rest
// of the test and stops it (and the reader) on cleanup. Run uses ReadMessage
// (auto-committing), so settling is observed against the DATABASE — the
// rollup is the durable outcome the contract cares about.
func startAnalyticsItConsumer(t *testing.T, consumer *kafka.AnalyticsConsumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = consumer.Close()
	})
}

// analyticsItPollUntil polls settled (against the read model) until it
// holds or the 45 s settle window closes.
func analyticsItPollUntil(t *testing.T, what string, settled func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if settled() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for the analytics consumer to settle: %s", what)
}

// analyticsItRows queries the read model the way cmd/inventory-reports
// does, keyed by "sku/binId".
func analyticsItRows(t *testing.T, pool *pgxpool.Pool, from, to time.Time) map[string]report.Row {
	t.Helper()
	rep, err := analyticsstore.NewPostgresReport(pool).Query(context.Background(), report.ReportQuery{
		From: from, To: to, Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("query report: %v", err)
	}
	rows := map[string]report.Row{}
	for _, r := range rep.Rows {
		rows[r.Key.SKU+"/"+r.Key.BinId] = r
	}
	return rows
}

// newAnalyticsItConsumer wires the projector exactly like
// cmd/inventory-projector: the real Postgres projection and consumed-events
// gate over the analytical pool.
func newAnalyticsItConsumer(brokers []string, topic string, pool *pgxpool.Pool) *kafka.AnalyticsConsumer {
	return kafka.NewAnalyticsConsumer(brokers, topic,
		analyticsstore.NewPostgresProjection(pool),
		analyticsstore.NewConsumedEventsRepo(pool),
		slog.New(slog.DiscardHandler))
}

// TestAnalyticsConsumerIntegration_ReplaysHistoryIntoProjection is the core
// end-to-end proof: a consumer started AFTER a mixed history already sits
// on the topic (a projector restart, or a backfill into a fresh group —
// FirstOffset replay) must project every flow/accuracy event exactly once,
// dispatching on the FULL type, and must commit past — without blocking on
// — a legacy flat-envelope poison message and a valid CloudEvent outside
// the projection contract (ProductClassified).
func TestAnalyticsConsumerIntegration_ReplaysHistoryIntoProjection(t *testing.T) {
	brokers, pool := analyticsItEnv(t)
	topic := analyticsItTopic(t, brokers)
	ctx := context.Background()

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	sku := "SKU-ANALYTICS-IT-" + run
	bin := "BIN-ANALYTICS-IT-" + run
	base := time.Now().UTC().Truncate(time.Hour)

	// Poison FIRST: everything behind it must still be projected, proving
	// the skip never blocks the partition (ADR-0024 §5 — this consumer has
	// no DLQ; it WARNs and moves on). The raw literal is deliberate: it is
	// poison exactly because it was NOT built by the CloudEvents helper.
	legacy := kafkago.Message{
		Key:   []byte(sku),
		Value: []byte(`{"event_id":"legacy-flat","event_type":"com.warehouse.wms.inventory-storage.stock.StockReceived","occurred_at":"2026-10-08T12:00:00Z","data":{"sku":"` + sku + `","quantity":99}}`),
	}
	// Valid CloudEvent, wrong contract: ProductClassified is on the topic
	// (ADR 0031) but moves no flow/accuracy row and must not be marked
	// processed.
	classified := analyticsItEvent(t, "it-classified-"+run, "product", "ProductClassified", sku, base,
		map[string]any{"sku": sku, "handling_tags": []string{"Fragile"}, "version": 1})

	// The projecting set. The StockReceived id is published TWICE (a
	// genuine broker redelivery): the consumed-event claim must count it
	// once.
	stockReceived := analyticsItEvent(t, "it-sr-"+run, "stock", "StockReceived", sku, base,
		map[string]any{"sku": sku, "quantity": 10})
	analyticsItPublish(t, brokers, topic,
		legacy,
		classified,
		stockReceived,
		stockReceived, // same CloudEvents id: redelivery
		analyticsItEvent(t, "it-stowed-"+run, "stock", "ItemStowed", sku, base,
			map[string]any{"sku": sku, "bin_id": bin, "quantity": 5}),
		analyticsItEvent(t, "it-reserved-"+run, "reservation", "StockReserved", "res-it-"+run, base,
			map[string]any{"sku": sku, "reservation_id": "res-it-" + run, "quantity": 4}),
		analyticsItEvent(t, "it-picked-"+run, "reservation", "StockPicked", "res-it-"+run, base,
			map[string]any{"sku": sku, "reservation_id": "res-it-" + run, "quantity": 4}),
		analyticsItEvent(t, "it-revoked-"+run, "reservation", "ReservationRevoked", "res-it-"+run, base,
			map[string]any{"sku": sku, "reservation_id": "res-it-" + run}),
		analyticsItEvent(t, "it-unlocated-"+run, "stock", "ItemUnlocated", sku, base,
			map[string]any{"sku": sku, "bin_id": bin, "quantity": 1}),
		analyticsItEvent(t, "it-cycle-"+run, "bin", "CycleCountCompleted", bin, base,
			map[string]any{"bin_id": bin, "counted": 5, "system": 5, "discrepancy": false}),
		analyticsItEvent(t, "it-discrepancy-"+run, "bin", "DiscrepancyDetected", bin, base,
			map[string]any{"bin_id": bin, "counted": 4, "system": 5}),
	)

	consumer := newAnalyticsItConsumer(brokers, topic, pool)
	startAnalyticsItConsumer(t, consumer)

	from, to := base, base.Add(time.Hour)
	analyticsItPollUntil(t, "all three rollup rows reach their expected values", func() bool {
		rows := analyticsItRows(t, pool, from, to)
		flow, ok := rows[sku+"/"]
		if !ok || flow.ReceivedQuantity != 10 || flow.PickedQuantity != 4 ||
			flow.ReservationsCreated != 1 || flow.ReservationsRevoked != 1 {
			return false
		}
		stow, ok := rows[sku+"/"+bin]
		if !ok || stow.StowedCount != 1 || stow.UnlocatedCount != 1 {
			return false
		}
		accuracy, ok := rows["/"+bin]
		return ok && accuracy.CycleCountsCompleted == 1 && accuracy.DiscrepanciesDetected == 1
	})

	rows := analyticsItRows(t, pool, from, to)

	// SKU flow row: received 10 (the redelivered id counted ONCE), picked 4,
	// one reservation created, one revoked — and nothing else.
	flow := rows[sku+"/"]
	if flow.ReceivedQuantity != 10 {
		t.Errorf("received = %d, want 10 (redelivered CloudEvents id must count once)", flow.ReceivedQuantity)
	}
	if flow.PickedQuantity != 4 || flow.ReservationsCreated != 1 || flow.ReservationsRevoked != 1 {
		t.Errorf("flow row = %+v, want picked 4 / created 1 / revoked 1", flow)
	}
	if flow.ReservationsExpired != 0 || flow.StowedCount != 0 || flow.CycleCountsCompleted != 0 {
		t.Errorf("flow row = %+v, want no expired/stowed/cycle counters", flow)
	}

	// (sku, bin) row: one stow, one unlocate.
	if stow := rows[sku+"/"+bin]; stow.StowedCount != 1 || stow.UnlocatedCount != 1 || stow.ReceivedQuantity != 0 {
		t.Errorf("(sku,bin) row = %+v, want stowed 1 / unlocated 1 / no received", stow)
	}

	// (bin) accuracy row: one cycle count, one discrepancy.
	if accuracy := rows["/"+bin]; accuracy.CycleCountsCompleted != 1 || accuracy.DiscrepanciesDetected != 1 {
		t.Errorf("accuracy row = %+v, want cycle 1 / discrepancy 1", accuracy)
	}

	// The non-projecting ProductClassified must NOT be in the consumed set
	// (it is acknowledged untouched).
	var classifiedProcessed int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM analytics_consumed_events WHERE event_id = $1`,
		"it-classified-"+run,
	).Scan(&classifiedProcessed); err != nil {
		t.Fatalf("count consumed classified: %v", err)
	}
	if classifiedProcessed != 0 {
		t.Errorf("ProductClassified was marked processed %d times, want 0 (non-projecting types are acknowledged untouched)", classifiedProcessed)
	}
	// The redelivered StockReceived id was claimed exactly once.
	var receivedClaimed int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM analytics_consumed_events WHERE event_id = $1`,
		"it-sr-"+run,
	).Scan(&receivedClaimed); err != nil {
		t.Fatalf("count consumed received: %v", err)
	}
	if receivedClaimed != 1 {
		t.Errorf("redelivered StockReceived id claimed %d times, want 1", receivedClaimed)
	}
}

// TestAnalyticsConsumerIntegration_EventsPublishedAfterCatchUpAreApplied is
// the freshness property: a projector that has already caught up must keep
// applying NEW occurrences as they arrive, with no restart — this is the
// live stream the report's freshness lag measures.
func TestAnalyticsConsumerIntegration_EventsPublishedAfterCatchUpAreApplied(t *testing.T) {
	brokers, pool := analyticsItEnv(t)
	topic := analyticsItTopic(t, brokers)

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	sku := "SKU-ANALYTICS-LIVE-" + run
	base := time.Now().UTC().Truncate(time.Hour)
	from, to := base, base.Add(time.Hour)

	consumer := newAnalyticsItConsumer(brokers, topic, pool)
	startAnalyticsItConsumer(t, consumer)

	// First occurrence: signals the consumer has joined and caught up.
	analyticsItPublish(t, brokers, topic,
		analyticsItEvent(t, "live-1-"+run, "stock", "StockReceived", sku, base,
			map[string]any{"sku": sku, "quantity": 7}))
	analyticsItPollUntil(t, "first live publish lands", func() bool {
		rows := analyticsItRows(t, pool, from, to)
		flow, ok := rows[sku+"/"]
		return ok && flow.ReceivedQuantity == 7
	})

	// Second occurrence AFTER catch-up must still land, no restart.
	analyticsItPublish(t, brokers, topic,
		analyticsItEvent(t, "live-2-"+run, "stock", "StockReceived", sku, base,
			map[string]any{"sku": sku, "quantity": 5}))
	analyticsItPollUntil(t, "second live publish lands", func() bool {
		rows := analyticsItRows(t, pool, from, to)
		flow, ok := rows[sku+"/"]
		return ok && flow.ReceivedQuantity == 12
	})

	rows := analyticsItRows(t, pool, from, to)
	if flow := rows[sku+"/"]; flow.ReceivedQuantity != 12 {
		t.Fatalf("received after live second publish = %d, want 12", flow.ReceivedQuantity)
	}
}

// TestAnalyticsConsumerIntegration_ReservationLifecycleOrdering is the
// ADR-0021 property that matters to the report: the reservation-lifecycle
// events of ONE reservation are keyed by the reservation id (Hash), so they
// land on ONE partition and the projector sees them in order — created
// before revoked — even though they are separate messages. Both project
// onto the same SKU row, claimed exactly once each.
func TestAnalyticsConsumerIntegration_ReservationLifecycleOrdering(t *testing.T) {
	brokers, pool := analyticsItEnv(t)
	topic := analyticsItTopic(t, brokers)

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	sku := "SKU-ANALYTICS-LIFE-" + run
	resID := "res-life-" + run
	base := time.Now().UTC().Truncate(time.Hour)
	from, to := base, base.Add(time.Hour)

	analyticsItPublish(t, brokers, topic,
		analyticsItEvent(t, "life-sr-"+run, "stock", "StockReceived", sku, base,
			map[string]any{"sku": sku, "quantity": 6}),
		analyticsItEvent(t, "life-res-"+run, "reservation", "StockReserved", resID, base.Add(time.Minute),
			map[string]any{"sku": sku, "reservation_id": resID, "quantity": 6}),
		analyticsItEvent(t, "life-rev-"+run, "reservation", "ReservationRevoked", resID, base.Add(2*time.Minute),
			map[string]any{"sku": sku, "reservation_id": resID}),
	)

	consumer := newAnalyticsItConsumer(brokers, topic, pool)
	startAnalyticsItConsumer(t, consumer)

	analyticsItPollUntil(t, "lifecycle row reaches received 6 / created 1 / revoked 1", func() bool {
		rows := analyticsItRows(t, pool, from, to)
		flow, ok := rows[sku+"/"]
		return ok && flow.ReceivedQuantity == 6 && flow.ReservationsCreated == 1 && flow.ReservationsRevoked == 1
	})

	// The projection's own event-id claim table agrees: one claim per
	// projecting occurrence (received + reserved + revoked).
	var claimed int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM analytics_processed_events WHERE event_id LIKE $1`,
		"life-%-"+run,
	).Scan(&claimed); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("count processed: %v", err)
	}
	if claimed != 3 {
		t.Fatalf("analytics_processed_events claims = %d, want 3 (received + reserved + revoked, each exactly once)", claimed)
	}
}
