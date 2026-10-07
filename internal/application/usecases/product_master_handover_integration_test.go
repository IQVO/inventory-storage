//go:build integration

// product-master hand-over (ADR 0034) end-to-end proof on REAL
// infrastructure booted via testcontainers (its own Kafka broker plus the
// package's shared Postgres; never an external broker, never t.Skip):
//
//	product-master-shaped ProductClassified on a Kafka topic
//	  -> ProductMasterConsumer -> ApplyProductClassification
//	  -> row in product_classifications (version-guarded, nothing in the outbox)
//	  -> StowStock of that Hazmat SKU: rejected from a non-hazmat bin,
//	     accepted into a hazmat bin.
package usecases_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/inventory-storage/internal/adapters/inbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// publishProductMasterClassified writes one CloudEvent exactly as
// product-master emits it (its source, the full type, key = subject = SKU).
func publishProductMasterClassified(t *testing.T, brokers []string, topic, id, sku string, version int64, tags ...string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"sku":                   sku,
		"handling_tags":         tags,
		"classification_source": "native",
		"version":               version,
	})
	value, _ := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/product-master",
		"type":            "com.warehouse.wms.product-master.product.ProductClassified",
		"subject":         sku,
		"time":            "2026-10-06T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:product-master:events:ProductClassified:v1",
		"data":            json.RawMessage(payload),
	})
	w := kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}, BatchTimeout: 50 * time.Millisecond}
	defer func() { _ = w.Close() }()
	msg := kafkago.Message{
		Key:     []byte(sku),
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

func localCopyVersion(t *testing.T, env *transferTestEnv, sku string) int64 {
	t.Helper()
	var version int64
	err := env.pool.QueryRow(context.Background(), `SELECT version FROM product_classifications WHERE sku = $1`, sku).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return -1
	}
	if err != nil {
		t.Fatalf("read local copy %s: %v", sku, err)
	}
	return version
}

func saveBin(t *testing.T, env *transferTestEnv, id string) shared.BinId {
	t.Helper()
	binID, _ := shared.NewBinId(id)
	bin, err := location.NewBin(binID, mustQty(t, 100))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := postgres.NewLocationRepo(env.pool).Save(context.Background(), bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}
	return binID
}

// productMasterTestEnv boots this file's own Kafka broker (testcontainers,
// terminated when the test ends) next to the package's shared,
// fully-migrated Postgres pool.
func productMasterTestEnv(t *testing.T, topic string) *transferTestEnv {
	t.Helper()
	ctx := context.Background()
	if transferSharedPool == nil {
		pool, err := bootSharedPostgresForTransfers(ctx, t)
		if err != nil {
			t.Fatalf("start postgres container: %v", err)
		}
		transferSharedPool = pool
	}
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("product-master-itest"))
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve kafka brokers: %v", err)
	}
	waitForGroupCoordinator(t, brokers[0])
	createTransferTopic(t, brokers[0], topic, 2)
	return &transferTestEnv{pool: transferSharedPool, brokers: brokers, topic: topic}
}

func TestIntegration_ProductMasterClassification_FeedsStowPlacement(t *testing.T) {
	ctx := context.Background()
	run := uniqueRun()
	topic := fmt.Sprintf("%s.itest-%s", inboundkafka.ProductMasterTopic, run)
	env := productMasterTestEnv(t, topic)

	sku := "SKU-PM-HZ-" + run
	var outboxBefore int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxBefore); err != nil {
		t.Fatalf("count outbox: %v", err)
	}

	// v2 Hazmat, then an out-of-order v1 Fragile and a redelivery of v2's
	// id: the copy must end at v2 Hazmat.
	publishProductMasterClassified(t, env.brokers, topic, "pm-2-"+run, sku, 2, "Hazmat")
	publishProductMasterClassified(t, env.brokers, topic, "pm-1-"+run, sku, 1, "Fragile")
	publishProductMasterClassified(t, env.brokers, topic, "pm-2-"+run, sku, 2, "Hazmat")

	group := "product-master-itest-" + run
	apply := &usecases.ApplyProductClassification{
		Classifications: postgres.NewProductClassificationRepo(env.pool),
		ProcessedEvents: postgres.NewProcessedEventRepo(env.pool),
		UnitOfWork:      postgres.NewUnitOfWork(env.pool),
	}
	consumer := inboundkafka.NewProductMasterConsumerForTopic(topic, env.brokers, group, apply, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { _ = consumer.Close() })

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	deadline := time.Now().Add(45 * time.Second)
	for !(localCopyVersion(t, env, sku) == 2 && groupLag(env, group) == 0) {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("consumer did not settle: version=%d lag=%d", localCopyVersion(t, env, sku), groupLag(env, group))
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done

	classifications := postgres.NewProductClassificationRepo(env.pool)
	c, err := classifications.FindBySKU(ctx, shared.SKU(sku))
	if err != nil || c == nil || !c.IsHazmat() || c.HasTag(product.Fragile) {
		t.Fatalf("local copy = %+v err=%v, want Hazmat only (v1 must not win)", c, err)
	}
	var processed int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id IN ($1, $2)`, "pm-1-"+run, "pm-2-"+run).Scan(&processed); err != nil {
		t.Fatalf("count processed: %v", err)
	}
	if processed != 2 {
		t.Fatalf("processed_events = %d, want 2 (the redelivered id is claimed once)", processed)
	}
	var outboxAfter int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxAfter); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxAfter != outboxBefore {
		t.Fatalf("outbox rows %d -> %d: consuming product-master must publish nothing", outboxBefore, outboxAfter)
	}

	// StowStock reads the SAME port, unchanged, with the facility-layout
	// lookup faked as in the unit tests.
	hazmatBin := saveBin(t, env, "BIN-HZ-"+run)
	ambientBin := saveBin(t, env, "BIN-STD-"+run)
	stow := &usecases.StowStock{
		Stock:           postgres.NewStockRepo(env.pool),
		Locations:       postgres.NewLocationRepo(env.pool),
		Events:          events.NewBufferedPublisher(),
		Clock:           memory.SystemClock{},
		Classifications: classifications,
		LocationLookup: &fakeLocationLookup{attrs: map[shared.BinId]product.SlotAttributes{
			hazmatBin:  {Known: true, Hazmat: true},
			ambientBin: {Known: true, Hazmat: false},
		}},
		UnitOfWork: postgres.NewUnitOfWork(env.pool),
	}

	if _, err := stow.Execute(ctx, shared.SKU(sku), mustQty(t, 5), ambientBin); !errors.Is(err, usecases.ErrHazmatZoneRequired) {
		t.Fatalf("stow into a non-hazmat bin: err = %v, want ErrHazmatZoneRequired", err)
	}
	unit, err := stow.Execute(ctx, shared.SKU(sku), mustQty(t, 5), hazmatBin)
	if err != nil {
		t.Fatalf("stow into a hazmat bin: %v", err)
	}
	if unit.BinID() != hazmatBin {
		t.Fatalf("stowed into %s, want %s", unit.BinID(), hazmatBin)
	}
}
