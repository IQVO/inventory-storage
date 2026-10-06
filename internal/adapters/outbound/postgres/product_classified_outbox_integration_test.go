//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ProductClassified goes through the transactional outbox on BOTH topics
// (ADR 0031). These tests use the REAL integration and analytics encoders
// (not the stub encoders of outbox_integration_test.go) against a real
// Postgres: they prove the wire form that lands in outbox_events, and that
// it commits — or rolls back — together with the classification row.

const productClassifiedType = "com.warehouse.wms.inventory-storage.product.ProductClassified"

type classifiedOutboxRow struct {
	topic, key string
	value      []byte
}

// classifiedOutboxRows returns every outbox row of the ProductClassified
// type, oldest first.
func classifiedOutboxRows(t *testing.T, pool *pgxpool.Pool) []classifiedOutboxRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT topic, key, value FROM outbox_events WHERE event_type = $1 ORDER BY id`, productClassifiedType)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	var out []classifiedOutboxRow
	for rows.Next() {
		var r classifiedOutboxRow
		var key []byte
		if err := rows.Scan(&r.topic, &key, &r.value); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}
		r.key = string(key)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox rows: %v", err)
	}
	return out
}

func classifyUseCase(pool *pgxpool.Pool, encoders ...outboundkafka.Encoder) *usecases.ClassifyProduct {
	return &usecases.ClassifyProduct{
		Classifications: postgres.NewProductClassificationRepo(pool),
		Events:          postgres.NewOutboxPublisher(pool, encoders...),
		Clock:           &fixedClock{t: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)},
		UnitOfWork:      postgres.NewUnitOfWork(pool),
	}
}

// TestOutbox_ClassifyProduct_CommitsClassificationAndBothTopicEvents proves
// ClassifyProduct's Save + Publish commit as ONE transaction, leaving one
// outbox row on each topic with the right type, subject/key and dataschema.
func TestOutbox_ClassifyProduct_CommitsClassificationAndBothTopicEvents(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	reservations := postgres.NewReservationRepo(pool)

	uc := classifyUseCase(pool,
		outboundkafka.NewPublisher(nil, reservations),
		outboundkafka.NewAnalyticsPublisher(nil, reservations, nil),
	)

	sku, _ := shared.NewSKU("OUTBOX-PC-SKU-1")
	if _, err := uc.Execute(ctx, sku,
		[]product.HandlingTag{product.Hazmat, product.TemperatureSensitive}, product.Chilled, product.DOTHazardClass(3)); err != nil {
		t.Fatalf("ClassifyProduct: %v", err)
	}

	// The aggregate committed.
	found, err := postgres.NewProductClassificationRepo(pool).FindBySKU(ctx, sku)
	if err != nil || found == nil {
		t.Fatalf("classification not persisted: %v err=%v", found, err)
	}

	// Exactly one unpublished row per topic, in the same commit.
	rows := classifiedOutboxRows(t, pool)
	if len(rows) != 2 {
		t.Fatalf("ProductClassified outbox rows = %d, want 2 (integration + analytics)", len(rows))
	}
	if got := countOutbox(t, pool, "published_at IS NULL AND event_type = '"+productClassifiedType+"'"); got != 2 {
		t.Fatalf("unpublished ProductClassified rows = %d, want 2", got)
	}

	wantSchema := map[string]string{
		outboundkafka.Topic:          "urn:warehouse:inventory-storage:events:ProductClassified:v1",
		outboundkafka.AnalyticsTopic: "urn:warehouse:inventory-storage:analytics:ProductClassified:v1",
	}
	for _, r := range rows {
		schema, ok := wantSchema[r.topic]
		if !ok {
			t.Fatalf("unexpected topic %q", r.topic)
		}
		delete(wantSchema, r.topic)

		if r.key != "OUTBOX-PC-SKU-1" {
			t.Errorf("%s key = %q, want the SKU", r.topic, r.key)
		}
		e, err := cloudevents.Decode(r.value)
		if err != nil {
			t.Fatalf("%s value is not a valid CloudEvent: %v", r.topic, err)
		}
		if e.Type() != productClassifiedType {
			t.Errorf("%s type = %q, want %q", r.topic, e.Type(), productClassifiedType)
		}
		if e.Subject() != "OUTBOX-PC-SKU-1" {
			t.Errorf("%s subject = %q, want the SKU", r.topic, e.Subject())
		}
		if e.DataSchema() != schema {
			t.Errorf("%s dataschema = %q, want %q", r.topic, e.DataSchema(), schema)
		}
		var data struct {
			SKU              string   `json:"sku"`
			HandlingTags     []string `json:"handling_tags"`
			TemperatureClass string   `json:"temperature_class"`
			DOTHazardClass   int      `json:"dot_hazard_class"`
		}
		if err := e.DataAs(&data); err != nil {
			t.Fatalf("%s data: %v", r.topic, err)
		}
		if data.SKU != "OUTBOX-PC-SKU-1" || data.TemperatureClass != "Chilled" || data.DOTHazardClass != 3 ||
			len(data.HandlingTags) != 2 || data.HandlingTags[0] != "Hazmat" || data.HandlingTags[1] != "TemperatureSensitive" {
			t.Errorf("%s data = %+v", r.topic, data)
		}
	}
	if len(wantSchema) != 0 {
		t.Errorf("topics never written: %v", wantSchema)
	}
}

// TestOutbox_ClassifyProduct_SecondTopicEncodeFailure_RollsBackBoth proves
// the two topic rows and the classification are one unit: the integration
// encoder succeeds first (its row is inserted), then the analytics encoder
// fails — the integration row and the classification must NOT survive.
func TestOutbox_ClassifyProduct_SecondTopicEncodeFailure_RollsBackBoth(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	reservations := postgres.NewReservationRepo(pool)

	uc := classifyUseCase(pool,
		outboundkafka.NewPublisher(nil, reservations),
		failingEncoder{err: errors.New("analytics encode boom")},
	)

	sku, _ := shared.NewSKU("OUTBOX-PC-SKU-2")
	if _, err := uc.Execute(ctx, sku, []product.HandlingTag{product.Fragile}, "", 0); err == nil {
		t.Fatal("expected the second encoder's failure to fail Execute")
	}

	if rows := classifiedOutboxRows(t, pool); len(rows) != 0 {
		t.Fatalf("outbox rows after a rolled-back classify = %d, want 0 (the first topic's row must roll back too)", len(rows))
	}
	found, err := postgres.NewProductClassificationRepo(pool).FindBySKU(ctx, sku)
	if err != nil {
		t.Fatalf("FindBySKU: %v", err)
	}
	if found != nil {
		t.Fatal("classification survived a failed publish: the unit of work did not roll back")
	}
}

// TestOutbox_ClassifyProduct_Reclassify_RaisesAnEventEachTime proves a
// replacement is also a publish (full-state replacement for consumers),
// keyed by the same SKU so both land on one partition in order.
func TestOutbox_ClassifyProduct_Reclassify_RaisesAnEventEachTime(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	reservations := postgres.NewReservationRepo(pool)
	uc := classifyUseCase(pool, outboundkafka.NewPublisher(nil, reservations))

	sku, _ := shared.NewSKU("OUTBOX-PC-SKU-3")
	for _, tags := range [][]product.HandlingTag{{product.Fragile}, {product.Fragile, product.HighValue}} {
		if _, err := uc.Execute(ctx, sku, tags, "", 0); err != nil {
			t.Fatalf("ClassifyProduct %v: %v", tags, err)
		}
	}
	rows := classifiedOutboxRows(t, pool)
	if len(rows) != 2 || rows[0].key != rows[1].key {
		t.Fatalf("rows = %+v, want 2 events with the same key", rows)
	}
}
