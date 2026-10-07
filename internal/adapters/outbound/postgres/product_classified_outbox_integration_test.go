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
)

// ADR 0034: the normal write path no longer raises ProductClassified. The
// ONLY emitter left is the one-shot republish-product-classifications
// backfill (stage B), which enqueues the legacy event through the outbox
// for the integration topic. The product-master consumer's use case
// (stage C) writes the local copy and must leave the outbox untouched.

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

// seedLegacyRow inserts a row the way the retired PUT endpoint left it:
// no version or source given, so migration 0033's defaults apply.
func seedLegacyRow(t *testing.T, pool *pgxpool.Pool, sku string, tags []string, temp string, dot *int) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO product_classifications (sku, handling_tags, temperature_class, dot_hazard_class) VALUES ($1, $2, $3, $4)`,
		sku, tags, temp, dot); err != nil {
		t.Fatalf("seed legacy row %s: %v", sku, err)
	}
}

func republishUseCase(pool *pgxpool.Pool, batch int, encoders ...outboundkafka.Encoder) *usecases.RepublishProductClassifications {
	return &usecases.RepublishProductClassifications{
		Catalogue:  postgres.NewProductClassificationRepo(pool),
		Events:     postgres.NewOutboxPublisher(pool, encoders...),
		Clock:      &fixedClock{t: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
		BatchSize:  batch,
	}
}

// TestOutbox_Republish_EnqueuesTheLegacyEventPerRow: every row becomes one
// integration-topic outbox row carrying the unchanged legacy wire contract
// (type, key = subject = SKU, dataschema, payload), in batches.
func TestOutbox_Republish_EnqueuesTheLegacyEventPerRow(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	three := 3
	seedLegacyRow(t, pool, "BF-SKU-1", []string{"Hazmat", "TemperatureSensitive"}, "Chilled", &three)
	seedLegacyRow(t, pool, "BF-SKU-2", []string{"Fragile"}, "", nil)
	seedLegacyRow(t, pool, "BF-SKU-3", []string{"HighValue"}, "", nil)

	n, err := republishUseCase(pool, 2, outboundkafka.NewPublisher(nil, nil)).Execute(ctx)
	if err != nil {
		t.Fatalf("republish: %v", err)
	}
	if n != 3 {
		t.Fatalf("republished %d, want 3", n)
	}

	rows := classifiedOutboxRows(t, pool)
	if len(rows) != 3 {
		t.Fatalf("outbox rows = %d, want 3", len(rows))
	}
	wantSKUs := []string{"BF-SKU-1", "BF-SKU-2", "BF-SKU-3"}
	for i, r := range rows {
		if r.topic != outboundkafka.Topic {
			t.Errorf("row %d topic = %q, want %q (integration topic only)", i, r.topic, outboundkafka.Topic)
		}
		if r.key != wantSKUs[i] {
			t.Errorf("row %d key = %q, want %q", i, r.key, wantSKUs[i])
		}
		e, err := cloudevents.Decode(r.value)
		if err != nil {
			t.Fatalf("row %d is not a valid CloudEvent: %v", i, err)
		}
		if e.Type() != productClassifiedType || e.Subject() != wantSKUs[i] ||
			e.DataSchema() != "urn:warehouse:inventory-storage:events:ProductClassified:v1" ||
			e.Source() != "/warehouse/inventory-storage" {
			t.Errorf("row %d envelope = type %q subject %q schema %q source %q", i, e.Type(), e.Subject(), e.DataSchema(), e.Source())
		}
	}

	var first struct {
		SKU              string   `json:"sku"`
		HandlingTags     []string `json:"handling_tags"`
		TemperatureClass string   `json:"temperature_class"`
		DOTHazardClass   int      `json:"dot_hazard_class"`
	}
	e, _ := cloudevents.Decode(rows[0].value)
	if err := e.DataAs(&first); err != nil {
		t.Fatalf("data: %v", err)
	}
	if first.SKU != "BF-SKU-1" || first.TemperatureClass != "Chilled" || first.DOTHazardClass != 3 ||
		len(first.HandlingTags) != 2 || first.HandlingTags[0] != "Hazmat" || first.HandlingTags[1] != "TemperatureSensitive" {
		t.Errorf("data = %+v", first)
	}

	if got := countOutbox(t, pool, "topic = '"+outboundkafka.AnalyticsTopic+"'"); got != 0 {
		t.Errorf("analytics outbox rows = %d, want 0 (the backfill writes the integration topic only)", got)
	}
}

// Re-running enqueues the same full-state messages again (harmless
// downstream) with fresh CloudEvents ids.
func TestOutbox_Republish_RerunEnqueuesAgain(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	seedLegacyRow(t, pool, "BF-RERUN-1", []string{"Fragile"}, "", nil)
	uc := republishUseCase(pool, 0, outboundkafka.NewPublisher(nil, nil))

	for run := 1; run <= 2; run++ {
		if n, err := uc.Execute(ctx); err != nil || n != 1 {
			t.Fatalf("run %d: n=%d err=%v", run, n, err)
		}
	}
	rows := classifiedOutboxRows(t, pool)
	if len(rows) != 2 || rows[0].key != rows[1].key {
		t.Fatalf("rows = %d, want 2 with the same key", len(rows))
	}
	e1, _ := cloudevents.Decode(rows[0].value)
	e2, _ := cloudevents.Decode(rows[1].value)
	if e1.ID() == e2.ID() {
		t.Fatal("two runs reused one CloudEvents id")
	}
}

// A batch whose publish fails leaves nothing behind for that batch.
func TestOutbox_Republish_EncodeFailureRollsBackTheBatch(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	seedLegacyRow(t, pool, "BF-FAIL-1", []string{"Fragile"}, "", nil)
	seedLegacyRow(t, pool, "BF-FAIL-2", []string{"Fragile"}, "", nil)

	uc := republishUseCase(pool, 10,
		outboundkafka.NewPublisher(nil, nil),
		failingEncoder{err: errors.New("second encoder boom")},
	)
	if _, err := uc.Execute(ctx); err == nil {
		t.Fatal("expected the encoder failure to fail the backfill")
	}
	if rows := classifiedOutboxRows(t, pool); len(rows) != 0 {
		t.Fatalf("outbox rows after a rolled-back batch = %d, want 0", len(rows))
	}
}

func applyUseCase(pool *pgxpool.Pool) *usecases.ApplyProductClassification {
	return &usecases.ApplyProductClassification{
		Classifications: postgres.NewProductClassificationRepo(pool),
		ProcessedEvents: postgres.NewProcessedEventRepo(pool),
		UnitOfWork:      postgres.NewUnitOfWork(pool),
	}
}

func storedVersion(t *testing.T, pool *pgxpool.Pool, sku string) (int64, string) {
	t.Helper()
	var version int64
	var source string
	if err := pool.QueryRow(context.Background(),
		`SELECT version, classification_source FROM product_classifications WHERE sku = $1`, sku).Scan(&version, &source); err != nil {
		t.Fatalf("read version of %s: %v", sku, err)
	}
	return version, source
}

func countProcessed(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM processed_events`).Scan(&n); err != nil {
		t.Fatalf("count processed_events: %v", err)
	}
	return n
}

// The local copy follows the version guard and the id dedupe, and the
// consumer's use case writes NOTHING to the outbox (no publish loop).
func TestApplyProductClassification_Postgres_VersionGuardDedupeNoOutbox(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	seedLegacyRow(t, pool, "PM-SKU-1", []string{"Fragile"}, "", nil)
	if v, s := storedVersion(t, pool, "PM-SKU-1"); v != 0 || s != "inventory-storage" {
		t.Fatalf("legacy row defaults = %d/%q, want 0/inventory-storage", v, s)
	}
	uc := applyUseCase(pool)
	update := func(id string, version int64, tags ...string) usecases.ProductClassificationUpdate {
		return usecases.ProductClassificationUpdate{EventID: id, SKU: "PM-SKU-1", HandlingTags: tags, ClassificationSource: "native", Version: version}
	}

	steps := []struct {
		u    usecases.ProductClassificationUpdate
		want usecases.ApplyOutcome
	}{
		{update("ce-1", 2, "Hazmat"), usecases.ClassificationApplied},      // beats legacy v0
		{update("ce-2", 1, "Oversized"), usecases.ClassificationStale},     // older
		{update("ce-3", 2, "Oversized"), usecases.ClassificationStale},     // equal
		{update("ce-1", 9, "Oversized"), usecases.ClassificationDuplicate}, // replayed id
		{update("ce-4", 3, "HighValue"), usecases.ClassificationApplied},
	}
	for i, s := range steps {
		got, err := uc.Execute(ctx, s.u)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if got != s.want {
			t.Fatalf("step %d: outcome %s, want %s", i, got, s.want)
		}
	}

	found, err := postgres.NewProductClassificationRepo(pool).FindBySKU(ctx, "PM-SKU-1")
	if err != nil || found == nil {
		t.Fatalf("FindBySKU: %v %v", found, err)
	}
	if !found.HasTag(product.HighValue) || found.HasTag(product.Fragile) || found.HasTag(product.Oversized) {
		t.Fatalf("local copy tags = %v, want [HighValue]", found.HandlingTags())
	}
	if v, s := storedVersion(t, pool, "PM-SKU-1"); v != 3 || s != "native" {
		t.Fatalf("stored = %d/%q, want 3/native", v, s)
	}
	if n := countProcessed(t, pool); n != 4 {
		t.Fatalf("processed_events rows = %d, want 4 (ce-1..ce-4)", n)
	}
	if n := countOutbox(t, pool, "true"); n != 0 {
		t.Fatalf("outbox rows = %d, want 0: applying product-master's events must never publish", n)
	}
}

// failingLocalCopy fails the upsert AFTER the claim ran in the same
// transaction.
type failingLocalCopy struct{}

func (failingLocalCopy) ApplyIfNewer(context.Context, *product.ProductClassification, int64, string) (bool, error) {
	return false, errors.New("upsert boom")
}

// A failed upsert rolls the claim back with it, so the redelivery applies.
func TestApplyProductClassification_Postgres_FailedUpsertUnclaims(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	failing := &usecases.ApplyProductClassification{
		Classifications: failingLocalCopy{},
		ProcessedEvents: postgres.NewProcessedEventRepo(pool),
		UnitOfWork:      postgres.NewUnitOfWork(pool),
	}
	u := usecases.ProductClassificationUpdate{EventID: "ce-r", SKU: "PM-SKU-R", HandlingTags: []string{"Fragile"}, ClassificationSource: "native", Version: 1}
	if _, err := failing.Execute(ctx, u); err == nil {
		t.Fatal("expected the upsert failure to surface")
	}
	if n := countProcessed(t, pool); n != 0 {
		t.Fatalf("processed_events rows = %d after a rolled-back handler, want 0", n)
	}

	got, err := applyUseCase(pool).Execute(ctx, u)
	if err != nil || got != usecases.ClassificationApplied {
		t.Fatalf("redelivery: outcome=%s err=%v, want APPLIED", got, err)
	}
}
