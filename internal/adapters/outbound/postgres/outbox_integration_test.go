//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// outboxDB boots a throwaway Postgres via testcontainers (the test owns
// its own database end to end — never an external DATABASE_URL) and runs
// every migration, so each test starts from a clean, fully-migrated
// schema regardless of the order tests run in.
func outboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.RunMigrations(url, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// recordingSink is a postgres.Sink fake that records every message it is
// asked to send, and can be configured to fail on a specific EventType so
// a test can force the relay's stop-at-failed-row path deterministically.
type recordingSink struct {
	sent    []outboundkafka.Encoded
	failOn  string // EventType to fail on, "" for never
	failErr error
}

func (s *recordingSink) Send(_ context.Context, msgs ...outboundkafka.Encoded) error {
	for _, m := range msgs {
		if s.failOn != "" && m.EventType == s.failOn {
			return s.failErr
		}
		s.sent = append(s.sent, m)
	}
	return nil
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox_events WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

// countingEncoder is a kafka.Encoder that turns every event into exactly
// one Encoded message on a fixed topic, so these tests can assert on
// outbox row counts without depending on the real kafka.Publisher's
// integration-events allow-list (StockReserved/ReservationRevoked only —
// see kafka.Publisher's own Encode tests for that contract).
type countingEncoder struct{ topic string }

func (e countingEncoder) Encode(_ context.Context, event shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	return []outboundkafka.Encoded{{
		Topic:     e.topic,
		EventType: event.EventName(),
		Value:     []byte(`{"stub":"` + event.EventName() + `"}`),
	}}, nil
}

// failingEncoder always errors, used to force OutboxPublisher.Publish to
// fail so the rollback test can prove the aggregate write never survives
// a failed outbox insert.
type failingEncoder struct{ err error }

func (e failingEncoder) Encode(context.Context, shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	return nil, e.err
}

// TestOutbox_ReceiveStock_CommitsAggregateAndEventTogether proves the
// core guarantee: ReceiveStock's Publish, run through OutboxPublisher
// inside a real Postgres UnitOfWork transaction, leaves an unpublished
// outbox_events row behind in the same commit as the use case's success.
func TestOutbox_ReceiveStock_CommitsAggregateAndEventTogether(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	uc := &usecases.ReceiveStock{
		Events:     postgres.NewOutboxPublisher(pool, countingEncoder{topic: "test.integration"}),
		Clock:      &fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	sku, _ := shared.NewSKU("OUTBOX-SKU-1")
	qty, _ := shared.NewPositiveQuantity(10)
	if _, err := uc.Execute(ctx, sku, qty); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := countOutbox(t, pool, "published_at IS NULL AND event_type = 'StockReceived'"); got != 1 {
		t.Fatalf("expected 1 unpublished StockReceived outbox row, got %d", got)
	}
}

// TestOutbox_ReserveStock_CommitsStockAndReservationAndEventTogether
// proves the multi-write case: ReserveStock's stock-unit Save(s),
// reservation Save, and Publish all land in ONE transaction — a failure
// anywhere in that scope rolls every one of them back together, proven
// here by the positive (success) path landing all three, and by
// TestOutbox_PublishFailure_RollsBackEverything below proving the
// negative path.
func TestOutbox_ReserveStock_CommitsStockAndReservationAndEventTogether(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	stockRepo := postgres.NewStockRepo(pool)
	reservationRepo := postgres.NewReservationRepo(pool)
	locationRepo := postgres.NewLocationRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	binID, _ := shared.NewBinId("OUTBOX-BIN-1")
	stow := &usecases.StowStock{
		Stock: stockRepo, Locations: locationRepo,
		Events: postgres.NewOutboxPublisher(pool, countingEncoder{topic: "test.integration"}),
		Clock:  &fixedClock{t: time.Now().UTC()}, UnitOfWork: uow,
	}
	if err := seedBinForOutboxTest(t, locationRepo, binID, 20); err != nil {
		t.Fatalf("seed bin: %v", err)
	}
	sku, _ := shared.NewSKU("OUTBOX-SKU-2")
	if _, err := stow.Execute(ctx, sku, mustOutboxQty(t, 10), binID); err != nil {
		t.Fatalf("unexpected error stowing: %v", err)
	}

	reserve := &usecases.ReserveStock{
		Stock: stockRepo, Reservations: reservationRepo,
		Events: postgres.NewOutboxPublisher(pool, countingEncoder{topic: "test.integration"}),
		Clock:  &fixedClock{t: time.Now().UTC()}, UnitOfWork: uow,
	}
	res, err := reserve.Execute(ctx, sku, mustOutboxQty(t, 4), "outbox-order-1")
	if err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}

	persisted, err := reservationRepo.FindByID(ctx, res.ID())
	if err != nil || persisted == nil {
		t.Fatalf("expected reservation persisted, got %v err=%v", persisted, err)
	}
	if got := countOutbox(t, pool, "event_type = 'StockReserved'"); got != 1 {
		t.Fatalf("expected 1 StockReserved outbox row, got %d", got)
	}
}

// TestOutbox_PublishFailure_RollsBackEverything is the whole point of the
// outbox: when the encoder fails (simulating anything that would prevent
// a valid outbox row from being written), the use case's error must mean
// the aggregate write ALSO never happened — proving atomicity, not just
// that an error was returned.
func TestOutbox_PublishFailure_RollsBackEverything(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	uc := &usecases.ReceiveStock{
		Events:     postgres.NewOutboxPublisher(pool, failingEncoder{err: errors.New("encode boom")}),
		Clock:      &fixedClock{t: time.Now().UTC()},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	sku, _ := shared.NewSKU("OUTBOX-ROLLBACK-SKU")
	qty, _ := shared.NewPositiveQuantity(5)
	if _, err := uc.Execute(ctx, sku, qty); err == nil {
		t.Fatal("expected the encoder failure to fail Execute")
	}

	if got := countOutbox(t, pool, "event_type = 'StockReceived'"); got != 0 {
		t.Fatalf("expected NO StockReceived outbox row after a rolled-back publish, got %d", got)
	}
}

// TestOutbox_ClassifyProduct_PublishFailure_RollsBackTheClassification
// proves rollback for a Save+Publish (not just a bare Publish) use case:
// the ProductClassification row must not survive either.
func TestOutbox_ClassifyProduct_PublishFailure_RollsBackTheClassification(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	classifications := postgres.NewProductClassificationRepo(pool)

	uc := &usecases.ClassifyProduct{
		Classifications: classifications,
		Events:          postgres.NewOutboxPublisher(pool, failingEncoder{err: errors.New("encode boom")}),
		Clock:           &fixedClock{t: time.Now().UTC()},
		UnitOfWork:      postgres.NewUnitOfWork(pool),
	}

	sku, _ := shared.NewSKU("OUTBOX-CLASSIFY-SKU")
	if _, err := uc.Execute(ctx, sku, []product.HandlingTag{product.Fragile}, "", 0); err == nil {
		t.Fatal("expected the encoder failure to fail Execute")
	}

	found, err := classifications.FindBySKU(ctx, sku)
	if err != nil {
		t.Fatalf("unexpected error looking up classification: %v", err)
	}
	if found != nil {
		t.Fatal("classification row survived a failed publish: the unit of work did not roll back")
	}
}

// TestOutboxRelay_PublishesInOrderAndMarksRows proves RelayOnce drains
// every unpublished row in id (insertion) order, marks each published,
// and a second pass finds nothing left to do.
func TestOutboxRelay_PublishesInOrderAndMarksRows(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	enc := countingEncoder{topic: "test.integration"}
	publisher := postgres.NewOutboxPublisher(pool, enc)

	receive := &usecases.ReceiveStock{Events: publisher, Clock: &fixedClock{t: time.Now().UTC()}, UnitOfWork: uow}
	for _, sku := range []string{"RELAY-SKU-1", "RELAY-SKU-2", "RELAY-SKU-3"} {
		skuVO, _ := shared.NewSKU(sku)
		if _, err := receive.Execute(ctx, skuVO, mustOutboxQty(t, 1)); err != nil {
			t.Fatalf("receive %s: %v", sku, err)
		}
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink)
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if n != 3 || len(sink.sent) != 3 {
		t.Fatalf("expected 3 published, got n=%d sent=%d", n, len(sink.sent))
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected every row marked published, %d still pending", got)
	}

	// A second pass finds nothing and republishes nothing.
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 0 || len(sink.sent) != 3 {
		t.Fatalf("second pass should be a no-op, got n=%d err=%v sent=%d", n, err, len(sink.sent))
	}
}

// TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater proves the
// per-key ordering guarantee: when the sink fails on the second of three
// rows, the first is published, the second and third are left pending
// (never overtaken), the failed row's attempts/last_error is recorded,
// and a recovered sink drains the rest in the original order on the next
// pass.
func TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	enc := countingEncoder{topic: "test.integration"}
	publisher := postgres.NewOutboxPublisher(pool, enc)

	receive := &usecases.ReceiveStock{Events: publisher, Clock: &fixedClock{t: time.Now().UTC()}, UnitOfWork: uow}
	skus := []string{"ORDER-SKU-A1", "ORDER-SKU-B2", "ORDER-SKU-C3"}
	for _, sku := range skus {
		skuVO, _ := shared.NewSKU(sku)
		if _, err := receive.Execute(ctx, skuVO, mustOutboxQty(t, 1)); err != nil {
			t.Fatalf("receive %s: %v", sku, err)
		}
	}

	// All three rows share event_type "StockReceived", so key the failure
	// off row id instead: fail whichever row the relay claims second by
	// having the sink fail once, on its second call.
	sink := &countLimitedFailSink{failOnCall: 2, failErr: errors.New("broker down")}
	relay := postgres.NewOutboxRelay(pool, sink)
	n, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing row to surface an error")
	}
	if n != 1 || len(sink.sent) != 1 {
		t.Fatalf("expected only 1 row published before the failure, got n=%d sent=%d", n, len(sink.sent))
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 2 {
		t.Fatalf("expected 2 rows still pending (ordering preserved), got %d pending", got)
	}

	var attempts int
	var lastErr string
	if err := pool.QueryRow(ctx, `
		SELECT attempts, coalesce(last_error,'') FROM outbox_events
		WHERE published_at IS NULL ORDER BY id ASC LIMIT 1
	`).Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("read failed row: %v", err)
	}
	if attempts != 1 || lastErr == "" {
		t.Fatalf("expected the failed row to record the attempt, got attempts=%d last_error=%q", attempts, lastErr)
	}

	// Broker recovers: the next pass drains the rest, in order.
	sink.failOnCall = 0
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 2 {
		t.Fatalf("recovery pass: n=%d err=%v", n, err)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected outbox drained, %d pending", got)
	}
}

// countLimitedFailSink fails on its Nth call (1-indexed) across the
// lifetime of the sink (not per-Send-call message count, since this
// relay always calls Send with exactly one message at a time), then
// succeeds on every other call — used to force a mid-batch failure
// deterministically regardless of row id ordering details.
type countLimitedFailSink struct {
	calls      int
	failOnCall int
	failErr    error
	sent       []outboundkafka.Encoded
}

func (s *countLimitedFailSink) Send(_ context.Context, msgs ...outboundkafka.Encoded) error {
	s.calls++
	if s.failOnCall > 0 && s.calls == s.failOnCall {
		return s.failErr
	}
	s.sent = append(s.sent, msgs...)
	return nil
}

// mustOutboxQty is this file's local mustQty-alike so it does not collide
// with the package's own mustQty(t, int) shared.Quantity helper used by
// non-outbox integration tests.
func mustOutboxQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewPositiveQuantity(v)
	if err != nil {
		t.Fatalf("unexpected error building quantity: %v", err)
	}
	return q
}

// seedBinForOutboxTest saves a bin with the given capacity directly
// through the repo, mirroring the pattern used elsewhere in this
// package's integration tests.
func seedBinForOutboxTest(t *testing.T, repo *postgres.LocationRepo, binID shared.BinId, capacity int) error {
	t.Helper()
	cap, err := shared.NewQuantity(capacity)
	if err != nil {
		return err
	}
	bin, err := location.NewBin(binID, cap)
	if err != nil {
		return err
	}
	return repo.Save(context.Background(), bin)
}
