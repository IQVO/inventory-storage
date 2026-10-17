//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ADR 0040 against a real Postgres: BinStockLevelChanged is enqueued in the
// SAME transaction as the ledger change, carries absolute levels and a
// persisted per-(bin, sku) version that is gapless across rollbacks and
// concurrent changes.

const binLevelType = "com.warehouse.wms.inventory-storage.stock.BinStockLevelChanged"

type levelRow struct {
	Key     string
	Subject string
	Data    struct {
		SiteCode string `json:"site_code"`
		Bin      string `json:"bin"`
		SKU      string `json:"sku"`
		OnHand   int    `json:"on_hand"`
		Reserved int    `json:"reserved"`
		Version  int64  `json:"version"`
		Reason   string `json:"reason"`
	}
}

func levelRows(t *testing.T, pool *pgxpool.Pool) []levelRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT key, value FROM outbox_events WHERE event_type = $1 ORDER BY id`, binLevelType)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	var out []levelRow
	for rows.Next() {
		var key, value []byte
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatalf("scan: %v", err)
		}
		e, err := cloudevents.Decode(value)
		if err != nil {
			t.Fatalf("outbox value is not a CloudEvent: %v", err)
		}
		r := levelRow{Key: string(key), Subject: e.Subject()}
		if err := json.Unmarshal(e.Data(), &r.Data); err != nil {
			t.Fatalf("data: %v", err)
		}
		if e.Type() != binLevelType || e.DataSchema() != "urn:warehouse:inventory-storage:events:BinStockLevelChanged:v1" {
			t.Fatalf("envelope = %s", e.String())
		}
		out = append(out, r)
	}
	return out
}

func levelVersion(t *testing.T, pool *pgxpool.Pool, bin shared.BinId, sku string) (int64, bool) {
	t.Helper()
	var v int64
	err := pool.QueryRow(context.Background(),
		`SELECT version FROM bin_sku_levels WHERE bin_id = $1 AND sku = $2`, bin.String(), sku).Scan(&v)
	if err != nil {
		return 0, false
	}
	return v, true
}

func levelEmitter(pool *pgxpool.Pool, encoders ...outboundkafka.Encoder) *usecases.BinLevelEmitter {
	return &usecases.BinLevelEmitter{
		Ledger:    postgres.NewBinLevelLedger(pool),
		Locations: postgres.NewLocationRepo(pool),
		Events:    postgres.NewOutboxPublisher(pool, encoders...),
		Clock:     &fixedClock{t: relocationAt},
	}
}

func relocateWithLevels(pool *pgxpool.Pool, encoders ...outboundkafka.Encoder) *usecases.RelocateStock {
	uc := relocateUseCase(pool, encoders...)
	uc.Levels = levelEmitter(pool, encoders...)
	return uc
}

// A relocation raises two events, source then destination, with absolute
// levels; a second one advances both versions.
func TestBinLevel_RelocationEnqueuesSourceThenDestinationWithAbsoluteLevels(t *testing.T) {
	pool := outboxDB(t)
	seedRelocationWorld(t, pool)
	enc := outboundkafka.NewPublisher(nil, nil)
	ctx := context.Background()

	if _, err := relocateWithLevels(pool, enc).Execute(ctx, pgRelocateCmd(pgRelTo, 4)); err != nil {
		t.Fatalf("relocate: %v", err)
	}
	if _, err := relocateWithLevels(pool, enc).Execute(ctx, pgRelocateCmd(pgRelTo, 3)); err != nil {
		t.Fatalf("relocate 2: %v", err)
	}

	got := levelRows(t, pool)
	if len(got) != 4 {
		t.Fatalf("level events = %d, want 4 (two per relocation)", len(got))
	}
	type want struct {
		bin             shared.BinId
		onHand, version int
	}
	for i, w := range []want{{pgRelFrom, 6, 1}, {pgRelTo, 4, 1}, {pgRelFrom, 3, 2}, {pgRelTo, 7, 2}} {
		r := got[i]
		if r.Data.Bin != w.bin.String() || r.Data.OnHand != w.onHand || r.Data.Version != int64(w.version) ||
			r.Data.Reserved != 0 || r.Data.Reason != "RELOCATE" || r.Data.SiteCode != "WH1" || r.Data.SKU != "SKU-REL" {
			t.Fatalf("event %d = %+v, want bin %s on_hand %d version %d RELOCATE", i, r.Data, w.bin, w.onHand, w.version)
		}
		if wantKey := w.bin.String() + ":SKU-REL"; r.Key != wantKey || r.Subject != wantKey {
			t.Fatalf("event %d key %q subject %q, want %q", i, r.Key, r.Subject, wantKey)
		}
	}
}

// failingLevelEncoder fails to encode the level event of one bin, so the
// outbox insert (the last write of the transaction) fails after the version
// bumps of BOTH bins have happened.
type failingLevelEncoder struct{ bin shared.BinId }

var errLevelEncode = errors.New("level encode failed")

func (f failingLevelEncoder) Encode(_ context.Context, e shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	if ev, ok := e.(shared.BinStockLevelChanged); ok && ev.BinID == f.bin {
		return nil, errLevelEncode
	}
	return nil, nil
}

func TestBinLevel_AFailedTransactionRollsBackTheVersionBumpsAndTheLedger(t *testing.T) {
	pool := outboxDB(t)
	seedRelocationWorld(t, pool)

	_, err := relocateWithLevels(pool, failingLevelEncoder{bin: pgRelTo}).Execute(context.Background(), pgRelocateCmd(pgRelTo, 4))
	if !errors.Is(err, errLevelEncode) {
		t.Fatalf("err = %v, want the encoder's error", err)
	}
	if skuOnHandAt(t, pool, pgRelFrom) != 10 || skuOnHandAt(t, pool, pgRelTo) != 0 {
		t.Fatal("stock moved although the transaction failed")
	}
	if _, ok := levelVersion(t, pool, pgRelFrom, "SKU-REL"); ok {
		t.Fatal("the source's version bump survived the rollback")
	}
	if _, ok := levelVersion(t, pool, pgRelTo, "SKU-REL"); ok {
		t.Fatal("the destination's version bump survived the rollback")
	}

	// The next successful change starts at 1: no gap left by the failure.
	if _, err := relocateWithLevels(pool, outboundkafka.NewPublisher(nil, nil)).Execute(context.Background(), pgRelocateCmd(pgRelTo, 4)); err != nil {
		t.Fatalf("relocate after failure: %v", err)
	}
	rows := levelRows(t, pool)
	if len(rows) != 2 || rows[0].Data.Version != 1 || rows[1].Data.Version != 1 {
		t.Fatalf("level events after a rollback = %+v, want two, both version 1", rows)
	}
}

// N concurrent transactions each change the same (bin, sku): the version row
// lock serialises them, so the versions are exactly 1..N, no gap and no
// duplicate, and the persisted counter ends at N.
func TestBinLevel_ConcurrentChangesOfOnePairGetGaplessVersions(t *testing.T) {
	const n = 12
	pool := outboxDB(t)
	seedRelocationWorld(t, pool)
	em := levelEmitter(pool, outboundkafka.NewPublisher(nil, nil))
	uow := postgres.NewUnitOfWork(pool)

	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = em.Atomically(context.Background(), uow, func(ctx context.Context) error {
				return em.Track(ctx, shared.BinLevelStow, pgRelFrom, "SKU-REL")
			})
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("change %d: %v", i, err)
		}
	}
	rows := levelRows(t, pool)
	versions := make([]int, 0, len(rows))
	for _, r := range rows {
		if r.Data.OnHand != 10 {
			t.Fatalf("on_hand = %d, want 10", r.Data.OnHand)
		}
		versions = append(versions, int(r.Data.Version))
	}
	sort.Ints(versions)
	if len(versions) != n {
		t.Fatalf("events = %d, want %d", len(versions), n)
	}
	for i, v := range versions {
		if v != i+1 {
			t.Fatalf("versions = %v, want 1..%d gapless", versions, n)
		}
	}
	if v, _ := levelVersion(t, pool, pgRelFrom, "SKU-REL"); v != n {
		t.Fatalf("persisted version = %d, want %d", v, n)
	}
}

// The version lives in the database: a fresh emitter and ledger (a restart)
// continue the sequence instead of starting over.
func TestBinLevel_VersionSurvivesARestart(t *testing.T) {
	pool := outboxDB(t)
	seedRelocationWorld(t, pool)
	uow := postgres.NewUnitOfWork(pool)

	for i := 0; i < 3; i++ {
		em := levelEmitter(pool, outboundkafka.NewPublisher(nil, nil)) // a "new process" each time
		if err := em.Atomically(context.Background(), uow, func(ctx context.Context) error {
			return em.Track(ctx, shared.BinLevelCount, pgRelFrom, "SKU-REL")
		}); err != nil {
			t.Fatalf("change %d: %v", i, err)
		}
	}
	rows := levelRows(t, pool)
	if len(rows) != 3 || rows[0].Data.Version != 1 || rows[1].Data.Version != 2 || rows[2].Data.Version != 3 {
		t.Fatalf("events = %+v, want versions 1, 2, 3", rows)
	}
}

// A bin the layout cannot place (not a location code, not registered) raises
// no event and consumes no version.
func TestBinLevel_UnknownBinRaisesNothing(t *testing.T) {
	pool := outboxDB(t)
	em := levelEmitter(pool, outboundkafka.NewPublisher(nil, nil))

	err := em.Atomically(context.Background(), postgres.NewUnitOfWork(pool), func(ctx context.Context) error {
		return em.Track(ctx, shared.BinLevelStow, "no-such-bin", "SKU-REL")
	})
	if err != nil {
		t.Fatalf("track: %v", err)
	}
	if rows := levelRows(t, pool); len(rows) != 0 {
		t.Fatalf("events = %+v, want none", rows)
	}
	if _, ok := levelVersion(t, pool, "no-such-bin", "SKU-REL"); ok {
		t.Fatal("a version was consumed for an unplaceable bin")
	}
}

// Migration 0037 is additive and reversible: up creates the table and the
// level-read index, down removes both and leaves the schema at 0036, and it
// re-applies.
func TestMigration0037_UpAndDown(t *testing.T) {
	m, err := migrate.New("file://"+migrationsDir(t), emptyDB(t))
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(36); err != nil {
		t.Fatalf("migrate to 0036: %v", err)
	}
	if err := m.Steps(1); err != nil {
		t.Fatalf("up 0037: %v", err)
	}
	if v, dirty, _ := m.Version(); v != 37 || dirty {
		t.Fatalf("version = %d dirty=%v, want 37 clean", v, dirty)
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 0037: %v", err)
	}
	if v, dirty, _ := m.Version(); v != 36 || dirty {
		t.Fatalf("version after down = %d dirty=%v, want 36 clean", v, dirty)
	}
	if err := m.Steps(1); err != nil {
		t.Fatalf("re-apply 0037: %v", err)
	}
}

func TestMigration0037_CreatesTheTableAndIndex(t *testing.T) {
	pool := outboxDB(t)
	var tbl, idx *string
	if err := pool.QueryRow(context.Background(),
		`SELECT to_regclass('bin_sku_levels')::text, to_regclass('idx_stock_units_bin_sku')::text`).Scan(&tbl, &idx); err != nil {
		t.Fatalf("regclass: %v", err)
	}
	if tbl == nil || idx == nil {
		t.Fatalf("table %v index %v, want both present", tbl, idx)
	}
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO bin_sku_levels (bin_id, sku, version) VALUES ('B', 'S', 0)`); err == nil {
		t.Fatal("version 0 must violate the CHECK (versions start at 1)")
	}
}
