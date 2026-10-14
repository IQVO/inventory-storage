//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// ADR 0039 against a real Postgres: the relocation moves both bins and every
// touched unit in ONE transaction with the relocations row and the
// StockRelocated outbox row, and two concurrent movers of the same stock can
// never both win.

const (
	stockRelocatedType = "com.warehouse.wms.inventory-storage.stock.StockRelocated"
	pgRelFrom          = shared.BinId("WH1-STOR-AMB-A01-01-01-A")
	pgRelTo            = shared.BinId("WH1-STOR-AMB-A02-01-01-A")
	pgRelTo2           = shared.BinId("WH1-STOR-AMB-A03-01-01-A")
)

var relocationAt = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

// seedRelocationWorld registers three WH1 bins and stows 10 units of
// SKU-REL into the source bin.
func seedRelocationWorld(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	bins := postgres.NewLocationRepo(pool)
	for _, id := range []shared.BinId{pgRelFrom, pgRelTo, pgRelTo2} {
		b, _ := location.NewBin(id, 50)
		if id == pgRelFrom {
			_ = b.Occupy(10)
		}
		if err := bins.Save(ctx, b); err != nil {
			t.Fatalf("seed bin %s: %v", id, err)
		}
	}
	unit, _ := stock.NewStockUnit("su-rel-src", "SKU-REL", pgRelFrom, 10)
	if err := postgres.NewStockRepo(pool).Save(ctx, unit); err != nil {
		t.Fatalf("seed unit: %v", err)
	}
}

func relocateUseCase(pool *pgxpool.Pool, encoders ...outboundkafka.Encoder) *usecases.RelocateStock {
	return &usecases.RelocateStock{
		Stock:       postgres.NewStockRepo(pool),
		Locations:   postgres.NewLocationRepo(pool),
		Relocations: postgres.NewRelocationRepo(pool),
		Events:      postgres.NewOutboxPublisher(pool, encoders...),
		Clock:       &fixedClock{t: relocationAt},
		UnitOfWork:  postgres.NewUnitOfWork(pool),
	}
}

func pgRelocateCmd(to shared.BinId, qty int) usecases.RelocateStockCommand {
	return usecases.RelocateStockCommand{
		SiteID: "WH1", SKU: "SKU-REL", FromBin: pgRelFrom, ToBin: to, Quantity: shared.Quantity(qty),
		Reason: relocation.ReasonSlotting, SourceRef: "plan-1/move-1",
	}
}

func binOccupied(t *testing.T, pool *pgxpool.Pool, id shared.BinId) int {
	t.Helper()
	b, err := postgres.NewLocationRepo(pool).FindByID(context.Background(), id)
	if err != nil || b == nil {
		t.Fatalf("load bin %s: %v", id, err)
	}
	return b.Occupied().Int()
}

func skuOnHandAt(t *testing.T, pool *pgxpool.Pool, bin shared.BinId) int {
	t.Helper()
	var total int
	if err := pool.QueryRow(context.Background(),
		`SELECT COALESCE(SUM(quantity), 0) FROM stock_units WHERE sku = 'SKU-REL' AND bin_id = $1`, bin.String()).Scan(&total); err != nil {
		t.Fatalf("sum stock at %s: %v", bin, err)
	}
	return total
}

func relocatedOutboxRows(t *testing.T, pool *pgxpool.Pool) []classifiedOutboxRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT topic, key, value FROM outbox_events WHERE event_type = $1 ORDER BY id`, stockRelocatedType)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	var out []classifiedOutboxRow
	for rows.Next() {
		var r classifiedOutboxRow
		var key []byte
		if err := rows.Scan(&r.topic, &key, &r.value); err != nil {
			t.Fatalf("scan: %v", err)
		}
		r.key = string(key)
		out = append(out, r)
	}
	return out
}

func TestRelocation_MovesTheLedgerAndEnqueuesTheExactCloudEvent(t *testing.T) {
	pool := outboxDB(t)
	seedRelocationWorld(t, pool)
	ctx := context.Background()

	rel, err := relocateUseCase(pool, outboundkafka.NewPublisher(nil, nil)).Execute(ctx, pgRelocateCmd(pgRelTo, 4))
	if err != nil {
		t.Fatalf("relocate: %v", err)
	}

	if got := skuOnHandAt(t, pool, pgRelFrom); got != 6 {
		t.Fatalf("on hand at source = %d, want 6", got)
	}
	if got := skuOnHandAt(t, pool, pgRelTo); got != 4 {
		t.Fatalf("on hand at destination = %d, want 4", got)
	}
	if binOccupied(t, pool, pgRelFrom) != 6 || binOccupied(t, pool, pgRelTo) != 4 {
		t.Fatal("bin occupancy did not move with the stock")
	}
	dest, _ := postgres.NewStockRepo(pool).FindByID(ctx, rel.DestinationStockUnitID())
	if dest == nil || dest.SiteID() != "WH1" || dest.BinID() != pgRelTo {
		t.Fatalf("destination unit = %+v, want WH1 custody in %s", dest, pgRelTo)
	}

	assertStoredRelocation(t, pool, rel)
	assertStockRelocatedOutboxRow(t, pool, rel)
}

func assertStoredRelocation(t *testing.T, pool *pgxpool.Pool, rel *relocation.Relocation) {
	t.Helper()
	got, err := postgres.NewRelocationRepo(pool).FindByID(context.Background(), rel.ID())
	if err != nil || got == nil {
		t.Fatalf("FindByID: %v, %v", got, err)
	}
	if got.SiteID() != "WH1" || got.SKU() != "SKU-REL" || got.FromBin() != pgRelFrom || got.ToBin() != pgRelTo ||
		got.Quantity() != 4 || got.Reason() != relocation.ReasonSlotting || got.SourceRef() != "plan-1/move-1" {
		t.Fatalf("stored relocation lost a field: %+v", got)
	}
	if got.DestinationStockUnitID() != rel.DestinationStockUnitID() || !got.OccurredAt().Equal(relocationAt) {
		t.Fatalf("stored outcome: dest %q at %v", got.DestinationStockUnitID(), got.OccurredAt())
	}
	if !reflect.DeepEqual(got.Legs(), []relocation.Leg{{StockUnitID: "su-rel-src", Quantity: 4}}) {
		t.Fatalf("stored legs = %+v", got.Legs())
	}
}

func assertStockRelocatedOutboxRow(t *testing.T, pool *pgxpool.Pool, rel *relocation.Relocation) {
	t.Helper()
	rows := relocatedOutboxRows(t, pool)
	if len(rows) != 1 {
		t.Fatalf("StockRelocated outbox rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.topic != outboundkafka.Topic || r.key != rel.ID() {
		t.Fatalf("row topic %q key %q, want %q / %q", r.topic, r.key, outboundkafka.Topic, rel.ID())
	}
	e, err := cloudevents.Decode(r.value)
	if err != nil {
		t.Fatalf("outbox value is not a CloudEvent: %v", err)
	}
	if e.SpecVersion() != "1.0" || e.Type() != stockRelocatedType || e.Subject() != rel.ID() ||
		e.Source() != "/warehouse/inventory-storage" || e.DataContentType() != "application/json" ||
		e.DataSchema() != "urn:warehouse:inventory-storage:events:StockRelocated:v1" || !e.Time().Equal(relocationAt) || e.ID() == "" {
		t.Fatalf("envelope = %s", e.String())
	}
	var data map[string]any
	if err := json.Unmarshal(e.Data(), &data); err != nil {
		t.Fatalf("data: %v", err)
	}
	want := map[string]any{
		"relocation_id": rel.ID(), "site_code": "WH1", "sku": "SKU-REL",
		"from_bin": string(pgRelFrom), "to_bin": string(pgRelTo), "quantity": float64(4),
		"reason": "SLOTTING", "source_ref": "plan-1/move-1", "occurred_at": "2026-10-13T12:00:00Z",
	}
	if !reflect.DeepEqual(data, want) {
		t.Fatalf("data = %v\nwant  %v", data, want)
	}
}

// failingRelocatedEncoder fails to encode StockRelocated, so the outbox
// insert — the LAST write of the relocation — fails.
type failingRelocatedEncoder struct{}

var errEncode = errors.New("encode failed")

func (failingRelocatedEncoder) Encode(_ context.Context, e shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	if _, ok := e.(shared.StockRelocated); ok {
		return nil, errEncode
	}
	return nil, nil
}

func TestRelocation_IsAtomic_AFailedOutboxWriteRollsBackTheWholeMove(t *testing.T) {
	pool := outboxDB(t)
	seedRelocationWorld(t, pool)

	_, err := relocateUseCase(pool, failingRelocatedEncoder{}).Execute(context.Background(), pgRelocateCmd(pgRelTo, 4))
	if !errors.Is(err, errEncode) {
		t.Fatalf("err = %v, want the encoder's error", err)
	}
	if skuOnHandAt(t, pool, pgRelFrom) != 10 || skuOnHandAt(t, pool, pgRelTo) != 0 {
		t.Fatal("stock moved although the transaction failed")
	}
	if binOccupied(t, pool, pgRelFrom) != 10 || binOccupied(t, pool, pgRelTo) != 0 {
		t.Fatal("bin occupancy changed although the transaction failed")
	}
	var n int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM relocations`).Scan(&n)
	if n != 0 {
		t.Fatalf("relocations rows = %d, want 0", n)
	}
	var units int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM stock_units WHERE sku = 'SKU-REL'`).Scan(&units)
	if units != 1 {
		t.Fatalf("stock units = %d, want 1 (no destination unit)", units)
	}
}

// TestRelocation_ConcurrentMoversOfTheSameStock: two movers each want 6 of
// the 10 units at the source. Both read the same versions; the version guard
// (ADR 0019) lets exactly one commit. The loser fails with
// ErrConcurrentModification (or ErrInsufficientUsable when it read after the
// winner committed) and the ledger never goes negative.
func TestRelocation_ConcurrentMoversOfTheSameStock(t *testing.T) {
	pool := outboxDB(t)
	seedRelocationWorld(t, pool)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	for i, to := range []shared.BinId{pgRelTo, pgRelTo2} {
		wg.Add(1)
		go func(i int, to shared.BinId) {
			defer wg.Done()
			<-start
			_, errs[i] = relocateUseCase(pool, outboundkafka.NewPublisher(nil, nil)).Execute(context.Background(), pgRelocateCmd(to, 6))
		}(i, to)
	}
	close(start)
	wg.Wait()

	wins := 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, usecases.ErrConcurrentModification), errors.Is(err, stock.ErrInsufficientUsable):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("winners = %d (errors %v), want exactly 1", wins, errs)
	}
	if got := skuOnHandAt(t, pool, pgRelFrom); got != 4 {
		t.Fatalf("on hand at source = %d, want 4", got)
	}
	if moved := skuOnHandAt(t, pool, pgRelTo) + skuOnHandAt(t, pool, pgRelTo2); moved != 6 {
		t.Fatalf("moved = %d, want 6 (stock was created or lost)", moved)
	}
	if binOccupied(t, pool, pgRelFrom) != 4 {
		t.Fatalf("source occupancy = %d, want 4", binOccupied(t, pool, pgRelFrom))
	}
	if rows := relocatedOutboxRows(t, pool); len(rows) != 1 {
		t.Fatalf("StockRelocated outbox rows = %d, want 1", len(rows))
	}
}

func TestRelocationRepo_UnknownIDIsNil(t *testing.T) {
	pool := outboxDB(t)
	got, err := postgres.NewRelocationRepo(pool).FindByID(context.Background(), "rel-missing")
	if err != nil || got != nil {
		t.Fatalf("FindByID(missing) = %v, %v; want nil, nil", got, err)
	}
}

func TestRelocationRepo_DuplicateIDIsRejected(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewRelocationRepo(pool)
	spec := relocation.Spec{ID: "rel-dup", SiteID: "WH1", SKU: "S", FromBin: pgRelFrom, ToBin: pgRelTo, Quantity: 1, Reason: relocation.ReasonManual}
	rel := relocation.Rehydrate(spec, []relocation.Leg{{StockUnitID: "su-1", Quantity: 1}}, "su-2", relocationAt)
	if err := repo.Save(context.Background(), rel); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if err := repo.Save(context.Background(), rel); err == nil {
		t.Fatal("a second save of the same relocation id must fail (a relocation is immutable)")
	}
}
