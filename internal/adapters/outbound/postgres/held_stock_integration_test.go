//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

var heldBase = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)

func heldRepo(t *testing.T) (*postgres.HeldStockRepo, *pgxpool.Pool) {
	t.Helper()
	pool := outboxDB(t)
	return postgres.NewHeldStockRepo(pool), pool
}

func newHeldUnit(t *testing.T, repo *postgres.HeldStockRepo, line int, sku string, qty int, reason stock.HoldReason, at time.Time) *stock.HeldUnit {
	t.Helper()
	ctx := context.Background()
	id, err := repo.NextID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h, err := stock.NewHeldUnit(id, stock.HeldSource{
		ReceiptID: "rcpt-1", ASNNumber: "ASN-1", LineNo: line, SKU: shared.SKU(sku), Quantity: mustQty(t, qty),
		OwnerRef: "seller-a", SSCC: "00012345678901234567",
	}, reason, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, h); err != nil {
		t.Fatalf("save: %v", err)
	}
	return h
}

func TestHeldStock_RoundTripKeepsSourceAndProvenance(t *testing.T) {
	repo, _ := heldRepo(t)
	ctx := context.Background()
	h := newHeldUnit(t, repo, 3, "SKU-H1", 4, stock.ReasonDamaged, heldBase)

	got, err := repo.FindByID(ctx, h.ID())
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.State() != stock.StateHeld || got.Reason() != stock.ReasonDamaged || got.Quantity().Int() != 4 || got.Usable() != 0 {
		t.Fatalf("state %s reason %s qty %d usable %d", got.State(), got.Reason(), got.Quantity().Int(), got.Usable())
	}
	if got.ReceiptID() != "rcpt-1" || got.ASNNumber() != "ASN-1" || got.LineNo() != 3 || got.SKU() != "SKU-H1" ||
		got.OwnerRef() != "seller-a" || got.SSCC() != "00012345678901234567" || !got.HeldAt().Equal(heldBase) {
		t.Fatalf("source lost: %+v", got)
	}
	if got.Version() != 1 || !got.DecidedAt().IsZero() || got.Disposition() != "" {
		t.Fatalf("fresh row: version %d decidedAt %v disposition %q", got.Version(), got.DecidedAt(), got.Disposition())
	}
	if missing, err := repo.FindByID(ctx, "hu-nope"); err != nil || missing != nil {
		t.Fatalf("unknown id = %v %v, want nil nil", missing, err)
	}
}

func TestHeldStock_DispositionIsPersistedAsAudit(t *testing.T) {
	repo, _ := heldRepo(t)
	ctx := context.Background()
	h := newHeldUnit(t, repo, 1, "SKU-H1", 4, stock.ReasonDamaged, heldBase)

	decided := heldBase.Add(2 * time.Hour)
	if err := h.Decide(stock.DispositionReturnToVendor, "", decided); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, h); err != nil {
		t.Fatalf("save decision: %v", err)
	}
	got, _ := repo.FindByID(ctx, h.ID())
	if got.State() != stock.StateRemoved || got.Disposition() != stock.DispositionReturnToVendor || !got.DecidedAt().Equal(decided) || got.Version() != 2 {
		t.Fatalf("after RTV: %s %s %v v%d", got.State(), got.Disposition(), got.DecidedAt(), got.Version())
	}
	if got.Quantity().Int() != 4 {
		t.Fatalf("quantity = %d: a disposition conserves the booked quantity", got.Quantity().Int())
	}
}

func TestHeldStock_ReworkKeepsItHeldWithReasonUpdated(t *testing.T) {
	repo, _ := heldRepo(t)
	ctx := context.Background()
	h := newHeldUnit(t, repo, 1, "SKU-H1", 4, stock.ReasonDamaged, heldBase)
	if err := h.Decide(stock.DispositionRework, stock.ReasonSuspect, heldBase.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, h); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.FindByID(ctx, h.ID())
	if got.State() != stock.StateHeld || got.Reason() != stock.ReasonSuspect || got.ReworkCount() != 1 || got.Disposition() != stock.DispositionRework {
		t.Fatalf("after REWORK: %s %s %d %s", got.State(), got.Reason(), got.ReworkCount(), got.Disposition())
	}
	listed, _ := repo.ListHeld(ctx, stock.HeldQuery{Reason: stock.ReasonSuspect})
	if len(listed) != 1 {
		t.Fatalf("a reworked unit is still held and listed under its new reason: %d", len(listed))
	}
}

// Two writers load the same unit; the second save must fail, not overwrite.
func TestHeldStock_ConcurrentDecisionsAreVersionGuarded(t *testing.T) {
	repo, _ := heldRepo(t)
	ctx := context.Background()
	h := newHeldUnit(t, repo, 1, "SKU-H1", 4, stock.ReasonDamaged, heldBase)

	a, _ := repo.FindByID(ctx, h.ID())
	b, _ := repo.FindByID(ctx, h.ID())
	if err := a.Decide(stock.DispositionScrap, "", heldBase.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := b.Decide(stock.DispositionRelease, "", heldBase.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("first writer: %v", err)
	}
	if err := repo.Save(ctx, b); !errors.Is(err, usecases.ErrConcurrentModification) {
		t.Fatalf("second writer = %v, want ErrConcurrentModification", err)
	}
	got, _ := repo.FindByID(ctx, h.ID())
	if got.Disposition() != stock.DispositionScrap {
		t.Fatalf("the losing writer overwrote the decision: %s", got.Disposition())
	}
}

func TestHeldStock_ListFiltersOrdersAndPages(t *testing.T) {
	repo, _ := heldRepo(t)
	ctx := context.Background()
	oldest := newHeldUnit(t, repo, 1, "SKU-A", 1, stock.ReasonDamaged, heldBase)
	middle := newHeldUnit(t, repo, 2, "SKU-B", 2, stock.ReasonQCPending, heldBase.Add(time.Hour))
	newest := newHeldUnit(t, repo, 3, "SKU-A", 3, stock.ReasonDamaged, heldBase.Add(2*time.Hour))
	closed := newHeldUnit(t, repo, 4, "SKU-A", 9, stock.ReasonDamaged, heldBase.Add(30*time.Minute))
	if err := closed.Decide(stock.DispositionScrap, "", heldBase.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, closed); err != nil {
		t.Fatal(err)
	}

	ids := func(q stock.HeldQuery) []string {
		t.Helper()
		got, err := repo.ListHeld(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(got))
		for _, h := range got {
			out = append(out, h.ID())
		}
		return out
	}
	eq := func(name string, got []string, want ...*stock.HeldUnit) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: got %v, want %d units", name, got, len(want))
		}
		for i, h := range want {
			if got[i] != h.ID() {
				t.Fatalf("%s: got %v, want order %s...", name, got, h.ID())
			}
		}
	}
	eq("all, oldest first, closed excluded", ids(stock.HeldQuery{}), oldest, middle, newest)
	eq("by sku", ids(stock.HeldQuery{SKU: "SKU-A"}), oldest, newest)
	eq("by reason", ids(stock.HeldQuery{Reason: stock.ReasonQCPending}), middle)
	eq("older than", ids(stock.HeldQuery{HeldBefore: heldBase.Add(90 * time.Minute)}), oldest, middle)
	eq("limit", ids(stock.HeldQuery{Limit: 2}), oldest, middle)
	eq("after the first page", ids(stock.HeldQuery{After: &stock.HeldPageAfter{HeldAt: middle.HeldAt(), ID: middle.ID()}, Limit: 2}), newest)
}

func TestHeldStock_CartonMarksAreIdempotentAndIndependentOfOrder(t *testing.T) {
	repo, _ := heldRepo(t)
	ctx := context.Background()
	// A carton that arrived BEFORE the line event.
	if err := repo.MarkCarton(ctx, "rcpt-1", 1, "LPN-2", "SSCC-2"); err != nil {
		t.Fatal(err)
	}
	h := newHeldUnit(t, repo, 1, "SKU-H1", 4, stock.ReasonDamaged, heldBase)
	for range 2 {
		if err := repo.MarkCarton(ctx, "rcpt-1", 1, "LPN-1", "SSCC-1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.MarkCarton(ctx, "rcpt-1", 2, "LPN-OTHER", ""); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.FindByID(ctx, h.ID())
	if len(got.LPNs()) != 2 || got.LPNs()[0] != "LPN-2" || got.LPNs()[1] != "LPN-1" {
		t.Fatalf("lpns = %v, want [LPN-2 LPN-1] (arrival order, no duplicate, no other line's carton)", got.LPNs())
	}
	if got.Quantity().Int() != 4 {
		t.Fatalf("quantity = %d: a carton mark books nothing", got.Quantity().Int())
	}
}

func TestHeldStock_RejectsRowsThatBreakTheInvariants(t *testing.T) {
	_, pool := heldRepo(t)
	ctx := context.Background()
	bad := map[string]string{
		"unknown reason":       `INSERT INTO held_stock (id, receipt_id, line_no, sku, quantity, reason, state, held_at) VALUES ('x1','r',1,'S',1,'BROKEN','HELD',now())`,
		"zero quantity":        `INSERT INTO held_stock (id, receipt_id, line_no, sku, quantity, reason, state, held_at) VALUES ('x2','r',1,'S',0,'DAMAGED','HELD',now())`,
		"removed without disp": `INSERT INTO held_stock (id, receipt_id, line_no, sku, quantity, reason, state, held_at) VALUES ('x3','r',1,'S',1,'DAMAGED','REMOVED',now())`,
		"unknown disposition":  `INSERT INTO held_stock (id, receipt_id, line_no, sku, quantity, reason, state, disposition, decided_at, held_at) VALUES ('x4','r',1,'S',1,'DAMAGED','REMOVED','DONATE',now(),now())`,
		"line zero":            `INSERT INTO held_stock (id, receipt_id, line_no, sku, quantity, reason, state, held_at) VALUES ('x5','r',0,'S',1,'DAMAGED','HELD',now())`,
	}
	for name, stmt := range bad {
		if _, err := pool.Exec(ctx, stmt); err == nil {
			t.Errorf("%s: the schema accepted an invalid row", name)
		}
	}
}

// A HELD row that ever reached stock_units still must not count in a bin's
// level (defence in depth, ADR 0052): the ledger excludes HELD like UNLOCATED.
func TestHeldStock_BinLevelNeverCountsAHeldStockUnitRow(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	for _, stmt := range []string{
		`INSERT INTO bins (id, capacity, occupied) VALUES ('BIN-H', 100, 0)`,
		`INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state) VALUES ('su-ok', 'SKU-H', 'BIN-H', 5, 1, 'AVAILABLE')`,
		`INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state) VALUES ('su-held', 'SKU-H', 'BIN-H', 7, 0, 'HELD')`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	level, err := postgres.NewBinLevelLedger(pool).Advance(ctx, "BIN-H", "SKU-H", "")
	if err != nil {
		t.Fatal(err)
	}
	if level.OnHand.Int() != 5 || level.Reserved.Int() != 1 {
		t.Fatalf("level = on_hand %d reserved %d, want 5 and 1 (the HELD row excluded)", level.OnHand.Int(), level.Reserved.Int())
	}
}

// Migration 0041 is additive and reversible: up creates the two tables and
// leaves existing rows untouched, down removes them and leaves the schema at
// 0040, and it re-applies.
func TestMigration0041_UpDownAndExistingRowsUntouched(t *testing.T) {
	url := emptyDB(t)
	m, err := migrate.New("file://"+migrationsDir(t), url)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Migrate(40); err != nil {
		t.Fatalf("migrate to 0040: %v", err)
	}

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, stmt := range []string{
		`INSERT INTO bins (id, capacity, occupied) VALUES ('BIN-OLD', 10, 3)`,
		`INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state) VALUES ('su-old', 'SKU-OLD', 'BIN-OLD', 3, 0, 'AVAILABLE')`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	snapshot := func() string {
		var s string
		if err := pool.QueryRow(ctx, `SELECT id||'|'||sku||'|'||bin_id||'|'||quantity||'|'||reserved||'|'||state||'|'||COALESCE(site_id,'')||'|'||version FROM stock_units`).Scan(&s); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		return s
	}
	before := snapshot()

	if err := m.Steps(1); err != nil {
		t.Fatalf("up 0041: %v", err)
	}
	if v, dirty, _ := m.Version(); v != 41 || dirty {
		t.Fatalf("version = %d dirty=%v, want 41 clean", v, dirty)
	}
	var held, marks *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('held_stock')::text, to_regclass('held_stock_carton_marks')::text`).Scan(&held, &marks); err != nil || held == nil || marks == nil {
		t.Fatalf("tables after up: %v %v %v", held, marks, err)
	}
	if after := snapshot(); after != before {
		t.Fatalf("existing row changed by the migration:\n before %s\n after  %s", before, after)
	}

	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 0041: %v", err)
	}
	if v, dirty, _ := m.Version(); v != 40 || dirty {
		t.Fatalf("version after down = %d dirty=%v, want 40 clean", v, dirty)
	}
	if err := pool.QueryRow(ctx, `SELECT to_regclass('held_stock')::text, to_regclass('held_stock_carton_marks')::text`).Scan(&held, &marks); err != nil || held != nil || marks != nil {
		t.Fatalf("tables after down: %v %v %v, want both gone", held, marks, err)
	}
	if after := snapshot(); after != before {
		t.Fatalf("existing row changed by the rollback: %s", after)
	}
	if err := m.Steps(1); err != nil {
		t.Fatalf("re-apply 0041: %v", err)
	}
}
