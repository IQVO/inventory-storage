//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// Containers, owner-segregated stock and the packaging view (ADR 0045, 0046)
// against a real Postgres: the repositories, then the whole receipt -> stow ->
// break path through the real unit of work and outbox.

const (
	ctCaseGTIN = "24006381333935"
	ctEachGTIN = "04006381333931"
	ctCaseBin  = shared.BinId("S1-A-Z-1-1-1-1")
	ctLooseBin = shared.BinId("S1-A-Z-1-1-1-2")
	ctEachOnly = shared.BinId("S1-A-Z-1-1-1-3")
	ctNow      = "2026-10-08T14:00:00Z"
)

// handlingUnits is the event-fed local copy of each bin's handling unit (in the
// service it is filled from facility-layout events; the Kafka-fed copy is
// proven in the end-to-end test).
type handlingUnits map[shared.BinId]container.HandlingUnit

func (h handlingUnits) HandlingUnit(_ context.Context, bin shared.BinId) (container.HandlingUnit, error) {
	return h[bin], nil
}

func ctRegister(t *testing.T, lpn string) *container.Container {
	t.Helper()
	c, err := container.Register(container.RegisterSpec{
		LPN: lpn, SKU: "SKU-C", GTIN: ctCaseGTIN, UnitsPerPack: 12, Quantity: 12, OwnerRef: "seller-a", Lot: "L1",
		Expiry: "2027-03-01", ReceiptID: "R-1", LineNo: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestContainerRepo_AddIsCreateOnlyAndSaveIsVersionGuarded(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewContainerRepo(pool)

	added, err := repo.Add(ctx, ctRegister(t, "LPN-1"))
	if err != nil || !added {
		t.Fatalf("first add = %v %v", added, err)
	}
	if again, err := repo.Add(ctx, ctRegister(t, "LPN-1")); err != nil || again {
		t.Fatalf("a duplicate LPN must report false and write nothing, got %v %v", again, err)
	}

	loaded, err := repo.FindByLPN(ctx, "LPN-1")
	if err != nil || loaded == nil {
		t.Fatalf("find: %v %v", loaded, err)
	}
	if missing, err := repo.FindByLPN(ctx, "NOPE"); err != nil || missing != nil {
		t.Fatalf("unknown LPN = %v %v, want nil", missing, err)
	}

	stale, _ := repo.FindByLPN(ctx, "LPN-1")
	if err := loaded.Stow(ctBin(ctCaseBin), "S1", "su-1"); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := stale.Stow(ctBin(ctCaseBin), "S1", "su-1"); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, stale); !errors.Is(err, usecases.ErrConcurrentModification) {
		t.Fatalf("a stale save = %v, want ErrConcurrentModification", err)
	}

	reloaded, _ := repo.FindByLPN(ctx, "LPN-1")
	byBin, _ := repo.FindByBin(ctx, ctCaseBin)
	bySKU, _ := repo.FindStowedBySKU(ctx, "SKU-C", "seller-a")
	otherOwner, _ := repo.FindStowedBySKU(ctx, "SKU-C", "seller-b")
	fleet, _ := repo.FindStowedBySKU(ctx, "SKU-C", "")
	byUnit, _ := repo.FindByStockUnitID(ctx, "su-1")
	sum, _ := repo.SumQuantity(ctx, "R-1", 1)
	otherLine, _ := repo.SumQuantity(ctx, "R-1", 2)
	got := map[string]any{
		"byBin": len(byBin), "bySKU": len(bySKU), "otherOwner": len(otherOwner), "fleet": len(fleet),
		"byUnit": byUnit != nil && byUnit.LPN() == "LPN-1", "sum": sum.Int(), "otherLine": otherLine.Int(),
		"site": loaded.SiteID().String(), "owner": loaded.OwnerRef().String(), "lot": loaded.Lot(), "expiry": loaded.Expiry(), "version": reloaded.Version(),
	}
	want := map[string]any{
		"byBin": 1, "bySKU": 1, "otherOwner": 0, "fleet": 0, "byUnit": true, "sum": 12, "otherLine": 0,
		"site": "S1", "owner": "seller-a", "lot": "L1", "expiry": "2027-03-01", "version": 2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facts differ:\n got  %v\n want %v", got, want)
	}
}

func ctBin(b shared.BinId) shared.BinId { return b }

func TestReceiptLinkRepos_BookingsPendingLinksAndThePackagingView(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	bookings := postgres.NewReceiptBookingRepo(pool)
	pending := postgres.NewPendingLinkRepo(pool)
	view := postgres.NewPackagingViewRepo(pool)

	// bookings sum across events of one line
	_ = bookings.AddBooking(ctx, "R-1", 1, "SKU-C", "seller-a", 24)
	_ = bookings.AddBooking(ctx, "R-1", 1, "SKU-C", "seller-a", 12)
	booked, _ := bookings.Booked(ctx, "R-1", 1)
	none, _ := bookings.Booked(ctx, "R-1", 9)

	// pending links are idempotent and settle by line or by SKU
	link := container.PendingLink{ReceiptID: "R-1", LineNo: 1, LPN: "LPN-1", SSCC: "LPN-1", SKU: "SKU-C", GTIN: ctCaseGTIN, Quantity: 12, OwnerRef: "seller-a", Lot: "L1", Expiry: "2027-03-01"}
	_ = pending.Add(ctx, link)
	_ = pending.Add(ctx, link)
	byLine, _ := pending.FindPendingByLine(ctx, "R-1", 1)
	bySKU, _ := pending.FindPendingBySKU(ctx, "SKU-C")
	_ = pending.Reject(ctx, "R-1", 1, "LPN-1", "quantity-mismatch")
	afterReject, _ := pending.FindPendingByLine(ctx, "R-1", 1)
	_ = pending.Add(ctx, container.PendingLink{ReceiptID: "R-1", LineNo: 1, LPN: "LPN-2", SKU: "SKU-C", GTIN: ctCaseGTIN, Quantity: 12})
	_ = pending.Remove(ctx, "R-1", 1, "LPN-2")
	afterRemove, _ := pending.FindPendingBySKU(ctx, "SKU-C")

	// the packaging view: newest version wins, retired stays resolvable
	from := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	level := packaging.PackLevel{GTIN: gtin(t, ctCaseGTIN), SKU: "SKU-C", Level: packaging.LevelCase, UnitsPerPack: 12, ChildGTIN: ctEachGTIN, EffectiveFrom: from, Version: 2}
	first, _ := view.ApplyDeclared(ctx, level)
	again, _ := view.ApplyDeclared(ctx, level)
	older := level
	older.Version, older.UnitsPerPack = 1, 6
	stale, _ := view.ApplyDeclared(ctx, older)
	byGTIN, _ := view.FindByGTIN(ctx, ctCaseGTIN)
	active, _ := view.FindActive(ctx, "SKU-C", packaging.LevelCase, from.Add(time.Hour))
	before, _ := view.FindActive(ctx, "SKU-C", packaging.LevelCase, from.Add(-time.Hour))
	retired, _ := view.ApplyRetired(ctx, ctCaseGTIN, from.Add(48*time.Hour), 3)
	unknown, _ := view.ApplyRetired(ctx, "04006381333931", from, 3)
	afterRetire, _ := view.FindActive(ctx, "SKU-C", packaging.LevelCase, from.Add(72*time.Hour))
	stillByGTIN, _ := view.FindByGTIN(ctx, ctCaseGTIN)

	got := map[string]any{
		"booked": booked.Int(), "none": none.Int(), "byLine": len(byLine), "bySKU": len(bySKU), "afterReject": len(afterReject), "afterRemove": len(afterRemove),
		"first": first, "again": again, "stale": stale, "units": byGTIN.UnitsPerPack, "active": active != nil, "before": before != nil,
		"retired": retired, "unknown": unknown, "afterRetire": afterRetire != nil, "stillByGTIN": stillByGTIN != nil && stillByGTIN.EffectiveTo != nil,
	}
	want := map[string]any{
		"booked": 36, "none": 0, "byLine": 1, "bySKU": 1, "afterReject": 0, "afterRemove": 0,
		"first": true, "again": false, "stale": false, "units": 12, "active": true, "before": false,
		"retired": true, "unknown": false, "afterRetire": false, "stillByGTIN": true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facts differ:\n got  %v\n want %v", got, want)
	}
}

func TestStockRepo_OwnerAndPackStateRoundTripAndLegacyRowsStayFleetLoose(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewStockRepo(pool)
	if _, err := pool.Exec(ctx, `INSERT INTO bins (id, capacity, occupied) VALUES ('B-LEGACY', 100, 0)`); err != nil {
		t.Fatal(err)
	}
	// a row written by a version that knew nothing about owners
	if _, err := pool.Exec(ctx, `INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state) VALUES ('su-legacy', 'SKU-C', 'B-LEGACY', 5, 0, 'AVAILABLE')`); err != nil {
		t.Fatal(err)
	}
	sealed, _ := stock.NewSealedCaseUnit("su-case", "SKU-C", "B-LEGACY", 12, "S1", "seller-a")
	loose, _ := stock.NewLooseUnit("su-loose", "SKU-C", "B-LEGACY", 3, "S1", "seller-b")
	for _, u := range []*stock.StockUnit{sealed, loose} {
		if err := repo.Save(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	read := func(id string) map[string]any {
		u, err := repo.FindByID(ctx, id)
		if err != nil || u == nil {
			t.Fatalf("find %s: %v %v", id, u, err)
		}
		return map[string]any{"owner": u.OwnerRef().String(), "pack": string(u.PackState()), "qty": u.Quantity().Int()}
	}
	got := map[string]any{"legacy": read("su-legacy"), "case": read("su-case"), "loose": read("su-loose")}
	want := map[string]any{
		"legacy": map[string]any{"owner": "", "pack": "LOOSE", "qty": 5},
		"case":   map[string]any{"owner": "seller-a", "pack": "SEALED_CASE", "qty": 12},
		"loose":  map[string]any{"owner": "seller-b", "pack": "LOOSE", "qty": 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facts differ:\n got  %v\n want %v", got, want)
	}
}

// ------------------------------------------------------------- end to end ----

type ctWorld struct {
	pool       *pgxpool.Pool
	book       *usecases.BookInboundReceiptLine
	link       *usecases.LinkReceiptCarton
	pack       *usecases.ApplyPackLevel
	stow       *usecases.StowContainer
	brk        *usecases.BreakCase
	cases      *usecases.ReserveFullCases
	reserve    *usecases.ReserveStock
	usable     *usecases.GetUsable
	containers *postgres.ContainerRepo
	stocks     *postgres.StockRepo
}

func newCTWorld(t *testing.T) *ctWorld {
	t.Helper()
	pool := outboxDB(t)
	ctx := context.Background()
	enc := outboundkafka.NewPublisher(nil, nil)
	// StockReceived has a wire form on the analytics topic only.
	analytics := outboundkafka.NewAnalyticsPublisher(nil, postgres.NewReservationRepo(pool), nil)
	events := postgres.NewOutboxPublisher(pool, enc, analytics)
	clock := &fixedClock{t: time.Date(2026, 10, 8, 14, 0, 0, 0, time.UTC)}
	uow := postgres.NewUnitOfWork(pool)
	stocks := postgres.NewStockRepo(pool)
	locations := postgres.NewLocationRepo(pool)
	containers := postgres.NewContainerRepo(pool)
	levels := levelEmitter(pool, enc, analytics)
	linker := &usecases.ContainerLinker{
		Containers: containers, Pending: postgres.NewPendingLinkRepo(pool), Bookings: postgres.NewReceiptBookingRepo(pool),
		Packaging: postgres.NewPackagingViewRepo(pool), Events: events, Clock: clock,
	}
	hu := handlingUnits{ctCaseBin: container.HandlingCase, ctLooseBin: container.HandlingEach, ctEachOnly: container.HandlingEach}
	for _, id := range []shared.BinId{ctCaseBin, ctLooseBin, ctEachOnly} {
		b := location.RehydrateBinAtSite(id, 1000, 0, "S1", 1)
		if err := locations.Save(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	reserve := &usecases.ReserveStock{Stock: stocks, Reservations: postgres.NewReservationRepo(pool), Events: events, Clock: clock, UnitOfWork: uow, Levels: levels}
	return &ctWorld{
		pool: pool, containers: containers, stocks: stocks, reserve: reserve,
		book: &usecases.BookInboundReceiptLine{
			Receive:         &usecases.ReceiveStock{Events: events, Clock: clock, UnitOfWork: uow},
			ProcessedEvents: postgres.NewProcessedEventRepo(pool), UnitOfWork: uow, Bookings: postgres.NewReceiptBookingRepo(pool), Linker: linker,
		},
		link:   &usecases.LinkReceiptCarton{ProcessedEvents: postgres.NewProcessedEventRepo(pool), Pending: postgres.NewPendingLinkRepo(pool), Linker: linker, UnitOfWork: uow},
		pack:   &usecases.ApplyPackLevel{Projection: postgres.NewPackagingViewRepo(pool), ProcessedEvents: postgres.NewProcessedEventRepo(pool), Linker: linker, UnitOfWork: uow},
		stow:   &usecases.StowContainer{Containers: containers, Stock: stocks, Locations: locations, HandlingUnits: hu, Events: events, Clock: clock, UnitOfWork: uow, Levels: levels},
		brk:    &usecases.BreakCase{Containers: containers, Stock: stocks, Locations: locations, HandlingUnits: hu, Events: events, Clock: clock, UnitOfWork: uow, Levels: levels},
		cases:  &usecases.ReserveFullCases{Reserve: reserve, Containers: containers},
		usable: &usecases.GetUsable{Stock: stocks},
	}
}

func (w *ctWorld) declare(t *testing.T) {
	t.Helper()
	_, err := w.pack.Declared(context.Background(), usecases.PackLevelDeclared{
		EventID: "pm-1", SKU: "SKU-C", Level: "CASE", GTIN: ctCaseGTIN, ChildGTIN: ctEachGTIN, UnitsPerPack: 12,
		EffectiveFrom: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (w *ctWorld) line(t *testing.T, id, receipt string, qty int) {
	t.Helper()
	if _, err := w.book.Execute(context.Background(), usecases.InboundReceiptLine{
		EventID: id, ReceiptID: receipt, ASNNumber: "ASN-1", LineNo: 1, SKU: "SKU-C", Quantity: qty, Condition: "Good", OwnerRef: "seller-a",
	}); err != nil {
		t.Fatal(err)
	}
}

func (w *ctWorld) carton(t *testing.T, id, receipt, lpn string) {
	t.Helper()
	if _, _, err := w.link.Execute(context.Background(), usecases.ReceiptCarton{
		EventID: id, ReceiptID: receipt, ASNNumber: "ASN-1", SSCC: lpn, LPN: lpn, Sealed: true, OwnerRef: "seller-a",
		Contents: []usecases.ReceiptCartonContent{{LineNo: 1, SKU: "SKU-C", GTIN: ctCaseGTIN, Quantity: 12, Lot: "L1", Expiry: "2027-03-01", Condition: "Good"}},
	}); err != nil {
		t.Fatal(err)
	}
}

func ctCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func outboxCount(t *testing.T, pool *pgxpool.Pool, eventType string) int {
	return ctCount(t, pool, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, eventType)
}

const (
	evStockReceived = "com.warehouse.wms.inventory-storage.stock.StockReceived"
	evRegistered    = "com.warehouse.wms.inventory-storage.container.ContainerRegistered"
	evStowed        = "com.warehouse.wms.inventory-storage.container.ContainerStowed"
	evBroken        = "com.warehouse.wms.inventory-storage.container.CaseBroken"
	evEmptied       = "com.warehouse.wms.inventory-storage.container.ContainerEmptied"
)

// Three sealed cases and one line of 36 eaches, in either arrival order, give
// three containers and exactly one StockReceived; a redelivery of either event
// changes nothing.
func TestReceiptCartons_EitherArrivalOrderGivesThreeContainersAndOneBooking(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cartonsLast bool
	}{{"cartons first", false}, {"line first", true}} {
		t.Run(tc.name, func(t *testing.T) {
			w := newCTWorld(t)
			w.declare(t)
			cartons := func() {
				for _, lpn := range []string{"LPN-1", "LPN-2", "LPN-3"} {
					w.carton(t, "ev-"+lpn, "R-1", lpn)
				}
			}
			if tc.cartonsLast {
				w.line(t, "ev-line", "R-1", 36)
				cartons()
			} else {
				cartons()
				w.line(t, "ev-line", "R-1", 36)
			}
			// redeliveries: same ids, and a re-sent carton under a new id
			w.line(t, "ev-line", "R-1", 36)
			w.carton(t, "ev-LPN-1", "R-1", "LPN-1")
			w.carton(t, "ev-LPN-1-again", "R-1", "LPN-1")

			sum, _ := w.containers.SumQuantity(context.Background(), "R-1", 1)
			got := map[string]any{
				"containers": ctCount(t, w.pool, `SELECT count(*) FROM containers`), "sum": sum.Int(),
				"registered": outboxCount(t, w.pool, evRegistered), "stockReceived": outboxCount(t, w.pool, evStockReceived),
				"booked":  ctCount(t, w.pool, `SELECT booked_eaches FROM receipt_line_bookings WHERE receipt_id = 'R-1' AND line_no = 1`),
				"pending": ctCount(t, w.pool, `SELECT count(*) FROM container_pending_links`),
				"staged":  ctCount(t, w.pool, `SELECT coalesce(sum(quantity),0) FROM stock_units`),
			}
			want := map[string]any{"containers": 3, "sum": 36, "registered": 3, "stockReceived": 1, "booked": 36, "pending": 0, "staged": 0}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("facts differ:\n got  %v\n want %v", got, want)
			}
		})
	}
}

func TestReceiptCartons_CartonsBeyondTheBookingAndUnknownGTINsWaitInThePendingTable(t *testing.T) {
	w := newCTWorld(t)
	w.line(t, "ev-line", "R-1", 24)
	for _, lpn := range []string{"LPN-1", "LPN-2", "LPN-3"} {
		w.carton(t, "ev-"+lpn, "R-1", lpn)
	}
	noGTIN := map[string]int{
		"containers": ctCount(t, w.pool, `SELECT count(*) FROM containers`),
		"pending":    ctCount(t, w.pool, `SELECT count(*) FROM container_pending_links WHERE status = 'PENDING'`),
	}
	if !reflect.DeepEqual(noGTIN, map[string]int{"containers": 0, "pending": 3}) {
		t.Fatalf("without the pack level every carton waits: %v", noGTIN)
	}
	w.declare(t) // the GTIN becomes known: two fit the 24 booked, the third keeps waiting
	got := map[string]int{
		"containers": ctCount(t, w.pool, `SELECT count(*) FROM containers`),
		"pending":    ctCount(t, w.pool, `SELECT count(*) FROM container_pending_links WHERE status = 'PENDING'`),
		"registered": outboxCount(t, w.pool, evRegistered),
	}
	if !reflect.DeepEqual(got, map[string]int{"containers": 2, "pending": 1, "registered": 2}) {
		t.Fatalf("after the pack level: %v", got)
	}
}

func TestContainers_StowBreakAndReserveOverRealPostgres(t *testing.T) {
	w := newCTWorld(t)
	ctx := context.Background()
	w.declare(t)
	w.line(t, "ev-line", "R-1", 36)
	for _, lpn := range []string{"LPN-1", "LPN-2", "LPN-3"} {
		w.carton(t, "ev-"+lpn, "R-1", lpn)
	}

	// an each-only bin refuses a case; a case bin takes it and sets the site
	if _, err := w.stow.Execute(ctx, "LPN-1", ctEachOnly); !errors.Is(err, container.ErrBinHandlingUnitMismatch) {
		t.Fatalf("stow to an each-only bin = %v", err)
	}
	for _, lpn := range []string{"LPN-1", "LPN-2", "LPN-3"} {
		if _, err := w.stow.Execute(ctx, lpn, ctCaseBin); err != nil {
			t.Fatalf("stow %s: %v", lpn, err)
		}
	}
	owned, _ := w.usable.ExecuteFor(ctx, "SKU-C", "seller-a")
	fleet, _ := w.usable.Execute(ctx, "SKU-C")
	other, _ := w.usable.ExecuteFor(ctx, "SKU-C", "seller-b")

	// owner B cannot take owner A's case; owner A reserves one whole case
	_, errB := w.cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-C", Cases: 1, DemandRef: "D-b", OwnerRef: "seller-b"})
	res, errA := w.cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-C", Cases: 1, DemandRef: "D-a", OwnerRef: "seller-a"})
	if errA != nil {
		t.Fatalf("reserve a full case: %v", errA)
	}

	// the reserved case (the oldest, LPN-1) cannot be broken; LPN-3 can
	_, errReserved := w.brk.Execute(ctx, "LPN-1", ctLooseBin)
	broken, errBreak := w.brk.Execute(ctx, "LPN-3", ctLooseBin)
	_, errAgain := w.brk.Execute(ctx, "LPN-3", ctLooseBin)
	if errBreak != nil {
		t.Fatalf("break: %v", errBreak)
	}
	after, _ := w.usable.ExecuteFor(ctx, "SKU-C", "seller-a")

	got := map[string]any{
		"ownerUsable": owned.Usable.Int(), "ownerCases": owned.ReservableCases, "fleetUsable": fleet.Usable.Int(), "otherUsable": other.Usable.Int(),
		"ownerB": errors.Is(errB, usecases.ErrInsufficientUsable), "reserved": res.Quantity().Int(), "reservedOwner": res.OwnerRef().String(),
		"breakReserved": errors.Is(errReserved, container.ErrContainerReserved), "state": string(broken.State()),
		"breakAgain":  errors.Is(errAgain, container.ErrContainerNotSealed),
		"afterUsable": after.Usable.Int(), "afterLoose": after.LooseUsable.Int(), "afterCases": after.ReservableCases,
		"caseBinOccupied":  ctCount(t, w.pool, `SELECT occupied FROM bins WHERE id = $1`, ctCaseBin.String()),
		"looseBinOccupied": ctCount(t, w.pool, `SELECT occupied FROM bins WHERE id = $1`, ctLooseBin.String()),
		"looseRows":        ctCount(t, w.pool, `SELECT count(*) FROM stock_units WHERE bin_id = $1 AND state <> 'REMOVED' AND coalesce(pack_state,'LOOSE') = 'LOOSE' AND owner_ref = 'seller-a' AND quantity = 12`, ctLooseBin.String()),
		"stowed":           outboxCount(t, w.pool, evStowed), "caseBroken": outboxCount(t, w.pool, evBroken), "emptied": outboxCount(t, w.pool, evEmptied),
	}
	want := map[string]any{
		"ownerUsable": 36, "ownerCases": 3, "fleetUsable": 0, "otherUsable": 0,
		"ownerB": true, "reserved": 12, "reservedOwner": "seller-a",
		"breakReserved": true, "state": "EMPTY", "breakAgain": true,
		"afterUsable": 24 + 12 - 12, "afterLoose": 12, "afterCases": 1,
		"caseBinOccupied": 24, "looseBinOccupied": 12, "looseRows": 1,
		"stowed": 3, "caseBroken": 1, "emptied": 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facts differ:\n got  %v\n want %v", got, want)
	}
}

// The CloudEvents on the outbox: container events carry the contract's
// snake_case fields and BinStockLevelChanged names the owner only when there
// is one.
func TestContainers_OutboxEventsCarryTheContractPayloads(t *testing.T) {
	w := newCTWorld(t)
	ctx := context.Background()
	w.declare(t)
	w.line(t, "ev-line", "R-1", 12)
	w.carton(t, "ev-LPN-1", "R-1", "LPN-1")
	if _, err := w.stow.Execute(ctx, "LPN-1", ctCaseBin); err != nil {
		t.Fatal(err)
	}
	if _, err := w.brk.Execute(ctx, "LPN-1", ctLooseBin); err != nil {
		t.Fatal(err)
	}

	data := func(eventType string) map[string]any {
		var value []byte
		if err := w.pool.QueryRow(ctx, `SELECT value FROM outbox_events WHERE event_type = $1 ORDER BY id LIMIT 1`, eventType).Scan(&value); err != nil {
			t.Fatalf("%s: %v", eventType, err)
		}
		e, err := cloudevents.Decode(value)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := jsonUnmarshal(e.Data(), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	registered, stowed, broken := data(evRegistered), data(evStowed), data(evBroken)
	delete(broken, "at")
	delete(stowed, "at")
	got := map[string]any{"registered": registered, "stowed": stowed, "broken": broken}
	if got["registered"].(map[string]any)["lpn"] != "LPN-1" || got["registered"].(map[string]any)["owner_ref"] != "seller-a" ||
		got["registered"].(map[string]any)["units_per_pack"] != float64(12) {
		t.Fatalf("ContainerRegistered = %v", registered)
	}
	if !reflect.DeepEqual(broken, map[string]any{
		"lpn": "LPN-1", "sku": "SKU-C", "owner_ref": "seller-a", "qty_eaches": float64(12), "from_bin": ctCaseBin.String(), "loose_bin": ctLooseBin.String(),
	}) {
		t.Fatalf("CaseBroken = %v", broken)
	}
	if stowed["lpn"] != "LPN-1" || stowed["bin"] != ctCaseBin.String() {
		t.Fatalf("ContainerStowed = %v", stowed)
	}

	// the owner's share is a level of its own; fleet stock keeps its payload
	owners := map[string]int{}
	rows, err := w.pool.Query(ctx, `SELECT value FROM outbox_events WHERE event_type = $1`, binLevelType)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var value []byte
		_ = rows.Scan(&value)
		e, _ := cloudevents.Decode(value)
		var m map[string]any
		_ = jsonUnmarshal(e.Data(), &m)
		owner, _ := m["owner_ref"].(string)
		owners[owner]++
	}
	if owners["seller-a"] == 0 || owners[""] != 0 {
		t.Fatalf("BinStockLevelChanged owners = %v, want only seller-a", owners)
	}
}

var _ ports.EventPublisher = (*postgres.OutboxPublisher)(nil)
