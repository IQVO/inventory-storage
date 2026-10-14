package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

const (
	relFrom  = shared.BinId("WH1-STOR-AMB-A01-01-01-A")
	relTo    = shared.BinId("WH1-STOR-AMB-A02-01-01-A")
	relOther = shared.BinId("WH2-STOR-AMB-A02-01-01-A")
)

// relocationEnv is the in-memory world for RelocateStock tests: two WH1
// bins (derived site from the location code) with 10 units of SKU-1 in the
// source bin.
type relocationEnv struct {
	env
	Relocations *memory.RelocationRepo
	uc          *usecases.RelocateStock
	source      *stock.StockUnit
}

func newRelocationEnv(t *testing.T) relocationEnv {
	t.Helper()
	e := relocationEnv{env: newEnv(), Relocations: memory.NewRelocationRepo()}
	ctx := context.Background()
	from, _ := location.NewBin(relFrom, 50)
	to, _ := location.NewBin(relTo, 20)
	other, _ := location.NewBin(relOther, 20)
	if err := from.Occupy(10); err != nil {
		t.Fatal(err)
	}
	for _, b := range []*location.Bin{from, to, other} {
		if err := e.Locations.Save(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	unit, _ := stock.NewStockUnit("su-src", "SKU-1", relFrom, 10)
	if err := e.Stock.Save(ctx, unit); err != nil {
		t.Fatal(err)
	}
	e.source = unit
	e.uc = &usecases.RelocateStock{Stock: e.Stock, Locations: e.Locations, Relocations: e.Relocations, Events: e.Events, Clock: e.Clock}
	return e
}

func relocateCmd() usecases.RelocateStockCommand {
	return usecases.RelocateStockCommand{
		SiteID: "WH1", SKU: "SKU-1", FromBin: relFrom, ToBin: relTo, Quantity: 4,
		Reason: relocation.ReasonManual, SourceRef: "ticket-9",
	}
}

func TestRelocateStock_MovesStockAndRecordsTheRelocation(t *testing.T) {
	e := newRelocationEnv(t)
	ctx := context.Background()

	rel, err := e.uc.Execute(ctx, relocateCmd())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if e.source.Quantity() != 6 {
		t.Fatalf("source unit quantity %d, want 6", e.source.Quantity())
	}
	dest, _ := e.Stock.FindByID(ctx, rel.DestinationStockUnitID())
	if dest == nil || dest.BinID() != relTo || dest.Quantity() != 4 || dest.SiteID() != "WH1" {
		t.Fatalf("destination unit wrong: %+v", dest)
	}
	from, _ := e.Locations.FindByID(ctx, relFrom)
	to, _ := e.Locations.FindByID(ctx, relTo)
	if from.Occupied() != 6 || to.Occupied() != 4 {
		t.Fatalf("occupancy from %d to %d, want 6/4", from.Occupied(), to.Occupied())
	}
	stored, _ := e.Relocations.FindByID(ctx, rel.ID())
	if stored == nil || stored.Quantity() != 4 || stored.SourceRef() != "ticket-9" || len(stored.Legs()) != 1 {
		t.Fatalf("relocation not recorded: %+v", stored)
	}
	if !rel.OccurredAt().Equal(e.Clock.Now()) {
		t.Fatalf("occurred at %v, want the clock's now", rel.OccurredAt())
	}
	assertOneStockRelocated(t, e, rel)
}

func assertOneStockRelocated(t *testing.T, e relocationEnv, rel *relocation.Relocation) {
	t.Helper()
	evs := e.Events.Events()
	if len(evs) != 1 {
		t.Fatalf("published %d events, want exactly one StockRelocated", len(evs))
	}
	ev, ok := evs[0].(shared.StockRelocated)
	if !ok {
		t.Fatalf("published %T, want StockRelocated", evs[0])
	}
	if ev.RelocationID != rel.ID() || ev.SiteID != "WH1" || ev.FromBin != relFrom || ev.ToBin != relTo ||
		ev.Quantity != 4 || ev.Reason != "MANUAL" || ev.SourceRef != "ticket-9" {
		t.Fatalf("event payload wrong: %+v", ev)
	}
}

func TestRelocateStock_ResolvesTheSiteFromTheSourceBinWhenNoneIsGiven(t *testing.T) {
	e := newRelocationEnv(t)
	cmd := relocateCmd()
	cmd.SiteID = ""

	rel, err := e.uc.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if rel.SiteID() != "WH1" {
		t.Fatalf("site %q, want WH1 (the source bin's site)", rel.SiteID())
	}
}

func TestRelocateStock_UnknownSourceSiteIsRejectedWhenResolving(t *testing.T) {
	e := newRelocationEnv(t)
	legacy, _ := location.NewBin("BIN-LEGACY", 10)
	_ = e.Locations.Save(context.Background(), legacy)
	cmd := relocateCmd()
	cmd.SiteID = ""
	cmd.FromBin = "BIN-LEGACY"

	if _, err := e.uc.Execute(context.Background(), cmd); !errors.Is(err, relocation.ErrBinNotAtSite) {
		t.Fatalf("err = %v, want ErrBinNotAtSite", err)
	}
}

func TestRelocateStock_Rejections(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*usecases.RelocateStockCommand)
		want error
	}{
		{"unknown source bin", func(c *usecases.RelocateStockCommand) { c.FromBin = "WH1-STOR-AMB-Z99-01-01-A" }, usecases.ErrBinNotFound},
		{"unknown destination bin", func(c *usecases.RelocateStockCommand) { c.ToBin = "WH1-STOR-AMB-Z99-01-01-A" }, usecases.ErrBinNotFound},
		{"unknown source bin while resolving the site", func(c *usecases.RelocateStockCommand) { c.SiteID = ""; c.FromBin = "WH1-NOPE-AMB-Z99-01-01-A" }, usecases.ErrBinNotFound},
		{"same bin", func(c *usecases.RelocateStockCommand) { c.ToBin = c.FromBin }, relocation.ErrSameBin},
		{"destination in another site", func(c *usecases.RelocateStockCommand) { c.ToBin = relOther }, relocation.ErrBinNotAtSite},
		{"site the bins are not in", func(c *usecases.RelocateStockCommand) { c.SiteID = "WH2" }, relocation.ErrBinNotAtSite},
		{"more than usable", func(c *usecases.RelocateStockCommand) { c.Quantity = 11 }, stock.ErrInsufficientUsable},
		{"zero quantity", func(c *usecases.RelocateStockCommand) { c.Quantity = 0 }, shared.ErrZeroQuantity},
		{"unknown reason", func(c *usecases.RelocateStockCommand) { c.Reason = "WHIM" }, relocation.ErrUnknownReason},
	}
	for _, tc := range cases {
		e := newRelocationEnv(t)
		cmd := relocateCmd()
		tc.mut(&cmd)

		_, err := e.uc.Execute(context.Background(), cmd)
		if !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
		assertNothingMoved(t, e, tc.name)
	}
}

func assertNothingMoved(t *testing.T, e relocationEnv, name string) {
	t.Helper()
	if e.source.Quantity() != 10 {
		t.Fatalf("%s: source quantity %d, want 10", name, e.source.Quantity())
	}
	if len(e.Events.Events()) != 0 {
		t.Fatalf("%s: a rejected relocation published %d events", name, len(e.Events.Events()))
	}
	units, _ := e.Stock.FindBySKU(context.Background(), "SKU-1")
	if len(units) != 1 {
		t.Fatalf("%s: a rejected relocation created a unit (%d units)", name, len(units))
	}
}

func TestRelocateStock_DestinationFull(t *testing.T) {
	e := newRelocationEnv(t)
	small, _ := location.NewBin("WH1-STOR-AMB-A03-01-01-A", 3)
	_ = e.Locations.Save(context.Background(), small)
	cmd := relocateCmd()
	cmd.ToBin = small.ID()

	if _, err := e.uc.Execute(context.Background(), cmd); !errors.Is(err, location.ErrBinFull) {
		t.Fatalf("err = %v, want ErrBinFull", err)
	}
	assertNothingMoved(t, e, "destination full")
}

func TestRelocateStock_RunsInsideTheUnitOfWork(t *testing.T) {
	e := newRelocationEnv(t)
	uow := &countingUoW{}
	e.uc.UnitOfWork = uow

	if _, err := e.uc.Execute(context.Background(), relocateCmd()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork.Execute called %d times, want 1", uow.calls)
	}
}

// countingUoW is a pass-through UnitOfWork that counts its scopes.
type countingUoW struct{ calls int }

func (u *countingUoW) Execute(ctx context.Context, fn func(context.Context) error) error {
	u.calls++
	return fn(ctx)
}

func TestRelocateStock_PropagatesAdapterFailures(t *testing.T) {
	cases := []struct {
		name  string
		setup func(e *relocationEnv)
	}{
		{"bin lookup", func(e *relocationEnv) {
			e.uc.Locations = &failingLocationRepo{delegate: e.Locations, failFindByID: true}
		}},
		{"bin lookup while resolving the site", func(e *relocationEnv) {
			e.uc.Locations = &failingLocationRepo{delegate: e.Locations, failFindByID: true}
		}},
		{"bin save", func(e *relocationEnv) {
			e.uc.Locations = &failingLocationRepo{delegate: e.Locations, failSave: true}
		}},
		{"units lookup", func(e *relocationEnv) {
			e.uc.Stock = &failingStockRepo{delegate: e.Stock, failFindByBin: true}
		}},
		{"destination id", func(e *relocationEnv) {
			e.uc.Stock = &failingStockRepo{delegate: e.Stock, failNextID: true}
		}},
		{"unit save", func(e *relocationEnv) {
			e.uc.Stock = &failingStockRepo{delegate: e.Stock, failSave: true}
		}},
		{"relocation id", func(e *relocationEnv) {
			e.uc.Relocations = &failingRelocationRepo{delegate: e.Relocations, failNextID: true}
		}},
		{"relocation save", func(e *relocationEnv) {
			e.uc.Relocations = &failingRelocationRepo{delegate: e.Relocations, failSave: true}
		}},
		{"publish", func(e *relocationEnv) { e.uc.Events = failingEvents{} }},
	}
	for i, tc := range cases {
		e := newRelocationEnv(t)
		tc.setup(&e)
		cmd := relocateCmd()
		if i == 1 {
			cmd.SiteID = ""
		}
		if _, err := e.uc.Execute(context.Background(), cmd); !errors.Is(err, errFake) {
			t.Fatalf("%s: err = %v, want the adapter's error", tc.name, err)
		}
	}
}

func TestRelocateStock_SavesBinsInIDOrder(t *testing.T) {
	e := newRelocationEnv(t)
	rec := &recordingLocationRepo{delegate: e.Locations}
	e.uc.Locations = rec
	cmd := relocateCmd()
	// Destination id sorts BEFORE the source id.
	cmd.FromBin, cmd.ToBin = relTo, relFrom
	unit, _ := stock.NewStockUnit("su-2", "SKU-1", relTo, 5)
	_ = e.Stock.Save(context.Background(), unit)
	to, _ := e.Locations.FindByID(context.Background(), relTo)
	_ = to.Occupy(5)

	if _, err := e.uc.Execute(context.Background(), cmd); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(rec.saved) != 2 || rec.saved[0] != relFrom || rec.saved[1] != relTo {
		t.Fatalf("bins saved in order %v, want ascending id %v", rec.saved, []shared.BinId{relFrom, relTo})
	}
}

// recordingLocationRepo records the order of Save calls.
type recordingLocationRepo struct {
	delegate *memory.LocationRepo
	saved    []shared.BinId
}

func (r *recordingLocationRepo) Save(ctx context.Context, bin *location.Bin) error {
	r.saved = append(r.saved, bin.ID())
	return r.delegate.Save(ctx, bin)
}

func (r *recordingLocationRepo) FindByID(ctx context.Context, id shared.BinId) (*location.Bin, error) {
	return r.delegate.FindByID(ctx, id)
}

// failingRelocationRepo wraps a real RelocationRepo with switchable failures.
type failingRelocationRepo struct {
	delegate     *memory.RelocationRepo
	failSave     bool
	failFindByID bool
	failNextID   bool
}

func (f *failingRelocationRepo) Save(ctx context.Context, r *relocation.Relocation) error {
	if f.failSave {
		return errFake
	}
	return f.delegate.Save(ctx, r)
}

func (f *failingRelocationRepo) FindByID(ctx context.Context, id string) (*relocation.Relocation, error) {
	if f.failFindByID {
		return nil, errFake
	}
	return f.delegate.FindByID(ctx, id)
}

func (f *failingRelocationRepo) NextID(ctx context.Context) (string, error) {
	if f.failNextID {
		return "", errFake
	}
	return f.delegate.NextID(ctx)
}

// --- GetRelocation ---------------------------------------------------------------

func TestGetRelocation(t *testing.T) {
	e := newRelocationEnv(t)
	ctx := context.Background()
	rel, err := e.uc.Execute(ctx, relocateCmd())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	get := &usecases.GetRelocation{Relocations: e.Relocations}

	got, err := get.Execute(ctx, rel.ID())
	if err != nil || got.ID() != rel.ID() {
		t.Fatalf("GetRelocation = %v, %v", got, err)
	}
	if _, err := get.Execute(ctx, "rel-missing"); !errors.Is(err, usecases.ErrRelocationNotFound) {
		t.Fatalf("missing: err = %v, want ErrRelocationNotFound", err)
	}
	failing := &usecases.GetRelocation{Relocations: &failingRelocationRepo{delegate: e.Relocations, failFindByID: true}}
	if _, err := failing.Execute(ctx, rel.ID()); !errors.Is(err, errFake) {
		t.Fatalf("repo failure: err = %v, want errFake", err)
	}
}
