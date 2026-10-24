package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// BinStockLevelChanged (ADR 0040) per use case, against the in-memory
// adapters: exactly one event per affected (bin, sku) with the exact absolute
// levels, none on a replay or a rejection.

const (
	lvBinA = shared.BinId("WH1-STOR-AMB-A01-01-01-A")
	lvBinB = shared.BinId("WH1-STOR-AMB-A02-01-01-A")
)

type levelEnv struct {
	env
	Levels *usecases.BinLevelEmitter
}

func newLevelEnv(t *testing.T) levelEnv {
	t.Helper()
	e := newEnv()
	seedBin(t, e, lvBinA, 100)
	seedBin(t, e, lvBinB, 100)
	return levelEnv{env: e, Levels: &usecases.BinLevelEmitter{
		Ledger: memory.NewBinLevelLedger(e.Stock), Locations: e.Locations, Events: e.Events, Clock: e.Clock,
	}}
}

func (e levelEnv) stowUC() *usecases.StowStock {
	return &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock, Levels: e.Levels}
}

func (e levelEnv) reserveUC() *usecases.ReserveStock {
	return &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Timeout: time.Hour, Levels: e.Levels}
}

func (e levelEnv) stow(t *testing.T, sku string, bin shared.BinId, qty int) {
	t.Helper()
	if _, err := e.stowUC().Execute(context.Background(), mustSKU(t, sku), mustQty(t, qty), bin); err != nil {
		t.Fatalf("stow: %v", err)
	}
}

// levelEvents returns every BinStockLevelChanged published so far, in order.
func (e levelEnv) levelEvents() []shared.BinStockLevelChanged {
	var out []shared.BinStockLevelChanged
	for _, ev := range e.Events.Events() {
		if level, ok := ev.(shared.BinStockLevelChanged); ok {
			out = append(out, level)
		}
	}
	return out
}

type wantLevel struct {
	bin      shared.BinId
	sku      string
	onHand   int
	reserved int
	version  int64
	reason   shared.BinLevelReason
}

func assertLevel(t *testing.T, got shared.BinStockLevelChanged, want wantLevel) {
	t.Helper()
	if got.SiteID != "WH1" || got.BinID != want.bin || got.SKU.String() != want.sku ||
		got.OnHand.Int() != want.onHand || got.Reserved.Int() != want.reserved ||
		got.Version != want.version || got.Reason != want.reason {
		t.Fatalf("level event = %+v, want %+v (site WH1)", got, want)
	}
}

// byBin indexes level events by bin (the multi-bin tests do not depend on the
// order the allocator drew the bins in).
func byBin(evs []shared.BinStockLevelChanged) map[shared.BinId]shared.BinStockLevelChanged {
	out := make(map[shared.BinId]shared.BinStockLevelChanged, len(evs))
	for _, ev := range evs {
		out[ev.BinID] = ev
	}
	return out
}

func TestBinLevels_Stow_RaisesAbsoluteLevelsWithRisingVersions(t *testing.T) {
	e := newLevelEnv(t)

	e.stow(t, "SKU-1", lvBinA, 5)
	e.stow(t, "SKU-1", lvBinA, 3)

	evs := e.levelEvents()
	if len(evs) != 2 {
		t.Fatalf("level events = %d, want 2", len(evs))
	}
	assertLevel(t, evs[0], wantLevel{lvBinA, "SKU-1", 5, 0, 1, shared.BinLevelStow})
	assertLevel(t, evs[1], wantLevel{lvBinA, "SKU-1", 8, 0, 2, shared.BinLevelStow})
	if !evs[1].OccurredAt().Equal(e.Clock.Now()) {
		t.Fatalf("occurred at %v, want the clock's now", evs[1].OccurredAt())
	}
}

func TestBinLevels_Stow_RejectedStowRaisesNothing(t *testing.T) {
	e := newLevelEnv(t)

	if _, err := e.stowUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 101), lvBinA); err == nil {
		t.Fatal("a stow over the bin capacity must be rejected")
	}
	if n := len(e.levelEvents()); n != 0 {
		t.Fatalf("level events = %d after a rejected stow, want 0", n)
	}
}

func TestBinLevels_Stow_LevelsAreScopedToTheSKUOfTheBin(t *testing.T) {
	e := newLevelEnv(t)

	e.stow(t, "SKU-1", lvBinA, 5)
	e.stow(t, "SKU-2", lvBinA, 7)

	evs := e.levelEvents()
	assertLevel(t, evs[1], wantLevel{lvBinA, "SKU-2", 7, 0, 1, shared.BinLevelStow})
}

func TestBinLevels_UnknownSiteBinRaisesNothing(t *testing.T) {
	e := newLevelEnv(t)
	seedBin(t, e.env, mustBinID(t, "A-1-1"), 10)

	e.stow(t, "SKU-1", mustBinID(t, "A-1-1"), 5)

	if n := len(e.levelEvents()); n != 0 {
		t.Fatalf("level events = %d for a bin with no determinable site, want 0", n)
	}
}

func TestBinLevels_NilEmitterRaisesNothing(t *testing.T) {
	e := newLevelEnv(t)
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), lvBinA); err != nil {
		t.Fatalf("stow: %v", err)
	}
	if n := len(e.levelEvents()); n != 0 {
		t.Fatalf("level events = %d without an emitter, want 0", n)
	}
}

func TestBinLevels_Reserve_RaisesReservedAndLeavesOnHand(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)

	if _, err := e.reserveUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 4), "order-1"); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	evs := e.levelEvents()
	if len(evs) != 2 {
		t.Fatalf("level events = %d, want stow + reserve", len(evs))
	}
	assertLevel(t, evs[1], wantLevel{lvBinA, "SKU-1", 10, 4, 2, shared.BinLevelReserve})
}

func TestBinLevels_Reserve_ReplayAndRejectionRaiseNothing(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	uc := e.reserveUC()
	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 4), "order-1"); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	before := len(e.levelEvents())

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 4), "order-1"); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 50), "order-2"); err == nil {
		t.Fatal("reserving more than usable must be rejected")
	}

	if got := len(e.levelEvents()); got != before {
		t.Fatalf("level events %d -> %d across a replay and a rejection, want no change", before, got)
	}
}

func TestBinLevels_Reserve_DrawingFromTwoBinsRaisesOneEventPerBin(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 4)
	e.stow(t, "SKU-1", lvBinB, 6)

	if _, err := e.reserveUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 10), "order-1"); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	evs := e.levelEvents()
	if len(evs) != 4 {
		t.Fatalf("level events = %d, want 2 stows + 1 reserve per bin", len(evs))
	}
	got := byBin(evs[2:])
	assertLevel(t, got[lvBinA], wantLevel{lvBinA, "SKU-1", 4, 4, 2, shared.BinLevelReserve})
	assertLevel(t, got[lvBinB], wantLevel{lvBinB, "SKU-1", 6, 6, 2, shared.BinLevelReserve})
}

func TestBinLevels_Revoke_ReleasesReservedAndRaisesRevoke(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	res, err := e.reserveUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 4), "order-1")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	revoke := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Levels: e.Levels}
	if err := revoke.Execute(context.Background(), res.ID()); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	evs := e.levelEvents()
	if len(evs) != 3 {
		t.Fatalf("level events = %d, want stow + reserve + revoke", len(evs))
	}
	assertLevel(t, evs[2], wantLevel{lvBinA, "SKU-1", 10, 0, 3, shared.BinLevelRevoke})
	if err := revoke.Execute(context.Background(), res.ID()); err == nil {
		t.Fatal("a second revoke must be rejected")
	}
	if got := len(e.levelEvents()); got != 3 {
		t.Fatalf("level events = %d after a rejected second revoke, want still 3", got)
	}
}

func TestBinLevels_LazyExpiry_RaisesRevoke(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	if _, err := e.reserveUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 4), "order-1"); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	e.Clock.Advance(2 * time.Hour)

	read := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Levels: e.Levels}
	if _, err := read.Execute(context.Background(), "order-1"); err != nil {
		t.Fatalf("read: %v", err)
	}

	evs := e.levelEvents()
	assertLevel(t, evs[len(evs)-1], wantLevel{lvBinA, "SKU-1", 10, 0, 3, shared.BinLevelRevoke})
}

func TestBinLevels_ConfirmPick_RaisesPickWithTheRemainingLevels(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	res, err := e.reserveUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), "order-1")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	pick := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Levels: e.Levels}
	if err := pick.Execute(context.Background(), res.ID()); err != nil {
		t.Fatalf("pick: %v", err)
	}

	evs := e.levelEvents()
	assertLevel(t, evs[len(evs)-1], wantLevel{lvBinA, "SKU-1", 4, 0, 3, shared.BinLevelPick})
	if err := pick.Execute(context.Background(), res.ID()); err == nil {
		t.Fatal("picking a confirmed reservation again must be rejected")
	}
	if got := len(e.levelEvents()); got != 3 {
		t.Fatalf("level events = %d after a rejected re-pick, want still 3", got)
	}
}

// Two reservations of one order drawn from the same (bin, sku) are confirmed
// by ONE completion: the transaction raises ONE event with the final level,
// not one per reservation.
func TestBinLevels_ConfirmPicksForOrder_OneEventPerBinSKUWithTheFinalLevel(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	reserve := e.reserveUC()
	for _, qty := range []int{3, 2} {
		if _, err := reserve.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, qty), "order-1"); err != nil {
			t.Fatalf("reserve %d: %v", qty, err)
		}
	}
	before := len(e.levelEvents())

	uow := &snapshotUnitOfWork{claims: &snapshotClaims{claimed: map[string]bool{}}, progress: &snapshotProgress{counts: map[string]int{}}}
	confirm := &usecases.ConfirmPick{Stock: e.Stock, Locations: e.Locations, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, UnitOfWork: uow, Levels: e.Levels}
	uc := &usecases.ConfirmPicksForOrder{
		Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock, Confirm: confirm,
		ProcessedEvents: uow.claims, PickProgress: uow.progress, UnitOfWork: uow, Levels: e.Levels,
	}
	for _, id := range []string{"ce-1", "ce-2"} {
		if _, err := uc.Execute(context.Background(), pickCompleted(id, "order-1")); err != nil {
			t.Fatalf("pick %s: %v", id, err)
		}
	}

	evs := e.levelEvents()[before:]
	if len(evs) != 1 {
		t.Fatalf("level events = %d for two reservations of one (bin, sku), want exactly 1", len(evs))
	}
	assertLevel(t, evs[0], wantLevel{lvBinA, "SKU-1", 5, 0, 4, shared.BinLevelPick})
}

func TestBinLevels_CycleCountShortfall_RaisesCountForTheUnlocatedSKU(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock, Levels: e.Levels}

	if _, err := uc.Execute(context.Background(), lvBinA, mustQty(t, 4)); err != nil {
		t.Fatalf("count: %v", err)
	}

	evs := e.levelEvents()
	if len(evs) != 2 {
		t.Fatalf("level events = %d, want stow + count", len(evs))
	}
	assertLevel(t, evs[1], wantLevel{lvBinA, "SKU-1", 0, 0, 2, shared.BinLevelCount})
}

func TestBinLevels_CycleCountWithoutAShortfall_RaisesNothing(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	uc := &usecases.RunCycleCount{Stock: e.Stock, Events: e.Events, Clock: e.Clock, Levels: e.Levels}

	for _, counted := range []int{10, 15} { // match, then overage
		if _, err := uc.Execute(context.Background(), lvBinA, mustQty(t, counted)); err != nil {
			t.Fatalf("count %d: %v", counted, err)
		}
	}

	if got := len(e.levelEvents()); got != 1 {
		t.Fatalf("level events = %d, want only the stow's (a match or overage moves no ledger level)", got)
	}
}

func TestBinLevels_Relocate_RaisesSourceThenDestination(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	uc := &usecases.RelocateStock{Stock: e.Stock, Locations: e.Locations, Relocations: memory.NewRelocationRepo(), Events: e.Events, Clock: e.Clock, Levels: e.Levels}

	if _, err := uc.Execute(context.Background(), usecases.RelocateStockCommand{
		SiteID: "WH1", SKU: mustSKU(t, "SKU-1"), FromBin: lvBinA, ToBin: lvBinB, Quantity: 4, Reason: "SLOTTING",
	}); err != nil {
		t.Fatalf("relocate: %v", err)
	}

	evs := e.levelEvents()
	if len(evs) != 3 {
		t.Fatalf("level events = %d, want stow + source + destination", len(evs))
	}
	assertLevel(t, evs[1], wantLevel{lvBinA, "SKU-1", 6, 0, 2, shared.BinLevelRelocate})
	assertLevel(t, evs[2], wantLevel{lvBinB, "SKU-1", 4, 0, 1, shared.BinLevelRelocate})
}

func TestBinLevels_Relocate_RejectedMoveRaisesNothing(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 10)
	uc := &usecases.RelocateStock{Stock: e.Stock, Locations: e.Locations, Relocations: memory.NewRelocationRepo(), Events: e.Events, Clock: e.Clock, Levels: e.Levels}

	_, err := uc.Execute(context.Background(), usecases.RelocateStockCommand{
		SiteID: "WH1", SKU: mustSKU(t, "SKU-1"), FromBin: lvBinA, ToBin: lvBinB, Quantity: 11, Reason: "SLOTTING",
	})
	if err == nil {
		t.Fatal("relocating more than the usable quantity must be rejected")
	}
	if got := len(e.levelEvents()); got != 1 {
		t.Fatalf("level events = %d after a rejected relocation, want only the stow's", got)
	}
}

func TestBinLevels_TransferAllocation_RaisesReserveForTheDrawnBin(t *testing.T) {
	e := newLevelEnv(t)
	site := mustSiteID(t, "WH1")
	unit, err := stock.NewStockUnitAtSite("su-t1", mustSKU(t, "SKU-T"), lvBinA, mustQty(t, 8), site)
	if err != nil {
		t.Fatalf("unit: %v", err)
	}
	if err := e.Stock.Save(context.Background(), unit); err != nil {
		t.Fatalf("seed: %v", err)
	}
	uc := &usecases.AllocateTransferStock{
		Stock: e.Stock, Reservations: e.Reservations, Transfers: memory.NewTransferAllocationRepo(),
		Events: e.Events, Clock: e.Clock, Levels: e.Levels,
	}

	if _, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "WH1", "SKU-T", 5)); err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if _, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "WH1", "SKU-T", 5)); err != nil {
		t.Fatalf("replay: %v", err)
	}

	evs := e.levelEvents()
	if len(evs) != 1 {
		t.Fatalf("level events = %d, want exactly one (the replay changes nothing)", len(evs))
	}
	assertLevel(t, evs[0], wantLevel{lvBinA, "SKU-T", 8, 5, 1, shared.BinLevelReserve})
}

func TestBinLevels_TransferStow_RaisesStowForEveryLegBin(t *testing.T) {
	re := newReceiptEnv()
	seedAllocatedLine(t, re, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, re, string(lvBinA), "WH1", 10)
	seedDestBin(t, re, string(lvBinB), "WH1", 10)
	levels := &usecases.BinLevelEmitter{Ledger: memory.NewBinLevelLedger(re.Stock), Locations: re.Locations, Events: re.Events, Clock: re.Clock}
	if _, err := re.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "WH1", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}
	uc := re.stowUseCase()
	uc.Levels = levels

	if _, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{string(lvBinA), mustQtyT(t, 3)}, [2]any{string(lvBinB), mustQtyT(t, 2)})); err != nil {
		t.Fatalf("stow: %v", err)
	}
	if _, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{string(lvBinA), mustQtyT(t, 3)}, [2]any{string(lvBinB), mustQtyT(t, 2)})); err != nil {
		t.Fatalf("replay: %v", err)
	}

	var evs []shared.BinStockLevelChanged
	for _, ev := range re.Events.Events() {
		if level, ok := ev.(shared.BinStockLevelChanged); ok {
			evs = append(evs, level)
		}
	}
	if len(evs) != 2 {
		t.Fatalf("level events = %d, want one per leg bin (the replay raises none)", len(evs))
	}
	got := byBin(evs)
	assertLevel(t, got[lvBinA], wantLevel{lvBinA, "SKU-T", 3, 0, 1, shared.BinLevelStow})
	assertLevel(t, got[lvBinB], wantLevel{lvBinB, "SKU-T", 2, 0, 1, shared.BinLevelStow})
}

// REPLENISHMENT is reachable since ADR 0041 (a completed REPLENISH task); the
// end-to-end test is TestBinLevels_ReplenishTask_RaisesReplenishmentLevels….
func TestBinLevels_RelocationReasonMapping(t *testing.T) {
	cases := map[string]shared.BinLevelReason{
		"SLOTTING":      shared.BinLevelRelocate,
		"CONSOLIDATION": shared.BinLevelRelocate,
		"MANUAL":        shared.BinLevelRelocate,
		"REPLENISHMENT": shared.BinLevelReplenishment,
	}
	for reason, want := range cases {
		if got := usecases.RelocationLevelReason(reason); got != want {
			t.Errorf("relocation reason %s maps to %s, want %s", reason, got, want)
		}
	}
}

// A Track outside any Atomically bracket publishes at once, so a change is
// never silently dropped; a nil emitter's Track is a no-op.
func TestBinLevels_TrackOutsideABracketPublishesImmediately(t *testing.T) {
	e := newLevelEnv(t)
	e.stow(t, "SKU-1", lvBinA, 5)

	if err := e.Levels.Track(context.Background(), shared.BinLevelCount, lvBinA, mustSKU(t, "SKU-1")); err != nil {
		t.Fatalf("track: %v", err)
	}
	var none *usecases.BinLevelEmitter
	if err := none.Track(context.Background(), shared.BinLevelCount, lvBinA, mustSKU(t, "SKU-1")); err != nil {
		t.Fatalf("nil track: %v", err)
	}

	evs := e.levelEvents()
	if len(evs) != 2 {
		t.Fatalf("level events = %d, want the stow's and the immediate one", len(evs))
	}
	assertLevel(t, evs[1], wantLevel{lvBinA, "SKU-1", 5, 0, 2, shared.BinLevelCount})
}

func TestBinLevels_AdvanceFailurePropagates(t *testing.T) {
	e := newLevelEnv(t)
	e.Levels.Ledger = failingLedger{}

	if _, err := e.stowUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), lvBinA); err == nil {
		t.Fatal("a failing level ledger must fail the stow (the event is part of the transaction)")
	}
}

func TestBinLevels_LocationLookupFailurePropagates(t *testing.T) {
	e := newLevelEnv(t)
	e.Levels.Locations = &failingLocationRepo{delegate: e.Locations, failFindByID: true}
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock, Levels: e.Levels}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), lvBinA); err == nil {
		t.Fatal("a failing site lookup must fail the stow")
	}
}

func TestBinLevels_PublishFailurePropagates(t *testing.T) {
	e := newLevelEnv(t)
	e.Levels.Events = selectiveFailingEvents{delegate: e.Events, failName: "BinStockLevelChanged"}

	if _, err := e.stowUC().Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), lvBinA); err == nil {
		t.Fatal("a failing level publish must fail the stow")
	}
}

// failingLedger fails every Advance.
type failingLedger struct{}

func (failingLedger) Advance(context.Context, shared.BinId, shared.SKU, shared.OwnerRef) (shared.BinLevel, error) {
	return shared.BinLevel{}, errFake
}
