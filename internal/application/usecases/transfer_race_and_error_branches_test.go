package usecases_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// This file targets the transfer use cases' concurrency-shaped branches:
// the IN-TRANSACTION re-checks (a duplicate appearing between the outer
// read and the transaction), the unique-constraint race recovery, and
// error propagation from every port the happy-path tests never fail.
// These are the branches that keep a duplicate command from
// double-allocating stock when two consumers race — exactly the paths a
// single-threaded happy-path test can never reach.

// NOTE: no UnitOfWork is wired in these tests (atomically falls back to
// running the closure directly), which is what makes the scripted
// in-transaction re-checks deterministic.

// seedReservation writes an ACTIVE reservation row exactly as
// AllocateTransferStock would have for the raced winner.
func seedReservation(t *testing.T, e transferEnv, id string, sku shared.SKU, qty int) {
	t.Helper()
	res, err := reservation.New(
		id, sku, mustQty(t, qty), usecases.TransferDemandRef("tr-1", "tl-1"),
		[]reservation.Allocation{{StockUnitID: "su-a", Quantity: mustQty(t, qty)}},
		fixedTransferTime, time.Hour,
	)
	if err != nil {
		t.Fatalf("seed reservation %s: %v", id, err)
	}
	if err := e.Reservations.Save(context.Background(), res); err != nil {
		t.Fatalf("seed reservation save %s: %v", id, err)
	}
}

// --- scripted TransferAllocationRepo ---------------------------------------

// racingTransferRepo returns nil on the FIRST FindByTransferLineID (the
// outer read) and a pre-built ALLOCATED ledger row on every later one,
// simulating a concurrent duplicate that committed in between. With no
// UnitOfWork wired the closure runs immediately, so the second read is
// the in-transaction re-check.
type racingTransferRepo struct {
	delegate *memory.TransferAllocationRepo
	calls    int
	staged   *transfer.Allocation
}

func (r *racingTransferRepo) FindByTransferLineID(ctx context.Context, lineID string) (*transfer.Allocation, error) {
	r.calls++
	if r.calls == 1 {
		return nil, nil // outer read: nothing decided yet
	}
	// In-transaction re-check: a concurrent duplicate committed.
	if r.staged == nil {
		alloc, err := transfer.NewAllocated(
			"tr-1", lineID, mustSiteIDT(tUnused, "SITE-A"), mustSKUT(tUnused, "SKU-T"),
			mustQtyT(tUnused, 6), "res-raced", fixedTransferTime,
		)
		if err != nil {
			return nil, err
		}
		r.staged = alloc
	}
	return r.staged, nil
}

func (r *racingTransferRepo) Save(ctx context.Context, a *transfer.Allocation) error {
	return r.delegate.Save(ctx, a)
}

// failingTransferRepo fails every read/write with errFake.
type failingTransferRepo struct {
	delegate *memory.TransferAllocationRepo
}

func (f *failingTransferRepo) FindByTransferLineID(context.Context, string) (*transfer.Allocation, error) {
	return nil, errFake
}
func (f *failingTransferRepo) Save(ctx context.Context, a *transfer.Allocation) error {
	return f.delegate.Save(ctx, a)
}

// --- AllocateTransferStock: in-transaction replay & conflicts --------------

// A duplicate that lands INSIDE the transaction must answer the original
// outcome, not allocate twice.
func TestAllocateTransferStock_InTxReplayAnswersOriginal(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, siteA, 10)
	// The raced winner's reservation, seeded so the replay can return it.
	seedReservation(t, e, "res-raced", sku, 6)

	uc := e.usecase(clock)
	uc.Transfers = &racingTransferRepo{delegate: e.Transfers}

	res, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6))
	if err != nil {
		t.Fatalf("raced execute: %v", err)
	}
	if !res.Replay {
		t.Fatal("an in-transaction duplicate must be flagged as a replay")
	}
	if res.Allocation.ReservationID() != "res-raced" {
		t.Fatalf("reservation = %q, want the raced winner's res-raced", res.Allocation.ReservationID())
	}
	if res.Reservation == nil || res.Reservation.ID() != "res-raced" {
		t.Fatalf("replay must carry the original reservation, got %+v", res.Reservation)
	}
	// Stock was never touched by the loser.
	assertUnitUsable(t, e, "su-a", 10, "the racing loser must not decrement stock")
}

// The in-transaction re-check with a DIFFERENT payload is an idempotency
// conflict: the original stands, the racing command is rejected.
func TestAllocateTransferStock_InTxConflictPublishesRejection(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, siteA, 10)

	uc := e.usecase(clock)
	racing := &racingTransferRepo{delegate: e.Transfers}
	uc.Transfers = racing

	// A 9-unit command racing a 6-unit winner: different payload.
	res, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 9))
	if err != nil {
		t.Fatalf("raced conflict: %v", err)
	}
	if res.Replay {
		t.Fatal("a conflicting racing command is not a replay")
	}
	if res.Allocation.Outcome() != transfer.OutcomeRejected ||
		res.Allocation.RejectionReason() != transfer.ReasonIdempotencyConflict {
		t.Fatalf("outcome = %+v, want REJECTED/IDEMPOTENCY_CONFLICT", res.Allocation)
	}

	// The conflict published its rejection event in the same transaction.
	found := false
	for _, ev := range e.Events.Events() {
		if r, ok := ev.(shared.TransferStockAllocationRejected); ok && r.Reason == usecases.RejectionIdempotencyConflict {
			found = true
			if r.RequestedQuantity != mustQtyT(tUnused, 9) {
				t.Fatalf("conflict event must describe THIS command's quantity 9, got %v", r.RequestedQuantity)
			}
		}
	}
	if !found {
		t.Fatal("IDEMPOTENCY_CONFLICT rejection was not published for the in-tx conflict")
	}
	assertUnitUsable(t, e, "su-a", 10, "a conflicted racing command must not touch stock")
}

// An outer-read repo failure surfaces unchanged.
func TestAllocateTransferStock_ReadFailureSurfacesError(t *testing.T) {
	e := newTransferEnv()
	uc := e.usecase(memory.NewFixedClock(fixedTransferTime))
	uc.Transfers = &failingTransferRepo{delegate: e.Transfers}
	if _, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6)); !errors.Is(err, errFake) {
		t.Fatalf("outer read failure = %v, want errFake", err)
	}
}

// The unique-constraint race: our transaction lost to a concurrent
// duplicate, rolled back, and the re-read must answer the original.
func TestAllocateTransferStock_UniqueConstraintRaceAnswersOriginal(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, siteA, 10)
	// The concurrent winner's durable rows, seeded exactly as its own
	// committed transaction would have left them.
	seedReservation(t, e, "res-winner", sku, 6)
	winner, err := transfer.NewAllocated("tr-1", "tl-1", siteA, sku, mustQty(t, 6), "res-winner", fixedTransferTime)
	if err != nil {
		t.Fatalf("seed winner: %v", err)
	}

	uc := e.usecase(clock)
	uc.Transfers = &uniqueRacingTransferRepo{delegate: e.Transfers, winner: winner}

	res, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6))
	if err != nil {
		t.Fatalf("unique race: %v", err)
	}
	if !res.Replay {
		t.Fatal("losing the unique race must answer the winner's outcome as a replay")
	}
	if res.Allocation.ReservationID() != "res-winner" {
		t.Fatalf("reservation = %q, want the winner's res-winner", res.Allocation.ReservationID())
	}
	if res.Reservation == nil || res.Reservation.ID() != "res-winner" {
		t.Fatalf("replay must carry the winner's reservation, got %+v", res.Reservation)
	}
	// NOTE: the loser's in-memory stock decrement is NOT asserted here.
	// Without a real transactional UnitOfWork the memory repo cannot roll
	// the draw back (atomically falls through to direct execution); the
	// all-or-nothing proof belongs to the testcontainers integration
	// suite (transfer_allocation_integration_test.go). What this test
	// pins is the routing: losing the race answers the winner's durable
	// outcome instead of erroring or double-allocating.
}

// uniqueRacingTransferRepo simulates losing the insert race: reads see
// nothing until Save, which fails with ErrTransferLineAlreadyDecided;
// the recovery re-read returns the concurrent winner.
type uniqueRacingTransferRepo struct {
	delegate *memory.TransferAllocationRepo
	winner   *transfer.Allocation
	saved    bool
}

func (r *uniqueRacingTransferRepo) FindByTransferLineID(ctx context.Context, lineID string) (*transfer.Allocation, error) {
	if r.saved {
		return r.winner, nil // recovery re-read after the rolled-back insert
	}
	return nil, nil
}

func (r *uniqueRacingTransferRepo) Save(context.Context, *transfer.Allocation) error {
	r.saved = true
	return usecases.ErrTransferLineAlreadyDecided
}

// The reservation lookup on the replay path surfaces its failure too.
func TestAllocateTransferStock_ReplayReservationReadFailureSurfaces(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, siteA, 10)

	uc := e.usecase(clock)
	if _, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6)); err != nil {
		t.Fatalf("first: %v", err)
	}

	uc2 := e.usecase(clock)
	uc2.Reservations = &failingReservationRepo{delegate: e.Reservations, failFindByID: true}
	if _, err := uc2.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6)); !errors.Is(err, errFake) {
		t.Fatalf("replay reservation read = %v, want errFake", err)
	}
}

// --- StowTransferStock: in-transaction replay & errors ---------------------

// A stow whose receipt was already STOWED by the time the in-transaction
// re-check runs answers the original units, not a second set.
func TestStowTransferStock_InTxAlreadyStowedAnswersOriginal(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)
	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	// Stow once for real — this creates the destination units.
	if _, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err != nil {
		t.Fatalf("first stow: %v", err)
	}
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 1 {
		t.Fatalf("destination units = %d, want 1 after the first stow", len(units))
	}

	// Now a racing stow: outer read sees STAGED, in-tx re-check sees
	// STOWED — it must answer the original units.
	uc := e.stowUseCase()
	uc.Receipts = &stowRacingReceiptRepo{delegate: e.Receipts, stowedAfter: 1}
	res, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)}))
	if err != nil {
		t.Fatalf("racing stow: %v", err)
	}
	if !res.Replay {
		t.Fatal("an in-transaction already-stowed re-check must be a replay")
	}
	if len(res.StockUnits) != 1 || res.StockUnits[0].ID() != units[0].ID() {
		t.Fatalf("replay must return the ORIGINAL unit %s, got %+v", units[0].ID(), res.StockUnits)
	}
	// Still exactly one destination unit.
	after, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(after) != 1 {
		t.Fatalf("destination units after racing stow = %d, want still 1", len(after))
	}
}

// stowRacingReceiptRepo returns the delegate's state for the first N
// reads, then flips the receipt to STOWED — simulating a concurrent stow
// committing between the outer read and the in-transaction re-check.
type stowRacingReceiptRepo struct {
	delegate    *memory.TransferReceiptRepo
	stowedAfter int
	calls       int
}

func (r *stowRacingReceiptRepo) FindByTransferLineID(ctx context.Context, lineID string) (*transfer.Receipt, error) {
	r.calls++
	rec, err := r.delegate.FindByTransferLineID(ctx, lineID)
	if err != nil || rec == nil {
		return rec, err
	}
	if r.calls > r.stowedAfter {
		// In-transaction view: already stowed by the concurrent winner.
		if err := rec.MarkStowed(nil, fixedTransferTime); err != nil {
			return nil, err
		}
	}
	return rec, nil
}

func (r *stowRacingReceiptRepo) Save(ctx context.Context, receipt *transfer.Receipt) error {
	return r.delegate.Save(ctx, receipt)
}

func (r *stowRacingReceiptRepo) SaveStowed(ctx context.Context, receipt *transfer.Receipt) error {
	return r.delegate.SaveStowed(ctx, receipt)
}

// The unique-state race: our stow lost to a concurrent winner; the
// recovery re-read must answer the original.
func TestStowTransferStock_UniqueStateRaceAnswersOriginal(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)
	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	uc := e.stowUseCase()
	uc.Receipts = &stowStateRacingRepo{delegate: e.Receipts}

	// The winner's stow leg must resolve to a real unit for the replay
	// read — seed it as the winner's own transaction would have.
	winnerBin, _ := shared.NewBinId("BIN-WINNER")
	winnerUnit, err := stock.NewStockUnitAtSite("su-winner", mustSKUT(t, "SKU-T"), winnerBin, mustQtyT(t, 5), mustSiteIDT(t, "SITE-DEST"))
	if err != nil {
		t.Fatalf("seed winner unit: %v", err)
	}
	if err := e.Stock.Save(context.Background(), winnerUnit); err != nil {
		t.Fatalf("seed winner unit save: %v", err)
	}

	res, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)}))
	if err != nil {
		t.Fatalf("state race: %v", err)
	}
	if res == nil {
		t.Fatal("state race must still answer a result")
	}
	if !res.Replay {
		t.Fatal("losing the state race must answer the winner's outcome as a replay")
	}
	if res.Receipt.State() != transfer.ReceiptStowed {
		t.Fatalf("replayed receipt state = %s, want STOWED", res.Receipt.State())
	}
}

// stowStateRacingRepo: reads pass through until the first SaveStowed
// attempt; that save fails with ErrTransferReceiptAlreadyStowed (the
// concurrent winner), and the recovery re-read returns a STOWED receipt
// built via RehydrateReceipt (not via mutating the delegate's copy, so
// the loser's own writes never land anywhere).
type stowStateRacingRepo struct {
	delegate  *memory.TransferReceiptRepo
	saveCalls int
}

func (r *stowStateRacingRepo) FindByTransferLineID(ctx context.Context, lineID string) (*transfer.Receipt, error) {
	rec, err := r.delegate.FindByTransferLineID(ctx, lineID)
	if err != nil || rec == nil {
		return rec, err
	}
	if r.saveCalls > 0 {
		// Recovery view: the concurrent winner already stowed. Build the
		// STOWED aggregate the way the winner's own repo row would
		// rehydrate: state, legs and timestamp included.
		winnerBin, _ := shared.NewBinId("BIN-WINNER")
		return transfer.RehydrateReceipt(
			rec.TransferID(), rec.TransferLineID(), rec.DestinationSiteID(), rec.SKU(),
			rec.ReservationID(), rec.ExpectedQuantity(), rec.ReceivedQuantity(), rec.Variance(),
			transfer.ReceiptStowed, []transfer.StowLeg{{StockUnitID: "su-winner", BinID: winnerBin, Quantity: rec.ReceivedQuantity()}},
			fixedTransferTime, fixedTransferTime,
		)
	}
	return rec, nil
}

func (r *stowStateRacingRepo) Save(ctx context.Context, receipt *transfer.Receipt) error {
	return r.delegate.Save(ctx, receipt)
}

func (r *stowStateRacingRepo) SaveStowed(ctx context.Context, receipt *transfer.Receipt) error {
	r.saveCalls++
	if r.saveCalls == 1 {
		return usecases.ErrTransferReceiptAlreadyStowed
	}
	return r.delegate.SaveStowed(ctx, receipt)
}

// A quarantine whose exception save loses the race returns the
// concurrent winner's row (Replay=true).
func TestStageTransferReceipt_QuarantineSaveRaceReturnsWinner(t *testing.T) {
	e := newReceiptEnv()

	winner, err := transfer.NewException(
		"tr-1", "tl-1", mustSiteIDT(t, "SITE-DEST"), mustSKUT(t, "SKU-T"), mustQtyT(t, 5),
		transfer.ExceptionUnknownTransfer, "lost the race", fixedTransferTime,
	)
	if err != nil {
		t.Fatalf("seed winner exception: %v", err)
	}

	uc := e.stageUseCase()
	uc.Exceptions = &racingExceptionRepo{delegate: e.Exceptions, winner: winner}

	res, err := uc.Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5))
	if err == nil {
		t.Fatal("a quarantined scan must surface its Problem")
	}
	if res == nil || res.Exception == nil {
		t.Fatalf("race must answer the winner's exception row, got %+v", res)
	}
	if !res.Replay {
		t.Fatal("losing the exception-save race must be flagged as a replay")
	}
	if res.Exception.TransferLineID() != winner.TransferLineID() || res.Exception.Kind() != winner.Kind() {
		t.Fatalf("exception = %+v, want the winner's row", res.Exception)
	}
	if res.Exception.Detail() != winner.Detail() {
		t.Fatalf("exception detail = %q, want the winner's %q", res.Exception.Detail(), winner.Detail())
	}
}

// racingExceptionRepo: FindByScan sees nothing (first scan), Save fails
// with errFake (the race), and the recovery FindByScan returns the
// concurrent winner.
type racingExceptionRepo struct {
	delegate *memory.InventoryExceptionRepo
	winner   *transfer.Exception
	saved    bool
}

func (r *racingExceptionRepo) FindByScan(ctx context.Context, lineID string, site shared.SiteID, sku shared.SKU, qty shared.Quantity) (*transfer.Exception, error) {
	if r.saved {
		return r.winner, nil
	}
	return nil, nil
}

func (r *racingExceptionRepo) Save(context.Context, *transfer.Exception) error {
	r.saved = true
	return errFake // the unique constraint fired; recovery re-reads
}

// The ledger read inside decide() surfaces its failure.
func TestStageTransferReceipt_InTxLedgerReadFailureSurfaces(t *testing.T) {
	e := newReceiptEnv()
	uc := e.stageUseCase()
	uc.Receipts = &passThroughOnceReceiptRepo{delegate: e.Receipts}
	uc.Transfers = &failingTransferRepo{delegate: e.Transfers}
	if _, err := uc.Execute(context.Background(), receiptCmd("tr-9", "tl-9", "SITE-DEST", "SKU-T", 5)); !errors.Is(err, errFake) {
		t.Fatalf("in-tx ledger read = %v, want errFake", err)
	}
}

// passThroughOnceReceiptRepo returns nil on the first read (so Execute
// proceeds into the transaction) and delegates afterwards.
type passThroughOnceReceiptRepo struct {
	delegate *memory.TransferReceiptRepo
	calls    int
}

func (r *passThroughOnceReceiptRepo) FindByTransferLineID(ctx context.Context, lineID string) (*transfer.Receipt, error) {
	r.calls++
	if r.calls == 1 {
		return nil, nil
	}
	return r.delegate.FindByTransferLineID(ctx, lineID)
}

func (r *passThroughOnceReceiptRepo) Save(ctx context.Context, receipt *transfer.Receipt) error {
	return r.delegate.Save(ctx, receipt)
}

func (r *passThroughOnceReceiptRepo) SaveStowed(ctx context.Context, receipt *transfer.Receipt) error {
	return r.delegate.SaveStowed(ctx, receipt)
}

// The quarantine replay check (FindByScan) failure surfaces.
func TestStageTransferReceipt_QuarantineReplayCheckFailureSurfaces(t *testing.T) {
	e := newReceiptEnv()
	uc := e.stageUseCase()
	uc.Receipts = &passThroughOnceReceiptRepo{delegate: e.Receipts}
	uc.Exceptions = &failingScanExceptionRepo{}
	if _, err := uc.Execute(context.Background(), receiptCmd("tr-9", "tl-9", "SITE-DEST", "SKU-T", 5)); !errors.Is(err, errFake) {
		t.Fatalf("quarantine replay check = %v, want errFake", err)
	}
}

// failingScanExceptionRepo fails FindByScan.
type failingScanExceptionRepo struct{}

func (failingScanExceptionRepo) FindByScan(context.Context, string, shared.SiteID, shared.SKU, shared.Quantity) (*transfer.Exception, error) {
	return nil, errFake
}

func (failingScanExceptionRepo) Save(context.Context, *transfer.Exception) error { return nil }

// The stow happy-path re-read finding a missing stock unit (data
// corruption) surfaces the naming error.
func TestStowTransferStock_ReplayMissingStockUnitSurfacesError(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)
	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	// Stow once, then delete the created unit behind the receipt's back
	// — the replay path must name the missing unit.
	if _, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err != nil {
		t.Fatalf("first stow: %v", err)
	}
	stowed, _ := e.Receipts.FindByTransferLineID(context.Background(), "tl-1")
	unitID := stowed.StowLegs()[0].StockUnitID

	hollow := &hollowStockRepo{delegate: e.Stock, missing: map[string]bool{unitID: true}}
	uc := e.stowUseCase()
	uc.Stock = hollow
	_, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)}))
	if err == nil || !strings.Contains(err.Error(), "missing stock unit") {
		t.Fatalf("replay with a deleted unit must name the missing stock unit, got %v", err)
	}
}

// hollowStockRepo delegates everything except FindByID for the missing set.
type hollowStockRepo struct {
	delegate *memory.StockRepo
	missing  map[string]bool
}

func (r *hollowStockRepo) Save(ctx context.Context, unit *stock.StockUnit) error {
	return r.delegate.Save(ctx, unit)
}

func (r *hollowStockRepo) FindByID(ctx context.Context, id string) (*stock.StockUnit, error) {
	if r.missing[id] {
		return nil, nil // the row vanished: data corruption
	}
	return r.delegate.FindByID(ctx, id)
}

func (r *hollowStockRepo) FindBySKU(ctx context.Context, sku shared.SKU) ([]*stock.StockUnit, error) {
	return r.delegate.FindBySKU(ctx, sku)
}

func (r *hollowStockRepo) FindByBin(ctx context.Context, binID shared.BinId) ([]*stock.StockUnit, error) {
	return r.delegate.FindByBin(ctx, binID)
}

func (r *hollowStockRepo) FindBySKUAtSite(ctx context.Context, sku shared.SKU, site shared.SiteID) ([]*stock.StockUnit, error) {
	return r.delegate.FindBySKUAtSite(ctx, sku, site)
}

func (r *hollowStockRepo) NextID(ctx context.Context) (string, error) {
	return r.delegate.NextID(ctx)
}

// The outbox (Events) failure on the staged-receipt publish propagates
// as the rollback signal: with a real UnitOfWork wired, the receipt the
// closure wrote must NOT survive the rolled-back transaction.
func TestStageTransferReceipt_PublishFailurePropagates(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)

	// recordingUoW commits only when the closure returned nil — exactly
	// the all-or-nothing contract the Postgres adapter provides.
	var committed bool
	uow := unitOfWorkFunc(func(ctx context.Context, fn func(ctx context.Context) error) error {
		if err := fn(ctx); err != nil {
			return err
		}
		committed = true
		return nil
	})

	uc := e.stageUseCase()
	uc.Events = failingEvents{}
	uc.UnitOfWork = uow
	if _, err := uc.Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); !errors.Is(err, errFake) {
		t.Fatalf("publish failure = %v, want errFake", err)
	}
	if committed {
		t.Fatal("a failed publish must roll the transaction back, not commit it")
	}
}

// unitOfWorkFunc adapts a closure to ports.UnitOfWork.
type unitOfWorkFunc func(ctx context.Context, fn func(ctx context.Context) error) error

func (f unitOfWorkFunc) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return f(ctx, fn)
}

// BufferedPublisher without a Clock still works (it is pure buffering).
var _ events.BufferedPublisher = events.BufferedPublisher{}
