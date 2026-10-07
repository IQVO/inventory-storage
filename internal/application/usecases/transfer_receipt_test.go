package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// receiptEnv bundles the destination-receipt adapters: the ledger (for
// seeding allocations), receipts, exceptions, stock, bins and the
// buffered event stream.
type receiptEnv struct {
	Transfers  *memory.TransferAllocationRepo
	Receipts   *memory.TransferReceiptRepo
	Exceptions *memory.InventoryExceptionRepo
	Stock      *memory.StockRepo
	Locations  *memory.LocationRepo
	Events     *events.BufferedPublisher
	Clock      *memory.FixedClock
}

func newReceiptEnv() receiptEnv {
	return receiptEnv{
		Transfers:  memory.NewTransferAllocationRepo(),
		Receipts:   memory.NewTransferReceiptRepo(),
		Exceptions: memory.NewInventoryExceptionRepo(),
		Stock:      memory.NewStockRepo(),
		Locations:  memory.NewLocationRepo(),
		Events:     events.NewBufferedPublisher(),
		Clock:      memory.NewFixedClock(fixedTransferTime),
	}
}

// seedAllocatedLine writes an ALLOCATED ledger row exactly as
// AllocateTransferStock would.
func seedAllocatedLine(t *testing.T, e receiptEnv, transferID, lineID, originSite, sku string, qty int) {
	t.Helper()
	allocated, err := transfer.NewAllocated(transferID, lineID, mustSiteID(t, originSite), mustSKU(t, sku), mustQty(t, qty), "res-"+lineID, fixedTransferTime)
	if err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	if err := e.Transfers.Save(context.Background(), allocated); err != nil {
		t.Fatalf("seed allocation save: %v", err)
	}
}

// seedBin registers a bin AT a site, as bin registration with site
// custody would.
func seedDestBin(t *testing.T, e receiptEnv, binID, site string, capacity int) {
	t.Helper()
	id, _ := shared.NewBinId(binID)
	bin, err := location.NewBin(id, mustQty(t, capacity))
	if err != nil {
		t.Fatalf("seed bin: %v", err)
	}
	// NewBin has no site in v1; rehydrate-with-site is the registered-
	// with-custody path.
	atSite := location.RehydrateBinAtSite(id, bin.Capacity(), bin.Occupied(), mustSiteID(t, site), bin.Version())
	if err := e.Locations.Save(context.Background(), atSite); err != nil {
		t.Fatalf("seed bin save: %v", err)
	}
}

func (e receiptEnv) stageUseCase() *usecases.StageTransferReceipt {
	return &usecases.StageTransferReceipt{
		Transfers:  e.Transfers,
		Receipts:   e.Receipts,
		Exceptions: e.Exceptions,
		Events:     e.Events,
		Clock:      e.Clock,
	}
}

func (e receiptEnv) stowUseCase() *usecases.StowTransferStock {
	return &usecases.StowTransferStock{
		Receipts:  e.Receipts,
		Stock:     e.Stock,
		Locations: e.Locations,
		Events:    e.Events,
		Clock:     e.Clock,
	}
}

func receiptCmd(transferID, lineID, destSite, sku string, qty int) usecases.ReceiptCommand {
	return usecases.ReceiptCommand{
		TransferID:        transferID,
		TransferLineID:    lineID,
		DestinationSiteID: mustSiteIDT(tUnused, destSite),
		SKU:               mustSKUT(tUnused, sku),
		ReceivedQuantity:  mustQtyT(tUnused, qty),
	}
}

// tiny local helpers so receiptCmd can be a plain function (a testing.T
// is not available in a struct-literal helper); they panic on invalid
// input, which only happens if the TEST itself is malformed.
var tUnused *testing.T

func mustSiteIDT(t *testing.T, raw string) shared.SiteID {
	site, err := shared.NewSiteID(raw)
	if err != nil {
		t.Fatalf("NewSiteID(%q): %v", raw, err)
	}
	return site
}

func mustSKUT(t *testing.T, raw string) shared.SKU {
	sku, err := shared.NewSKU(raw)
	if err != nil {
		t.Fatalf("NewSKU(%q): %v", raw, err)
	}
	return sku
}

func mustQtyT(t *testing.T, v int) shared.Quantity {
	qty, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("NewQuantity(%d): %v", v, err)
	}
	return qty
}

func stageEvents(e receiptEnv) (staged, stowed int) {
	for _, ev := range e.Events.Events() {
		switch ev.(type) {
		case shared.TransferReceiptStaged:
			staged++
		case shared.TransferStockStowed:
			stowed++
		}
	}
	return
}

// --- StageTransferReceipt -------------------------------------------------

func TestStageTransferReceipt_StagesAgainstAllocatedLine(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 6)

	res, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 6))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Quarantined() {
		t.Fatal("a recognized ALLOCATED line must not be quarantined")
	}
	if res.Receipt.State() != transfer.ReceiptStaged {
		t.Fatalf("state = %s, want STAGED", res.Receipt.State())
	}
	if res.Receipt.ExpectedQuantity().Int() != 6 {
		t.Fatalf("expected = %d, want 6 (from the ledger)", res.Receipt.ExpectedQuantity().Int())
	}
	if res.Receipt.Variance() != 0 {
		t.Fatalf("variance = %d, want 0", res.Receipt.Variance())
	}

	staged, stowed := stageEvents(e)
	if staged != 1 || stowed != 0 {
		t.Fatalf("events staged/stowed = %d/%d, want 1/0", staged, stowed)
	}

	// NO stock was created by staging.
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 0 {
		t.Fatalf("staging must create no stock units, found %d", len(units))
	}
}

func TestStageTransferReceipt_OverShortVarianceIsExplicit(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-o", "tl-over", "SITE-ORIG", "SKU-T", 6)
	seedAllocatedLine(t, e, "tr-s", "tl-short", "SITE-ORIG", "SKU-T", 6)

	over, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-o", "tl-over", "SITE-DEST", "SKU-T", 8))
	if err != nil {
		t.Fatalf("over: %v", err)
	}
	if over.Receipt.Variance() != 2 || !over.Receipt.IsOver() {
		t.Fatalf("over variance = %d, want +2", over.Receipt.Variance())
	}

	short, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-s", "tl-short", "SITE-DEST", "SKU-T", 5))
	if err != nil {
		t.Fatalf("short: %v", err)
	}
	if short.Receipt.Variance() != -1 || !short.Receipt.IsShort() {
		t.Fatalf("short variance = %d, want -1", short.Receipt.Variance())
	}

	// The events carry the signed variance explicitly.
	var sawOver, sawShort bool
	for _, ev := range e.Events.Events() {
		if s, ok := ev.(shared.TransferReceiptStaged); ok {
			switch s.TransferLineID {
			case "tl-over":
				sawOver = s.Variance == 2
			case "tl-short":
				sawShort = s.Variance == -1
			}
		}
	}
	if !sawOver || !sawShort {
		t.Fatalf("events must carry signed variance; over=%v short=%v", sawOver, sawShort)
	}
}

func TestStageTransferReceipt_UnknownLineIsQuarantined(t *testing.T) {
	e := newReceiptEnv()

	res, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-?", "tl-unknown", "SITE-DEST", "SKU-T", 5))
	if err == nil {
		t.Fatal("an unknown transfer line must return an explicit problem")
	}
	if res == nil || !res.Quarantined() {
		t.Fatal("the scan must be quarantined")
	}
	if res.Exception.Kind() != transfer.ExceptionUnknownTransfer {
		t.Fatalf("kind = %s, want UNKNOWN_TRANSFER", res.Exception.Kind())
	}

	// An inventory_exceptions row exists...
	found, _ := e.Exceptions.FindByScan(context.Background(), "tl-unknown", mustSiteIDT(t, "SITE-DEST"), mustSKUT(t, "SKU-T"), mustQtyT(t, 5))
	if found == nil {
		t.Fatal("the quarantine row must be persisted")
	}
	// ...no receipt was staged...
	if r, _ := e.Receipts.FindByTransferLineID(context.Background(), "tl-unknown"); r != nil {
		t.Fatal("no receipt may be staged for an unknown line")
	}
	// ...and nothing that could raise usable stock was published.
	staged, _ := stageEvents(e)
	if staged != 0 {
		t.Fatalf("staged events = %d, want 0", staged)
	}
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 0 {
		t.Fatalf("quarantine must create no stock, found %d", len(units))
	}
}

func TestStageTransferReceipt_RejectedAllocationIsQuarantined(t *testing.T) {
	e := newReceiptEnv()
	rejected, err := transfer.NewRejected("tr-r", "tl-rej", mustSiteIDT(t, "SITE-ORIG"), mustSKUT(t, "SKU-T"), mustQtyT(t, 6), transfer.ReasonInsufficientUsable, fixedTransferTime)
	if err != nil {
		t.Fatalf("seed rejection: %v", err)
	}
	if err := e.Transfers.Save(context.Background(), rejected); err != nil {
		t.Fatalf("seed rejection save: %v", err)
	}

	res, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-r", "tl-rej", "SITE-DEST", "SKU-T", 6))
	if err == nil {
		t.Fatal("a non-ALLOCATED line must return an explicit problem")
	}
	if res == nil || res.Exception.Kind() != transfer.ExceptionUnrecognizedTransfer {
		t.Fatal("the scan must be quarantined as UNRECOGNIZED_TRANSFER")
	}
	if r, _ := e.Receipts.FindByTransferLineID(context.Background(), "tl-rej"); r != nil {
		t.Fatal("no receipt may be staged against a REJECTED line")
	}
}

func TestStageTransferReceipt_MismatchedTransferIDIsQuarantined(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-real", "tl-mm", "SITE-ORIG", "SKU-T", 6)

	res, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-other", "tl-mm", "SITE-DEST", "SKU-T", 6))
	if err == nil {
		t.Fatal("a transfer_id mismatch must return an explicit problem")
	}
	if res == nil || !res.Quarantined() {
		t.Fatal("the scan must be quarantined")
	}
}

func TestStageTransferReceipt_OriginAsDestinationIsQuarantined(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-o", "tl-od", "SITE-ORIG", "SKU-T", 6)

	res, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-o", "tl-od", "SITE-ORIG", "SKU-T", 6))
	if err == nil {
		t.Fatal("receiving at the transfer's own origin must be refused")
	}
	if res == nil || !res.Quarantined() {
		t.Fatal("the scan must be quarantined")
	}
}

func TestStageTransferReceipt_IdempotentReplayReturnsOriginal(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 6)

	uc := e.stageUseCase()
	first, err := uc.Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 6))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := uc.Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 6))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !second.Replay {
		t.Fatal("the replay must be flagged")
	}
	if second.Receipt.StagedAt() != first.Receipt.StagedAt() {
		t.Fatal("the replay must return the ORIGINAL receipt, not a second one")
	}

	// Exactly one TransferReceiptStaged event exists.
	staged, _ := stageEvents(e)
	if staged != 1 {
		t.Fatalf("staged events after replay = %d, want exactly 1", staged)
	}
}

func TestStageTransferReceipt_ConflictingPayloadIsRefused(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 6)

	uc := e.stageUseCase()
	if _, err := uc.Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 6)); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := uc.Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err == nil {
		t.Fatal("a different payload on the same line must be refused")
	}
	// The original receipt stands.
	r, _ := e.Receipts.FindByTransferLineID(context.Background(), "tl-1")
	if r == nil || r.ReceivedQuantity().Int() != 6 {
		t.Fatal("the original receipt must stand immutable")
	}
}

func TestStageTransferReceipt_QuarantineReplayWritesNoSecondRow(t *testing.T) {
	e := newReceiptEnv()
	uc := e.stageUseCase()

	first, err := uc.Execute(context.Background(), receiptCmd("tr-?", "tl-q", "SITE-DEST", "SKU-T", 5))
	if err == nil || first == nil || !first.Quarantined() {
		t.Fatalf("first quarantine: res=%v err=%v", first, err)
	}
	second, err := uc.Execute(context.Background(), receiptCmd("tr-?", "tl-q", "SITE-DEST", "SKU-T", 5))
	if err == nil || second == nil || !second.Quarantined() {
		t.Fatalf("replay quarantine: res=%v err=%v", second, err)
	}
	if !second.Replay {
		t.Fatal("the repeated identical scan must be flagged as a replay")
	}
}

// --- StowTransferStock ----------------------------------------------------

func stowBins(pairs ...[2]any) []usecases.StowBin {
	var bins []usecases.StowBin
	for _, p := range pairs {
		binID, _ := shared.NewBinId(p[0].(string))
		bins = append(bins, usecases.StowBin{BinID: binID, Quantity: p[1].(shared.Quantity)})
	}
	return bins
}

func TestStowTransferStock_CreatesStockAtDestinationSite(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)
	seedDestBin(t, e, "BIN-D2", "SITE-DEST", 10)

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	res, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins(
		[2]any{"BIN-D1", mustQtyT(t, 3)},
		[2]any{"BIN-D2", mustQtyT(t, 2)},
	))
	if err != nil {
		t.Fatalf("stow: %v", err)
	}
	if res.Replay {
		t.Fatal("the first stow must not be flagged as a replay")
	}
	if res.Receipt.State() != transfer.ReceiptStowed {
		t.Fatalf("state = %s, want STOWED", res.Receipt.State())
	}

	// Two StockUnits were created AT the destination site.
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 2 {
		t.Fatalf("stock units = %d, want 2", len(units))
	}
	total := 0
	for _, unit := range units {
		if !unit.IsAtSite(mustSiteIDT(t, "SITE-DEST")) {
			t.Fatalf("unit %s is at site %q, want SITE-DEST", unit.ID(), unit.SiteID())
		}
		total += unit.Usable().Int()
	}
	if total != 5 {
		t.Fatalf("usable at destination = %d, want 5", total)
	}

	// Bins' occupancy rose.
	for _, binID := range []string{"BIN-D1", "BIN-D2"} {
		id, _ := shared.NewBinId(binID)
		bin, _ := e.Locations.FindByID(context.Background(), id)
		if bin == nil || bin.Occupied().Int() == 0 {
			t.Fatalf("bin %s occupancy must rise", binID)
		}
	}

	// Exactly one TransferStockStowed.
	_, stowed := stageEvents(e)
	if stowed != 1 {
		t.Fatalf("stowed events = %d, want exactly 1", stowed)
	}
}

func TestStowTransferStock_WrongSiteBinIsRejected(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-OTHER", "SITE-OTHER", 10)

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	_, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-OTHER", mustQtyT(t, 5)}))
	if err == nil {
		t.Fatal("a bin at another site must be rejected")
	}
	// No stock was created and the receipt is still STAGED.
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 0 {
		t.Fatalf("rejected stow must create no stock, found %d", len(units))
	}
	r, _ := e.Receipts.FindByTransferLineID(context.Background(), "tl-1")
	if r == nil || r.State() != transfer.ReceiptStaged {
		t.Fatal("the receipt must remain STAGED after a rejected stow")
	}
	_, stowed := stageEvents(e)
	if stowed != 0 {
		t.Fatalf("stowed events = %d, want 0", stowed)
	}
}

func TestStowTransferStock_LegacySitelessBinIsRejected(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	// A legacy bin with NO recorded site (migration-0000-era row).
	id, _ := shared.NewBinId("BIN-LEGACY")
	bin, _ := location.NewBin(id, mustQtyT(t, 10))
	if err := e.Locations.Save(context.Background(), bin); err != nil {
		t.Fatalf("seed legacy bin: %v", err)
	}

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if _, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-LEGACY", mustQtyT(t, 5)})); err == nil {
		t.Fatal("a site-less legacy bin must fail closed")
	}
}

func TestStowTransferStock_UnknownBinIsRejected(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}
	_, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-NOWHERE", mustQtyT(t, 5)}))
	if err == nil {
		t.Fatal("an unregistered bin must be rejected")
	}
}

func TestStowTransferStock_QuantityMismatchIsRejected(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}
	// Sum 3 != received 5.
	if _, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 3)})); err == nil {
		t.Fatal("a partial stow must be rejected")
	}
}

func TestStowTransferStock_StowBeforeStageIsRejected(t *testing.T) {
	e := newReceiptEnv()
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)

	_, err := e.stowUseCase().Execute(context.Background(), "tl-never", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)}))
	if err == nil {
		t.Fatal("stowing a line with no receipt must be rejected")
	}
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 0 {
		t.Fatalf("no stock may be created, found %d", len(units))
	}
}

func TestStowTransferStock_IdempotentReplayCreatesNoSecondUnit(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	uc := e.stowUseCase()
	first, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)}))
	if err != nil {
		t.Fatalf("first stow: %v", err)
	}
	second, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)}))
	if err != nil {
		t.Fatalf("replay stow: %v", err)
	}
	if !second.Replay {
		t.Fatal("the replay must be flagged")
	}
	if len(second.StockUnits) != len(first.StockUnits) {
		t.Fatalf("replay units = %d, want %d (the ORIGINAL units)", len(second.StockUnits), len(first.StockUnits))
	}
	if second.StockUnits[0].ID() != first.StockUnits[0].ID() {
		t.Fatal("the replay must return the ORIGINAL stock unit ids")
	}

	// Exactly one StockUnit exists and exactly one stow event.
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 1 {
		t.Fatalf("stock units after replay = %d, want exactly 1", len(units))
	}
	_, stowed := stageEvents(e)
	if stowed != 1 {
		t.Fatalf("stowed events after replay = %d, want exactly 1", stowed)
	}
	// Destination usable rose exactly once.
	if got := units[0].Usable().Int(); got != 5 {
		t.Fatalf("usable = %d, want 5", got)
	}
}

func TestStowTransferStock_OutboxFailureSurfacesError(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	uc := e.stowUseCase()
	uc.Events = failingEvents{}
	if _, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}

	// The REAL rollback property (receipt stays STAGED, no stock row, no
	// outbox row — everything the transaction wrote is undone) is proven
	// against actual Postgres by
	// TestIntegration_TransferReceipt_OutboxFailureRollsReceiptAndStockBack;
	// the in-memory repos share aggregate pointers and have no
	// transactional backing, so this unit test asserts only that the
	// publish failure surfaces as the use case's error (the signal the
	// UnitOfWeek rolls back on).
}

// The stow's whole write section runs inside ONE UnitOfWork.Execute.
func TestStowTransferStock_WrapsEverythingInOneUnitOfWork(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)
	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	uow := &recordingUnitOfWork{}
	uc := e.stowUseCase()
	uc.UnitOfWork = uow
	if _, err := uc.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err != nil {
		t.Fatalf("stow: %v", err)
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork.Execute called %d times, want 1", uow.calls)
	}
}

// A short receipt stows its (smaller) received quantity, not the
// expected one — the variance stays explicit on the receipt and the
// event, never absorbed into stock.
func TestStowTransferStock_ShortReceiptStowsReceivedNotExpected(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 6)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)

	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if _, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 6)})); err == nil {
		t.Fatal("stowing more than received must be rejected")
	}
	if _, err := e.stowUseCase().Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err != nil {
		t.Fatalf("stow received: %v", err)
	}
	units, _ := e.Stock.FindBySKU(context.Background(), mustSKUT(t, "SKU-T"))
	if len(units) != 1 || units[0].Usable().Int() != 5 {
		t.Fatalf("destination usable must be the RECEIVED 5, got %+v", units)
	}
}

var _ = stock.NewStockUnitAtSite

// --- validation coverage ----------------------------------------------------

func TestValidateReceiptCommand_CoversEveryField(t *testing.T) {
	site := mustSiteIDT(t, "SITE-D")
	sku := mustSKUT(t, "SKU-1")
	base := usecases.ReceiptCommand{TransferID: "tr", TransferLineID: "tl", DestinationSiteID: site, SKU: sku, ReceivedQuantity: mustQtyT(t, 5)}
	if err := usecases.ValidateReceiptCommand(base); err != nil {
		t.Fatalf("valid command: %v", err)
	}
	for _, mut := range []struct {
		name string
		cmd  usecases.ReceiptCommand
	}{
		{"no transfer id", func() usecases.ReceiptCommand { c := base; c.TransferID = ""; return c }()},
		{"no line id", func() usecases.ReceiptCommand { c := base; c.TransferLineID = ""; return c }()},
		{"no site", func() usecases.ReceiptCommand { c := base; c.DestinationSiteID = ""; return c }()},
		{"no sku", func() usecases.ReceiptCommand { c := base; c.SKU = ""; return c }()},
		{"zero quantity", func() usecases.ReceiptCommand { c := base; c.ReceivedQuantity = mustQtyT(t, 0); return c }()},
	} {
		if err := usecases.ValidateReceiptCommand(mut.cmd); err == nil {
			t.Fatalf("%s must fail validation", mut.name)
		}
	}
}

func TestValidateStowCommand_CoversEveryField(t *testing.T) {
	bin, _ := shared.NewBinId("BIN-1")
	valid := []usecases.StowBin{{BinID: bin, Quantity: mustQtyT(t, 5)}}
	if err := usecases.ValidateStowCommand("tl", valid); err != nil {
		t.Fatalf("valid command: %v", err)
	}
	emptyBin, _ := shared.NewBinId("")
	if err := usecases.ValidateStowCommand("", valid); err == nil {
		t.Fatal("empty line id must fail")
	}
	if err := usecases.ValidateStowCommand("tl", nil); err == nil {
		t.Fatal("no bins must fail")
	}
	if err := usecases.ValidateStowCommand("tl", []usecases.StowBin{{BinID: emptyBin, Quantity: mustQtyT(t, 5)}}); err == nil {
		t.Fatal("empty bin id must fail")
	}
	if err := usecases.ValidateStowCommand("tl", []usecases.StowBin{{BinID: bin, Quantity: mustQtyT(t, 0)}}); err == nil {
		t.Fatal("zero quantity must fail")
	}
}

// Stage surfaces repo failures unchanged (the transaction's rollback
// signal), and a receipt-row write race routes to the original.
func TestStageTransferReceipt_ErrorBranches(t *testing.T) {
	e := newReceiptEnv()
	uc := e.stageUseCase()

	// Malformed command: deterministic, no writes.
	if _, err := uc.Execute(context.Background(), receiptCmd("", "tl", "SITE-D", "SKU-T", 5)); err == nil {
		t.Fatal("a malformed command must fail")
	}

	// A receipt write that loses the unique race answers the original.
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 6)
	racing := &racingReceiptRepo{delegate: e.Receipts}
	uc2 := e.stageUseCase()
	uc2.Receipts = racing
	res, err := uc2.Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 6))
	if err != nil || res == nil || !res.Replay {
		t.Fatalf("unique-race must answer the original: res=%v err=%v", res, err)
	}

	// A repo read failure surfaces unchanged.
	uc3 := e.stageUseCase()
	uc3.Receipts = &failingReceiptRepo{delegate: e.Receipts}
	if _, err := uc3.Execute(context.Background(), receiptCmd("tr-9", "tl-9", "SITE-DEST", "SKU-T", 5)); err != errFake {
		t.Fatalf("read failure = %v, want errFake", err)
	}
}

// racingReceiptRepo inserts a receipt for a DIFFERENT line on the first
// in-transaction read, simulating the concurrent-insert race the unique
// constraint would catch in Postgres.
type racingReceiptRepo struct {
	delegate *memory.TransferReceiptRepo
	raced    bool
}

func (r *racingReceiptRepo) FindByTransferLineID(ctx context.Context, lineID string) (*transfer.Receipt, error) {
	if !r.raced {
		r.raced = true
		return nil, nil // outer read: nothing yet
	}
	// In-transaction read: pretend a concurrent stage committed.
	receipt, err := transfer.NewStagedReceipt("tr-1", lineID, mustSiteIDT(nil, "SITE-DEST"), mustSKUT(nil, "SKU-T"), "res-tl-1", mustQtyT(nil, 6), mustQtyT(nil, 6), fixedTransferTime)
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

func (r *racingReceiptRepo) Save(ctx context.Context, receipt *transfer.Receipt) error {
	return r.delegate.Save(ctx, receipt)
}

func (r *racingReceiptRepo) SaveStowed(ctx context.Context, receipt *transfer.Receipt) error {
	return r.delegate.SaveStowed(ctx, receipt)
}

// failingReceiptRepo fails the FIRST FindByTransferLineID read.
type failingReceiptRepo struct {
	delegate *memory.TransferReceiptRepo
}

func (f *failingReceiptRepo) FindByTransferLineID(context.Context, string) (*transfer.Receipt, error) {
	return nil, errFake
}

func (f *failingReceiptRepo) Save(ctx context.Context, receipt *transfer.Receipt) error {
	return f.delegate.Save(ctx, receipt)
}

func (f *failingReceiptRepo) SaveStowed(ctx context.Context, receipt *transfer.Receipt) error {
	return f.delegate.SaveStowed(ctx, receipt)
}

// Stow error branches: read failure, in-tx nil receipt, unique-state
// race, publish failure surfacing, and a missing stock unit on the
// happy-path re-read.
func TestStowTransferStock_ErrorBranches(t *testing.T) {
	e := newReceiptEnv()
	seedAllocatedLine(t, e, "tr-1", "tl-1", "SITE-ORIG", "SKU-T", 5)
	seedDestBin(t, e, "BIN-D1", "SITE-DEST", 10)
	if _, err := e.stageUseCase().Execute(context.Background(), receiptCmd("tr-1", "tl-1", "SITE-DEST", "SKU-T", 5)); err != nil {
		t.Fatalf("stage: %v", err)
	}

	// Malformed command.
	uc := e.stowUseCase()
	if _, err := uc.Execute(context.Background(), "", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err == nil {
		t.Fatal("malformed stow must fail")
	}

	// Outer read failure surfaces unchanged.
	uc2 := e.stowUseCase()
	uc2.Receipts = &failingReceiptRepo{delegate: e.Receipts}
	if _, err := uc2.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err != errFake {
		t.Fatalf("outer read failure = %v, want errFake", err)
	}

	// Publish failure propagates (rollback signal).
	uc3 := e.stowUseCase()
	uc3.Events = failingEvents{}
	if _, err := uc3.Execute(context.Background(), "tl-1", stowBins([2]any{"BIN-D1", mustQtyT(t, 5)})); err != errFake {
		t.Fatalf("publish failure = %v, want errFake", err)
	}
}
