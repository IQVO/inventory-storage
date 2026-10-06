package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// transferEnv bundles the transfer-allocation adapters: site-scoped stock,
// reservations, the ledger and a buffered event stream.
type transferEnv struct {
	Stock        *memory.StockRepo
	Reservations *memory.ReservationRepo
	Transfers    *memory.TransferAllocationRepo
	Events       *events.BufferedPublisher
}

func newTransferEnv() transferEnv {
	return transferEnv{
		Stock:        memory.NewStockRepo(),
		Reservations: memory.NewReservationRepo(),
		Transfers:    memory.NewTransferAllocationRepo(),
		Events:       events.NewBufferedPublisher(),
	}
}

func mustSiteID(t *testing.T, raw string) shared.SiteID {
	t.Helper()
	site, err := shared.NewSiteID(raw)
	if err != nil {
		t.Fatalf("NewSiteID(%q): %v", raw, err)
	}
	return site
}

// seedUnit stows qty of sku at site and returns the unit id.
func seedUnit(t *testing.T, e transferEnv, id string, sku shared.SKU, site shared.SiteID, qty int) {
	t.Helper()
	bin, _ := shared.NewBinId("BIN-" + id)
	unit, err := stock.NewStockUnitAtSite(id, sku, bin, mustQty(t, qty), site)
	if err != nil {
		t.Fatalf("NewStockUnitAtSite: %v", err)
	}
	if err := e.Stock.Save(context.Background(), unit); err != nil {
		t.Fatalf("seed stock %s: %v", id, err)
	}
}

func (e transferEnv) usecase(clock *memory.FixedClock) *usecases.AllocateTransferStock {
	return &usecases.AllocateTransferStock{
		Stock:        e.Stock,
		Reservations: e.Reservations,
		Transfers:    e.Transfers,
		Events:       e.Events,
		Clock:        clock,
	}
}

var fixedTransferTime = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func TestAllocateTransferStock_AllocatesFromOriginSiteOnly(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA, siteB := mustSiteID(t, "SITE-A"), mustSiteID(t, "SITE-B")
	sku := mustSKU(t, "SKU-T")

	seedUnit(t, e, "su-a", sku, siteA, 10)
	seedUnit(t, e, "su-b", sku, siteB, 10)

	uc := e.usecase(clock)
	result, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Allocation.Outcome() != transfer.OutcomeAllocated {
		t.Fatalf("outcome = %s, want ALLOCATED", result.Allocation.Outcome())
	}
	if result.Reservation == nil {
		t.Fatal("allocated result must carry the reservation")
	}

	// Exactly 6 left SITE-A's usable; SITE-B is untouched.
	assertUnitUsable(t, e, "su-a", 4, "origin site must be decremented")
	assertUnitUsable(t, e, "su-b", 10, "other site's stock must be untouched")

	// The ledger holds the correlation.
	ledger, err := e.Transfers.FindByTransferLineID(context.Background(), "tl-1")
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}
	if ledger == nil || ledger.ReservationID() != result.Reservation.ID() {
		t.Fatalf("ledger row missing or mismatched: %+v", ledger)
	}

	// The allocated event was published with the transfer correlation.
	found := false
	for _, ev := range e.Events.Events() {
		if a, ok := ev.(shared.TransferStockAllocated); ok {
			found = true
			if a.TransferID != "tr-1" || a.TransferLineID != "tl-1" || a.ReservationID != result.Reservation.ID() {
				t.Fatalf("allocated event correlation wrong: %+v", a)
			}
			if len(a.Allocations) != 1 || a.Allocations[0].StockUnitID != "su-a" {
				t.Fatalf("allocated event legs must point at the origin unit: %+v", a.Allocations)
			}
		}
	}
	if !found {
		t.Fatal("TransferStockAllocated was not published")
	}
}

func TestAllocateTransferStock_ReplayReturnsOriginalWithoutTouchingStock(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, siteA, 10)

	uc := e.usecase(clock)
	first, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6))
	if err != nil {
		t.Fatalf("first: %v", err)
	}

	second, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if !second.Replay {
		t.Fatal("second execution must be flagged as a replay")
	}
	if second.Allocation.ReservationID() != first.Allocation.ReservationID() {
		t.Fatalf("replay returned a different reservation: %s vs %s", second.Allocation.ReservationID(), first.Allocation.ReservationID())
	}
	// ONE reservation, ONE decrement — not two.
	assertUnitUsable(t, e, "su-a", 4, "replay must not decrement again")
	resForLine, err := e.Reservations.FindByDemandRef(context.Background(), usecases.TransferDemandRef("tr-1", "tl-1"))
	if err != nil {
		t.Fatalf("reservations by demandRef: %v", err)
	}
	if len(resForLine) != 1 {
		t.Fatalf("transfer reservations = %d, want exactly 1", len(resForLine))
	}
}

func TestAllocateTransferStock_UnknownOriginSiteIsRejected(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, mustSiteID(t, "SITE-A"), 10)

	uc := e.usecase(clock)
	result, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-NOWHERE", "SKU-T", 6))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allocation.Outcome() != transfer.OutcomeRejected {
		t.Fatalf("outcome = %s, want REJECTED", result.Allocation.Outcome())
	}
	if result.Allocation.RejectionReason() != transfer.ReasonOriginSiteUnknown {
		t.Fatalf("reason = %s, want ORIGIN_SITE_UNKNOWN", result.Allocation.RejectionReason())
	}

	// Exactly one rejection event, closed reason code.
	rejections := 0
	for _, ev := range e.Events.Events() {
		if r, ok := ev.(shared.TransferStockAllocationRejected); ok {
			rejections++
			if r.Reason != usecases.RejectionOriginSiteUnknown {
				t.Fatalf("rejection reason = %s, want ORIGIN_SITE_UNKNOWN", r.Reason)
			}
		}
	}
	if rejections != 1 {
		t.Fatalf("rejection events = %d, want exactly 1", rejections)
	}

	// Stock untouched.
	assertUnitUsable(t, e, "su-a", 10, "a rejection must not touch stock")

	// Replaying the rejected command returns the same rejection, stably.
	again, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-NOWHERE", "SKU-T", 6))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if again.Allocation.Outcome() != transfer.OutcomeRejected || !again.Replay {
		t.Fatalf("replayed rejection = %+v (replay=%v), want the original REJECTED", again.Allocation, again.Replay)
	}
}

func TestAllocateTransferStock_InsufficientUsableIsRejected(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, siteA, 3)
	seedUnit(t, e, "su-b", sku, mustSiteID(t, "SITE-B"), 100)

	uc := e.usecase(clock)
	result, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allocation.Outcome() != transfer.OutcomeRejected ||
		result.Allocation.RejectionReason() != transfer.ReasonInsufficientUsable {
		t.Fatalf("outcome = %+v, want REJECTED/INSUFFICIENT_USABLE", result.Allocation)
	}

	// The other site's abundance must NOT have been drawn.
	assertUnitUsable(t, e, "su-b", 100, "another site's stock must never cover a site-scoped ask")
}

func TestAllocateTransferStock_ConflictingReplayIsIdempotencyConflict(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	siteA := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-T")
	seedUnit(t, e, "su-a", sku, siteA, 10)

	uc := e.usecase(clock)
	if _, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 6)); err != nil {
		t.Fatalf("first: %v", err)
	}

	// Same line id, DIFFERENT quantity: an idempotency conflict.
	result, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 9))
	if err != nil {
		t.Fatalf("conflict: %v", err)
	}
	if result.Replay {
		t.Fatal("a conflicting command is not a replay")
	}
	if result.Allocation.Outcome() != transfer.OutcomeRejected ||
		result.Allocation.RejectionReason() != transfer.ReasonIdempotencyConflict {
		t.Fatalf("conflict outcome = %+v, want REJECTED/IDEMPOTENCY_CONFLICT", result.Allocation)
	}

	// The conflict published a rejection event with the closed reason.
	found := false
	for _, ev := range e.Events.Events() {
		if r, ok := ev.(shared.TransferStockAllocationRejected); ok && r.Reason == usecases.RejectionIdempotencyConflict {
			found = true
		}
	}
	if !found {
		t.Fatal("IDEMPOTENCY_CONFLICT rejection event was not published")
	}

	// The ORIGINAL ledger decision still stands.
	ledger, _ := e.Transfers.FindByTransferLineID(context.Background(), "tl-1")
	if ledger == nil || ledger.Outcome() != transfer.OutcomeAllocated || ledger.RequestedQuantity().Int() != 6 {
		t.Fatalf("original ledger decision must be immutable, got %+v", ledger)
	}
}

func TestAllocateTransferStock_LegacySitelessStockIsNotAllocatable(t *testing.T) {
	e := newTransferEnv()
	clock := memory.NewFixedClock(fixedTransferTime)
	sku := mustSKU(t, "SKU-T")
	// A legacy unit with NO site custody.
	bin, _ := shared.NewBinId("BIN-LEGACY")
	unit, err := stock.NewStockUnit("su-legacy", sku, bin, mustQty(t, 50))
	if err != nil {
		t.Fatalf("NewStockUnit: %v", err)
	}
	if err := e.Stock.Save(context.Background(), unit); err != nil {
		t.Fatalf("seed: %v", err)
	}

	uc := e.usecase(clock)
	result, err := uc.Execute(context.Background(), cmd("tr-1", "tl-1", "SITE-A", "SKU-T", 5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allocation.Outcome() != transfer.OutcomeRejected ||
		result.Allocation.RejectionReason() != transfer.ReasonOriginSiteUnknown {
		t.Fatalf("outcome = %+v, want REJECTED/ORIGIN_SITE_UNKNOWN (legacy stock is unallocatable for transfers)", result.Allocation)
	}
	assertUnitUsable(t, e, "su-legacy", 50, "legacy stock must not be touched")
}

func TestValidateTransferCommand(t *testing.T) {
	qty := mustQty(t, 5)
	cases := []struct {
		name string
		cmd  usecases.TransferCommand
	}{
		{"empty transfer id", usecases.TransferCommand{TransferLineID: "tl", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: qty}},
		{"empty line id", usecases.TransferCommand{TransferID: "tr", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: qty}},
		{"empty site", usecases.TransferCommand{TransferID: "tr", TransferLineID: "tl", SKU: mustSKU(t, "SKU"), Quantity: qty}},
		{"empty sku", usecases.TransferCommand{TransferID: "tr", TransferLineID: "tl", OriginSiteID: mustSiteID(t, "S"), Quantity: qty}},
		{"zero quantity", usecases.TransferCommand{TransferID: "tr", TransferLineID: "tl", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: mustQty(t, 0)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := usecases.ValidateCommand(tc.cmd); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
	if err := usecases.ValidateCommand(usecases.TransferCommand{TransferID: "tr", TransferLineID: "tl", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: qty}); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}
}

// helpers

func cmd(transferID, lineID, site, sku string, qty int) usecases.TransferCommand {
	return usecases.TransferCommand{
		TransferID:     transferID,
		TransferLineID: lineID,
		OriginSiteID:   shared.SiteID(site),
		SKU:            shared.SKU(sku),
		Quantity:       shared.Quantity(qty),
	}
}

func assertUnitUsable(t *testing.T, e transferEnv, id string, want int, msg string) {
	t.Helper()
	unit, err := e.Stock.FindByID(context.Background(), id)
	if err != nil {
		t.Fatalf("read %s: %v", id, err)
	}
	if unit == nil {
		t.Fatalf("%s missing", id)
	}
	if unit.Usable().Int() != want {
		t.Fatalf("%s usable = %d, want %d (%s)", id, unit.Usable().Int(), want, msg)
	}
}
