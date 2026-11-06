package usecases_test

import (
	"context"
	"errors"
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

// releaseEnv bundles the release-leg adapters on top of the allocation
// ones: stock, reservations, BOTH ledgers and a buffered event stream.
type releaseEnv struct {
	Stock        *memory.StockRepo
	Reservations *memory.ReservationRepo
	Transfers    *memory.TransferAllocationRepo
	Releases     *memory.TransferReleaseRepo
	Events       *events.BufferedPublisher
}

func newReleaseEnv() releaseEnv {
	return releaseEnv{
		Stock:        memory.NewStockRepo(),
		Reservations: memory.NewReservationRepo(),
		Transfers:    memory.NewTransferAllocationRepo(),
		Releases:     memory.NewTransferReleaseRepo(),
		Events:       events.NewBufferedPublisher(),
	}
}

func (e releaseEnv) allocateUC(clock *memory.FixedClock) *usecases.AllocateTransferStock {
	return &usecases.AllocateTransferStock{
		Stock:        e.Stock,
		Reservations: e.Reservations,
		Transfers:    e.Transfers,
		Events:       e.Events,
		Clock:        clock,
	}
}

func (e releaseEnv) releaseUC(clock *memory.FixedClock) *usecases.ReleaseTransferAllocation {
	return &usecases.ReleaseTransferAllocation{
		Stock:        e.Stock,
		Reservations: e.Reservations,
		Transfers:    e.Transfers,
		Releases:     e.Releases,
		Events:       e.Events,
		Clock:        clock,
	}
}

// allocateLine drives the allocation leg for one line and returns the
// decided ledger row (the reservation correlation the release command
// must name).
func allocateLine(t *testing.T, e releaseEnv, clock *memory.FixedClock, transferID, lineID, site, sku string, qty int) *transfer.Allocation {
	t.Helper()
	result, err := e.allocateUC(clock).Execute(context.Background(), cmd(transferID, lineID, site, sku, qty))
	if err != nil {
		t.Fatalf("allocate %s: %v", lineID, err)
	}
	return result.Allocation
}

func releaseCmd(allocation *transfer.Allocation, reason string) usecases.TransferReleaseCommand {
	return usecases.TransferReleaseCommand{
		TransferID:     allocation.TransferID(),
		TransferLineID: allocation.TransferLineID(),
		ReservationID:  allocation.ReservationID(),
		OriginSiteID:   allocation.OriginSiteID(),
		SKU:            allocation.SKU(),
		Quantity:       allocation.RequestedQuantity(),
		Reason:         reason,
	}
}

// releasedEvents returns every TransferStockAllocationReleased the
// buffered publisher collected for the line.
func releasedEvents(e releaseEnv, lineID string) []shared.TransferStockAllocationReleased {
	var out []shared.TransferStockAllocationReleased
	for _, ev := range e.Events.Events() {
		if r, ok := ev.(shared.TransferStockAllocationReleased); ok && r.TransferLineID == lineID {
			out = append(out, r)
		}
	}
	return out
}

func assertUnitUsableR(t *testing.T, e releaseEnv, id string, want int, msg string) {
	t.Helper()
	unit, err := e.Stock.FindByID(context.Background(), id)
	if err != nil || unit == nil {
		t.Fatalf("read %s: %v", id, err)
	}
	if unit.Usable().Int() != want {
		t.Fatalf("%s usable = %d, want %d (%s)", id, unit.Usable().Int(), want, msg)
	}
}

var fixedReleaseTime = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func TestReleaseTransferAllocation_RevokesAndReturnsToUsable(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)
	site := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-R")
	seedUnitR(t, e, "su-r1", sku, site, 10)

	allocation := allocateLine(t, e, clock, "tr-1", "tl-1", "SITE-A", "SKU-R", 6)
	assertUnitUsableR(t, e, "su-r1", 4, "allocation must decrement")

	result, err := e.releaseUC(clock).Execute(context.Background(), releaseCmd(allocation, "planner cancelled"))
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if result.Replay {
		t.Fatal("first release must not be a replay")
	}
	if result.Release.Outcome() != transfer.ReleaseOutcomeReleased {
		t.Fatalf("outcome = %s, want RELEASED", result.Release.Outcome())
	}

	// The held quantity is back in usable, exactly once.
	assertUnitUsableR(t, e, "su-r1", 10, "release must return the hold to usable")

	assertReservationRevokedR(t, e, allocation.ReservationID())
	assertReleasedDecision(t, e, allocation, site, sku)
	assertNoReservationRevoked(t, e)
}

// assertReservationRevokedR proves the hold itself was resolved.
func assertReservationRevokedR(t *testing.T, e releaseEnv, reservationID string) {
	t.Helper()
	res, err := e.Reservations.FindByID(context.Background(), reservationID)
	if err != nil || res == nil {
		t.Fatalf("read reservation: %v", err)
	}
	if res.Status() != reservation.StatusRevoked {
		t.Fatalf("reservation status = %s, want REVOKED", res.Status())
	}
}

// assertReleasedDecision proves the ledger row and the single fact reply
// carrying the command's reason.
func assertReleasedDecision(t *testing.T, e releaseEnv, allocation *transfer.Allocation, site shared.SiteID, sku shared.SKU) {
	t.Helper()
	ledger, err := e.Releases.FindByTransferLineID(context.Background(), allocation.TransferLineID())
	if err != nil || ledger == nil {
		t.Fatalf("release ledger row missing: %v", err)
	}
	if ledger.Outcome() != transfer.ReleaseOutcomeReleased || ledger.Reason() != "planner cancelled" {
		t.Fatalf("ledger = %s/%s", ledger.Outcome(), ledger.Reason())
	}
	evs := releasedEvents(e, allocation.TransferLineID())
	if len(evs) != 1 {
		t.Fatalf("released events = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Outcome != "RELEASED" || ev.Reason != "planner cancelled" || ev.ReservationID != allocation.ReservationID() ||
		ev.TransferID != allocation.TransferID() || ev.OriginSiteID != site || ev.SKU != sku || ev.Quantity.Int() != allocation.RequestedQuantity().Int() {
		t.Fatalf("released event wrong: %+v", ev)
	}
}

// assertNoReservationRevoked: transfer reservations never emitted
// StockReserved, so a revoke event would corrupt wes-work-planning's
// observed-usable projection.
func assertNoReservationRevoked(t *testing.T, e releaseEnv) {
	t.Helper()
	for _, ev := range e.Events.Events() {
		if _, ok := ev.(shared.ReservationRevoked); ok {
			t.Fatal("ReservationRevoked must NOT be published on the release leg")
		}
	}
}

func TestReleaseTransferAllocation_ReplayReturnsOriginalOutcome(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)
	site := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-RP")
	seedUnitR(t, e, "su-rp", sku, site, 10)
	allocation := allocateLine(t, e, clock, "tr-2", "tl-2", "SITE-A", "SKU-RP", 6)

	uc := e.releaseUC(clock)
	if _, err := uc.Execute(context.Background(), releaseCmd(allocation, "first")); err != nil {
		t.Fatalf("first release: %v", err)
	}
	again, err := uc.Execute(context.Background(), releaseCmd(allocation, "first"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !again.Replay || again.Release.Outcome() != transfer.ReleaseOutcomeReleased {
		t.Fatalf("replay = %v/%s, want true/RELEASED", again.Replay, again.Release.Outcome())
	}
	assertUnitUsableR(t, e, "su-rp", 10, "replay must not release a second time")
	if evs := releasedEvents(e, "tl-2"); len(evs) != 1 {
		t.Fatalf("released events after replay = %d, want exactly 1", len(evs))
	}
}

func TestReleaseTransferAllocation_ConflictingPayloadAnswersConflict(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)
	site := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-CF")
	seedUnitR(t, e, "su-cf", sku, site, 10)
	allocation := allocateLine(t, e, clock, "tr-3", "tl-3", "SITE-A", "SKU-CF", 6)

	uc := e.releaseUC(clock)
	if _, err := uc.Execute(context.Background(), releaseCmd(allocation, "first")); err != nil {
		t.Fatalf("first release: %v", err)
	}

	conflict := releaseCmd(allocation, "a DIFFERENT reason")
	result, err := uc.Execute(context.Background(), conflict)
	if err != nil {
		t.Fatalf("conflict answer: %v", err)
	}
	if result.Replay {
		t.Fatal("a conflict is not a replay")
	}
	if result.Release.Outcome() != transfer.ReleaseOutcomeReleased || result.Release.Reason() != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflict answer = %s/%s, want RELEASED/IDEMPOTENCY_CONFLICT",
			result.Release.Outcome(), result.Release.Reason())
	}

	// The conflict answer is published; the ledger still holds exactly
	// one row with the ORIGINAL reason.
	if evs := releasedEvents(e, "tl-3"); len(evs) != 2 {
		t.Fatalf("released events = %d, want 2 (original + conflict answer)", len(evs))
	}
	ledger, _ := e.Releases.FindByTransferLineID(context.Background(), "tl-3")
	if ledger == nil || ledger.Reason() != "first" {
		t.Fatalf("ledger reason = %v, want the original \"first\"", ledger)
	}
	assertUnitUsableR(t, e, "su-cf", 10, "conflict must not move stock")
}

func TestReleaseTransferAllocation_UnknownLineRefused(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)

	_, err := e.releaseUC(clock).Execute(context.Background(), usecases.TransferReleaseCommand{
		TransferID: "tr-x", TransferLineID: "tl-never-decided", ReservationID: "res-x",
		OriginSiteID: mustSiteID(t, "SITE-A"), SKU: mustSKU(t, "SKU-X"), Quantity: mustQty(t, 1),
		Reason: "cancel",
	})
	if !errors.Is(err, usecases.ErrUnknownTransferLine) {
		t.Fatalf("err = %v, want ErrUnknownTransferLine", err)
	}
	if evs := releasedEvents(e, "tl-never-decided"); len(evs) != 0 {
		t.Fatalf("refusal must publish nothing, got %d events", len(evs))
	}
	ledger, _ := e.Releases.FindByTransferLineID(context.Background(), "tl-never-decided")
	if ledger != nil {
		t.Fatal("refusal must not write a ledger row")
	}
}

func TestReleaseTransferAllocation_LedgerMismatchRefused(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)
	site := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-MM")
	seedUnitR(t, e, "su-mm", sku, site, 10)
	allocation := allocateLine(t, e, clock, "tr-4", "tl-4", "SITE-A", "SKU-MM", 6)

	wrong := releaseCmd(allocation, "cancel")
	wrong.ReservationID = "res-somebody-else"
	_, err := e.releaseUC(clock).Execute(context.Background(), wrong)
	if !errors.Is(err, usecases.ErrTransferReleaseMismatch) {
		t.Fatalf("err = %v, want ErrTransferReleaseMismatch", err)
	}
	if evs := releasedEvents(e, "tl-4"); len(evs) != 0 {
		t.Fatalf("mismatch must publish nothing, got %d events", len(evs))
	}
	// The hold is untouched: the reservation still holds the stock.
	assertUnitUsableR(t, e, "su-mm", 4, "mismatch refusal must not release")
}

func TestReleaseTransferAllocation_ExpiredReservationAnswersAlreadyResolved(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)
	site := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-EX")
	seedUnitR(t, e, "su-ex", sku, site, 10)

	uc := e.allocateUC(clock)
	uc.Timeout = time.Hour
	result, err := uc.Execute(context.Background(), cmd("tr-5", "tl-5", "SITE-A", "SKU-EX", 6))
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	allocation := result.Allocation

	// Past the timeout: the release's lazy-expiry step resolves the
	// reservation as EXPIRED (returning the hold itself), so the release
	// answers ALREADY_RESOLVED and must NOT return the quantity twice.
	clock.Advance(2 * time.Hour)
	releaseResult, err := e.releaseUC(clock).Execute(context.Background(), releaseCmd(allocation, "too late"))
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if releaseResult.Release.Outcome() != transfer.ReleaseOutcomeAlreadyResolved {
		t.Fatalf("outcome = %s, want ALREADY_RESOLVED", releaseResult.Release.Outcome())
	}
	assertUnitUsableR(t, e, "su-ex", 10, "expiry returned the hold exactly once")

	res, _ := e.Reservations.FindByID(context.Background(), allocation.ReservationID())
	if res.Status() != reservation.StatusExpired {
		t.Fatalf("reservation status = %s, want EXPIRED", res.Status())
	}
	// The saga still settles: ledger row + one fact reply.
	if evs := releasedEvents(e, "tl-5"); len(evs) != 1 || evs[0].Outcome != "ALREADY_RESOLVED" {
		t.Fatalf("released events = %+v, want one ALREADY_RESOLVED", evs)
	}
}

func TestReleaseTransferAllocation_ConfirmedReservationAnswersAlreadyResolved(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)
	site := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-CO")
	seedUnitR(t, e, "su-co", sku, site, 10)
	allocation := allocateLine(t, e, clock, "tr-6", "tl-6", "SITE-A", "SKU-CO", 6)

	// The stock was picked: the reservation is CONFIRMED, its quantity
	// consumed from the unit.
	res, err := e.Reservations.FindByID(context.Background(), allocation.ReservationID())
	if err != nil || res == nil {
		t.Fatalf("read reservation: %v", err)
	}
	if err := res.Confirm(clock.Now()); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if err := e.Reservations.Save(context.Background(), res); err != nil {
		t.Fatalf("save reservation: %v", err)
	}

	releaseResult, err := e.releaseUC(clock).Execute(context.Background(), releaseCmd(allocation, "cancel after pick"))
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if releaseResult.Release.Outcome() != transfer.ReleaseOutcomeAlreadyResolved {
		t.Fatalf("outcome = %s, want ALREADY_RESOLVED", releaseResult.Release.Outcome())
	}
	// Nothing returned to usable: the stock is gone (picked), not held.
	assertUnitUsableR(t, e, "su-co", 4, "confirmed stock must not come back")
}

func TestReleaseTransferAllocation_RejectedAllocationSettlesAlreadyResolved(t *testing.T) {
	e := newReleaseEnv()
	clock := memory.NewFixedClock(fixedReleaseTime)
	site := mustSiteID(t, "SITE-A")
	sku := mustSKU(t, "SKU-RJ")
	seedUnitR(t, e, "su-rj", sku, site, 3)

	// Allocation REJECTED (insufficient usable): no hold ever existed.
	allocResult, err := e.allocateUC(clock).Execute(context.Background(), cmd("tr-7", "tl-7", "SITE-A", "SKU-RJ", 6))
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if allocResult.Allocation.Outcome() != transfer.OutcomeRejected {
		t.Fatalf("allocation outcome = %s, want REJECTED", allocResult.Allocation.Outcome())
	}

	releaseResult, err := e.releaseUC(clock).Execute(context.Background(), usecases.TransferReleaseCommand{
		TransferID: "tr-7", TransferLineID: "tl-7", ReservationID: "res-none",
		OriginSiteID: site, SKU: sku, Quantity: mustQty(t, 6), Reason: "cancel",
	})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if releaseResult.Release.Outcome() != transfer.ReleaseOutcomeAlreadyResolved {
		t.Fatalf("outcome = %s, want ALREADY_RESOLVED", releaseResult.Release.Outcome())
	}
	assertUnitUsableR(t, e, "su-rj", 3, "nothing was ever held")
	if evs := releasedEvents(e, "tl-7"); len(evs) != 1 {
		t.Fatalf("released events = %d, want 1", len(evs))
	}
}

func TestValidateReleaseCommand(t *testing.T) {
	qty := mustQty(t, 1)
	valid := usecases.TransferReleaseCommand{
		TransferID: "tr", TransferLineID: "tl", ReservationID: "res",
		OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: qty, Reason: "cancel",
	}
	if err := usecases.ValidateReleaseCommand(valid); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}

	cases := []struct {
		name string
		cmd  usecases.TransferReleaseCommand
	}{
		{"empty transfer id", usecases.TransferReleaseCommand{TransferLineID: "tl", ReservationID: "res", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: qty}},
		{"empty line id", usecases.TransferReleaseCommand{TransferID: "tr", ReservationID: "res", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: qty}},
		{"empty reservation id", usecases.TransferReleaseCommand{TransferID: "tr", TransferLineID: "tl", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: qty}},
		{"empty site", usecases.TransferReleaseCommand{TransferID: "tr", TransferLineID: "tl", ReservationID: "res", SKU: mustSKU(t, "SKU"), Quantity: qty}},
		{"empty sku", usecases.TransferReleaseCommand{TransferID: "tr", TransferLineID: "tl", ReservationID: "res", OriginSiteID: mustSiteID(t, "S"), Quantity: qty}},
		{"zero quantity", usecases.TransferReleaseCommand{TransferID: "tr", TransferLineID: "tl", ReservationID: "res", OriginSiteID: mustSiteID(t, "S"), SKU: mustSKU(t, "SKU"), Quantity: mustQty(t, 0)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := usecases.ValidateReleaseCommand(tc.cmd)
			if !errors.Is(err, usecases.ErrMalformedTransferCommand) {
				t.Fatalf("err = %v, want ErrMalformedTransferCommand", err)
			}
		})
	}
}

// seedUnit stows qty of sku at site and returns the unit id (release-leg
// twin of the allocation test's helper, scoped to releaseEnv).
func seedUnitR(t *testing.T, e releaseEnv, id string, sku shared.SKU, site shared.SiteID, qty int) {
	t.Helper()
	bin, _ := shared.NewBinId("BIN-" + id)
	unit, err := stock.NewStockUnitAtSite(id, sku, bin, mustQty(t, qty), site)
	if err != nil {
		t.Fatalf("new unit: %v", err)
	}
	if err := e.Stock.Save(context.Background(), unit); err != nil {
		t.Fatalf("seed stock %s: %v", id, err)
	}
}
