package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// recordingHeldMetrics sums the held-unit deltas per reason.
type recordingHeldMetrics struct{ units map[string]int }

func (r *recordingHeldMetrics) HeldUnits(_ context.Context, reason string, delta int) {
	if r.units == nil {
		r.units = map[string]int{}
	}
	r.units[reason] += delta
}

type noopSettler struct{}

func (noopSettler) Settle(context.Context, string, int) (usecases.SettleResult, error) {
	return usecases.SettleResult{}, nil
}

type heldWorld struct {
	env       env
	held      *memory.HeldStockRepo
	metrics   *recordingHeldMetrics
	receipt   *recordingReceiptMetrics
	processed *memory.ProcessedEventRepo
	book      *usecases.BookInboundReceiptLine
	decide    *usecases.DecideHeldDisposition
	list      *usecases.ListHeldStock
	link      *usecases.LinkReceiptCarton
}

func newHeldWorld() *heldWorld {
	e := newEnv()
	w := &heldWorld{
		env: e, held: memory.NewHeldStockRepo(), metrics: &recordingHeldMetrics{}, receipt: &recordingReceiptMetrics{},
		processed: memory.NewProcessedEventRepo(),
	}
	hold := &usecases.HoldStock{Held: w.held, Events: e.Events, Clock: e.Clock}
	receive := &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}
	w.book = &usecases.BookInboundReceiptLine{
		Receive: receive, Hold: hold, ProcessedEvents: w.processed, Metrics: w.receipt, HeldMetrics: w.metrics,
	}
	w.decide = &usecases.DecideHeldDisposition{Held: w.held, Receive: receive, Events: e.Events, Clock: e.Clock, Metrics: w.metrics}
	w.list = &usecases.ListHeldStock{Held: w.held, Clock: e.Clock}
	w.link = &usecases.LinkReceiptCarton{
		ProcessedEvents: w.processed, Pending: memory.NewPendingLinkRepo(), Linker: noopSettler{}, Held: w.held,
	}
	return w
}

func damagedLine(id string, qty int) usecases.InboundReceiptLine {
	l := goodLine(id, qty)
	l.Condition = "Damaged"
	l.OwnerRef = "seller-a"
	l.SSCC = "00012345678901234567"
	return l
}

func (w *heldWorld) eventsNamed(name string) int {
	n := 0
	for _, ev := range w.env.Events.Events() {
		if ev.EventName() == name {
			n++
		}
	}
	return n
}

func (w *heldWorld) bookDamaged(t *testing.T, id string, qty int) *stock.HeldUnit {
	t.Helper()
	out, err := w.book.Execute(context.Background(), damagedLine(id, qty))
	if err != nil || out != usecases.LineHeld {
		t.Fatalf("book = %s %v, want HELD", out, err)
	}
	units := w.held.All()
	return units[len(units)-1]
}

func TestBook_DamagedLineIsHeldNotUsableNotStaged(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 4)

	if h.State() != stock.StateHeld || h.Reason() != stock.ReasonDamaged || h.Quantity().Int() != 4 || h.Usable() != 0 {
		t.Fatalf("held unit = state %s reason %s qty %d usable %d", h.State(), h.Reason(), h.Quantity().Int(), h.Usable())
	}
	if h.ReceiptID() != "rcpt-1" || h.ASNNumber() != "ASN-1001" || h.LineNo() != 1 || h.SKU() != "SKU-1" {
		t.Fatalf("source = %s %s %d %s", h.ReceiptID(), h.ASNNumber(), h.LineNo(), h.SKU())
	}
	if h.OwnerRef() != "seller-a" || h.SSCC() != "00012345678901234567" {
		t.Fatalf("pack provenance lost: owner %q sscc %q", h.OwnerRef(), h.SSCC())
	}
	if w.eventsNamed("StockHeld") != 1 || w.eventsNamed("StockReceived") != 0 {
		t.Fatalf("StockHeld %d StockReceived %d, want 1 and 0: held units are never staged", w.eventsNamed("StockHeld"), w.eventsNamed("StockReceived"))
	}
	if w.receipt.units[ports.InboundReceiptOutcomeHeld] != 4 || w.metrics.units["DAMAGED"] != 4 {
		t.Fatalf("metrics: receipt %v held %v", w.receipt.units, w.metrics.units)
	}
}

func TestBook_DamagedLine_RedeliveryHoldsOnce(t *testing.T) {
	w := newHeldWorld()
	w.bookDamaged(t, "ce-1", 4)
	out, err := w.book.Execute(context.Background(), damagedLine("ce-1", 4))
	if err != nil || out != usecases.LineDuplicate {
		t.Fatalf("redelivery = %s %v, want DUPLICATE", out, err)
	}
	if len(w.held.All()) != 1 || w.eventsNamed("StockHeld") != 1 || w.metrics.units["DAMAGED"] != 4 {
		t.Fatalf("a redelivery held again: %d units, %d events, metric %v", len(w.held.All()), w.eventsNamed("StockHeld"), w.metrics.units)
	}
}

func TestBook_GoodLineIsUnchangedByHeldStock(t *testing.T) {
	w := newHeldWorld()
	if out, err := w.book.Execute(context.Background(), goodLine("ce-9", 7)); err != nil || out != usecases.LineBooked {
		t.Fatalf("good = %s %v", out, err)
	}
	if len(w.held.All()) != 0 || w.eventsNamed("StockReceived") != 1 || w.eventsNamed("StockHeld") != 0 {
		t.Fatal("a Good line must not touch held stock")
	}
}

func TestBook_DamagedLine_FailureRollsBackTheClaim(t *testing.T) {
	e := newEnv()
	processed := &resettableClaims{}
	uc := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		Hold:            &usecases.HoldStock{Held: memory.NewHeldStockRepo(), Events: failingEvents{}, Clock: e.Clock},
		ProcessedEvents: processed,
		UnitOfWork:      rollbackUnitOfWork{onRollback: processed.reset},
	}
	if _, err := uc.Execute(context.Background(), damagedLine("ce-1", 3)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the publisher's error", err)
	}
	if claimed, _ := processed.Claim(context.Background(), usecases.InboundReceiptConsumerName, "ce-1"); !claimed {
		t.Fatal("the claim survived a failed hold: the retry would be a duplicate")
	}
}

func TestBook_DamagedLine_WithoutHoldWiringFails(t *testing.T) {
	e := newEnv()
	uc := &usecases.BookInboundReceiptLine{
		Receive: &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}, ProcessedEvents: memory.NewProcessedEventRepo(),
	}
	if _, err := uc.Execute(context.Background(), damagedLine("ce-1", 3)); err == nil {
		t.Fatal("a Damaged line must never be silently dropped when held stock is not wired")
	}
}

func decideCmd(id string, d stock.Disposition) usecases.DecideHeldDispositionCommand {
	return usecases.DecideHeldDispositionCommand{HeldUnitID: id, Disposition: d}
}

func TestDecide_ReturnToVendor_RemovesAndRecordsAudit(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 4)

	got, err := w.decide.Execute(context.Background(), decideCmd(h.ID(), stock.DispositionReturnToVendor))
	if err != nil {
		t.Fatal(err)
	}
	if got.State() != stock.StateRemoved || got.Disposition() != stock.DispositionReturnToVendor || got.Quantity().Int() != 4 {
		t.Fatalf("after RTV: %s %s %d", got.State(), got.Disposition(), got.Quantity().Int())
	}
	stored, _ := w.held.FindByID(context.Background(), h.ID())
	if stored.State() != stock.StateRemoved || stored.Disposition() != stock.DispositionReturnToVendor || stored.DecidedAt().IsZero() {
		t.Fatalf("the audit was not persisted: %s %s %v", stored.State(), stored.Disposition(), stored.DecidedAt())
	}
	if w.eventsNamed("StockDispositioned") != 1 || w.eventsNamed("StockReleased") != 0 || w.eventsNamed("StockReceived") != 0 {
		t.Fatal("RTV raises StockDispositioned only; nothing returns to usable flow")
	}
	if w.metrics.units["DAMAGED"] != 0 {
		t.Fatalf("held gauge = %v, want back to 0 after RTV", w.metrics.units)
	}
}

func TestDecide_Scrap_RemovesWithoutRelease(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 2)
	if _, err := w.decide.Execute(context.Background(), decideCmd(h.ID(), stock.DispositionScrap)); err != nil {
		t.Fatal(err)
	}
	if w.eventsNamed("StockDispositioned") != 1 || w.eventsNamed("StockReceived") != 0 {
		t.Fatal("SCRAP raises StockDispositioned only")
	}
}

func TestDecide_Release_ReceivesAsFreshStagedStock(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 4)
	if _, err := w.decide.Execute(context.Background(), decideCmd(h.ID(), stock.DispositionRelease)); err != nil {
		t.Fatal(err)
	}
	if w.eventsNamed("StockReleased") != 1 || w.eventsNamed("StockDispositioned") != 1 || w.eventsNamed("StockReceived") != 1 {
		t.Fatalf("RELEASE events: released %d dispositioned %d received %d, want 1 each",
			w.eventsNamed("StockReleased"), w.eventsNamed("StockDispositioned"), w.eventsNamed("StockReceived"))
	}
	for _, ev := range w.env.Events.Events() {
		if rec, ok := ev.(shared.StockReceived); ok && (rec.SKU != "SKU-1" || rec.Quantity.Int() != 4) {
			t.Fatalf("the released quantity is not conserved: %s %d", rec.SKU, rec.Quantity.Int())
		}
	}
	if w.metrics.units["DAMAGED"] != 0 {
		t.Fatalf("held gauge = %v, want 0 after RELEASE", w.metrics.units)
	}
}

func TestDecide_Rework_StaysHeldAndMovesTheGauge(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 5)
	cmd := decideCmd(h.ID(), stock.DispositionRework)
	cmd.Reason = stock.ReasonSuspect
	got, err := w.decide.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got.State() != stock.StateHeld || got.Reason() != stock.ReasonSuspect || got.Usable() != 0 {
		t.Fatalf("after REWORK: %s %s usable %d", got.State(), got.Reason(), got.Usable())
	}
	if w.metrics.units["DAMAGED"] != 0 || w.metrics.units["SUSPECT"] != 5 {
		t.Fatalf("gauge = %v, want DAMAGED 0 and SUSPECT 5", w.metrics.units)
	}
	if w.eventsNamed("StockDispositioned") != 1 || w.eventsNamed("StockReceived") != 0 {
		t.Fatal("REWORK raises StockDispositioned only")
	}
}

func TestDecide_ClosedUnitIsRefusedAndRaisesNothing(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 4)
	if _, err := w.decide.Execute(context.Background(), decideCmd(h.ID(), stock.DispositionScrap)); err != nil {
		t.Fatal(err)
	}
	before := len(w.env.Events.Events())
	if _, err := w.decide.Execute(context.Background(), decideCmd(h.ID(), stock.DispositionRelease)); !errors.Is(err, stock.ErrHeldUnitClosed) {
		t.Fatalf("err = %v, want ErrHeldUnitClosed", err)
	}
	if len(w.env.Events.Events()) != before {
		t.Fatal("a refused decision published an event")
	}
}

func TestDecide_UnknownUnit(t *testing.T) {
	w := newHeldWorld()
	if _, err := w.decide.Execute(context.Background(), decideCmd("hu-nope", stock.DispositionScrap)); !errors.Is(err, usecases.ErrHeldUnitNotFound) {
		t.Fatalf("err = %v, want ErrHeldUnitNotFound", err)
	}
}

func TestDecide_InvalidDispositionAndReason(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 4)
	if _, err := w.decide.Execute(context.Background(), decideCmd(h.ID(), "DONATE")); !errors.Is(err, stock.ErrInvalidDisposition) {
		t.Fatalf("err = %v, want ErrInvalidDisposition", err)
	}
	cmd := decideCmd(h.ID(), stock.DispositionRework)
	cmd.Reason = "BROKEN"
	if _, err := w.decide.Execute(context.Background(), cmd); !errors.Is(err, stock.ErrInvalidHoldReason) {
		t.Fatalf("err = %v, want ErrInvalidHoldReason", err)
	}
	stored, _ := w.held.FindByID(context.Background(), h.ID())
	if stored.State() != stock.StateHeld || stored.Reason() != stock.ReasonDamaged {
		t.Fatal("a refused decision changed the unit")
	}
}

func TestDecide_FailureDoesNotCountMetricsTwice(t *testing.T) {
	e := newEnv()
	held := memory.NewHeldStockRepo()
	metrics := &recordingHeldMetrics{}
	hu, _ := stock.NewHeldUnit("hu-1", stock.HeldSource{ReceiptID: "r", ASNNumber: "A", LineNo: 1, SKU: "SKU-1", Quantity: 3}, stock.ReasonDamaged, time.Now())
	_ = held.Save(context.Background(), hu)
	uc := &usecases.DecideHeldDisposition{Held: held, Receive: &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}, Events: failingEvents{}, Clock: e.Clock, Metrics: metrics}
	if _, err := uc.Execute(context.Background(), decideCmd("hu-1", stock.DispositionScrap)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the publisher's error", err)
	}
	if len(metrics.units) != 0 {
		t.Fatalf("a failed decision was counted: %v", metrics.units)
	}
}

func TestList_FiltersByReasonSkuAgeAndPages(t *testing.T) {
	w := newHeldWorld()
	for i, c := range []struct {
		id, sku string
		qty     int
	}{{"ce-1", "SKU-1", 1}, {"ce-2", "SKU-2", 2}, {"ce-3", "SKU-1", 3}} {
		l := damagedLine(c.id, c.qty)
		l.SKU = c.sku
		l.LineNo = i + 1
		if _, err := w.book.Execute(context.Background(), l); err != nil {
			t.Fatal(err)
		}
		w.env.Clock.Advance(time.Hour)
	}
	ctx := context.Background()

	all, _ := w.list.Execute(ctx, usecases.ListHeldStockQuery{})
	if len(all) != 3 || all[0].Quantity().Int() != 1 || all[2].Quantity().Int() != 3 {
		t.Fatalf("all = %d units, want 3 oldest first", len(all))
	}
	bySKU, _ := w.list.Execute(ctx, usecases.ListHeldStockQuery{SKU: "SKU-1"})
	if len(bySKU) != 2 {
		t.Fatalf("by sku = %d", len(bySKU))
	}
	none, _ := w.list.Execute(ctx, usecases.ListHeldStockQuery{Reason: stock.ReasonSuspect})
	if len(none) != 0 {
		t.Fatalf("by other reason = %d", len(none))
	}
	// The clock reads base+3h: held for MORE than 2h means held before base+1h.
	older, _ := w.list.Execute(ctx, usecases.ListHeldStockQuery{OlderThan: 2 * time.Hour})
	if len(older) != 1 || older[0].ID() != all[0].ID() {
		t.Fatalf("older than = %d", len(older))
	}
	first, _ := w.list.Execute(ctx, usecases.ListHeldStockQuery{Limit: 2})
	next, _ := w.list.Execute(ctx, usecases.ListHeldStockQuery{After: &stock.HeldPageAfter{HeldAt: first[1].HeldAt(), ID: first[1].ID()}, Limit: 2})
	if len(first) != 2 || len(next) != 1 || next[0].ID() != all[2].ID() {
		t.Fatalf("paging: first %d next %d", len(first), len(next))
	}
}

func TestList_ExcludesClosedUnits(t *testing.T) {
	w := newHeldWorld()
	h := w.bookDamaged(t, "ce-1", 4)
	if _, err := w.decide.Execute(context.Background(), decideCmd(h.ID(), stock.DispositionScrap)); err != nil {
		t.Fatal(err)
	}
	got, _ := w.list.Execute(context.Background(), usecases.ListHeldStockQuery{})
	if len(got) != 0 {
		t.Fatalf("a scrapped unit is still listed as held: %d", len(got))
	}
}

func damagedCarton(id, lpn string) usecases.ReceiptCarton {
	return usecases.ReceiptCarton{
		EventID: id, ReceiptID: "rcpt-1", ASNNumber: "ASN-1001", SSCC: "00012345678901234567", LPN: lpn, Sealed: true,
		Contents: []usecases.ReceiptCartonContent{{LineNo: 1, SKU: "SKU-1", GTIN: "00012345678905", Quantity: 4, Condition: "Damaged"}},
	}
}

// The line event is the sole quantity authority: a Damaged carton content
// marks its LPN on the held line and never opens a second hold.
func TestCarton_DamagedContentMarksTheHeldLineWithoutDoubleCounting(t *testing.T) {
	w := newHeldWorld()
	ctx := context.Background()
	h := w.bookDamaged(t, "ce-1", 4)

	out, _, err := w.link.Execute(ctx, damagedCarton("cc-1", "LPN-D1"))
	if err != nil || out != usecases.CartonNoContainer {
		t.Fatalf("carton = %s %v, want NO_CONTAINER", out, err)
	}
	if _, _, err := w.link.Execute(ctx, damagedCarton("cc-1", "LPN-D1")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.link.Execute(ctx, damagedCarton("cc-2", "LPN-D2")); err != nil {
		t.Fatal(err)
	}
	got, _ := w.held.FindByID(ctx, h.ID())
	if len(got.LPNs()) != 2 || got.LPNs()[0] != "LPN-D1" || got.LPNs()[1] != "LPN-D2" {
		t.Fatalf("lpns = %v", got.LPNs())
	}
	if len(w.held.All()) != 1 || got.Quantity().Int() != 4 || w.metrics.units["DAMAGED"] != 4 {
		t.Fatalf("units %d qty %d gauge %v: the carton must not book a second quantity", len(w.held.All()), got.Quantity().Int(), w.metrics.units)
	}
}

func TestCarton_DamagedMarkIsOrderIndependent(t *testing.T) {
	w := newHeldWorld()
	ctx := context.Background()
	if _, _, err := w.link.Execute(ctx, damagedCarton("cc-1", "LPN-D1")); err != nil {
		t.Fatal(err)
	}
	h := w.bookDamaged(t, "ce-1", 4)
	got, _ := w.held.FindByID(ctx, h.ID())
	if len(got.LPNs()) != 1 || got.LPNs()[0] != "LPN-D1" {
		t.Fatalf("a carton that arrived first was lost: %v", got.LPNs())
	}
}

func TestCarton_DamagedWithoutLPNFallsBackToSSCC(t *testing.T) {
	w := newHeldWorld()
	ctx := context.Background()
	h := w.bookDamaged(t, "ce-1", 4)
	if _, _, err := w.link.Execute(ctx, damagedCarton("cc-1", "")); err != nil {
		t.Fatal(err)
	}
	got, _ := w.held.FindByID(ctx, h.ID())
	if len(got.LPNs()) != 1 || got.LPNs()[0] != "00012345678901234567" {
		t.Fatalf("lpns = %v, want the sscc", got.LPNs())
	}
}
