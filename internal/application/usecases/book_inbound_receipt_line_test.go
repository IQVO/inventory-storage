package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// recordingReceiptMetrics records units by outcome.
type recordingReceiptMetrics struct{ units map[string]int }

func (r *recordingReceiptMetrics) InboundReceiptUnits(_ context.Context, outcome string, units int) {
	if r.units == nil {
		r.units = map[string]int{}
	}
	r.units[outcome] += units
}

func goodLine(id string, qty int) usecases.InboundReceiptLine {
	return usecases.InboundReceiptLine{
		EventID: id, ReceiptID: "rcpt-1", ASNNumber: "ASN-1001", LineNo: 1,
		SKU: "SKU-1", Quantity: qty, Condition: "Good",
	}
}

func newBook(e env) (*usecases.BookInboundReceiptLine, *memory.ProcessedEventRepo, *recordingReceiptMetrics) {
	processed := memory.NewProcessedEventRepo()
	metrics := &recordingReceiptMetrics{}
	return &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		ProcessedEvents: processed,
		Metrics:         metrics,
	}, processed, metrics
}

func stockReceivedCount(e env) int {
	n := 0
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "StockReceived" {
			n++
		}
	}
	return n
}

func TestBookInboundReceiptLine_GoodLineIsBookedAsStagedReceipt(t *testing.T) {
	e := newEnv()
	uc, _, metrics := newBook(e)

	outcome, err := uc.Execute(context.Background(), goodLine("ce-1", 40))
	if err != nil || outcome != usecases.LineBooked {
		t.Fatalf("outcome=%s err=%v, want BOOKED", outcome, err)
	}
	if n := stockReceivedCount(e); n != 1 {
		t.Fatalf("StockReceived events = %d, want 1", n)
	}
	if metrics.units[ports.InboundReceiptOutcomeBooked] != 40 {
		t.Fatalf("booked units metric = %v, want 40", metrics.units)
	}
}

func TestBookInboundReceiptLine_DamagedLineIsNotBooked(t *testing.T) {
	e := newEnv()
	uc, _, metrics := newBook(e)
	l := goodLine("ce-1", 4)
	l.Condition = "Damaged"

	outcome, err := uc.Execute(context.Background(), l)
	if err != nil || outcome != usecases.LineDamagedNotBooked {
		t.Fatalf("outcome=%s err=%v, want DAMAGED_NOT_BOOKED", outcome, err)
	}
	if n := stockReceivedCount(e); n != 0 {
		t.Fatalf("StockReceived events = %d, want 0 (Damaged is never booked)", n)
	}
	if metrics.units[ports.InboundReceiptOutcomeDamaged] != 4 || metrics.units[ports.InboundReceiptOutcomeBooked] != 0 {
		t.Fatalf("metric = %v, want damaged_not_booked=4", metrics.units)
	}
}

func TestBookInboundReceiptLine_DuplicateEventIDAppliesOnce(t *testing.T) {
	e := newEnv()
	uc, _, metrics := newBook(e)
	if _, err := uc.Execute(context.Background(), goodLine("ce-1", 40)); err != nil {
		t.Fatal(err)
	}

	outcome, err := uc.Execute(context.Background(), goodLine("ce-1", 40))
	if err != nil || outcome != usecases.LineDuplicate {
		t.Fatalf("outcome=%s err=%v, want DUPLICATE", outcome, err)
	}
	if n := stockReceivedCount(e); n != 1 {
		t.Fatalf("StockReceived events = %d, want 1", n)
	}
	if metrics.units[ports.InboundReceiptOutcomeBooked] != 40 {
		t.Fatalf("metric = %v, a duplicate must not be counted again", metrics.units)
	}
}

func TestBookInboundReceiptLine_DuplicateDamagedIsCountedOnce(t *testing.T) {
	e := newEnv()
	uc, _, metrics := newBook(e)
	l := goodLine("ce-1", 4)
	l.Condition = "Damaged"
	for range 2 {
		if _, err := uc.Execute(context.Background(), l); err != nil {
			t.Fatal(err)
		}
	}
	if metrics.units[ports.InboundReceiptOutcomeDamaged] != 4 {
		t.Fatalf("metric = %v, want damaged_not_booked=4 (counted once)", metrics.units)
	}
}

func TestBookInboundReceiptLine_MalformedLinesAreDeterministic(t *testing.T) {
	cases := map[string]func(l *usecases.InboundReceiptLine){
		"empty event id":    func(l *usecases.InboundReceiptLine) { l.EventID = "" },
		"empty sku":         func(l *usecases.InboundReceiptLine) { l.SKU = "" },
		"zero quantity":     func(l *usecases.InboundReceiptLine) { l.Quantity = 0 },
		"negative quantity": func(l *usecases.InboundReceiptLine) { l.Quantity = -3 },
		"unknown condition": func(l *usecases.InboundReceiptLine) { l.Condition = "Quarantined" },
		"empty condition":   func(l *usecases.InboundReceiptLine) { l.Condition = "" },
		"lower-case good":   func(l *usecases.InboundReceiptLine) { l.Condition = "good" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv()
			uc, processed, metrics := newBook(e)
			l := goodLine("ce-1", 10)
			mutate(&l)

			_, err := uc.Execute(context.Background(), l)
			if !errors.Is(err, usecases.ErrMalformedInboundReceiptLine) {
				t.Fatalf("err = %v, want ErrMalformedInboundReceiptLine", err)
			}
			if n := stockReceivedCount(e); n != 0 {
				t.Fatalf("a malformed line raised %d StockReceived events", n)
			}
			if claimed, _ := processed.Claim(context.Background(), usecases.InboundReceiptConsumerName, "ce-1"); !claimed {
				t.Fatal("a malformed line must not claim its event id")
			}
			if len(metrics.units) != 0 {
				t.Fatalf("a malformed line was counted: %v", metrics.units)
			}
		})
	}
}

// A ReceiveStock failure rolls the claim back (stand-in for the Postgres
// transaction), so the retry books the line instead of calling it a duplicate.
func TestBookInboundReceiptLine_TransientReceiveFailureRollsBackTheClaim(t *testing.T) {
	e := newEnv()
	processed := &resettableClaims{}
	metrics := &recordingReceiptMetrics{}
	uc := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: failingEvents{}, Clock: e.Clock},
		ProcessedEvents: processed,
		UnitOfWork:      rollbackUnitOfWork{onRollback: processed.reset},
		Metrics:         metrics,
	}

	if _, err := uc.Execute(context.Background(), goodLine("ce-1", 10)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the publisher's transient error", err)
	}
	if claimed, _ := processed.Claim(context.Background(), usecases.InboundReceiptConsumerName, "ce-1"); !claimed {
		t.Fatal("the claim survived a failed receive: a retry would be treated as a duplicate")
	}
	if len(metrics.units) != 0 {
		t.Fatalf("a failed attempt was counted: %v", metrics.units)
	}
}

func TestBookInboundReceiptLine_ClaimFailurePropagates(t *testing.T) {
	e := newEnv()
	uc := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		ProcessedEvents: failingClaims{},
	}
	if _, err := uc.Execute(context.Background(), goodLine("ce-1", 10)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want errFake", err)
	}
	if n := stockReceivedCount(e); n != 0 {
		t.Fatal("the receive ran although the claim failed")
	}
}

func TestBookInboundReceiptLine_WorksWithoutMetrics(t *testing.T) {
	e := newEnv()
	uc := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		ProcessedEvents: memory.NewProcessedEventRepo(),
	}
	if outcome, err := uc.Execute(context.Background(), goodLine("ce-1", 1)); err != nil || outcome != usecases.LineBooked {
		t.Fatalf("outcome=%s err=%v", outcome, err)
	}
}
