package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

func stagedReturnLine(id string, qty int) usecases.ReturnLine {
	return usecases.ReturnLine{
		EventID:       id,
		DispositionID: "disp-1",
		ReceiptID:     "rcpt-1",
		RmaID:         "rma-1",
		SiteID:        "WH1",
		LineNo:        1,
		SKU:           "SKU-1",
		Quantity:      qty,
		Condition:     "Good",
		ReceivedAt:    "2026-10-22T14:05:00Z",
	}
}

func newBookReturn(e env) (*usecases.BookReturnLine, *memory.ProcessedEventRepo) {
	processed := memory.NewProcessedEventRepo()
	return &usecases.BookReturnLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		ProcessedEvents: processed,
	}, processed
}

func TestBookReturnLine_BookedThroughReceiveStock(t *testing.T) {
	e := newEnv()
	uc, processed := newBookReturn(e)

	outcome, err := uc.Execute(context.Background(), stagedReturnLine("ce-1", 2))
	if err != nil || outcome != usecases.ReturnLineBooked {
		t.Fatalf("outcome=%s err=%v, want BOOKED", outcome, err)
	}
	if n := stockReceivedCount(e); n != 1 {
		t.Fatalf("StockReceived events = %d, want 1", n)
	}
	if claimed, _ := processed.Claim(context.Background(), usecases.ReturnLineConsumerName, "ce-1"); claimed {
		t.Fatal("the id was not claimed under return-line-staged")
	}
}

// The claim lands under the RETURNS consumer name, not the inbound one:
// that claim row is the provenance record for the flow (ADR 0044), and
// the two flows never share dedupe scope.
func TestBookReturnLine_ClaimsUnderItsOwnConsumerName(t *testing.T) {
	e := newEnv()
	processed := memory.NewProcessedEventRepo()
	returns := &usecases.BookReturnLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		ProcessedEvents: processed,
	}
	inbound := &usecases.BookInboundReceiptLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		ProcessedEvents: processed,
	}

	if outcome, err := returns.Execute(context.Background(), stagedReturnLine("ce-same", 2)); err != nil || outcome != usecases.ReturnLineBooked {
		t.Fatalf("returns outcome=%s err=%v, want BOOKED", outcome, err)
	}
	// The SAME CloudEvents id through the inbound flow still books: the
	// claim scopes are disjoint, so cross-flow replays are not silently
	// swallowed.
	if outcome, err := inbound.Execute(context.Background(), goodLine("ce-same", 5)); err != nil || outcome != usecases.LineBooked {
		t.Fatalf("inbound outcome=%s err=%v, want BOOKED", outcome, err)
	}
	if n := stockReceivedCount(e); n != 2 {
		t.Fatalf("StockReceived events = %d, want 2", n)
	}
}

func TestBookReturnLine_DuplicateEventIDAppliesOnce(t *testing.T) {
	e := newEnv()
	uc, _ := newBookReturn(e)
	if _, err := uc.Execute(context.Background(), stagedReturnLine("ce-1", 2)); err != nil {
		t.Fatal(err)
	}

	outcome, err := uc.Execute(context.Background(), stagedReturnLine("ce-1", 2))
	if err != nil || outcome != usecases.ReturnLineDuplicate {
		t.Fatalf("outcome=%s err=%v, want DUPLICATE", outcome, err)
	}
	if n := stockReceivedCount(e); n != 1 {
		t.Fatalf("StockReceived events = %d, want 1 (a redelivered id must not book twice)", n)
	}
}

func TestBookReturnLine_MalformedLinesAreDeterministic(t *testing.T) {
	cases := map[string]func(l *usecases.ReturnLine){
		"empty event id":       func(l *usecases.ReturnLine) { l.EventID = "" },
		"empty disposition id": func(l *usecases.ReturnLine) { l.DispositionID = "" },
		"empty sku":            func(l *usecases.ReturnLine) { l.SKU = "" },
		"zero quantity":        func(l *usecases.ReturnLine) { l.Quantity = 0 },
		"negative quantity":    func(l *usecases.ReturnLine) { l.Quantity = -3 },
		"damaged condition":    func(l *usecases.ReturnLine) { l.Condition = "Damaged" },
		"empty condition":      func(l *usecases.ReturnLine) { l.Condition = "" },
		"lower-case good":      func(l *usecases.ReturnLine) { l.Condition = "good" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv()
			uc, processed := newBookReturn(e)
			l := stagedReturnLine("ce-1", 2)
			mutate(&l)

			_, err := uc.Execute(context.Background(), l)
			if !errors.Is(err, usecases.ErrMalformedReturnLine) {
				t.Fatalf("err = %v, want ErrMalformedReturnLine", err)
			}
			if n := stockReceivedCount(e); n != 0 {
				t.Fatalf("a malformed line raised %d StockReceived events", n)
			}
			if claimed, _ := processed.Claim(context.Background(), usecases.ReturnLineConsumerName, "ce-1"); !claimed {
				t.Fatal("a malformed line must not claim its event id")
			}
		})
	}
}

// A ReceiveStock failure rolls the claim back (stand-in for the Postgres
// transaction), so the retry books the line instead of calling it a duplicate.
func TestBookReturnLine_TransientReceiveFailureRollsBackTheClaim(t *testing.T) {
	e := newEnv()
	processed := &resettableClaims{}
	uc := &usecases.BookReturnLine{
		Receive:         &usecases.ReceiveStock{Events: failingEvents{}, Clock: e.Clock},
		ProcessedEvents: processed,
		UnitOfWork:      rollbackUnitOfWork{onRollback: processed.reset},
	}

	if _, err := uc.Execute(context.Background(), stagedReturnLine("ce-1", 2)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the publisher's transient error", err)
	}
	if claimed, _ := processed.Claim(context.Background(), usecases.ReturnLineConsumerName, "ce-1"); !claimed {
		t.Fatal("the claim survived a failed receive: a retry would be treated as a duplicate")
	}
}

func TestBookReturnLine_ClaimFailurePropagates(t *testing.T) {
	e := newEnv()
	uc := &usecases.BookReturnLine{
		Receive:         &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock},
		ProcessedEvents: failingClaims{},
	}
	if _, err := uc.Execute(context.Background(), stagedReturnLine("ce-1", 2)); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want errFake", err)
	}
	if n := stockReceivedCount(e); n != 0 {
		t.Fatal("the receive ran although the claim failed")
	}
}
