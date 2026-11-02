package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

func TestReceiveStock_Succeeds(t *testing.T) {
	e := newEnv()
	uc := &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}

	receipt, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receipt.Quantity.Int() != 10 {
		t.Fatalf("expected quantity=10, got %d", receipt.Quantity.Int())
	}

	found := false
	for _, ev := range e.Events.Events() {
		if ev.EventName() == "StockReceived" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a StockReceived event to be published")
	}
}

func TestReceiveStock_RejectsEmptySKU(t *testing.T) {
	e := newEnv()
	uc := &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), "", mustQty(t, 10)); err == nil {
		t.Fatalf("expected error for empty SKU")
	}
}

func TestReceiveStock_RejectsZeroQuantity(t *testing.T) {
	e := newEnv()
	uc := &usecases.ReceiveStock{Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), 0); err == nil {
		t.Fatalf("expected error for zero quantity")
	}
}

func TestReceiveStock_EventPublishFails_PropagatesError(t *testing.T) {
	e := newEnv()
	uc := &usecases.ReceiveStock{Events: failingEvents{}, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 10)); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}
