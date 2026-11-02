package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

func TestGetUsable_NoStock_ReturnsZero(t *testing.T) {
	e := newEnv()
	uc := &usecases.GetUsable{Stock: e.Stock}

	usable, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usable.Usable.Int() != 0 {
		t.Fatalf("expected usable=0 for unknown SKU, got %d", usable.Usable.Int())
	}
}

func TestGetUsable_SumsAcrossMultipleUnits(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 5, 5)
	stowUnit(t, e, "SKU-1", "A-1-2", 5, 5)
	uc := &usecases.GetUsable{Stock: e.Stock}

	usable, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usable.Usable.Int() != 10 {
		t.Fatalf("expected usable=10, got %d", usable.Usable.Int())
	}
}

func TestGetUsable_StockFindBySKUFails_PropagatesError(t *testing.T) {
	e := newEnv()
	stockRepo := &failingStockRepo{delegate: e.Stock, failFindBySKU: true}
	uc := &usecases.GetUsable{Stock: stockRepo}

	if _, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1")); !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
}
