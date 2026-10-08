package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

func ptr(n int) *int { return &n }

// ReserveStock stores the line it was asked for (decision 18, ADR 0036) and
// the reservation read back by id and by demandRef carries it.
func TestReserveStock_ExecuteForLine_StoresTheLineNo(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	res, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(2))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := res.LineNo(); got == nil || *got != 2 {
		t.Fatalf("created LineNo = %v, want 2", got)
	}
	byID, _ := e.Reservations.FindByID(context.Background(), res.ID())
	if got := byID.LineNo(); got == nil || *got != 2 {
		t.Fatalf("stored LineNo = %v, want 2", got)
	}
	byDemand, _ := e.Reservations.FindByDemandRef(context.Background(), "order-1")
	if len(byDemand) != 1 || byDemand[0].LineNo() == nil || *byDemand[0].LineNo() != 2 {
		t.Fatalf("by demandRef = %+v, want one reservation for line 2", byDemand)
	}
}

// Execute (no line) keeps working and leaves the line unknown.
func TestReserveStock_Execute_LeavesTheLineNoUnknown(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	res, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.LineNo() != nil {
		t.Fatalf("LineNo = %d, want nil", *res.LineNo())
	}
}

func TestReserveStock_ExecuteForLine_RejectsANonPositiveLineNoBeforeAnyWrite(t *testing.T) {
	for _, n := range []int{0, -1, reservation.MaxLineNo + 1} {
		e := newEnv()
		stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
		uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

		_, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(n))
		if !errors.Is(err, reservation.ErrInvalidLineNo) {
			t.Fatalf("lineNo %d: err = %v, want ErrInvalidLineNo", n, err)
		}
		assertUsable(t, e, "SKU-1", 10)
	}
}

// Two lines of one order that happen to share SKU and quantity are two
// reservations once the line is known; before decision 18 the second was
// indistinguishable from a retry of the first.
func TestReserveStock_SameSKUAndQuantityOnDifferentLines_AreTwoReservations(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	l1, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(1))
	if err != nil {
		t.Fatalf("line 1: %v", err)
	}
	l2, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(2))
	if err != nil {
		t.Fatalf("line 2: %v", err)
	}
	if l1.ID() == l2.ID() {
		t.Fatal("line 2 was handed line 1's reservation")
	}
	assertUsable(t, e, "SKU-1", 6)
}

// A retry of the SAME line (same sku, quantity, line) is still a replay.
func TestReserveStock_RetryOfTheSameLine_IsAReplay(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	first, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(1))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	again, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(1))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first.ID() != again.ID() {
		t.Fatalf("retry created %s, want the first reservation %s", again.ID(), first.ID())
	}
	assertUsable(t, e, "SKU-1", 8)
}

// A line-aware request that finds an ACTIVE reservation of the same SKU and
// quantity with NO line (made by the pre-decision-18 code, e.g. a retry that
// straddles the deploy) gets that reservation back instead of holding the
// stock twice.
func TestReserveStock_LineAwareRetryOfALegacyReservation_IsAReplay(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	legacy, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1")
	if err != nil {
		t.Fatalf("legacy: %v", err)
	}
	again, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(1))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if again.ID() != legacy.ID() {
		t.Fatalf("retry created %s, want the legacy reservation %s", again.ID(), legacy.ID())
	}
	assertUsable(t, e, "SKU-1", 8)
}

// A revoked attempt for a line is history: the retry for that line is a new
// reservation that carries the same line.
func TestReserveStock_RetryAfterRevoke_CarriesTheSameLine(t *testing.T) {
	e := newEnv()
	stowUnit(t, e, "SKU-1", "A-1-1", 10, 10)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	revoke := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	first, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(3))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := revoke.Execute(context.Background(), first.ID()); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	retry, err := uc.ExecuteForLine(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 2), "order-1", ptr(3))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.ID() == first.ID() || retry.LineNo() == nil || *retry.LineNo() != 3 {
		t.Fatalf("retry = %s line %v, want a NEW reservation for line 3", retry.ID(), retry.LineNo())
	}
}
