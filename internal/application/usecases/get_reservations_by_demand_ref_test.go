package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

func reserveDemand(t *testing.T, e env, sku, binID, demandRef string, capacity, stowQty, reserveQty int) {
	t.Helper()
	stowUnit(t, e, sku, binID, capacity, stowQty)
	uc := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if _, err := uc.Execute(context.Background(), mustSKU(t, sku), mustQty(t, reserveQty), demandRef); err != nil {
		t.Fatalf("unexpected error reserving: %v", err)
	}
}

func TestGetReservationsByDemandRef_NoReservations_ReturnsEmpty(t *testing.T) {
	e := newEnv()
	uc := &usecases.GetReservationsByDemandRef{Reservations: e.Reservations}

	res, err := uc.Execute(context.Background(), "order-unknown")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("expected no reservations for unknown demandRef, got %d", len(res))
	}
}

func TestGetReservationsByDemandRef_ReturnsMatchingReservation(t *testing.T) {
	e := newEnv()
	reserveDemand(t, e, "SKU-1", "A-1-1", "order-1", 10, 10, 6)
	uc := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}

	res, err := uc.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 reservation, got %d", len(res))
	}
	if res[0].DemandRef() != "order-1" || res[0].Quantity().Int() != 6 {
		t.Fatalf("unexpected reservation: demandRef=%s quantity=%d", res[0].DemandRef(), res[0].Quantity().Int())
	}
}

// A demandRef can have multiple reservations across its lifetime — e.g. one
// revoked and a retry that succeeded — so this must return every match, not
// just the latest.
func TestGetReservationsByDemandRef_MultipleReservationsForSameDemand_ReturnsAll(t *testing.T) {
	e := newEnv()
	reserveDemand(t, e, "SKU-1", "A-1-1", "order-1", 20, 20, 5)

	revokeUC := &usecases.RevokeReservation{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	all, err := e.Reservations.FindByDemandRef(context.Background(), "order-1")
	if err != nil || len(all) != 1 {
		t.Fatalf("expected 1 reservation before revoke, got %d (err=%v)", len(all), err)
	}
	if err := revokeUC.Execute(context.Background(), all[0].ID()); err != nil {
		t.Fatalf("unexpected error revoking: %v", err)
	}

	// Retry: reserve again against the same demandRef after the revoke.
	reserveUC := &usecases.ReserveStock{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	if _, err := reserveUC.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), "order-1"); err != nil {
		t.Fatalf("unexpected error retrying reserve: %v", err)
	}

	uc := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, err := uc.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 reservations (revoked + retry), got %d", len(res))
	}
}

func TestGetReservationsByDemandRef_DoesNotMatchOtherDemandRefs(t *testing.T) {
	e := newEnv()
	reserveDemand(t, e, "SKU-1", "A-1-1", "order-1", 10, 5, 5)
	reserveDemand(t, e, "SKU-1", "A-1-2", "order-2", 10, 5, 5)

	uc := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: e.Reservations, Events: e.Events, Clock: e.Clock}
	res, err := uc.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 1 || res[0].DemandRef() != "order-1" {
		t.Fatalf("expected only order-1's reservation, got %+v", res)
	}
}

func TestGetReservationsByDemandRef_RepoFails_PropagatesError(t *testing.T) {
	e := newEnv()
	resRepo := &failingReservationRepo{delegate: e.Reservations, failFindByDemandRef: true}
	uc := &usecases.GetReservationsByDemandRef{Stock: e.Stock, Reservations: resRepo, Events: e.Events, Clock: e.Clock}

	if _, err := uc.Execute(context.Background(), "order-1"); err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}
