package http

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// carry (Task 6 review): a tote holding a held or unlocated unit is refused
// 409 unit-held / unit-unlocated by POST /totes/{lpn}/stowages BEFORE any
// write. The fixture has no unit of work on purpose: atomicity must not
// depend on one being wired, so both bins and the tote stay as they were.

const (
	unavailDecant = "WH1-RCV-DEC-D01-01-01-A"
	unavailASRS   = "WH1-AUTO-ASRS-ASRS1-00-00-S"
)

type unavailableToteFixture struct {
	srv       http.Handler
	stock     *memory.StockRepo
	locations *memory.LocationRepo
}

// newUnavailableToteFixture registers TOTE-1, stows it at the decant bin and
// fills it with 6 eaches of SKU-1.
func newUnavailableToteFixture(t *testing.T) unavailableToteFixture {
	t.Helper()
	stockRepo, locations, containers := memory.NewStockRepo(), memory.NewLocationRepo(), memory.NewContainerRepo()
	totes := memory.NewToteRepo(containers)
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC))
	for _, id := range []string{unavailDecant, unavailASRS} {
		if err := locations.Save(context.Background(), location.RehydrateBinAtSite(shared.BinId(id), 100, 0, "WH1", 1)); err != nil {
			t.Fatal(err)
		}
	}
	srv := NewRouter(&Server{
		RegisterTote: &usecases.RegisterTote{Totes: totes, Containers: containers, Events: publisher, Clock: clock},
		GetTote:      &usecases.GetTote{Totes: totes, Stock: stockRepo},
		FillTote:     &usecases.FillTote{Totes: totes, Stock: stockRepo, Locations: locations, Events: publisher, Clock: clock},
		StowTote: &usecases.StowTote{Totes: totes, Contents: stockRepo, Stock: stockRepo, Locations: locations,
			Events: publisher, Clock: clock},
	}, nil, "")
	mustStatus(t, do(t, srv, http.MethodPut, "/totes/TOTE-1", ""), http.StatusCreated)
	mustStatus(t, doKeyed(t, srv, "/totes/TOTE-1/stowages", `{"bin":"`+unavailDecant+`"}`, "k1"), http.StatusOK)
	mustStatus(t, doKeyed(t, srv, "/totes/TOTE-1/fills", `{"sku":"SKU-1","quantity":6}`, "k2"), http.StatusCreated)
	return unavailableToteFixture{srv: srv, stock: stockRepo, locations: locations}
}

// toteUnit returns the one unit in TOTE-1.
func (f unavailableToteFixture) toteUnit(t *testing.T) *stock.StockUnit {
	t.Helper()
	units, err := f.stock.FindByContainer(context.Background(), "TOTE-1")
	if err != nil || len(units) != 1 {
		t.Fatalf("tote units = %d, %v", len(units), err)
	}
	return units[0]
}

// assertNothingMoved: the decant bin still holds the 6 eaches, the AS/RS bin
// none, and the tote and its unit are still at the decant bin.
func (f unavailableToteFixture) assertNothingMoved(t *testing.T) {
	t.Helper()
	for id, want := range map[string]int{unavailDecant: 6, unavailASRS: 0} {
		bin, err := f.locations.FindByID(context.Background(), shared.BinId(id))
		if err != nil || bin.Occupied().Int() != want {
			t.Fatalf("bin %s occupied = %d, want %d (%v)", id, bin.Occupied().Int(), want, err)
		}
	}
	if u := f.toteUnit(t); u.BinID() != unavailDecant {
		t.Fatalf("unit moved to %s", u.BinID())
	}
	var body struct {
		BinID string `json:"binId"`
	}
	decode(t, do(t, f.srv, http.MethodGet, "/totes/TOTE-1", ""), &body)
	if body.BinID != unavailDecant {
		t.Fatalf("tote moved to %s", body.BinID)
	}
}

func TestStowToteWithAnUnlocatedUnitIsRefusedBeforeAnyWrite(t *testing.T) {
	f := newUnavailableToteFixture(t)
	f.toteUnit(t).MarkUnlocated()
	assertProblem(t, doKeyed(t, f.srv, "/totes/TOTE-1/stowages", `{"bin":"`+unavailASRS+`"}`, "k3"), http.StatusConflict, "unit-unlocated")
	f.assertNothingMoved(t)
}

func TestStowToteWithAHeldUnitIsRefusedBeforeAnyWrite(t *testing.T) {
	f := newUnavailableToteFixture(t)
	u := f.toteUnit(t)
	held := stock.RehydrateStockUnitAtSite(u.ID(), u.SKU(), u.BinID(), u.Quantity(), 0, stock.StateHeld, "WH1", u.Version()).WithContainer("TOTE-1")
	if err := f.stock.Save(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	assertProblem(t, doKeyed(t, f.srv, "/totes/TOTE-1/stowages", `{"bin":"`+unavailASRS+`"}`, "k3"), http.StatusConflict, "unit-held")
	f.assertNothingMoved(t)
}
