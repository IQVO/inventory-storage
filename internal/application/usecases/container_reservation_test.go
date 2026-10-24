package usecases_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// threeStowed has LPN-1..3 stowed (12 each) in one case bin, owner seller-a.
func threeStowed(t *testing.T) *cw {
	t.Helper()
	w := newCW()
	w.bin(t, "S1-A-Z-1-1-1-1", "S1", 1000, container.HandlingCase)
	w.declareCase(t, 12)
	w.bookLine(t, "line-1", 36)
	for i, lpn := range []string{"LPN-1", "LPN-2", "LPN-3"} {
		w.link(t, carton("c"+string(rune('1'+i)), lpn))
		if _, err := w.Stow.Execute(ctx, lpn, "S1-A-Z-1-1-1-1"); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func TestReserveFullCasesBindsWholeCasesAndTheirPickLocation(t *testing.T) {
	w := threeStowed(t)
	res, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 2, DemandRef: "D-1", OwnerRef: "seller-a"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Quantity() != 24 || len(res.Allocations()) != 2 || res.OwnerRef() != "seller-a" || res.Status() != reservation.StatusActive {
		t.Fatalf("reservation: qty %d allocations %d owner %q", res.Quantity(), len(res.Allocations()), res.OwnerRef())
	}
	for _, a := range res.Allocations() {
		if a.Quantity != 12 || a.BinID != "S1-A-Z-1-1-1-1" {
			t.Fatalf("allocation %+v must bind a whole case and name its bin", a)
		}
	}
	usable, _ := w.Usable.ExecuteFor(ctx, "SKU-1", "seller-a")
	if usable.ReservableCases != 1 {
		t.Fatalf("cases still reservable = %d, want 1", usable.ReservableCases)
	}
}

func TestReserveFullCasesIsAllOrNothing(t *testing.T) {
	w := threeStowed(t)
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 4, DemandRef: "D-1", OwnerRef: "seller-a"}); !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("4 cases of 3 = %v, want ErrInsufficientUsable", err)
	}
	usable, _ := w.Usable.ExecuteFor(ctx, "SKU-1", "seller-a")
	if usable.ReservableCases != 3 {
		t.Fatalf("a failed reservation held %d cases back", 3-usable.ReservableCases)
	}
}

func TestReserveFullCasesIsIdempotentPerDemand(t *testing.T) {
	w := threeStowed(t)
	cmd := usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D-1", OwnerRef: "seller-a"}
	first, err := w.Cases.Execute(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Cases.Execute(ctx, cmd)
	if err != nil || second.ID() != first.ID() {
		t.Fatalf("replay = %v %v, want the same reservation", second, err)
	}
	usable, _ := w.Usable.ExecuteFor(ctx, "SKU-1", "seller-a")
	if usable.ReservableCases != 2 {
		t.Fatalf("a replay reserved another case: %d left", usable.ReservableCases)
	}
	other := cmd
	other.DemandRef = "D-2"
	if r, err := w.Cases.Execute(ctx, other); err != nil || r.ID() == first.ID() {
		t.Fatalf("another demand must get its own case: %v %v", r, err)
	}
}

func TestReserveFullCasesValidation(t *testing.T) {
	w := threeStowed(t)
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 0, DemandRef: "D"}); !errors.Is(err, shared.ErrZeroQuantity) {
		t.Fatalf("0 cases = %v", err)
	}
	bad := 0
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D", LineNo: &bad}); !errors.Is(err, reservation.ErrInvalidLineNo) {
		t.Fatalf("line 0 = %v", err)
	}
}

func TestOwnerAStockCannotSatisfyOwnerBReservations(t *testing.T) {
	w := threeStowed(t)
	// whole cases
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D-b", OwnerRef: "seller-b"}); !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("owner B full cases = %v, want ErrInsufficientUsable", err)
	}
	// an absent owner means fleet stock only, never any owner
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D-f"}); !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("fleet full cases = %v, want ErrInsufficientUsable", err)
	}
	// loose eaches
	loose, _ := stock.NewLooseUnit("su-a", "SKU-1", "S1-A-Z-1-1-1-1", 50, "S1", "seller-a")
	if err := w.Stock.Save(ctx, loose); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Reserve.ExecuteFor(ctx, usecases.ReserveRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "D-b", OwnerRef: "seller-b"}); !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("owner B loose = %v, want ErrInsufficientUsable", err)
	}
	if _, err := w.Reserve.Execute(ctx, "SKU-1", 1, "D-f"); !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("legacy caller without an owner = %v, want ErrInsufficientUsable", err)
	}
	res, err := w.Reserve.ExecuteFor(ctx, usecases.ReserveRequest{SKU: "SKU-1", Quantity: 50, DemandRef: "D-a", OwnerRef: "seller-a"})
	if err != nil || res.OwnerRef() != "seller-a" {
		t.Fatalf("owner A loose = %v %v", res, err)
	}
	if _, err := w.Reserve.ExecuteFor(ctx, usecases.ReserveRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "D-a2", OwnerRef: "seller-a"}); !errors.Is(err, usecases.ErrInsufficientUsable) {
		t.Fatalf("a quantity reservation must not split a sealed case: %v", err)
	}
}

func TestSameDemandSameQuantityForAnotherOwnerIsNotAReplay(t *testing.T) {
	w := newCW()
	w.bin(t, "S1-A-Z-1-1-1-2", "S1", 100, container.HandlingEach)
	for i, owner := range []shared.OwnerRef{"seller-a", "seller-b"} {
		u, _ := stock.NewLooseUnit("su-"+string(rune('a'+i)), "SKU-1", "S1-A-Z-1-1-1-2", 10, "S1", owner)
		_ = w.Stock.Save(ctx, u)
	}
	a, err := w.Reserve.ExecuteFor(ctx, usecases.ReserveRequest{SKU: "SKU-1", Quantity: 5, DemandRef: "D", OwnerRef: "seller-a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := w.Reserve.ExecuteFor(ctx, usecases.ReserveRequest{SKU: "SKU-1", Quantity: 5, DemandRef: "D", OwnerRef: "seller-b"})
	if err != nil || b.ID() == a.ID() {
		t.Fatalf("owner B was handed owner A's reservation: %v %v", b, err)
	}
}

func TestConfirmingAWholeCasePickEmptiesTheContainer(t *testing.T) {
	w := threeStowed(t)
	res, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 1, DemandRef: "D-1", OwnerRef: "seller-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Confirm.Execute(ctx, res.ID()); err != nil {
		t.Fatal(err)
	}
	emptied := 0
	for _, lpn := range []string{"LPN-1", "LPN-2", "LPN-3"} {
		if w.containerOf(t, lpn).State() == container.StateEmpty {
			emptied++
		}
	}
	if emptied != 1 || countEvents(w.env, "ContainerEmptied") != 1 {
		t.Fatalf("emptied containers = %d, events = %d, want 1 and 1", emptied, countEvents(w.env, "ContainerEmptied"))
	}
	bin, _ := w.Locations.FindByID(ctx, "S1-A-Z-1-1-1-1")
	if bin.Occupied() != 24 {
		t.Fatalf("bin occupancy after the pick = %d, want 24", bin.Occupied())
	}
}

func TestRevokingAFullCaseReservationReleasesTheCases(t *testing.T) {
	w := threeStowed(t)
	res, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 3, DemandRef: "D-1", OwnerRef: "seller-a"})
	if err != nil {
		t.Fatal(err)
	}
	revoke := &usecases.RevokeReservation{Stock: w.Stock, Reservations: w.Reservations, Events: w.Events, Clock: w.Clock}
	if err := revoke.Execute(ctx, res.ID()); err != nil {
		t.Fatal(err)
	}
	if usable, _ := w.Usable.ExecuteFor(ctx, "SKU-1", "seller-a"); usable.ReservableCases != 3 {
		t.Fatalf("cases after revoke = %d, want 3", usable.ReservableCases)
	}
}

func TestExpiredFullCaseReservationReturnsTheCases(t *testing.T) {
	w := threeStowed(t)
	if _, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 3, DemandRef: "D-1", OwnerRef: "seller-a"}); err != nil {
		t.Fatal(err)
	}
	w.Clock.Advance(time.Hour)
	// a replay of the same demand notices the expiry and reserves afresh
	res, err := w.Cases.Execute(ctx, usecases.ReserveFullCasesCommand{SKU: "SKU-1", Cases: 3, DemandRef: "D-1", OwnerRef: "seller-a"})
	if err != nil || res.Status() != reservation.StatusActive {
		t.Fatalf("after expiry: %v %v", res, err)
	}
}

func TestCycleCountShortfallEmptiesTheContainerOfALostCase(t *testing.T) {
	w := stowed(t)
	count := &usecases.RunCycleCount{Stock: w.Stock, Events: w.Events, Clock: w.Clock, Containers: w.Containers}
	if _, err := count.Execute(ctx, "S1-A-Z-1-1-1-1", 0); err != nil {
		t.Fatal(err)
	}
	c := w.containerOf(t, "LPN-1")
	if c.State() != container.StateEmpty {
		t.Fatalf("container after a lost case = %s, want EMPTY", c.State())
	}
	var reason string
	for _, ev := range w.Events.Events() {
		if e, ok := ev.(shared.ContainerEmptied); ok {
			reason = e.Reason
		}
	}
	if reason != shared.ContainerEmptiedUnlocated {
		t.Fatalf("ContainerEmptied reason = %q", reason)
	}
}

func TestLooseUnitsAreUnaffectedByTheContainerHooks(t *testing.T) {
	w := newCW()
	w.bin(t, "S1-A-Z-1-1-1-2", "S1", 100, container.HandlingEach)
	u, _ := stock.NewLooseUnit("su-1", "SKU-1", "S1-A-Z-1-1-1-2", 5, "S1", "")
	_ = w.Stock.Save(ctx, u)
	count := &usecases.RunCycleCount{Stock: w.Stock, Events: w.Events, Clock: w.Clock, Containers: w.Containers}
	if _, err := count.Execute(ctx, "S1-A-Z-1-1-1-2", 0); err != nil {
		t.Fatal(err)
	}
	if countEvents(w.env, "ContainerEmptied") != 0 {
		t.Fatal("a lost loose unit must not raise ContainerEmptied")
	}
}

func TestGetContainerAndListBin(t *testing.T) {
	w := stowed(t)
	get := &usecases.GetContainer{Containers: w.Containers}
	c, err := get.Execute(ctx, "LPN-1")
	if err != nil || c.LPN() != "LPN-1" {
		t.Fatalf("get: %v %v", c, err)
	}
	if _, err := get.Execute(ctx, "NOPE"); !errors.Is(err, usecases.ErrContainerNotFound) {
		t.Fatalf("unknown = %v", err)
	}
	list := &usecases.ListBinContainers{Containers: w.Containers, Locations: w.Locations}
	in, err := list.Execute(ctx, "S1-A-Z-1-1-1-1")
	if err != nil || len(in) != 1 {
		t.Fatalf("list: %v %v", in, err)
	}
	if empty, err := list.Execute(ctx, "S1-A-Z-1-1-1-2"); err != nil || len(empty) != 0 {
		t.Fatalf("empty bin: %v %v", empty, err)
	}
	if _, err := list.Execute(ctx, "S1-NO-BIN"); !errors.Is(err, usecases.ErrBinNotFound) {
		t.Fatalf("unknown bin = %v", err)
	}
}

func TestBinLevelEventsAreScopedByOwner(t *testing.T) {
	w := stowed(t)
	var levels []shared.BinStockLevelChanged
	_ = levels
	emitter := &usecases.BinLevelEmitter{Ledger: newOwnerLedger(w), Locations: w.Locations, Events: w.Events, Clock: w.Clock}
	w.Stow.Levels = emitter
	w.bin(t, "S1-A-Z-1-1-2-1", "S1", 100, container.HandlingCase)
	if _, err := w.Stow.Execute(ctx, "LPN-1", "S1-A-Z-1-1-2-1"); err != nil {
		t.Fatal(err)
	}
	for _, ev := range w.Events.Events() {
		if e, ok := ev.(shared.BinStockLevelChanged); ok {
			if e.OwnerRef != "seller-a" || e.Reason != shared.BinLevelRelocate {
				t.Fatalf("level event %+v, want owner seller-a reason RELOCATE", e)
			}
			levels = append(levels, e)
		}
	}
	if len(levels) != 2 || levels[0].BinID != "S1-A-Z-1-1-1-1" || levels[0].OnHand != 0 || levels[1].OnHand != 12 {
		t.Fatalf("levels = %+v, want the source bin at 0 then the destination at 12", levels)
	}
}
