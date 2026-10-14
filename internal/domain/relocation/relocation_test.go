package relocation

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

const (
	fromCode = shared.BinId("WH1-STOR-AMB-A01-01-01-A")
	toCode   = shared.BinId("WH1-STOR-AMB-A02-01-01-A")
	otherWH  = shared.BinId("WH2-STOR-AMB-A02-01-01-A")
)

var at = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

func validSpec() Spec {
	return Spec{
		ID: "rel-1", SiteID: "WH1", SKU: "SKU-1", FromBin: fromCode, ToBin: toCode,
		Quantity: 5, Reason: ReasonSlotting, SourceRef: "plan-7/move-2",
	}
}

func TestNew_AcceptsAValidSpec(t *testing.T) {
	r, err := New(validSpec())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if r.ID() != "rel-1" || r.SiteID() != "WH1" || r.SKU() != "SKU-1" || r.FromBin() != fromCode ||
		r.ToBin() != toCode || r.Quantity() != 5 || r.Reason() != ReasonSlotting || r.SourceRef() != "plan-7/move-2" {
		t.Fatalf("New did not keep the spec: %+v", r)
	}
	if r.DestinationStockUnitID() != "" || len(r.Legs()) != 0 || !r.OccurredAt().IsZero() {
		t.Fatal("an unapplied relocation must carry no outcome")
	}
}

func TestNew_RejectsInvalidSpecs(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Spec)
		want error
	}{
		{"empty id", func(s *Spec) { s.ID = " " }, ErrEmptyID},
		{"empty site", func(s *Spec) { s.SiteID = "" }, shared.ErrEmptySiteID},
		{"blank site", func(s *Spec) { s.SiteID = "  " }, shared.ErrEmptySiteID},
		{"empty sku", func(s *Spec) { s.SKU = "" }, shared.ErrEmptySKU},
		{"empty from bin", func(s *Spec) { s.FromBin = "" }, shared.ErrEmptyBinID},
		{"empty to bin", func(s *Spec) { s.ToBin = "" }, shared.ErrEmptyBinID},
		{"same bin", func(s *Spec) { s.ToBin = s.FromBin }, ErrSameBin},
		{"zero quantity", func(s *Spec) { s.Quantity = 0 }, shared.ErrZeroQuantity},
		{"negative quantity", func(s *Spec) { s.Quantity = -1 }, shared.ErrZeroQuantity},
		{"unknown reason", func(s *Spec) { s.Reason = "WHIM" }, ErrUnknownReason},
		{"source ref too long", func(s *Spec) { s.SourceRef = string(make([]byte, MaxSourceRefLength+1)) }, ErrSourceRefTooLong},
	}
	for _, tc := range cases {
		spec := validSpec()
		tc.mut(&spec)
		if _, err := New(spec); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestNew_SourceRefAtTheLimitIsAccepted(t *testing.T) {
	spec := validSpec()
	spec.SourceRef = string(make([]byte, MaxSourceRefLength))
	if _, err := New(spec); err != nil {
		t.Fatalf("a source ref of exactly %d bytes must be accepted: %v", MaxSourceRefLength, err)
	}
}

func TestParseReason(t *testing.T) {
	for _, r := range []Reason{ReasonSlotting, ReasonConsolidation, ReasonManual} {
		got, err := ParseReason(string(r))
		if err != nil || got != r {
			t.Fatalf("ParseReason(%q) = %q, %v", r, got, err)
		}
	}
	if _, err := ParseReason("slotting"); !errors.Is(err, ErrUnknownReason) {
		t.Fatalf("reasons are case-sensitive wire values, got %v", err)
	}
	if _, err := ParseReason(""); !errors.Is(err, ErrUnknownReason) {
		t.Fatalf("an empty reason is not a reason, got %v", err)
	}
}

// --- Apply ---------------------------------------------------------------------

func unitAt(t *testing.T, id string, bin shared.BinId, qty, reserved int) *stock.StockUnit {
	t.Helper()
	state := stock.StateAvailable
	if reserved > 0 {
		state = stock.StateReserved
	}
	return stock.RehydrateStockUnit(id, "SKU-1", bin, shared.Quantity(qty), shared.Quantity(reserved), state, 3)
}

func bins(fromOccupied, toCapacity, toOccupied int) (*location.Bin, *location.Bin) {
	return location.RehydrateBin(fromCode, 100, shared.Quantity(fromOccupied), 2),
		location.RehydrateBin(toCode, shared.Quantity(toCapacity), shared.Quantity(toOccupied), 4)
}

func TestApply_MovesTheQuantityAcrossBinsAndUnits(t *testing.T) {
	r, _ := New(validSpec()) // 5 units
	from, to := bins(12, 20, 3)
	a := unitAt(t, "su-b", fromCode, 3, 0)
	b := unitAt(t, "su-a", fromCode, 4, 0)
	units := []*stock.StockUnit{a, b}

	out, err := r.Apply(units, from, to, "su-dest", at)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Deterministic draw: ascending unit id, so su-a (4) then su-b (1).
	assertUnit(t, b, 0, stock.StateRemoved)
	assertUnit(t, a, 2, stock.StateAvailable)
	if from.Occupied() != 7 || to.Occupied() != 8 {
		t.Fatalf("bins: from occupied %d (want 7), to occupied %d (want 8)", from.Occupied(), to.Occupied())
	}
	assertDestination(t, out.Destination)
	if len(out.Touched) != 2 || out.Touched[0] != b || out.Touched[1] != a {
		t.Fatalf("touched units must be the drawn units in draw order, got %v", out.Touched)
	}
	assertLegs(t, r.Legs(), []Leg{{StockUnitID: "su-a", Quantity: 4}, {StockUnitID: "su-b", Quantity: 1}})
	if r.DestinationStockUnitID() != "su-dest" || !r.OccurredAt().Equal(at) {
		t.Fatalf("outcome not recorded: dest %q at %v", r.DestinationStockUnitID(), r.OccurredAt())
	}
}

func assertUnit(t *testing.T, u *stock.StockUnit, qty int, state stock.State) {
	t.Helper()
	if u.Quantity().Int() != qty || u.State() != state {
		t.Fatalf("%s: quantity %d state %s, want %d/%s", u.ID(), u.Quantity(), u.State(), qty, state)
	}
}

func assertDestination(t *testing.T, dest *stock.StockUnit) {
	t.Helper()
	if dest.ID() != "su-dest" || dest.BinID() != toCode || dest.Quantity() != 5 || dest.SKU() != "SKU-1" {
		t.Fatalf("destination unit wrong: %+v", dest)
	}
	if dest.SiteID() != "WH1" || dest.State() != stock.StateAvailable {
		t.Fatalf("destination unit site %q state %s, want WH1/AVAILABLE", dest.SiteID(), dest.State())
	}
}

func assertLegs(t *testing.T, got, want []Leg) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("legs = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("legs = %+v, want %+v", got, want)
		}
	}
}

func TestApply_StopsDrawingOnceTheQuantityIsCovered(t *testing.T) {
	spec := validSpec()
	spec.Quantity = 4
	r, _ := New(spec)
	from, to := bins(9, 20, 0)
	first := unitAt(t, "su-1", fromCode, 4, 0)
	second := unitAt(t, "su-2", fromCode, 5, 0)

	out, err := r.Apply([]*stock.StockUnit{second, first}, from, to, "su-dest", at)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if second.Quantity() != 5 {
		t.Fatalf("su-2 must not be touched once su-1 covered the quantity, quantity %d", second.Quantity())
	}
	if len(out.Touched) != 1 || out.Touched[0] != first || len(r.Legs()) != 1 {
		t.Fatalf("only su-1 must be drawn, touched %v legs %v", out.Touched, r.Legs())
	}
}

func TestApply_SkipsReservedQuantityOtherSKUsOtherBinsAndDeadUnits(t *testing.T) {
	spec := validSpec()
	spec.Quantity = 3
	r, _ := New(spec)
	from, to := bins(30, 20, 0)
	reserved := unitAt(t, "su-1", fromCode, 5, 4) // 1 usable
	otherSKU := stock.RehydrateStockUnit("su-2", "SKU-2", fromCode, 10, 0, stock.StateAvailable, 1)
	otherBin := unitAt(t, "su-3", toCode, 10, 0)
	unlocated := stock.RehydrateStockUnit("su-4", "SKU-1", fromCode, 10, 0, stock.StateUnlocated, 1)
	fullyReserved := unitAt(t, "su-5", fromCode, 2, 2)
	free := unitAt(t, "su-6", fromCode, 2, 0)

	_, err := r.Apply([]*stock.StockUnit{reserved, otherSKU, otherBin, unlocated, fullyReserved, free}, from, to, "su-dest", at)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if reserved.Quantity() != 4 || reserved.Reserved() != 4 {
		t.Fatalf("only the usable unit of su-1 may move: quantity %d reserved %d", reserved.Quantity(), reserved.Reserved())
	}
	if otherSKU.Quantity() != 10 || otherBin.Quantity() != 10 || unlocated.Quantity() != 10 || fullyReserved.Quantity() != 2 {
		t.Fatal("a unit of another SKU, another bin, an unlocated unit or a fully reserved unit was drawn")
	}
	if free.Quantity() != 0 {
		t.Fatalf("su-6 quantity %d, want 0", free.Quantity())
	}
	assertLegs(t, r.Legs(), []Leg{{StockUnitID: "su-1", Quantity: 1}, {StockUnitID: "su-6", Quantity: 2}})
}

func TestApply_InsufficientUsableChangesNothing(t *testing.T) {
	r, _ := New(validSpec()) // 5 units
	from, to := bins(10, 20, 0)
	a := unitAt(t, "su-1", fromCode, 4, 0)
	b := unitAt(t, "su-2", fromCode, 6, 6)

	_, err := r.Apply([]*stock.StockUnit{a, b}, from, to, "su-dest", at)
	if !errors.Is(err, stock.ErrInsufficientUsable) {
		t.Fatalf("err = %v, want ErrInsufficientUsable", err)
	}
	if a.Quantity() != 4 || from.Occupied() != 10 || to.Occupied() != 0 || len(r.Legs()) != 0 {
		t.Fatal("a rejected relocation must not change any unit, bin or the relocation")
	}
}

func TestApply_ExactlyTheUsableQuantityIsEnough(t *testing.T) {
	r, _ := New(validSpec()) // 5 units
	from, to := bins(5, 5, 0)
	a := unitAt(t, "su-1", fromCode, 5, 0)

	if _, err := r.Apply([]*stock.StockUnit{a}, from, to, "su-dest", at); err != nil {
		t.Fatalf("moving exactly the usable quantity into exactly the free space must succeed: %v", err)
	}
	if !to.IsFull() || from.Occupied() != 0 {
		t.Fatalf("to occupied %d, from occupied %d", to.Occupied(), from.Occupied())
	}
}

func TestApply_NoUnitsIsInsufficient(t *testing.T) {
	r, _ := New(validSpec())
	from, to := bins(0, 20, 0)

	if _, err := r.Apply(nil, from, to, "su-dest", at); !errors.Is(err, stock.ErrInsufficientUsable) {
		t.Fatalf("err = %v, want ErrInsufficientUsable", err)
	}
}

func TestApply_DestinationFullChangesNothing(t *testing.T) {
	r, _ := New(validSpec()) // 5 units
	from, to := bins(5, 10, 6)
	a := unitAt(t, "su-1", fromCode, 5, 0)

	_, err := r.Apply([]*stock.StockUnit{a}, from, to, "su-dest", at)
	if !errors.Is(err, location.ErrBinFull) {
		t.Fatalf("err = %v, want ErrBinFull", err)
	}
	if a.Quantity() != 5 || from.Occupied() != 5 || to.Occupied() != 6 || len(r.Legs()) != 0 {
		t.Fatal("a rejected relocation must not change any unit, bin or the relocation")
	}
}

func TestApply_SourceOccupancyBelowTheQuantityIsRejected(t *testing.T) {
	r, _ := New(validSpec()) // 5 units, but the bin claims to hold only 2
	from, to := bins(2, 20, 0)
	a := unitAt(t, "su-1", fromCode, 5, 0)

	_, err := r.Apply([]*stock.StockUnit{a}, from, to, "su-dest", at)
	if !errors.Is(err, location.ErrReleaseExceedsOccupancy) {
		t.Fatalf("err = %v, want ErrReleaseExceedsOccupancy", err)
	}
	if a.Quantity() != 5 || to.Occupied() != 0 {
		t.Fatal("a rejected relocation must not change any unit or the destination bin")
	}
}

func TestApply_BinsMustBelongToTheSite(t *testing.T) {
	r, _ := New(validSpec())
	unit := unitAt(t, "su-1", fromCode, 5, 0)

	wrongTo := location.RehydrateBin(toCode, 20, 0, 1)
	foreignFrom := location.RehydrateBinAtSite(fromCode, 20, 5, "WH2", 1)
	if _, err := r.Apply([]*stock.StockUnit{unit}, foreignFrom, wrongTo, "su-dest", at); !errors.Is(err, ErrBinNotAtSite) {
		t.Fatalf("source at another site: err = %v, want ErrBinNotAtSite", err)
	}

	from := location.RehydrateBin(fromCode, 20, 5, 1)
	foreignTo := location.RehydrateBin(otherWH, 20, 0, 1)
	r2spec := validSpec()
	r2spec.ToBin = otherWH
	r2, _ := New(r2spec)
	if _, err := r2.Apply([]*stock.StockUnit{unit}, from, foreignTo, "su-dest", at); !errors.Is(err, ErrBinNotAtSite) {
		t.Fatalf("destination at another site: err = %v, want ErrBinNotAtSite", err)
	}
	if unit.Quantity() != 5 || from.Occupied() != 5 {
		t.Fatal("a rejected relocation must not change anything")
	}
}

func TestApply_BinsMustBeTheOnesNamed(t *testing.T) {
	r, _ := New(validSpec())
	unit := unitAt(t, "su-1", fromCode, 5, 0)
	from, to := bins(5, 20, 0)

	if _, err := r.Apply([]*stock.StockUnit{unit}, to, from, "su-dest", at); !errors.Is(err, ErrBinMismatch) {
		t.Fatalf("swapped bins: err = %v, want ErrBinMismatch", err)
	}
	if _, err := r.Apply([]*stock.StockUnit{unit}, from, from, "su-dest", at); !errors.Is(err, ErrBinMismatch) {
		t.Fatalf("wrong destination: err = %v, want ErrBinMismatch", err)
	}
}

func TestApply_TwiceIsRejected(t *testing.T) {
	r, _ := New(validSpec())
	from, to := bins(10, 20, 0)
	unit := unitAt(t, "su-1", fromCode, 10, 0)
	if _, err := r.Apply([]*stock.StockUnit{unit}, from, to, "su-dest", at); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := r.Apply([]*stock.StockUnit{unit}, from, to, "su-dest-2", at); !errors.Is(err, ErrAlreadyApplied) {
		t.Fatalf("second Apply: err = %v, want ErrAlreadyApplied", err)
	}
	if unit.Quantity() != 5 {
		t.Fatalf("the second Apply moved stock: quantity %d", unit.Quantity())
	}
}

func TestApply_RequiresADestinationUnitID(t *testing.T) {
	r, _ := New(validSpec())
	from, to := bins(10, 20, 0)
	unit := unitAt(t, "su-1", fromCode, 10, 0)

	if _, err := r.Apply([]*stock.StockUnit{unit}, from, to, "", at); !errors.Is(err, ErrEmptyID) {
		t.Fatalf("err = %v, want ErrEmptyID", err)
	}
	if unit.Quantity() != 10 {
		t.Fatal("nothing may move without a destination unit id")
	}
}

func TestStockRelocatedEvent(t *testing.T) {
	r, _ := New(validSpec())
	from, to := bins(10, 20, 0)
	unit := unitAt(t, "su-1", fromCode, 10, 0)
	if _, err := r.Apply([]*stock.StockUnit{unit}, from, to, "su-dest", at); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	e := r.Event()
	if e.EventName() != "StockRelocated" || !e.OccurredAt().Equal(at) {
		t.Fatalf("event name %q at %v", e.EventName(), e.OccurredAt())
	}
	if e.RelocationID != "rel-1" || e.SiteID != "WH1" || e.SKU != "SKU-1" || e.FromBin != fromCode ||
		e.ToBin != toCode || e.Quantity != 5 || e.Reason != "SLOTTING" || e.SourceRef != "plan-7/move-2" {
		t.Fatalf("event payload wrong: %+v", e)
	}
}

func TestRehydrate_RoundTripsEveryField(t *testing.T) {
	legs := []Leg{{StockUnitID: "su-1", Quantity: 5}}
	r := Rehydrate(validSpec(), legs, "su-dest", at)

	if r.ID() != "rel-1" || r.Quantity() != 5 || r.DestinationStockUnitID() != "su-dest" || !r.OccurredAt().Equal(at) {
		t.Fatalf("rehydrate lost a field: %+v", r)
	}
	legs[0].Quantity = 99
	if r.Legs()[0].Quantity != 5 {
		t.Fatal("Rehydrate must copy the legs")
	}
	got := r.Legs()
	got[0].Quantity = 77
	if r.Legs()[0].Quantity != 5 {
		t.Fatal("Legs must return a copy")
	}
}
