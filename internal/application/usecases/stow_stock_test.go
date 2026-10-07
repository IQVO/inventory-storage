package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func mustQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return q
}

func mustSKU(t *testing.T, v string) shared.SKU {
	t.Helper()
	sku, err := shared.NewSKU(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return sku
}

func mustBinID(t *testing.T, v string) shared.BinId {
	t.Helper()
	binID, err := shared.NewBinId(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return binID
}

func seedBin(t *testing.T, e env, binID shared.BinId, capacity int) {
	t.Helper()
	bin, err := location.NewBin(binID, mustQty(t, capacity))
	if err != nil {
		t.Fatalf("unexpected error seeding bin: %v", err)
	}
	if err := e.Locations.Save(context.Background(), bin); err != nil {
		t.Fatalf("unexpected error saving bin: %v", err)
	}
}

func TestStowStock_ValidItemAndLocation_Succeeds(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	unit, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unit.Quantity().Int() != 5 {
		t.Fatalf("expected quantity=5, got %d", unit.Quantity().Int())
	}

	bin, _ := e.Locations.FindByID(context.Background(), mustBinID(t, "A-1-1"))
	if bin.Occupied().Int() != 5 {
		t.Fatalf("expected bin occupied=5, got %d", bin.Occupied().Int())
	}
}

// Named invariant: "bin-capacity rejection" — enforced end-to-end through the use case.
func TestStowStock_ExceedsBinCapacity_Rejected(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 5)
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 6), mustBinID(t, "A-1-1"))
	if err != location.ErrBinFull {
		t.Fatalf("expected ErrBinFull, got %v", err)
	}
}

func TestStowStock_UnknownBin_Rejected(t *testing.T) {
	e := newEnv()
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), mustBinID(t, "A-1-1"))
	if err != usecases.ErrBinNotFound {
		t.Fatalf("expected ErrBinNotFound, got %v", err)
	}
}

func TestStowStock_LocationsFindByIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	locs := &failingLocationRepo{delegate: e.Locations, failFindByID: true}
	uc := &usecases.StowStock{Stock: e.Stock, Locations: locs, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestStowStock_StockNextIDFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	stockRepo := &failingStockRepo{delegate: e.Stock, failNextID: true}
	uc := &usecases.StowStock{Stock: stockRepo, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestStowStock_LocationsSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	locs := &failingLocationRepo{delegate: e.Locations, failSave: true}
	uc := &usecases.StowStock{Stock: e.Stock, Locations: locs, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestStowStock_StockSaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	stockRepo := &failingStockRepo{delegate: e.Stock, failSave: true}
	uc := &usecases.StowStock{Stock: stockRepo, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestStowStock_EventPublishFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: failingEvents{}, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 1), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

// --------------------------------------------------------------------
// Placement checks (ADR 0009): hazmat/temperature rules enforced against
// facility-layout's location-classification lookup.
// --------------------------------------------------------------------

// classifyAndSave seeds a ProductClassification directly into the local
// copy (as a legacy row), so these tests stay focused on StowStock's
// behaviour; how the copy is fed (ADR 0033) is tested elsewhere.
func classifyAndSave(t *testing.T, e env, sku shared.SKU, tags []product.HandlingTag, temp product.TemperatureClass) {
	t.Helper()
	classifyAndSaveWithDOT(t, e, sku, tags, temp, 0)
}

// classifyAndSaveWithDOT is classifyAndSave plus an explicit DOT hazard
// class, for the segregation tests below.
func classifyAndSaveWithDOT(t *testing.T, e env, sku shared.SKU, tags []product.HandlingTag, temp product.TemperatureClass, dot product.DOTHazardClass) {
	t.Helper()
	c, err := product.New(sku, tags, temp, dot)
	if err != nil {
		t.Fatalf("unexpected error building classification: %v", err)
	}
	if err := e.Classifications.Save(context.Background(), c); err != nil {
		t.Fatalf("unexpected error saving classification: %v", err)
	}
}

// Nil-safe: a StowStock built without Classifications/LocationLookup (as
// every test above this section does) behaves exactly as before this
// feature existed — no placement check at all, even for a SKU that
// happens to share a name with a classified one in another test's env.
func TestStowStock_NoClassificationPorts_PermissiveByDefault(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Unclassified SKU: even with both ports wired, no lookup-blocking occurs
// because there is nothing to enforce.
func TestStowStock_UnclassifiedSKU_NotBlocked(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	lookup := &fakeLocationLookup{failErr: errFake} // would fail-closed if consulted
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected unclassified sku to never be blocked, got %v", err)
	}
}

func TestStowStock_Hazmat_HazmatRatedZone_Succeeds(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "")
	lookup := &fakeLocationLookup{attrs: map[shared.BinId]product.SlotAttributes{
		"A-1-1": {Hazmat: true, Known: true},
	}}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStowStock_Hazmat_NonHazmatZone_Rejected(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "")
	lookup := &fakeLocationLookup{attrs: map[shared.BinId]product.SlotAttributes{
		"A-1-1": {Hazmat: false, Known: true},
	}}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != usecases.ErrHazmatZoneRequired {
		t.Fatalf("expected ErrHazmatZoneRequired, got %v", err)
	}
}

func TestStowStock_TemperatureSensitive_MatchingClass_Succeeds(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.TemperatureSensitive}, product.Chilled)
	lookup := &fakeLocationLookup{attrs: map[shared.BinId]product.SlotAttributes{
		"A-1-1": {TemperatureClass: product.Chilled, Known: true},
	}}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStowStock_TemperatureSensitive_MismatchedClass_Rejected(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.TemperatureSensitive}, product.Frozen)
	lookup := &fakeLocationLookup{attrs: map[shared.BinId]product.SlotAttributes{
		"A-1-1": {TemperatureClass: product.Ambient, Known: true},
	}}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != usecases.ErrTemperatureClassMismatch {
		t.Fatalf("expected ErrTemperatureClassMismatch, got %v", err)
	}
}

// Unknown bin (Known=false): fail-open — permits the stow even for a
// classified, rule-relevant SKU.
func TestStowStock_ClassifiedSKU_UnknownBin_FailsOpen(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "")
	lookup := &fakeLocationLookup{} // no entry for A-1-1 -> Known=false
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected fail-open for unknown bin, got %v", err)
	}
}

// Transport/5xx failure: fail-closed, but ONLY for a classified SKU that
// actually carries Hazmat or TemperatureSensitive.
func TestStowStock_ClassifiedSKU_TransportError_FailsClosed(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "")
	lookup := &fakeLocationLookup{failErr: errFake}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != usecases.ErrLocationClassificationUnavailable {
		t.Fatalf("expected ErrLocationClassificationUnavailable, got %v", err)
	}
}

// A classified SKU with no rule-relevant tag (e.g. Fragile alone) is never
// blocked by lookup unavailability — the lookup is not even consulted.
func TestStowStock_NonRuleRelevantClassification_TransportError_NotBlocked(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Fragile}, "")
	lookup := &fakeLocationLookup{failErr: errFake}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected fragile-only classification to never be blocked by lookup failure, got %v", err)
	}
}

func TestStowStock_ClassificationsFindBySKUFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 10)
	repo := &failingProductClassificationRepo{delegate: e.Classifications, failFindBySKU: true}
	lookup := &fakeLocationLookup{}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: repo, LocationLookup: lookup,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

// --------------------------------------------------------------------
// Same-bin DOT hazard-class segregation (ADR 0010): enforced AFTER the
// hazmat-zone/temperature-class checks above, purely from this service's
// own StockRepo/ProductClassificationRepo — no LocationLookup involved.
// --------------------------------------------------------------------

// seedOccupant stows a SKU into binID directly via the repos, bypassing
// StowStock's own checks entirely, so these tests can set up "another SKU
// already occupies this bin" without depending on StowStock behaviour
// being tested.
func seedOccupant(t *testing.T, e env, sku shared.SKU, binID shared.BinId, qty int) {
	t.Helper()
	id, err := e.Stock.NextID(context.Background())
	if err != nil {
		t.Fatalf("unexpected error minting id: %v", err)
	}
	unit, err := stock.NewStockUnit(id, sku, binID, mustQty(t, qty))
	if err != nil {
		t.Fatalf("unexpected error building stock unit: %v", err)
	}
	if err := e.Stock.Save(context.Background(), unit); err != nil {
		t.Fatalf("unexpected error saving stock unit: %v", err)
	}
}

// Class 1 (explosives) is incompatible with every other class, including
// a different class-1 SKU (rule 3's maximal restriction) — this exercises
// a concrete cross-class incompatible pair (1 vs 8) as the reject case.
func TestStowStock_Segregation_IncompatibleOccupant_Rejected(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-OCCUPANT"), []product.HandlingTag{product.Hazmat}, "", 8)
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 1)
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != usecases.ErrHazmatClassIncompatible {
		t.Fatalf("expected ErrHazmatClassIncompatible, got %v", err)
	}
}

// Compatible cross-class pair (class 3 flammable liquids vs class 4
// flammable solids): the stow succeeds even though both SKUs carry DOT
// hazard classes, because product.Incompatible reports them compatible.
func TestStowStock_Segregation_CompatibleOccupant_Succeeds(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-OCCUPANT"), []product.HandlingTag{product.Hazmat}, "", 4)
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 3)
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected compatible cross-class pair to succeed, got %v", err)
	}
}

// An occupant SKU with no registered classification at all never blocks
// the stow — fail-open, consistent with the rest of this service's
// classification design.
func TestStowStock_Segregation_UnclassifiedOccupant_FailsOpen(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5) // never classified
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 1)
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected unclassified occupant to fail open, got %v", err)
	}
}

// An occupant that IS classified, but carries no DOT hazard class, also
// never blocks the stow.
func TestStowStock_Segregation_OccupantWithoutDOTClass_FailsOpen(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	classifyAndSave(t, e, mustSKU(t, "SKU-OCCUPANT"), []product.HandlingTag{product.Hazmat}, "")
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 1)
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected occupant without a DOT class to fail open, got %v", err)
	}
}

// The incoming SKU itself carries no DOT hazard class — the segregation
// check never runs, even with an incompatible occupant present.
func TestStowStock_Segregation_IncomingWithoutDOTClass_NeverBlocked(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-OCCUPANT"), []product.HandlingTag{product.Hazmat}, "", 1)
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5)
	classifyAndSave(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "")
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected incoming sku without a DOT class to never be blocked, got %v", err)
	}
}

// Without a Classifications repo wired at all, segregation is not
// enforced — same nil-safe discipline as checkPlacement.
func TestStowStock_Segregation_NoClassificationsPort_PermissiveByDefault(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	uc := &usecases.StowStock{Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A second StockUnit of the SAME SKU already in the bin is never
// segregated against itself.
func TestStowStock_Segregation_SameSKUAlreadyInBin_NeverBlocked(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 1)
	seedOccupant(t, e, mustSKU(t, "SKU-1"), mustBinID(t, "A-1-1"), 5)
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected same sku already in bin to never self-block, got %v", err)
	}
}

// FindByBin failing propagates as a plain error (not swallowed/fail-open).
func TestStowStock_Segregation_FindByBinFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 1)
	stockRepo := &failingStockRepo{delegate: e.Stock, failFindByBin: true}
	uc := &usecases.StowStock{
		Stock: stockRepo, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

// The incoming SKU's own classification lookup failing inside
// checkSegregation propagates as a plain error. LocationLookup is
// deliberately NOT wired so checkPlacement short-circuits and the failure
// can only come from the segregation lookup.
func TestStowStock_Segregation_IncomingClassificationLookupFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	repo := &failingProductClassificationRepo{delegate: e.Classifications, failFindBySKU: true}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: repo,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake from incoming classification lookup, got %v", err)
	}
}

// Two StockUnits of the SAME occupant SKU in the bin: the dedup branch
// must skip the second unit's classification lookup — the stow still
// succeeds for a compatible pair.
func TestStowStock_Segregation_DuplicateOccupantSKU_LookedUpOnce_Succeeds(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-OCCUPANT"), []product.HandlingTag{product.Hazmat}, "", 4)
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5)
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 3)
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: e.Classifications,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != nil {
		t.Fatalf("expected duplicate occupant sku to be skipped without blocking, got %v", err)
	}
}

// An OCCUPANT's classification lookup failing propagates as a plain error
// (not swallowed/fail-open), while the incoming SKU's own lookup must
// still succeed — hence the failFindBySKUFor selective wrapper.
func TestStowStock_Segregation_OccupantClassificationLookupFails_PropagatesError(t *testing.T) {
	e := newEnv()
	seedBin(t, e, mustBinID(t, "A-1-1"), 100)
	seedOccupant(t, e, mustSKU(t, "SKU-OCCUPANT"), mustBinID(t, "A-1-1"), 5)
	classifyAndSaveWithDOT(t, e, mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 3)
	repo := &failingProductClassificationRepo{delegate: e.Classifications, failFindBySKUFor: mustSKU(t, "SKU-OCCUPANT")}
	uc := &usecases.StowStock{
		Stock: e.Stock, Locations: e.Locations, Events: e.Events, Clock: e.Clock,
		Classifications: repo,
	}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), mustQty(t, 5), mustBinID(t, "A-1-1"))
	if err != errFake {
		t.Fatalf("expected errFake from occupant classification lookup, got %v", err)
	}
}
