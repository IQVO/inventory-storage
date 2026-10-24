package usecases_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
)

func declared(id string, version int64, gtin string, units int) usecases.PackLevelDeclared {
	return usecases.PackLevelDeclared{
		EventID: id, SKU: "SKU-1", Level: "CASE", GTIN: gtin, ChildGTIN: eachGTIN, UnitsPerPack: units,
		EffectiveFrom: time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC), Version: version,
	}
}

func TestApplyPackLevelProjectsByGTINAndBySKULevel(t *testing.T) {
	w := newCW()
	out, err := w.Pack.Declared(ctx, declared("e1", 3, caseGTIN, 12))
	if err != nil || out != usecases.PackLevelApplied {
		t.Fatalf("declared = %s %v", out, err)
	}
	byGTIN, _ := w.Packaging.FindByGTIN(ctx, caseGTIN)
	if byGTIN == nil || byGTIN.UnitsPerPack != 12 || byGTIN.SKU != "SKU-1" {
		t.Fatalf("by gtin: %+v", byGTIN)
	}
	active, _ := w.Packaging.FindActive(ctx, "SKU-1", packaging.LevelCase, time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC))
	if active == nil || active.GTIN.String() != caseGTIN {
		t.Fatalf("by sku+level: %+v", active)
	}
	if none, _ := w.Packaging.FindActive(ctx, "SKU-1", packaging.LevelPallet, time.Now()); none != nil {
		t.Fatal("an undeclared level must not resolve")
	}
}

func TestApplyPackLevelIsInboxIdempotentAndVersionGuarded(t *testing.T) {
	w := newCW()
	if _, err := w.Pack.Declared(ctx, declared("e1", 3, caseGTIN, 12)); err != nil {
		t.Fatal(err)
	}
	if out, _ := w.Pack.Declared(ctx, declared("e1", 3, caseGTIN, 12)); out != usecases.PackLevelDuplicate {
		t.Fatalf("same event id = %s, want DUPLICATE", out)
	}
	if out, _ := w.Pack.Declared(ctx, declared("e0", 2, caseGTIN, 24)); out != usecases.PackLevelStale {
		t.Fatalf("older version = %s, want STALE", out)
	}
	if got, _ := w.Packaging.FindByGTIN(ctx, caseGTIN); got.UnitsPerPack != 12 {
		t.Fatalf("a stale event changed the view: %+v", got)
	}
}

func TestRetiredLevelStaysResolvableByGTINButIsNoLongerActive(t *testing.T) {
	w := newCW()
	if _, err := w.Pack.Declared(ctx, declared("e1", 3, caseGTIN, 12)); err != nil {
		t.Fatal(err)
	}
	end := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	out, err := w.Pack.Retired(ctx, usecases.PackLevelRetired{EventID: "e2", SKU: "SKU-1", Level: "CASE", GTIN: caseGTIN, EffectiveTo: end, Version: 4})
	if err != nil || out != usecases.PackLevelApplied {
		t.Fatalf("retired = %s %v", out, err)
	}
	if got, _ := w.Packaging.FindByGTIN(ctx, caseGTIN); got == nil || got.EffectiveTo == nil {
		t.Fatalf("retired level must stay resolvable by gtin: %+v", got)
	}
	if active, _ := w.Packaging.FindActive(ctx, "SKU-1", packaging.LevelCase, end.Add(time.Hour)); active != nil {
		t.Fatal("a retired level is not active")
	}
	if out, _ := w.Pack.Retired(ctx, usecases.PackLevelRetired{EventID: "e3", SKU: "SKU-1", Level: "CASE", GTIN: "00000000000000", EffectiveTo: end, Version: 5}); out == usecases.PackLevelApplied {
		t.Fatal("an unknown gtin cannot be retired")
	}
}

func TestRetireOfAnUnknownLevelIsStale(t *testing.T) {
	w := newCW()
	out, err := w.Pack.Retired(ctx, usecases.PackLevelRetired{EventID: "e", SKU: "SKU-1", Level: "CASE", GTIN: caseGTIN, EffectiveTo: time.Now(), Version: 4})
	if err != nil || out != usecases.PackLevelStale {
		t.Fatalf("retire of unknown = %s %v, want STALE", out, err)
	}
}

func TestSupersededCaseGetsANewGTINAndKeepsTheOldOneResolvable(t *testing.T) {
	w := newCW()
	if _, err := w.Pack.Declared(ctx, declared("e1", 3, caseGTIN, 12)); err != nil {
		t.Fatal(err)
	}
	end := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	_, _ = w.Pack.Retired(ctx, usecases.PackLevelRetired{EventID: "e2", SKU: "SKU-1", Level: "CASE", GTIN: caseGTIN, EffectiveTo: end, Version: 4})
	next := declared("e3", 5, "20012345000014", 24)
	next.EffectiveFrom = end
	if _, err := w.Pack.Declared(ctx, next); err != nil {
		t.Fatal(err)
	}
	active, _ := w.Packaging.FindActive(ctx, "SKU-1", packaging.LevelCase, end.Add(time.Hour))
	if active == nil || active.UnitsPerPack != 24 {
		t.Fatalf("active after supersession: %+v", active)
	}
	if old, _ := w.Packaging.FindByGTIN(ctx, caseGTIN); old == nil || old.UnitsPerPack != 12 {
		t.Fatalf("old gtin must stay resolvable for in-flight cartons: %+v", old)
	}
}

func TestMalformedPackLevels(t *testing.T) {
	w := newCW()
	bad := []usecases.PackLevelDeclared{
		declared("", 3, caseGTIN, 12),
		declared("e", 3, "123", 12),
		declared("e", 0, caseGTIN, 12),
		declared("e", 3, caseGTIN, 0),
	}
	for _, d := range bad {
		if _, err := w.Pack.Declared(ctx, d); !errors.Is(err, usecases.ErrMalformedPackLevel) {
			t.Errorf("%+v: err = %v", d, err)
		}
	}
	badRetire := []usecases.PackLevelRetired{
		{EventID: "", SKU: "SKU-1", GTIN: caseGTIN, Version: 1, EffectiveTo: time.Now()},
		{EventID: "e", SKU: "", GTIN: caseGTIN, Version: 1, EffectiveTo: time.Now()},
		{EventID: "e", SKU: "SKU-1", GTIN: "9", Version: 1, EffectiveTo: time.Now()},
		{EventID: "e", SKU: "SKU-1", GTIN: caseGTIN, Version: 0, EffectiveTo: time.Now()},
		{EventID: "e", SKU: "SKU-1", GTIN: caseGTIN, Version: 1},
	}
	for _, r := range badRetire {
		if _, err := w.Pack.Retired(ctx, r); !errors.Is(err, usecases.ErrMalformedPackLevel) {
			t.Errorf("%+v: err = %v", r, err)
		}
	}
}
