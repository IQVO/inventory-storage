package stock_test

import (
	"errors"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

func TestNewToteUnitIsLooseStockTaggedWithTheTote(t *testing.T) {
	u, err := stock.NewToteUnit("su-1", "SKU-1", "WH1-AUTO-ASRS-ASRS1-00-00-S", 6, "WH1", "seller-a", "TOTE-1")
	if err != nil {
		t.Fatal(err)
	}
	if u.ContainerLPN() != "TOTE-1" || !u.InContainer() || !u.IsLoose() || u.OwnerRef() != "seller-a" ||
		u.SiteID() != "WH1" || u.Quantity() != 6 || u.Usable() != 6 {
		t.Fatalf("tote unit = lpn %q loose %v owner %q site %q qty %d usable %d",
			u.ContainerLPN(), u.IsLoose(), u.OwnerRef(), u.SiteID(), u.Quantity(), u.Usable())
	}
}

func TestNewToteUnitNeedsTheToteAndTheStowRules(t *testing.T) {
	if _, err := stock.NewToteUnit("su-1", "SKU-1", "B-1", 6, "WH1", "", ""); !errors.Is(err, stock.ErrContainerRequired) {
		t.Fatalf("no lpn: err = %v", err)
	}
	if _, err := stock.NewToteUnit("su-1", "", "B-1", 6, "WH1", "", "TOTE-1"); !errors.Is(err, stock.ErrStowRequiresItemAndLocation) {
		t.Fatalf("no sku: err = %v", err)
	}
	if _, err := stock.NewToteUnit("su-1", "SKU-1", "B-1", 0, "WH1", "", "TOTE-1"); err == nil {
		t.Fatal("zero quantity accepted")
	}
}

func TestAPlainUnitIsInNoContainer(t *testing.T) {
	u, _ := stock.NewLooseUnit("su-1", "SKU-1", "B-1", 3, "WH1", "")
	if u.ContainerLPN() != "" || u.InContainer() {
		t.Fatalf("plain unit container = %q", u.ContainerLPN())
	}
}

func TestWithContainerTagsARehydratedOrSealedCaseUnit(t *testing.T) {
	u, _ := stock.NewSealedCaseUnit("su-2", "SKU-1", "B-1", 12, "WH1", "")
	if got := u.WithContainer("003400000000000017"); got != u || u.ContainerLPN() != "003400000000000017" || u.IsLoose() {
		t.Fatalf("WithContainer = %q loose %v", u.ContainerLPN(), u.IsLoose())
	}
}
