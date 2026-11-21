package location_test

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/location"
)

func TestHoldsContainersOnly(t *testing.T) {
	if !location.HoldsContainersOnly(location.TypeAutomatedStorage) || location.TypeAutomatedStorage != "AutomatedStorage" {
		t.Fatal("an AutomatedStorage bin holds container stock only (ADR 0054)")
	}
	for _, typ := range []string{"", "PalletRack", "Shelf", "automatedstorage", "AsrsPort"} {
		if location.HoldsContainersOnly(typ) {
			t.Fatalf("%q must accept loose stock (unknown type fails open, ADR 0013)", typ)
		}
	}
}
