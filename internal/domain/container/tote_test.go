package container_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

const asrsBin = shared.BinId("WH1-AUTO-ASRS-ASRS1-00-00-S")
const decantBin = shared.BinId("WH1-RCV-RCV-D01-01-01-A")

func TestRegisterToteAcceptsTheLicensePlateRule(t *testing.T) {
	for _, lpn := range []string{"T", "TOTE-000123", "003400000000000017", "a.b_c-1", strings.Repeat("9", 64)} {
		tote, err := container.RegisterTote(lpn)
		if err != nil {
			t.Fatalf("%q: %v", lpn, err)
		}
		if tote.LPN() != lpn || tote.Version() != 1 || tote.IsPlaced() || tote.SiteID() != "" || tote.BinID() != "" {
			t.Fatalf("%q: fresh tote = %+v", lpn, tote.Snapshot())
		}
	}
}

func TestRegisterToteRefusesAnInvalidLicensePlate(t *testing.T) {
	for _, lpn := range []string{"", strings.Repeat("9", 65), "TOTE/1", "TOTE 1", "TOTE:1", "TÖTE"} {
		if _, err := container.RegisterTote(lpn); !errors.Is(err, container.ErrInvalidLPN) {
			t.Fatalf("%q: err = %v, want ErrInvalidLPN", lpn, err)
		}
	}
}

func TestValidLPNIsTheContainerRule(t *testing.T) {
	if !container.ValidLPN("LPN-WH1-01J9Z") || container.ValidLPN("") || container.ValidLPN("a/b") {
		t.Fatal("ValidLPN must be the ADR 0045 rule ^[A-Za-z0-9._-]{1,64}$")
	}
}

func TestPlaceInSetsTheSiteOnTheFirstPlacement(t *testing.T) {
	tote, _ := container.RegisterTote("TOTE-1")
	if err := tote.PlaceIn(decantBin, "WH1"); err != nil {
		t.Fatal(err)
	}
	if tote.BinID() != decantBin || tote.SiteID() != "WH1" || !tote.IsPlaced() {
		t.Fatalf("after first placement: %+v", tote.Snapshot())
	}
	if err := tote.PlaceIn(asrsBin, "WH1"); err != nil {
		t.Fatal(err)
	}
	if tote.BinID() != asrsBin || tote.SiteID() != "WH1" {
		t.Fatalf("after second placement: %+v", tote.Snapshot())
	}
}

func TestPlaceInRefusesAnotherSiteAndLeavesTheToteUnchanged(t *testing.T) {
	tote, _ := container.RegisterTote("TOTE-1")
	_ = tote.PlaceIn(decantBin, "WH1")
	if err := tote.PlaceIn("WH2-AUTO-ASRS-ASRS1-00-00-S", "WH2"); !errors.Is(err, container.ErrContainerNotAtSite) {
		t.Fatalf("err = %v, want ErrContainerNotAtSite", err)
	}
	if tote.BinID() != decantBin || tote.SiteID() != "WH1" {
		t.Fatalf("a refused placement changed the tote: %+v", tote.Snapshot())
	}
}

func TestPlaceInRefusesAnEmptyBinOrSite(t *testing.T) {
	tote, _ := container.RegisterTote("TOTE-1")
	if err := tote.PlaceIn("", "WH1"); !errors.Is(err, shared.ErrEmptyBinID) {
		t.Fatalf("empty bin: err = %v", err)
	}
	if err := tote.PlaceIn(decantBin, ""); !errors.Is(err, container.ErrContainerNotAtSite) {
		t.Fatalf("empty site: err = %v", err)
	}
	if tote.IsPlaced() {
		t.Fatal("a refused placement placed the tote")
	}
}

func TestRehydrateToteRoundTrips(t *testing.T) {
	s := container.ToteSnapshot{LPN: "TOTE-9", SiteID: "WH1", BinID: asrsBin, Version: 7}
	if got := container.RehydrateTote(s).Snapshot(); got != s {
		t.Fatalf("round trip = %+v, want %+v", got, s)
	}
}
