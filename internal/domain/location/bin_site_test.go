package location

import (
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// --- effective site for intra-site relocation (ADR 0039) ---------------------

func TestEffectiveSite_RecordedCustodyWins(t *testing.T) {
	bin := RehydrateBinAtSite("WH9-STOR-AMB-A07-03-02-B", 10, 0, "WH1", 1)

	if got := bin.EffectiveSite(); got != "WH1" {
		t.Fatalf("EffectiveSite = %q, want the recorded custody WH1", got)
	}
}

func TestEffectiveSite_DerivedFromAWellFormedLocationCode(t *testing.T) {
	bin := RehydrateBin("WH1-STOR-AMB-A07-03-02-B", 10, 0, 1)

	if got := bin.EffectiveSite(); got != "WH1" {
		t.Fatalf("EffectiveSite = %q, want WH1 (site segment of the location code)", got)
	}
}

func TestEffectiveSite_UnknownForCodesThatAreNotLocationCodes(t *testing.T) {
	for _, id := range []shared.BinId{
		"A-1-1",                      // too few segments
		"WH1-STOR-AMB-A07-03-02",     // six segments
		"WH1-STOR-AMB-A07-03-02-B-X", // eight segments
		"WH1-STOR--A07-03-02-B",      // an empty segment
		"-STOR-AMB-A07-03-02-B",      // an empty site segment
		"BIN",                        // no separator at all
	} {
		bin := RehydrateBin(id, 10, 0, 1)
		if got := bin.EffectiveSite(); got != "" {
			t.Fatalf("EffectiveSite(%q) = %q, want unknown", id, got)
		}
	}
}

func TestBelongsToSite(t *testing.T) {
	derived := RehydrateBin("WH1-STOR-AMB-A07-03-02-B", 10, 0, 1)
	recorded := RehydrateBinAtSite("BIN-X", 10, 0, "WH2", 1)
	unknown := RehydrateBin("BIN-X", 10, 0, 1)

	cases := []struct {
		name string
		bin  *Bin
		site shared.SiteID
		want bool
	}{
		{"derived site matches", derived, "WH1", true},
		{"derived site differs", derived, "WH2", false},
		{"recorded site matches", recorded, "WH2", true},
		{"recorded site differs", recorded, "WH1", false},
		{"unknown site never matches", unknown, "BIN", false},
		{"empty site never matches a derived bin", derived, "", false},
		{"empty site never matches an unknown bin", unknown, "", false},
	}
	for _, tc := range cases {
		if got := tc.bin.BelongsToSite(tc.site); got != tc.want {
			t.Fatalf("%s: BelongsToSite(%q) = %v, want %v", tc.name, tc.site, got, tc.want)
		}
	}
}
