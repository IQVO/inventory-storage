package shared

import "testing"

func TestNewSiteID(t *testing.T) {
	site, err := NewSiteID("SITE-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if site.String() != "SITE-A" {
		t.Fatalf("site id = %q, want %q", site.String(), "SITE-A")
	}
}

func TestNewSiteID_RejectsEmpty(t *testing.T) {
	if _, err := NewSiteID(""); err == nil {
		t.Fatal("empty site id must be rejected")
	} else if err != ErrEmptySiteID {
		t.Fatalf("error = %v, want ErrEmptySiteID", err)
	}
}

func TestNewSiteID_RejectsWhitespaceOnly(t *testing.T) {
	if _, err := NewSiteID("   "); err == nil {
		t.Fatal("whitespace-only site id must be rejected")
	}
}

// TestNewSiteID_BlankIsTheContractsWhitespace pins NewSiteID to the OpenAPI
// `pattern: '\S'` (ECMA-262 \s), not Go's unicode.IsSpace: Schemathesis sent
// siteCode "\u0085" (valid per the schema) and got 400 empty-site-id.
func TestNewSiteID_BlankIsTheContractsWhitespace(t *testing.T) {
	for _, valid := range []string{"WH1", "\u0085", " WH1 ", "\u180e"} {
		if _, err := NewSiteID(valid); err != nil {
			t.Fatalf("NewSiteID(%q) = %v, want accepted (it matches the contract's \\S)", valid, err)
		}
	}
	for _, blank := range []string{"", " ", "\t\n", "\u00a0", "\ufeff", "\u2003\u3000", "\u2028"} {
		if _, err := NewSiteID(blank); err == nil {
			t.Fatalf("NewSiteID(%q) accepted, want ErrEmptySiteID (no non-whitespace character per ECMA-262)", blank)
		}
	}
}
