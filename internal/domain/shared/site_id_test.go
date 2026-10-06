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
