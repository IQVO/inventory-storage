package http

import (
	"net/http"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// TestStatusFor_ErrConcurrentModification_Maps409 pins ADR 0018's
// contract: a version-guarded Save's conflict must map to 409 Conflict,
// not the generic 500 the default case would otherwise produce.
func TestStatusFor_ErrConcurrentModification_Maps409(t *testing.T) {
	if got := statusFor(usecases.ErrConcurrentModification); got != http.StatusConflict {
		t.Fatalf("expected ErrConcurrentModification to map to 409, got %d", got)
	}
}

// TestProblemFor_ErrConcurrentModification_HasDedicatedSlugAndTitle pins
// the RFC 7807 body: it must NOT fall through to the generic
// "internal-error" slug — the caller needs a distinct type/title telling
// it to re-fetch and retry (ADR 0018).
func TestProblemFor_ErrConcurrentModification_HasDedicatedSlugAndTitle(t *testing.T) {
	info := problemFor(usecases.ErrConcurrentModification)
	if info.slug != "concurrent-modification" {
		t.Fatalf("expected slug %q, got %q", "concurrent-modification", info.slug)
	}
	if info.title == "" || info.title == "An unexpected internal error occurred" {
		t.Fatalf("expected a dedicated, non-generic title, got %q", info.title)
	}
}
