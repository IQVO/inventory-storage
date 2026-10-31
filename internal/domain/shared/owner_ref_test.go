package shared_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func TestNewOwnerRef(t *testing.T) {
	valid := []string{"", "seller-1", "A.b_c-9", strings.Repeat("x", shared.MaxOwnerRefLen)}
	for _, v := range valid {
		got, err := shared.NewOwnerRef(v)
		if err != nil || got.String() != v {
			t.Errorf("NewOwnerRef(%q) = %q, %v; want ok", v, got, err)
		}
	}
	invalid := []string{"has space", "semi;colon", "ünï", strings.Repeat("x", shared.MaxOwnerRefLen+1), "a/b"}
	for _, v := range invalid {
		if _, err := shared.NewOwnerRef(v); !errors.Is(err, shared.ErrInvalidOwnerRef) {
			t.Errorf("NewOwnerRef(%q) err = %v; want ErrInvalidOwnerRef", v, err)
		}
	}
}

func TestOwnerRefIsFleet(t *testing.T) {
	if !shared.OwnerRef("").IsFleet() {
		t.Error("empty owner must be fleet-owned")
	}
	if shared.OwnerRef("seller-1").IsFleet() {
		t.Error("a named owner is not fleet-owned")
	}
}
