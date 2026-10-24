//go:build integration

package postgres_test

import (
	"encoding/json"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/shared/gs1"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func gtin(t *testing.T, s string) gs1.GTIN {
	t.Helper()
	g, err := gs1.ParseGTIN(s)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
