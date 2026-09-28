package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/product"
)

func TestClassifyProduct_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		sku     string
		tags    []product.HandlingTag
		temp    product.TemperatureClass
		dot     product.DOTHazardClass
		wantErr error
	}{
		{
			name: "hazmat succeeds",
			sku:  "SKU-1",
			tags: []product.HandlingTag{product.Hazmat},
		},
		{
			name: "temperature sensitive with class succeeds",
			sku:  "SKU-2",
			tags: []product.HandlingTag{product.TemperatureSensitive},
			temp: product.Frozen,
		},
		{
			name:    "no tags rejected",
			sku:     "SKU-3",
			tags:    nil,
			wantErr: product.ErrNoHandlingTags,
		},
		{
			name:    "temperature sensitive without class rejected",
			sku:     "SKU-4",
			tags:    []product.HandlingTag{product.TemperatureSensitive},
			wantErr: product.ErrTemperatureClassRequired,
		},
		{
			name: "hazmat with dot hazard class succeeds",
			sku:  "SKU-5",
			tags: []product.HandlingTag{product.Hazmat},
			dot:  3,
		},
		{
			name:    "dot hazard class without hazmat rejected",
			sku:     "SKU-6",
			tags:    []product.HandlingTag{product.Fragile},
			dot:     3,
			wantErr: product.ErrDOTHazardClassNotApplicable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runClassifyProductCase(t, tt.sku, tt.tags, tt.temp, tt.dot, tt.wantErr)
		})
	}
}

// runClassifyProductCase executes one ClassifyProduct table case against a
// fresh env: the returned error must match wantErr exactly, a success must
// return the classification and persist it with the same DOT hazard class,
// and an error must return a nil classification.
func runClassifyProductCase(t *testing.T, sku string, tags []product.HandlingTag, temp product.TemperatureClass, dot product.DOTHazardClass, wantErr error) {
	t.Helper()
	e := newEnv()
	uc := &usecases.ClassifyProduct{Classifications: e.Classifications, Events: e.Events, Clock: e.Clock}

	c, err := uc.Execute(context.Background(), mustSKU(t, sku), tags, temp, dot)
	if err != wantErr {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if wantErr != nil {
		if c != nil {
			t.Fatalf("expected nil classification on error, got %+v", c)
		}
		return
	}
	if c == nil {
		t.Fatalf("expected a classification, got nil")
	}
	if c.DOTHazardClass() != dot {
		t.Fatalf("expected DOTHazardClass=%v, got %v", dot, c.DOTHazardClass())
	}

	stored, err := e.Classifications.FindBySKU(context.Background(), mustSKU(t, sku))
	if err != nil {
		t.Fatalf("unexpected error finding stored classification: %v", err)
	}
	if stored == nil {
		t.Fatalf("expected classification to be persisted")
	}
	if stored.DOTHazardClass() != dot {
		t.Fatalf("expected stored DOTHazardClass=%v, got %v", dot, stored.DOTHazardClass())
	}
}

func TestClassifyProduct_PublishesProductClassified(t *testing.T) {
	e := newEnv()
	uc := &usecases.ClassifyProduct{Classifications: e.Classifications, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, evt := range e.Events.Events() {
		if evt.EventName() == "ProductClassified" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected ProductClassified to be published")
	}
}

// Re-classifying the same SKU replaces its prior classification rather
// than erroring — a legitimate operational action.
func TestClassifyProduct_Reclassify_Replaces(t *testing.T) {
	e := newEnv()
	uc := &usecases.ClassifyProduct{Classifications: e.Classifications, Events: e.Events, Clock: e.Clock}

	sku := mustSKU(t, "SKU-1")
	if _, err := uc.Execute(context.Background(), sku, []product.HandlingTag{product.Fragile}, "", 0); err != nil {
		t.Fatalf("unexpected error on first classify: %v", err)
	}
	if _, err := uc.Execute(context.Background(), sku, []product.HandlingTag{product.Hazmat}, "", 0); err != nil {
		t.Fatalf("unexpected error on reclassify: %v", err)
	}

	stored, err := e.Classifications.FindBySKU(context.Background(), sku)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !stored.HasTag(product.Hazmat) || stored.HasTag(product.Fragile) {
		t.Fatalf("expected reclassification to replace tags, got %v", stored.HandlingTags())
	}
}

func TestClassifyProduct_SaveFails_PropagatesError(t *testing.T) {
	e := newEnv()
	repo := &failingProductClassificationRepo{delegate: e.Classifications, failSave: true}
	uc := &usecases.ClassifyProduct{Classifications: repo, Events: e.Events, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 0)
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}

func TestClassifyProduct_EventPublishFails_PropagatesError(t *testing.T) {
	e := newEnv()
	uc := &usecases.ClassifyProduct{Classifications: e.Classifications, Events: failingEvents{}, Clock: e.Clock}

	_, err := uc.Execute(context.Background(), mustSKU(t, "SKU-1"), []product.HandlingTag{product.Hazmat}, "", 0)
	if err != errFake {
		t.Fatalf("expected errFake, got %v", err)
	}
}
