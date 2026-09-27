package product

import (
	"testing"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

func mustSKU(t *testing.T, v string) shared.SKU {
	t.Helper()
	sku, err := shared.NewSKU(v)
	if err != nil {
		t.Fatalf("unexpected error building SKU: %v", err)
	}
	return sku
}

func TestParseHandlingTag(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantTag HandlingTag
		wantErr error
	}{
		{"hazmat", "Hazmat", Hazmat, nil},
		{"fragile", "Fragile", Fragile, nil},
		{"temperature sensitive", "TemperatureSensitive", TemperatureSensitive, nil},
		{"oversized", "Oversized", Oversized, nil},
		{"high value", "HighValue", HighValue, nil},
		{"unknown", "Flammable", "", ErrUnknownHandlingTag},
		{"empty", "", "", ErrUnknownHandlingTag},
		{"case sensitive rejected", "hazmat", "", ErrUnknownHandlingTag},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseHandlingTag(tt.input)
			if err != tt.wantErr {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
			if got != tt.wantTag {
				t.Fatalf("expected tag %v, got %v", tt.wantTag, got)
			}
		})
	}
}

func TestParseTemperatureClass(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    TemperatureClass
		wantErr error
	}{
		{"ambient", "Ambient", Ambient, nil},
		{"chilled", "Chilled", Chilled, nil},
		{"frozen", "Frozen", Frozen, nil},
		{"unknown", "Hot", "", ErrUnknownTemperatureClass},
		{"empty", "", "", ErrUnknownTemperatureClass},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTemperatureClass(tt.input)
			if err != tt.wantErr {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
			if got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestNew_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		sku     string
		tags    []HandlingTag
		temp    TemperatureClass
		dot     DOTHazardClass
		wantErr error
	}{
		{
			name:    "empty sku rejected",
			sku:     "",
			tags:    []HandlingTag{Fragile},
			wantErr: shared.ErrEmptySKU,
		},
		{
			name:    "no tags rejected",
			sku:     "SKU-1",
			tags:    nil,
			wantErr: ErrNoHandlingTags,
		},
		{
			name:    "unknown tag rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{"Explosive"},
			wantErr: ErrUnknownHandlingTag,
		},
		{
			name:    "duplicate tag rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{Fragile, Fragile},
			wantErr: ErrDuplicateHandlingTag,
		},
		{
			name:    "temperature sensitive without class rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{TemperatureSensitive},
			wantErr: ErrTemperatureClassRequired,
		},
		{
			name:    "temperature sensitive with invalid class rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{TemperatureSensitive},
			temp:    "Hot",
			wantErr: ErrUnknownTemperatureClass,
		},
		{
			name:    "temperature class without temperature sensitive tag rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{Fragile},
			temp:    Ambient,
			wantErr: ErrTemperatureClassNotApplicable,
		},
		{
			name:    "hazmat alone succeeds",
			sku:     "SKU-1",
			tags:    []HandlingTag{Hazmat},
			wantErr: nil,
		},
		{
			name:    "temperature sensitive with valid class succeeds",
			sku:     "SKU-1",
			tags:    []HandlingTag{TemperatureSensitive},
			temp:    Chilled,
			wantErr: nil,
		},
		{
			name:    "multiple tags succeed",
			sku:     "SKU-1",
			tags:    []HandlingTag{Hazmat, Fragile, HighValue},
			wantErr: nil,
		},
		{
			name:    "hazmat plus temperature sensitive frozen succeeds",
			sku:     "SKU-1",
			tags:    []HandlingTag{Hazmat, TemperatureSensitive},
			temp:    Frozen,
			wantErr: nil,
		},
		{
			name:    "hazmat with dot hazard class succeeds",
			sku:     "SKU-1",
			tags:    []HandlingTag{Hazmat},
			dot:     3,
			wantErr: nil,
		},
		{
			name:    "dot hazard class without hazmat tag rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{Fragile},
			dot:     3,
			wantErr: ErrDOTHazardClassNotApplicable,
		},
		{
			name:    "dot hazard class out of range rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{Hazmat},
			dot:     10,
			wantErr: ErrInvalidDOTHazardClass,
		},
		{
			name:    "dot hazard class zero out of range rejected",
			sku:     "SKU-1",
			tags:    []HandlingTag{Fragile},
			dot:     -1,
			wantErr: ErrDOTHazardClassNotApplicable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runClassificationCase(t, tt.sku, tt.tags, tt.temp, tt.dot, tt.wantErr)
		})
	}
}

// runClassificationCase executes one New table case: the returned error must
// match wantErr exactly, an error must return a nil classification, and a
// success must round-trip the SKU, temperature class and DOT hazard class.
func runClassificationCase(t *testing.T, sku string, tags []HandlingTag, temp TemperatureClass, dot DOTHazardClass, wantErr error) {
	t.Helper()
	s := shared.SKU(sku)
	c, err := New(s, tags, temp, dot)
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
	if c.SKU() != s {
		t.Fatalf("expected SKU=%v, got %v", s, c.SKU())
	}
	if c.TemperatureClass() != temp {
		t.Fatalf("expected TemperatureClass=%v, got %v", temp, c.TemperatureClass())
	}
	if c.DOTHazardClass() != dot {
		t.Fatalf("expected DOTHazardClass=%v, got %v", dot, c.DOTHazardClass())
	}
}

func TestProductClassification_HandlingTags_StableOrder(t *testing.T) {
	c, err := New(mustSKU(t, "SKU-1"), []HandlingTag{HighValue, Hazmat, Oversized}, "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := c.HandlingTags()
	want := []HandlingTag{Hazmat, Oversized, HighValue}
	if len(got) != len(want) {
		t.Fatalf("expected %d tags, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected tags in order %v, got %v", want, got)
		}
	}
}

func TestProductClassification_HasTag(t *testing.T) {
	c, err := New(mustSKU(t, "SKU-1"), []HandlingTag{Hazmat}, "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.HasTag(Hazmat) {
		t.Fatalf("expected HasTag(Hazmat) to be true")
	}
	if c.HasTag(Fragile) {
		t.Fatalf("expected HasTag(Fragile) to be false")
	}
	if !c.IsHazmat() {
		t.Fatalf("expected IsHazmat() to be true")
	}
	if c.IsTemperatureSensitive() {
		t.Fatalf("expected IsTemperatureSensitive() to be false")
	}
}

func TestProductClassification_IsTemperatureSensitive(t *testing.T) {
	c, err := New(mustSKU(t, "SKU-1"), []HandlingTag{TemperatureSensitive}, Frozen, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.IsTemperatureSensitive() {
		t.Fatalf("expected IsTemperatureSensitive() to be true")
	}
	if c.IsHazmat() {
		t.Fatalf("expected IsHazmat() to be false")
	}
}

func TestRehydrate_ReconstructsWithoutValidation(t *testing.T) {
	sku := mustSKU(t, "SKU-1")
	c := Rehydrate(sku, []HandlingTag{Hazmat, TemperatureSensitive}, Chilled, 4)
	if c.SKU() != sku {
		t.Fatalf("expected SKU=%v, got %v", sku, c.SKU())
	}
	if !c.HasTag(Hazmat) || !c.HasTag(TemperatureSensitive) {
		t.Fatalf("expected both tags to round-trip, got %v", c.HandlingTags())
	}
	if c.TemperatureClass() != Chilled {
		t.Fatalf("expected TemperatureClass=Chilled, got %v", c.TemperatureClass())
	}
	if c.DOTHazardClass() != 4 {
		t.Fatalf("expected DOTHazardClass=4, got %v", c.DOTHazardClass())
	}
}

func TestNewProductClassified_CarriesFieldsAndOccurredAt(t *testing.T) {
	c, err := New(mustSKU(t, "SKU-1"), []HandlingTag{Hazmat, TemperatureSensitive}, Frozen, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	occurredAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	event := NewProductClassified(c, occurredAt)

	if event.EventName() != "ProductClassified" {
		t.Fatalf("expected EventName=ProductClassified, got %s", event.EventName())
	}
	if !event.OccurredAt().Equal(occurredAt) {
		t.Fatalf("expected OccurredAt=%v, got %v", occurredAt, event.OccurredAt())
	}
	if event.SKU != c.SKU() {
		t.Fatalf("expected SKU=%v, got %v", c.SKU(), event.SKU)
	}
	if event.TemperatureClass != Frozen {
		t.Fatalf("expected TemperatureClass=Frozen, got %v", event.TemperatureClass)
	}
	if event.DOTHazardClass != 3 {
		t.Fatalf("expected DOTHazardClass=3, got %v", event.DOTHazardClass)
	}
	if len(event.HandlingTags) != 2 {
		t.Fatalf("expected 2 handling tags, got %d (%v)", len(event.HandlingTags), event.HandlingTags)
	}
}

var _ shared.DomainEvent = ProductClassified{}
