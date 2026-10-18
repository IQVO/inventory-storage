// Package product holds the ProductClassification aggregate: SKU-level
// master data describing how an item must be handled (hazmat, fragile,
// temperature-sensitive, oversized, high-value), independent of any
// StockUnit or bin. This service owns this classification as its source of
// truth — it is not derived from, or shared with, any other bounded
// context.
//
// TemperatureClass here is a deliberate, small duplication of the
// facility-layout bounded context's own TemperatureClass concept (see ADR
// 0009). Bounded contexts do not share Go types across repository
// boundaries; each side names and validates the concept in its own
// ubiquitous language, translating at the integration edge instead.
package product

import (
	"errors"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var (
	// ErrUnknownHandlingTag is returned when a tag value is not one of the
	// closed enum members.
	ErrUnknownHandlingTag = errors.New("unknown handling tag")
	// ErrUnknownTemperatureClass is returned when a temperature class value
	// is not one of the closed enum members.
	ErrUnknownTemperatureClass = errors.New("unknown temperature class")
	// ErrNoHandlingTags is returned when a classification is constructed
	// with no handling tags at all — a classification must say something.
	ErrNoHandlingTags = errors.New("classification requires at least one handling tag")
	// ErrTemperatureClassRequired is returned when HandlingTags contains
	// TemperatureSensitive but no valid TemperatureClass was supplied.
	ErrTemperatureClassRequired = errors.New("temperature-sensitive classification requires a temperature class")
	// ErrTemperatureClassNotApplicable is returned when a TemperatureClass
	// is supplied but HandlingTags does not contain TemperatureSensitive —
	// the value would be meaningless and is rejected rather than ignored.
	ErrTemperatureClassNotApplicable = errors.New("temperature class is only meaningful when the temperature-sensitive tag is present")
	// ErrDuplicateHandlingTag is returned when the same tag appears more
	// than once in the input list — HandlingTags is a set, not a list.
	ErrDuplicateHandlingTag = errors.New("duplicate handling tag")
	// ErrInvalidDOTHazardClass is returned when a DOTHazardClass value is
	// outside the valid 1-9 range.
	ErrInvalidDOTHazardClass = errors.New("dot hazard class must be between 1 and 9")
	// ErrDOTHazardClassNotApplicable is returned when a DOTHazardClass is
	// supplied but HandlingTags does not contain Hazmat — the value would
	// be meaningless and is rejected rather than ignored, mirroring
	// ErrTemperatureClassNotApplicable's shape exactly.
	ErrDOTHazardClassNotApplicable = errors.New("dot hazard class is only meaningful when the hazmat tag is present")
)

// HandlingTag is one member of the closed set of ways a SKU may require
// special handling. Unlike facility-layout's Zone.LocationType (an open
// tag list), this is deliberately closed: the taxonomy is fixed business
// vocabulary, not an extensible attribute bag.
type HandlingTag string

const (
	Hazmat               HandlingTag = "Hazmat"
	Fragile              HandlingTag = "Fragile"
	TemperatureSensitive HandlingTag = "TemperatureSensitive"
	Oversized            HandlingTag = "Oversized"
	HighValue            HandlingTag = "HighValue"
)

// ParseHandlingTag validates a string against the closed HandlingTag enum.
func ParseHandlingTag(value string) (HandlingTag, error) {
	switch HandlingTag(value) {
	case Hazmat, Fragile, TemperatureSensitive, Oversized, HighValue:
		return HandlingTag(value), nil
	default:
		return "", ErrUnknownHandlingTag
	}
}

// TemperatureClass is the storage temperature band a TemperatureSensitive
// SKU requires. This mirrors facility-layout's Zone.TemperatureClass value
// by name and meaning, but is a distinct type owned by this context — see
// the package doc comment and ADR 0009.
type TemperatureClass string

const (
	Ambient TemperatureClass = "Ambient"
	Chilled TemperatureClass = "Chilled"
	Frozen  TemperatureClass = "Frozen"
)

// ParseTemperatureClass validates a string against the closed
// TemperatureClass enum.
func ParseTemperatureClass(value string) (TemperatureClass, error) {
	switch TemperatureClass(value) {
	case Ambient, Chilled, Frozen:
		return TemperatureClass(value), nil
	default:
		return "", ErrUnknownTemperatureClass
	}
}

// DOTHazardClass is a US DOT hazard class (49 CFR §172.101's Hazardous
// Materials Table), restricted here to the nine top-level classes (1-9).
// Zero is the unspecified/zero-value — a Hazmat SKU may be registered
// without a DOTHazardClass at all, for backward compatibility with SKUs
// classified before this field existed (see ADR 0010). This is
// deliberately NOT a closed Go enum of named constants the way HandlingTag
// and TemperatureClass are: the real regulation's classes are just the
// integers 1-9, and encoding them as an int with a range-validating parser
// is more honest than inventing nine placeholder identifiers no one in
// this domain actually names.
type DOTHazardClass int

// DOTHazardClassUnspecified is the zero value: no DOT hazard class has
// been recorded for this classification. It is valid even when Hazmat is
// set — DOTHazardClass is optional, not required, by design (ADR 0010).
const DOTHazardClassUnspecified DOTHazardClass = 0

// ParseDOTHazardClass validates an int against the valid DOT hazard class
// range (1-9). Note that 0 is NOT accepted here — callers that want to
// represent "unspecified" use DOTHazardClassUnspecified (the zero value)
// directly rather than parsing it, exactly as TemperatureClass's "" empty
// string is never passed through ParseTemperatureClass.
func ParseDOTHazardClass(value int) (DOTHazardClass, error) {
	if value < 1 || value > 9 {
		return DOTHazardClassUnspecified, ErrInvalidDOTHazardClass
	}
	return DOTHazardClass(value), nil
}

// ProductClassification is the aggregate root for a SKU's handling
// classification — master data, independent of any StockUnit or bin.
//
// Invariants:
//   - TemperatureClass is required and non-empty if and only if
//     HandlingTags contains TemperatureSensitive. Absence of the
//     TemperatureSensitive tag means TemperatureClass must be empty.
//   - DOTHazardClass, when non-zero (DOTHazardClassUnspecified), is only
//     valid if HandlingTags contains Hazmat. Unlike TemperatureClass,
//     DOTHazardClass is never REQUIRED by the Hazmat tag — it remains
//     optional even on a Hazmat classification, so SKUs classified as
//     Hazmat before this field existed continue to validate unchanged
//     (see ADR 0010).
type ProductClassification struct {
	sku              shared.SKU
	handlingTags     map[HandlingTag]struct{}
	temperatureClass TemperatureClass
	dotHazardClass   DOTHazardClass
}

// New constructs a ProductClassification for a SKU, validating the closed
// HandlingTag enum, rejecting duplicates, and enforcing the
// TemperatureSensitive/TemperatureClass and Hazmat/DOTHazardClass
// invariants.
func New(sku shared.SKU, tags []HandlingTag, temperatureClass TemperatureClass, dotHazardClass DOTHazardClass) (*ProductClassification, error) {
	if sku == "" {
		return nil, shared.ErrEmptySKU
	}
	if len(tags) == 0 {
		return nil, ErrNoHandlingTags
	}

	set := make(map[HandlingTag]struct{}, len(tags))
	for _, tag := range tags {
		if _, err := ParseHandlingTag(string(tag)); err != nil {
			return nil, err
		}
		if _, exists := set[tag]; exists {
			return nil, ErrDuplicateHandlingTag
		}
		set[tag] = struct{}{}
	}

	_, tempSensitive := set[TemperatureSensitive]
	if tempSensitive {
		if temperatureClass == "" {
			return nil, ErrTemperatureClassRequired
		}
		if _, err := ParseTemperatureClass(string(temperatureClass)); err != nil {
			return nil, err
		}
	} else if temperatureClass != "" {
		return nil, ErrTemperatureClassNotApplicable
	}

	_, hazmat := set[Hazmat]
	if dotHazardClass != DOTHazardClassUnspecified {
		if !hazmat {
			return nil, ErrDOTHazardClassNotApplicable
		}
		if _, err := ParseDOTHazardClass(int(dotHazardClass)); err != nil {
			return nil, err
		}
	}

	return &ProductClassification{
		sku:              sku,
		handlingTags:     set,
		temperatureClass: temperatureClass,
		dotHazardClass:   dotHazardClass,
	}, nil
}

// Rehydrate reconstructs a ProductClassification from persisted state
// without re-running construction invariants (used by repositories).
func Rehydrate(sku shared.SKU, tags []HandlingTag, temperatureClass TemperatureClass, dotHazardClass DOTHazardClass) *ProductClassification {
	set := make(map[HandlingTag]struct{}, len(tags))
	for _, tag := range tags {
		set[tag] = struct{}{}
	}
	return &ProductClassification{sku: sku, handlingTags: set, temperatureClass: temperatureClass, dotHazardClass: dotHazardClass}
}

// SKU returns the classified SKU.
func (c *ProductClassification) SKU() shared.SKU { return c.sku }

// TemperatureClass returns the required temperature band, or "" if the SKU
// is not TemperatureSensitive.
func (c *ProductClassification) TemperatureClass() TemperatureClass { return c.temperatureClass }

// DOTHazardClass returns the registered DOT hazard class, or
// DOTHazardClassUnspecified (zero) if none was recorded — including for
// every Hazmat SKU classified before this field existed.
func (c *ProductClassification) DOTHazardClass() DOTHazardClass { return c.dotHazardClass }

// HandlingTags returns the classification's tags as a stable-ordered slice
// (enum declaration order), so callers get deterministic output without
// holding a reference to the internal set representation.
func (c *ProductClassification) HandlingTags() []HandlingTag {
	ordered := []HandlingTag{Hazmat, Fragile, TemperatureSensitive, Oversized, HighValue}
	tags := make([]HandlingTag, 0, len(c.handlingTags))
	for _, tag := range ordered {
		if _, ok := c.handlingTags[tag]; ok {
			tags = append(tags, tag)
		}
	}
	return tags
}

// HasTag reports whether the classification carries the given tag.
func (c *ProductClassification) HasTag(tag HandlingTag) bool {
	_, ok := c.handlingTags[tag]
	return ok
}

// IsHazmat is a convenience for the stow-time placement check.
func (c *ProductClassification) IsHazmat() bool { return c.HasTag(Hazmat) }

// IsTemperatureSensitive is a convenience for the stow-time placement check.
func (c *ProductClassification) IsTemperatureSensitive() bool {
	return c.HasTag(TemperatureSensitive)
}

// SlotAttributes is the placement-relevant subset of a facility-layout
// location's attributes, as seen from this bounded context — the result of
// the cross-context lookup StowStock uses to enforce hazmat/temperature
// placement rules. Known=false means "no constraint info available for
// this bin", which StowStock treats as permission to stow: this service
// does not own location modelling and cannot assume every bin is known to
// facility-layout (fail-open for unclassified/unknown bins — see ADR
// 0009).
type SlotAttributes struct {
	Hazmat           bool
	TemperatureClass TemperatureClass
	Known            bool
}
