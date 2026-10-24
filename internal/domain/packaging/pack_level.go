// Package packaging holds this service's local view of product-master's pack
// levels (the PackagingView, contract R8): which GTIN is which level of which
// SKU, and how many eaches one pack holds. product-master owns the data; this
// copy is fed only by its ProductPackLevelDeclared/Retired events.
package packaging

import (
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/shared/gs1"
)

// ErrInvalidPackLevel: a pack-level event cannot be projected (bad SKU, GTIN,
// level or quantity). It is deterministic: the consumer skips it.
var ErrInvalidPackLevel = errors.New("invalid pack level")

// Level is a pack level name (contract G6).
type Level string

const (
	LevelEach   Level = "EACH"
	LevelInner  Level = "INNER"
	LevelCase   Level = "CASE"
	LevelPallet Level = "PALLET"
)

// ParseLevel validates a wire level name.
func ParseLevel(v string) (Level, error) {
	switch l := Level(v); l {
	case LevelEach, LevelInner, LevelCase, LevelPallet:
		return l, nil
	}
	return "", fmt.Errorf("%w: unknown level %q", ErrInvalidPackLevel, v)
}

// PackLevel is one row of the PackagingView, keyed by GTIN and by
// (SKU, Level). A retired level stays resolvable by GTIN (in-flight cartons
// still carry it) but is no longer the active level of its SKU.
type PackLevel struct {
	GTIN          gs1.GTIN
	SKU           shared.SKU
	Level         Level
	UnitsPerPack  int
	ChildGTIN     string
	EffectiveFrom time.Time
	EffectiveTo   *time.Time
	// Version is product-master's product version carried by the event that
	// last wrote this row; the projection ignores older ones.
	Version int64
}

// NewPackLevel validates the wire fields of a ProductPackLevelDeclared.
func NewPackLevel(sku, level, gtin, childGTIN string, unitsPerPack int, effectiveFrom time.Time, version int64) (PackLevel, error) {
	s, err := shared.NewSKU(sku)
	if err != nil {
		return PackLevel{}, fmt.Errorf("%w: %w", ErrInvalidPackLevel, err)
	}
	l, err := ParseLevel(level)
	if err != nil {
		return PackLevel{}, err
	}
	g, err := gs1.ParseGTIN(gtin)
	if err != nil {
		return PackLevel{}, fmt.Errorf("%w: %w", ErrInvalidPackLevel, err)
	}
	if unitsPerPack < 1 || version < 1 {
		return PackLevel{}, fmt.Errorf("%w: units per pack %d, version %d", ErrInvalidPackLevel, unitsPerPack, version)
	}
	child := ""
	if childGTIN != "" {
		c, err := gs1.ParseGTIN(childGTIN)
		if err != nil {
			return PackLevel{}, fmt.Errorf("%w: child gtin: %w", ErrInvalidPackLevel, err)
		}
		child = c.String()
	}
	return PackLevel{GTIN: g, SKU: s, Level: l, UnitsPerPack: unitsPerPack, ChildGTIN: child, EffectiveFrom: effectiveFrom, Version: version}, nil
}

// Active reports whether the level is in force at instant at.
func (p PackLevel) Active(at time.Time) bool {
	if at.Before(p.EffectiveFrom) {
		return false
	}
	return p.EffectiveTo == nil || at.Before(*p.EffectiveTo)
}
