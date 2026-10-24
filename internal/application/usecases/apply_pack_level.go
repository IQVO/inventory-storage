package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/shared/gs1"
)

// ProductMasterPackLevelConsumerName is the processed_events consumer name
// under which ApplyPackLevel claims CloudEvents ids (the PackagingView inbox).
const ProductMasterPackLevelConsumerName = "product-master-pack-level"

// ErrMalformedPackLevel marks a pack-level event that can never be applied.
// It is deterministic: the consumer logs it and commits past the message.
var ErrMalformedPackLevel = errors.New("malformed product-master pack level")

// PackLevelDeclared is one ProductPackLevelDeclared occurrence (wire values).
type PackLevelDeclared struct {
	EventID       string
	SKU           string
	Level         string
	GTIN          string
	ChildGTIN     string
	UnitsPerPack  int
	EffectiveFrom time.Time
	Version       int64
}

// PackLevelRetired is one ProductPackLevelRetired occurrence (wire values).
type PackLevelRetired struct {
	EventID     string
	SKU         string
	Level       string
	GTIN        string
	EffectiveTo time.Time
	Version     int64
}

// PackLevelOutcome says what ApplyPackLevel did with one event.
type PackLevelOutcome string

const (
	// PackLevelApplied: the packaging view changed.
	PackLevelApplied PackLevelOutcome = "APPLIED"
	// PackLevelStale: the stored version is newer, or the GTIN is unknown (a
	// retire for a level never seen); no write.
	PackLevelStale PackLevelOutcome = "STALE"
	// PackLevelDuplicate: this CloudEvents id was already applied.
	PackLevelDuplicate PackLevelOutcome = "DUPLICATE"
)

// ApplyPackLevel keeps the local PackagingView in step with product-master
// (contract R8). The CloudEvents id claim, the version-guarded write and the
// re-evaluation of cartons waiting for a pack level run in ONE UnitOfWork.
// It raises no domain event on the pack-level path itself; a carton settled by
// a newly declared level raises ContainerRegistered like any other.
type ApplyPackLevel struct {
	Projection      ports.PackagingProjection
	ProcessedEvents ports.ProcessedEventRepo
	// Linker settles cartons whose GTIN just became known. Optional.
	Linker *ContainerLinker
	// UnitOfWork brackets the claim, the write and the settle. Optional.
	UnitOfWork ports.UnitOfWork
}

// Declared applies a ProductPackLevelDeclared.
func (uc *ApplyPackLevel) Declared(ctx context.Context, d PackLevelDeclared) (PackLevelOutcome, error) {
	if d.EventID == "" {
		return "", fmt.Errorf("%w: empty event id", ErrMalformedPackLevel)
	}
	level, err := packaging.NewPackLevel(d.SKU, d.Level, d.GTIN, d.ChildGTIN, d.UnitsPerPack, d.EffectiveFrom, d.Version)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedPackLevel, err)
	}
	return uc.apply(ctx, d.EventID, func(ctx context.Context) (bool, error) {
		applied, err := uc.Projection.ApplyDeclared(ctx, level)
		if err != nil || !applied || uc.Linker == nil {
			return applied, err
		}
		_, err = uc.Linker.SettleSKU(ctx, level.SKU)
		return true, err
	})
}

// Retired applies a ProductPackLevelRetired.
func (uc *ApplyPackLevel) Retired(ctx context.Context, r PackLevelRetired) (PackLevelOutcome, error) {
	if r.EventID == "" {
		return "", fmt.Errorf("%w: empty event id", ErrMalformedPackLevel)
	}
	if _, err := shared.NewSKU(r.SKU); err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedPackLevel, err)
	}
	gtin, err := gs1.ParseGTIN(r.GTIN)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrMalformedPackLevel, err)
	}
	if r.Version < 1 || r.EffectiveTo.IsZero() {
		return "", fmt.Errorf("%w: version %d, effective_to %v", ErrMalformedPackLevel, r.Version, r.EffectiveTo)
	}
	return uc.apply(ctx, r.EventID, func(ctx context.Context) (bool, error) {
		return uc.Projection.ApplyRetired(ctx, gtin.String(), r.EffectiveTo, r.Version)
	})
}

func (uc *ApplyPackLevel) apply(ctx context.Context, eventID string, write func(ctx context.Context) (bool, error)) (PackLevelOutcome, error) {
	outcome := PackLevelDuplicate
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, ProductMasterPackLevelConsumerName, eventID)
		if err != nil {
			return err
		}
		if !claimed {
			outcome = PackLevelDuplicate
			return nil
		}
		applied, err := write(ctx)
		if err != nil {
			return err
		}
		outcome = PackLevelStale
		if applied {
			outcome = PackLevelApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}
