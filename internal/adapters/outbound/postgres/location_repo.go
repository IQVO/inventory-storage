package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// LocationRepo is a pgxpool-backed implementation of ports.LocationRepo.
type LocationRepo struct {
	pool *pgxpool.Pool
}

func NewLocationRepo(pool *pgxpool.Pool) *LocationRepo {
	return &LocationRepo{pool: pool}
}

// Save is version-guarded (ADR 0019, optimistic concurrency) — see
// StockRepo.Save's doc comment for the verified single-statement
// ON CONFLICT ... WHERE RowsAffected() semantics this relies on.
func (r *LocationRepo) Save(ctx context.Context, bin *location.Bin) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO bins (id, capacity, occupied, version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			capacity = EXCLUDED.capacity, occupied = EXCLUDED.occupied,
			version = bins.version + 1
		WHERE bins.version = $5
	`, bin.ID().String(), bin.Capacity().Int(), bin.Occupied().Int(), bin.Version(), bin.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrConcurrentModification
	}
	return nil
}

func (r *LocationRepo) FindByID(ctx context.Context, id shared.BinId) (*location.Bin, error) {
	var capacity, occupied, version int
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT capacity, occupied, version FROM bins WHERE id = $1`, id.String()).Scan(&capacity, &occupied, &version)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rehydrateBin(id, capacity, occupied, version)
}

// rehydrateBin rebuilds a Bin from its row, returning a wrapped error (not a
// zero-value Quantity inside the aggregate) when a column violates a domain
// invariant.
func rehydrateBin(id shared.BinId, capacity, occupied, version int) (*location.Bin, error) {
	cap, err := rehydrateQuantity("bin capacity", capacity)
	if err != nil {
		return nil, fmt.Errorf("bin %q: %w", id, err)
	}
	occ, err := rehydrateQuantity("bin occupied", occupied)
	if err != nil {
		return nil, fmt.Errorf("bin %q: %w", id, err)
	}
	return location.RehydrateBin(id, cap, occ, version), nil
}
