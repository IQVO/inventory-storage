package postgres

import (
	"context"

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
	cap, _ := shared.NewQuantity(capacity)
	occ, _ := shared.NewQuantity(occupied)
	return location.RehydrateBin(id, cap, occ, version), nil
}
