package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

func (r *LocationRepo) Save(ctx context.Context, bin *location.Bin) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO bins (id, capacity, occupied)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET capacity = EXCLUDED.capacity, occupied = EXCLUDED.occupied
	`, bin.ID().String(), bin.Capacity().Int(), bin.Occupied().Int())
	return err
}

func (r *LocationRepo) FindByID(ctx context.Context, id shared.BinId) (*location.Bin, error) {
	var capacity, occupied int
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT capacity, occupied FROM bins WHERE id = $1`, id.String()).Scan(&capacity, &occupied)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cap, _ := shared.NewQuantity(capacity)
	occ, _ := shared.NewQuantity(occupied)
	return location.RehydrateBin(id, cap, occ), nil
}
