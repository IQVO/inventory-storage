package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OrderPickProgressRepo is the pgxpool-backed ports.OrderPickProgressRepo over
// order_pick_progress (migration 0034).
type OrderPickProgressRepo struct {
	pool *pgxpool.Pool
}

func NewOrderPickProgressRepo(pool *pgxpool.Pool) *OrderPickProgressRepo {
	return &OrderPickProgressRepo{pool: pool}
}

// RecordPick increments the order's counter in ONE upsert and returns the new
// value. It joins the ctx-bound UnitOfWork transaction, so it commits or rolls
// back together with the processed_events claim. Concurrent handlers for the
// same order serialize on the row lock, so each sees a distinct count.
func (r *OrderPickProgressRepo) RecordPick(ctx context.Context, demandRef string, now time.Time) (int, error) {
	var picked int
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		INSERT INTO order_pick_progress (demand_ref, picked_tasks, updated_at)
		VALUES ($1, 1, $2)
		ON CONFLICT (demand_ref) DO UPDATE SET
			picked_tasks = order_pick_progress.picked_tasks + 1,
			updated_at   = EXCLUDED.updated_at
		RETURNING picked_tasks
	`, demandRef, now).Scan(&picked)
	if err != nil {
		return 0, err
	}
	return picked, nil
}
