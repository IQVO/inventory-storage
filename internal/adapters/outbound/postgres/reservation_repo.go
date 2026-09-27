package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ReservationRepo is a pgxpool-backed implementation of ports.ReservationRepo.
type ReservationRepo struct {
	pool *pgxpool.Pool
}

func NewReservationRepo(pool *pgxpool.Pool) *ReservationRepo {
	return &ReservationRepo{pool: pool}
}

// Save upserts res and its allocations. When ctx already carries a
// UnitOfWork transaction (ADR 0017) this joins it (via beginOrJoin) so the
// reservation write, the stock-unit writes, and the outbox insert all
// commit together; called standalone it opens and owns its own
// transaction, same as before.
func (r *ReservationRepo) Save(ctx context.Context, res *reservation.Reservation) error {
	tx, commit, rollback, err := beginOrJoin(ctx, r.pool)
	if err != nil {
		return err
	}
	defer func() { _ = rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO reservations (id, sku, quantity, demand_ref, status, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status
	`, res.ID(), res.SKU().String(), res.Quantity().Int(), res.DemandRef(), string(res.Status()), res.CreatedAt(), res.ExpiresAt())
	if err != nil {
		return err
	}

	for _, alloc := range res.Allocations() {
		_, err = tx.Exec(ctx, `
			INSERT INTO reservation_allocations (reservation_id, stock_unit_id, quantity)
			VALUES ($1, $2, $3)
			ON CONFLICT (reservation_id, stock_unit_id) DO UPDATE SET quantity = EXCLUDED.quantity
		`, res.ID(), alloc.StockUnitID, alloc.Quantity.Int())
		if err != nil {
			return err
		}
	}

	return commit(ctx)
}

func (r *ReservationRepo) FindByID(ctx context.Context, id string) (*reservation.Reservation, error) {
	var sku, demandRef, status string
	var quantity int
	var createdAt, expiresAt time.Time
	q := querierFrom(ctx, r.pool)
	err := q.QueryRow(ctx, `
		SELECT sku, quantity, demand_ref, status, created_at, expires_at
		FROM reservations WHERE id = $1
	`, id).Scan(&sku, &quantity, &demandRef, &status, &createdAt, &expiresAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, err := q.Query(ctx, `SELECT stock_unit_id, quantity FROM reservation_allocations WHERE reservation_id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var allocations []reservation.Allocation
	for rows.Next() {
		var stockUnitID string
		var allocQty int
		if err := rows.Scan(&stockUnitID, &allocQty); err != nil {
			return nil, err
		}
		q, _ := shared.NewQuantity(allocQty)
		allocations = append(allocations, reservation.Allocation{StockUnitID: stockUnitID, Quantity: q})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	skuVO, _ := shared.NewSKU(sku)
	qty, _ := shared.NewQuantity(quantity)
	return reservation.Rehydrate(id, skuVO, qty, demandRef, allocations, reservation.Status(status), createdAt, expiresAt), nil
}

func (r *ReservationRepo) NextID(_ context.Context) (string, error) {
	return "res-" + uuid.NewString(), nil
}

// FindByDemandRef returns every reservation ever created against the given
// demandRef, ordered by created_at ascending so the caller sees them in the
// order they occurred (revoked-then-retried demand histories read
// naturally). Mirrors FindByID's scan/hydration pattern, just applied
// row-by-row instead of to a single Scan.
func (r *ReservationRepo) FindByDemandRef(ctx context.Context, demandRef string) ([]*reservation.Reservation, error) {
	q := querierFrom(ctx, r.pool)
	rows, err := q.Query(ctx, `
		SELECT id, sku, quantity, status, created_at, expires_at
		FROM reservations WHERE demand_ref = $1
		ORDER BY created_at ASC
	`, demandRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type base struct {
		id                   string
		sku, status          string
		quantity             int
		createdAt, expiresAt time.Time
	}
	var bases []base
	for rows.Next() {
		var b base
		if err := rows.Scan(&b.id, &b.sku, &b.quantity, &b.status, &b.createdAt, &b.expiresAt); err != nil {
			return nil, err
		}
		bases = append(bases, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]*reservation.Reservation, 0, len(bases))
	for _, b := range bases {
		allocRows, err := q.Query(ctx, `SELECT stock_unit_id, quantity FROM reservation_allocations WHERE reservation_id = $1`, b.id)
		if err != nil {
			return nil, err
		}

		var allocations []reservation.Allocation
		for allocRows.Next() {
			var stockUnitID string
			var allocQty int
			if err := allocRows.Scan(&stockUnitID, &allocQty); err != nil {
				allocRows.Close()
				return nil, err
			}
			q, _ := shared.NewQuantity(allocQty)
			allocations = append(allocations, reservation.Allocation{StockUnitID: stockUnitID, Quantity: q})
		}
		allocErr := allocRows.Err()
		allocRows.Close()
		if allocErr != nil {
			return nil, allocErr
		}

		skuVO, _ := shared.NewSKU(b.sku)
		qty, _ := shared.NewQuantity(b.quantity)
		results = append(results, reservation.Rehydrate(b.id, skuVO, qty, demandRef, allocations, reservation.Status(b.status), b.createdAt, b.expiresAt))
	}

	return results, nil
}
