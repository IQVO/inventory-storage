package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
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
//
// The reservation row itself is version-guarded (ADR 0019, optimistic
// concurrency) — see StockRepo.Save's doc comment for the verified
// single-statement ON CONFLICT ... WHERE RowsAffected() semantics this
// relies on. A stale-version write returns ErrConcurrentModification
// before ever touching reservation_allocations, so a lost-update race on
// the reservation row can never leave allocations partially written
// against a version that lost.
func (r *ReservationRepo) Save(ctx context.Context, res *reservation.Reservation) error {
	tx, commit, rollback, err := beginOrJoin(ctx, r.pool)
	if err != nil {
		return err
	}
	defer func() { _ = rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO reservations (id, sku, quantity, demand_ref, status, created_at, expires_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			version = reservations.version + 1
		WHERE reservations.version = $9
	`, res.ID(), res.SKU().String(), res.Quantity().Int(), res.DemandRef(), string(res.Status()), res.CreatedAt(), res.ExpiresAt(), res.Version(), res.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrConcurrentModification
	}

	for _, alloc := range res.Allocations() {
		_, err = tx.Exec(ctx, `
			INSERT INTO reservation_allocations (reservation_id, stock_unit_id, quantity, bin_id)
			VALUES ($1, $2, $3, NULLIF($4, ''))
			ON CONFLICT (reservation_id, stock_unit_id) DO UPDATE SET
				quantity = EXCLUDED.quantity,
				bin_id = COALESCE(EXCLUDED.bin_id, reservation_allocations.bin_id)
		`, res.ID(), alloc.StockUnitID, alloc.Quantity.Int(), alloc.BinID.String())
		if err != nil {
			return err
		}
	}

	return commit(ctx)
}

func (r *ReservationRepo) FindByID(ctx context.Context, id string) (*reservation.Reservation, error) {
	var sku, demandRef, status string
	var quantity, version int
	var createdAt, expiresAt time.Time
	q := querierFrom(ctx, r.pool)
	err := q.QueryRow(ctx, `
		SELECT sku, quantity, demand_ref, status, created_at, expires_at, version
		FROM reservations WHERE id = $1
	`, id).Scan(&sku, &quantity, &demandRef, &status, &createdAt, &expiresAt, &version)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	allocations, err := loadAllocations(ctx, q, id)
	if err != nil {
		return nil, err
	}

	return rehydrateReservation(id, sku, quantity, demandRef, allocations, status, createdAt, expiresAt, version)
}

// rehydrateReservation rebuilds a Reservation from its row, returning a
// wrapped error instead of embedding a zero-value SKU/Quantity when a column
// violates a domain invariant.
func rehydrateReservation(id, sku string, quantity int, demandRef string, allocations []reservation.Allocation, status string, createdAt, expiresAt time.Time, version int) (*reservation.Reservation, error) {
	skuVO, err := rehydrateSKU(sku)
	if err != nil {
		return nil, fmt.Errorf("reservation %q: %w", id, err)
	}
	qty, err := rehydrateQuantity("quantity", quantity)
	if err != nil {
		return nil, fmt.Errorf("reservation %q: %w", id, err)
	}
	return reservation.Rehydrate(id, skuVO, qty, demandRef, allocations, reservation.Status(status), createdAt, expiresAt, version), nil
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
		SELECT id, sku, quantity, status, created_at, expires_at, version
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
		version              int
	}
	var bases []base
	for rows.Next() {
		var b base
		if err := rows.Scan(&b.id, &b.sku, &b.quantity, &b.status, &b.createdAt, &b.expiresAt, &b.version); err != nil {
			return nil, err
		}
		bases = append(bases, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]*reservation.Reservation, 0, len(bases))
	for _, b := range bases {
		allocations, err := loadAllocations(ctx, q, b.id)
		if err != nil {
			return nil, err
		}

		r, err := rehydrateReservation(b.id, b.sku, b.quantity, demandRef, allocations, b.status, b.createdAt, b.expiresAt, b.version)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}

// loadAllocations reads a reservation's allocations, including each one's
// pick location (bin_id, ADR 0025). bin_id is nullable for legacy rows the
// 0008 backfill could not resolve; those hydrate with an empty BinID.
// Ordered by stock_unit_id so callers see a stable order.
func loadAllocations(ctx context.Context, q querier, reservationID string) ([]reservation.Allocation, error) {
	rows, err := q.Query(ctx, `
		SELECT stock_unit_id, quantity, COALESCE(bin_id, '')
		FROM reservation_allocations WHERE reservation_id = $1
		ORDER BY stock_unit_id
	`, reservationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var allocations []reservation.Allocation
	for rows.Next() {
		var stockUnitID, binID string
		var allocQty int
		if err := rows.Scan(&stockUnitID, &allocQty, &binID); err != nil {
			return nil, err
		}
		alloc, err := rehydrateAllocation(reservationID, stockUnitID, allocQty, binID)
		if err != nil {
			return nil, err
		}
		allocations = append(allocations, alloc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return allocations, nil
}

// rehydrateAllocation rebuilds one Allocation from its row. binID stays an
// unvalidated conversion on purpose: it is nullable for legacy rows the 0008
// backfill could not resolve, which hydrate with an empty BinID (see
// loadAllocations). The quantity, though, must be a valid Quantity.
func rehydrateAllocation(reservationID, stockUnitID string, quantity int, binID string) (reservation.Allocation, error) {
	qty, err := rehydrateQuantity("allocation quantity", quantity)
	if err != nil {
		return reservation.Allocation{}, fmt.Errorf("reservation %q allocation on stock unit %q: %w", reservationID, stockUnitID, err)
	}
	return reservation.Allocation{StockUnitID: stockUnitID, BinID: shared.BinId(binID), Quantity: qty}, nil
}
