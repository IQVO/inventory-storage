package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ToteRepo is the pgxpool-backed ports.ToteRepo over totes (migration 0042).
// Every call goes through querierFrom, so inside a UnitOfWork it joins the use
// case's transaction.
type ToteRepo struct {
	pool *pgxpool.Pool
}

func NewToteRepo(pool *pgxpool.Pool) *ToteRepo { return &ToteRepo{pool: pool} }

const toteColumns = `lpn, COALESCE(site_id, ''), COALESCE(bin_id, ''), version`

// Add claims the license plate as a TOTE and inserts the tote. It reports
// false, writing nothing, when the LPN names a sealed case or a tote already.
func (r *ToteRepo) Add(ctx context.Context, t *container.Tote) (bool, error) {
	// carry (Task 6 review, D1): claim and insert commit together even outside a UnitOfWork.
	return claimAndInsert(ctx, r.pool, func(q querier) (bool, error) {
		claimed, err := claimLicensePlate(ctx, q, t.LPN(), container.KindTote)
		if err != nil || !claimed {
			return false, err
		}
		return insertTote(ctx, q, t)
	})
}

func insertTote(ctx context.Context, q querier, t *container.Tote) (bool, error) {
	tag, err := q.Exec(ctx, `
		INSERT INTO totes (lpn, site_id, bin_id, version) VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), $4)
		ON CONFLICT (lpn) DO NOTHING
	`, t.LPN(), t.SiteID().String(), t.BinID().String(), t.Version())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Save updates an existing tote, version-guarded (ADR 0019).
func (r *ToteRepo) Save(ctx context.Context, t *container.Tote) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		UPDATE totes SET site_id = NULLIF($2, ''), bin_id = NULLIF($3, ''), version = version + 1, updated_at = now()
		WHERE lpn = $1 AND version = $4
	`, t.LPN(), t.SiteID().String(), t.BinID().String(), t.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrConcurrentModification
	}
	return nil
}

func (r *ToteRepo) FindByLPN(ctx context.Context, lpn string) (*container.Tote, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT `+toteColumns+` FROM totes WHERE lpn = $1`, lpn)
	t, err := scanTote(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

func (r *ToteRepo) FindByBinAfter(ctx context.Context, binID shared.BinId, afterLPN string, limit int) ([]*container.Tote, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx,
		`SELECT `+toteColumns+` FROM totes WHERE bin_id = $1 AND lpn > $2 ORDER BY lpn LIMIT $3`,
		binID.String(), afterLPN, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*container.Tote
	for rows.Next() {
		t, err := scanTote(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanTote(row rowScanner) (*container.Tote, error) {
	var lpn, site, bin string
	var version int
	if err := row.Scan(&lpn, &site, &bin, &version); err != nil {
		return nil, err
	}
	return container.RehydrateTote(container.ToteSnapshot{LPN: lpn, SiteID: shared.SiteID(site), BinID: shared.BinId(bin), Version: version}), nil
}

// claimAndInsert runs fn, a license-plate claim and the insert it guards, in
// the caller's UnitOfWork transaction, or in a transaction of its own when ctx
// carries none, so a failed insert never leaves its plate claimed (D1).
// carry (Task 6 review).
func claimAndInsert(ctx context.Context, pool *pgxpool.Pool, fn func(q querier) (bool, error)) (bool, error) {
	tx, commit, rollback, err := beginOrJoin(ctx, pool)
	if err != nil {
		return false, err
	}
	defer func() { _ = rollback(ctx) }()
	created, err := fn(tx)
	if err != nil {
		return false, err
	}
	return created, commit(ctx)
}

// claimLicensePlate inserts (lpn, kind) into license_plates inside the
// caller's transaction. It reports true when the plate is now, or already
// was, held by kind (a redelivery), and false when another kind holds it. A
// concurrent claim of the same plate blocks on the primary key until the first
// transaction ends.
func claimLicensePlate(ctx context.Context, q querier, lpn string, kind container.LicensePlateKind) (bool, error) {
	tag, err := q.Exec(ctx, `INSERT INTO license_plates (lpn, kind) VALUES ($1, $2) ON CONFLICT (lpn) DO NOTHING`, lpn, string(kind))
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var held string
	if err := q.QueryRow(ctx, `SELECT kind FROM license_plates WHERE lpn = $1`, lpn).Scan(&held); err != nil {
		return false, err
	}
	return held == string(kind), nil
}

var (
	_ ports.ToteRepo       = (*ToteRepo)(nil)
	_ ports.ContainerStock = (*StockRepo)(nil)
)
