package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// ContainerRepo is the pgxpool-backed ports.ContainerRepo over containers
// (migration 0038). Every call goes through querierFrom, so inside a
// UnitOfWork it joins the use case's transaction.
type ContainerRepo struct {
	pool *pgxpool.Pool
}

func NewContainerRepo(pool *pgxpool.Pool) *ContainerRepo { return &ContainerRepo{pool: pool} }

const containerColumns = `lpn, receipt_id, line_no, sku, gtin, units_per_pack, qty_eaches, owner_ref, site_id, bin_id,
	state, lot, expiry, stock_unit_id, version`

// Add inserts c; an LPN already registered is left untouched and reported as
// not created (registration is create-only).
func (r *ContainerRepo) Add(ctx context.Context, c *container.Container) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO containers (lpn, receipt_id, line_no, sku, gtin, units_per_pack, qty_eaches, owner_ref, site_id, bin_id,
			state, lot, expiry, stock_unit_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), $11, NULLIF($12, ''), NULLIF($13, ''), NULLIF($14, ''), $15)
		ON CONFLICT (lpn) DO NOTHING
	`, c.LPN(), c.ReceiptID(), c.LineNo(), c.SKU().String(), c.GTIN(), c.UnitsPerPack(), c.Quantity().Int(), c.OwnerRef().String(),
		c.SiteID().String(), c.BinID().String(), string(c.State()), c.Lot(), c.Expiry(), c.StockUnitID(), c.Version())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Save updates an existing container, version-guarded (ADR 0019): when the
// row's version no longer matches the one c was loaded at, nothing is
// written and ErrConcurrentModification is returned.
func (r *ContainerRepo) Save(ctx context.Context, c *container.Container) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		UPDATE containers SET site_id = NULLIF($2, ''), bin_id = NULLIF($3, ''), state = $4, stock_unit_id = NULLIF($5, ''),
			version = version + 1, updated_at = now()
		WHERE lpn = $1 AND version = $6
	`, c.LPN(), c.SiteID().String(), c.BinID().String(), string(c.State()), c.StockUnitID(), c.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrConcurrentModification
	}
	return nil
}

func (r *ContainerRepo) FindByLPN(ctx context.Context, lpn string) (*container.Container, error) {
	return r.findOne(ctx, `SELECT `+containerColumns+` FROM containers WHERE lpn = $1`, lpn)
}

func (r *ContainerRepo) FindByStockUnitID(ctx context.Context, stockUnitID string) (*container.Container, error) {
	return r.findOne(ctx, `SELECT `+containerColumns+` FROM containers WHERE stock_unit_id = $1`, stockUnitID)
}

func (r *ContainerRepo) findOne(ctx context.Context, query string, arg any) (*container.Container, error) {
	c, err := scanContainer(querierFrom(ctx, r.pool).QueryRow(ctx, query, arg))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return c, err
}

func (r *ContainerRepo) FindByBin(ctx context.Context, binID shared.BinId) ([]*container.Container, error) {
	return r.findMany(ctx, `SELECT `+containerColumns+` FROM containers WHERE bin_id = $1 AND state <> 'EMPTY' ORDER BY lpn`, binID.String())
}

// FindByBinAfter is the keyset-paged FindByBin: at most limit containers whose
// LPN sorts strictly after afterLPN (empty = from the start). LPN is the
// primary key, so it is the whole keyset; the "C" collation makes the order
// byte-wise, identical to the in-memory repository's.
func (r *ContainerRepo) FindByBinAfter(ctx context.Context, binID shared.BinId, afterLPN string, limit int) ([]*container.Container, error) {
	return r.findMany(ctx, `SELECT `+containerColumns+` FROM containers
		WHERE bin_id = $1 AND state <> 'EMPTY' AND lpn COLLATE "C" > $2
		ORDER BY lpn COLLATE "C" LIMIT $3`, binID.String(), afterLPN, limit)
}

func (r *ContainerRepo) FindStowedBySKU(ctx context.Context, sku shared.SKU, owner shared.OwnerRef) ([]*container.Container, error) {
	return r.findMany(ctx, `SELECT `+containerColumns+` FROM containers
		WHERE sku = $1 AND COALESCE(owner_ref, '') = $2 AND state = 'SEALED' AND bin_id IS NOT NULL
		ORDER BY created_at, lpn`, sku.String(), owner.String())
}

func (r *ContainerRepo) findMany(ctx context.Context, query string, args ...any) ([]*container.Container, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*container.Container
	for rows.Next() {
		c, err := scanContainer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (r *ContainerRepo) SumQuantity(ctx context.Context, receiptID string, lineNo int) (shared.Quantity, error) {
	var total int
	err := querierFrom(ctx, r.pool).QueryRow(ctx,
		`SELECT COALESCE(SUM(qty_eaches), 0) FROM containers WHERE receipt_id = $1 AND line_no = $2`, receiptID, lineNo).Scan(&total)
	if err != nil {
		return 0, err
	}
	return rehydrateQuantity("container quantity sum", total)
}

func scanContainer(row rowScanner) (*container.Container, error) {
	var lpn, receiptID, skuStr, gtin, state string
	var lineNo, unitsPerPack, qty, version int
	var owner, site, bin, lot, expiry, unit *string
	if err := row.Scan(&lpn, &receiptID, &lineNo, &skuStr, &gtin, &unitsPerPack, &qty, &owner, &site, &bin,
		&state, &lot, &expiry, &unit, &version); err != nil {
		return nil, err
	}
	sku, err := rehydrateSKU(skuStr)
	if err != nil {
		return nil, fmt.Errorf("container %q: %w", lpn, err)
	}
	quantity, err := rehydrateQuantity("qty_eaches", qty)
	if err != nil {
		return nil, fmt.Errorf("container %q: %w", lpn, err)
	}
	return container.Rehydrate(container.Snapshot{
		LPN: lpn, SKU: sku, GTIN: gtin, UnitsPerPack: unitsPerPack, Quantity: quantity,
		OwnerRef: shared.OwnerRef(deref(owner)), SiteID: shared.SiteID(deref(site)), BinID: shared.BinId(deref(bin)),
		State: container.State(state), Lot: deref(lot), Expiry: deref(expiry), StockUnitID: deref(unit),
		ReceiptID: receiptID, LineNo: lineNo, Version: version,
	}), nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

var _ ports.ContainerRepo = (*ContainerRepo)(nil)
