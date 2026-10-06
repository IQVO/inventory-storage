package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// StockRepo is a pgxpool-backed implementation of ports.StockRepo.
type StockRepo struct {
	pool *pgxpool.Pool
}

func NewStockRepo(pool *pgxpool.Pool) *StockRepo {
	return &StockRepo{pool: pool}
}

// Save is version-guarded (ADR 0019, optimistic concurrency): the
// conflict-action's own WHERE clause only fires the UPDATE branch when
// the row's CURRENT version still matches what unit was loaded at. If
// another writer already saved a newer version of this same row, the
// WHERE fails to match, the UPDATE branch does not run, and
// RowsAffected() is 0 — verified against a real Postgres (pgx correctly
// reports 0 in that case, not a false "success"), so this single
// statement form is used instead of an explicit INSERT/UPDATE branch
// pair. A brand-new id (no existing row) always takes the INSERT path
// regardless of the WHERE clause, so first-ever Save of an aggregate
// (version=1) is unaffected.
//
// Save does NOT use RETURNING and does NOT advance unit's in-memory
// version: after a successful Save the aggregate still carries the
// version it was loaded at, so it must be re-loaded (FindByID) before a
// second Save in the same flow (ADR 0019 §"Save and the in-memory version").
func (r *StockRepo) Save(ctx context.Context, unit *stock.StockUnit) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state, site_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8)
		ON CONFLICT (id) DO UPDATE SET
			sku = EXCLUDED.sku, bin_id = EXCLUDED.bin_id,
			quantity = EXCLUDED.quantity, reserved = EXCLUDED.reserved, state = EXCLUDED.state,
			site_id = COALESCE(EXCLUDED.site_id, stock_units.site_id),
			version = stock_units.version + 1
		WHERE stock_units.version = $9
	`, unit.ID(), unit.SKU().String(), unit.BinID().String(), unit.Quantity().Int(), unit.Reserved().Int(), string(unit.State()), unit.SiteID().String(), unit.Version(), unit.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrConcurrentModification
	}
	return nil
}

func (r *StockRepo) FindByID(ctx context.Context, id string) (*stock.StockUnit, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT id, sku, bin_id, quantity, reserved, state, site_id, version FROM stock_units WHERE id = $1`, id)
	unit, err := scanStockUnit(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return unit, err
}

func (r *StockRepo) FindBySKU(ctx context.Context, sku shared.SKU) ([]*stock.StockUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `SELECT id, sku, bin_id, quantity, reserved, state, site_id, version FROM stock_units WHERE sku = $1`, sku.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStockUnits(rows)
}

func (r *StockRepo) FindByBin(ctx context.Context, binID shared.BinId) ([]*stock.StockUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `SELECT id, sku, bin_id, quantity, reserved, state, site_id, version FROM stock_units WHERE bin_id = $1`, binID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStockUnits(rows)
}

// FindBySKUAtSite returns sku's stock units whose recorded site custody is
// originSiteID. Legacy site-less rows (site_id NULL) are deliberately
// excluded: a transfer must never draw from a unit whose site is not a
// validated fact (see ports.StockRepo.FindBySKUAtSite).
func (r *StockRepo) FindBySKUAtSite(ctx context.Context, sku shared.SKU, originSiteID shared.SiteID) ([]*stock.StockUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT id, sku, bin_id, quantity, reserved, state, site_id, version
		FROM stock_units
		WHERE sku = $1 AND site_id = $2
	`, sku.String(), originSiteID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStockUnits(rows)
}

func (r *StockRepo) NextID(_ context.Context) (string, error) {
	return "su-" + uuid.NewString(), nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanStockUnit(row rowScanner) (*stock.StockUnit, error) {
	var id, skuStr, binStr, state string
	var quantity, reserved, version int
	var siteID *string
	if err := row.Scan(&id, &skuStr, &binStr, &quantity, &reserved, &state, &siteID, &version); err != nil {
		return nil, err
	}
	sku, err := rehydrateSKU(skuStr)
	if err != nil {
		return nil, fmt.Errorf("stock unit %q: %w", id, err)
	}
	binID, err := rehydrateBinID(binStr)
	if err != nil {
		return nil, fmt.Errorf("stock unit %q: %w", id, err)
	}
	qty, err := rehydrateQuantity("quantity", quantity)
	if err != nil {
		return nil, fmt.Errorf("stock unit %q: %w", id, err)
	}
	res, err := rehydrateQuantity("reserved", reserved)
	if err != nil {
		return nil, fmt.Errorf("stock unit %q: %w", id, err)
	}
	if siteID != nil {
		// Site custody recorded: hydrate it as a validated fact.
		site, err := shared.NewSiteID(*siteID)
		if err != nil {
			return nil, fmt.Errorf("stock unit %q: %w", id, err)
		}
		return stock.RehydrateStockUnitAtSite(id, sku, binID, qty, res, stock.State(state), site, version), nil
	}
	// Legacy site-less row: no custody fact, never transfer-allocatable.
	return stock.RehydrateStockUnit(id, sku, binID, qty, res, stock.State(state), version), nil
}

func scanStockUnits(rows pgx.Rows) ([]*stock.StockUnit, error) {
	var units []*stock.StockUnit
	for rows.Next() {
		unit, err := scanStockUnit(rows)
		if err != nil {
			return nil, err
		}
		units = append(units, unit)
	}
	return units, rows.Err()
}
