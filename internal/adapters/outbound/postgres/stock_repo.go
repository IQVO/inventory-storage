package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

func (r *StockRepo) Save(ctx context.Context, unit *stock.StockUnit) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			sku = EXCLUDED.sku, bin_id = EXCLUDED.bin_id,
			quantity = EXCLUDED.quantity, reserved = EXCLUDED.reserved, state = EXCLUDED.state
	`, unit.ID(), unit.SKU().String(), unit.BinID().String(), unit.Quantity().Int(), unit.Reserved().Int(), string(unit.State()))
	return err
}

func (r *StockRepo) FindByID(ctx context.Context, id string) (*stock.StockUnit, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT id, sku, bin_id, quantity, reserved, state FROM stock_units WHERE id = $1`, id)
	unit, err := scanStockUnit(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return unit, err
}

func (r *StockRepo) FindBySKU(ctx context.Context, sku shared.SKU) ([]*stock.StockUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `SELECT id, sku, bin_id, quantity, reserved, state FROM stock_units WHERE sku = $1`, sku.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStockUnits(rows)
}

func (r *StockRepo) FindByBin(ctx context.Context, binID shared.BinId) ([]*stock.StockUnit, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `SELECT id, sku, bin_id, quantity, reserved, state FROM stock_units WHERE bin_id = $1`, binID.String())
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
	var quantity, reserved int
	if err := row.Scan(&id, &skuStr, &binStr, &quantity, &reserved, &state); err != nil {
		return nil, err
	}
	sku, _ := shared.NewSKU(skuStr)
	binID, _ := shared.NewBinId(binStr)
	qty, _ := shared.NewQuantity(quantity)
	res, _ := shared.NewQuantity(reserved)
	return stock.RehydrateStockUnit(id, sku, binID, qty, res, stock.State(state)), nil
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
