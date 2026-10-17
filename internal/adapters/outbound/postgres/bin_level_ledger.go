package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// BinLevelLedger is the pgxpool-backed ports.BinLevelLedger (ADR 0040).
//
// Advance runs through querierFrom, so inside a UnitOfWork it joins the ledger
// change's transaction. The upsert on bin_sku_levels takes the (bin, sku) row
// lock and keeps it until that transaction commits or rolls back; the level
// SUM that follows is a fresh statement under READ COMMITTED, so it sees every
// change committed before the lock was granted plus the transaction's own
// writes. Concurrent changes of one (bin, sku) therefore produce events whose
// versions are 1..N in commit order, each carrying the level as of its own
// commit.
type BinLevelLedger struct {
	pool *pgxpool.Pool
}

// NewBinLevelLedger builds the ledger over pool.
func NewBinLevelLedger(pool *pgxpool.Pool) *BinLevelLedger {
	return &BinLevelLedger{pool: pool}
}

// Advance implements ports.BinLevelLedger.
func (l *BinLevelLedger) Advance(ctx context.Context, bin shared.BinId, sku shared.SKU) (shared.BinLevel, error) {
	q := querierFrom(ctx, l.pool)

	var level shared.BinLevel
	if err := q.QueryRow(ctx, `
		INSERT INTO bin_sku_levels (bin_id, sku, version) VALUES ($1, $2, 1)
		ON CONFLICT (bin_id, sku) DO UPDATE SET version = bin_sku_levels.version + 1, updated_at = now()
		RETURNING version
	`, bin.String(), sku.String()).Scan(&level.Version); err != nil {
		return shared.BinLevel{}, fmt.Errorf("postgres: bump bin level version: %w", err)
	}

	var onHand, reserved int
	if err := q.QueryRow(ctx, `
		SELECT COALESCE(SUM(quantity), 0), COALESCE(SUM(reserved), 0)
		FROM stock_units
		WHERE bin_id = $1 AND sku = $2 AND state NOT IN ($3, $4)
	`, bin.String(), sku.String(), string(stock.StateUnlocated), string(stock.StateRemoved)).Scan(&onHand, &reserved); err != nil {
		return shared.BinLevel{}, fmt.Errorf("postgres: read bin level: %w", err)
	}
	level.OnHand = shared.Quantity(onHand)
	level.Reserved = shared.Quantity(reserved)
	return level, nil
}

var _ ports.BinLevelLedger = (*BinLevelLedger)(nil)
