package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/shared/gs1"
)

// ReceiptBookingRepo is the pgxpool-backed ports.ReceiptBookingRepo over
// receipt_line_bookings (migration 0038).
type ReceiptBookingRepo struct {
	pool *pgxpool.Pool
}

func NewReceiptBookingRepo(pool *pgxpool.Pool) *ReceiptBookingRepo {
	return &ReceiptBookingRepo{pool: pool}
}

// AddBooking adds qty to the line's booked eaches in the caller's transaction.
func (r *ReceiptBookingRepo) AddBooking(ctx context.Context, receiptID string, lineNo int, sku shared.SKU, owner shared.OwnerRef, qty shared.Quantity) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO receipt_line_bookings (receipt_id, line_no, sku, owner_ref, booked_eaches)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5)
		ON CONFLICT (receipt_id, line_no) DO UPDATE SET
			booked_eaches = receipt_line_bookings.booked_eaches + EXCLUDED.booked_eaches, updated_at = now()
	`, receiptID, lineNo, sku.String(), owner.String(), qty.Int())
	return err
}

func (r *ReceiptBookingRepo) Booked(ctx context.Context, receiptID string, lineNo int) (shared.Quantity, error) {
	var booked int
	err := querierFrom(ctx, r.pool).QueryRow(ctx,
		`SELECT booked_eaches FROM receipt_line_bookings WHERE receipt_id = $1 AND line_no = $2`, receiptID, lineNo).Scan(&booked)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return rehydrateQuantity("booked_eaches", booked)
}

// PendingLinkRepo is the pgxpool-backed ports.PendingLinkRepo over
// container_pending_links (migration 0038).
type PendingLinkRepo struct {
	pool *pgxpool.Pool
}

func NewPendingLinkRepo(pool *pgxpool.Pool) *PendingLinkRepo { return &PendingLinkRepo{pool: pool} }

func (r *PendingLinkRepo) Add(ctx context.Context, l container.PendingLink) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO container_pending_links (receipt_id, line_no, lpn, sscc, parent_sscc, sku, gtin, qty_eaches, owner_ref, lot, expiry)
		VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''))
		ON CONFLICT (receipt_id, line_no, lpn) DO NOTHING
	`, l.ReceiptID, l.LineNo, l.LPN, l.SSCC, l.ParentSSCC, l.SKU.String(), l.GTIN, l.Quantity.Int(), l.OwnerRef.String(), l.Lot, l.Expiry)
	return err
}

const pendingColumns = `receipt_id, line_no, lpn, COALESCE(sscc, ''), COALESCE(parent_sscc, ''), sku, gtin, qty_eaches,
	COALESCE(owner_ref, ''), COALESCE(lot, ''), COALESCE(expiry, ''), status, COALESCE(reason, '')`

func (r *PendingLinkRepo) FindPendingByLine(ctx context.Context, receiptID string, lineNo int) ([]container.PendingLink, error) {
	return r.find(ctx, `SELECT `+pendingColumns+` FROM container_pending_links
		WHERE receipt_id = $1 AND line_no = $2 AND status = 'PENDING' ORDER BY first_seen_at, lpn`, receiptID, lineNo)
}

func (r *PendingLinkRepo) FindPendingBySKU(ctx context.Context, sku shared.SKU) ([]container.PendingLink, error) {
	return r.find(ctx, `SELECT `+pendingColumns+` FROM container_pending_links
		WHERE sku = $1 AND status = 'PENDING' ORDER BY first_seen_at, lpn`, sku.String())
}

func (r *PendingLinkRepo) find(ctx context.Context, query string, args ...any) ([]container.PendingLink, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []container.PendingLink
	for rows.Next() {
		var l container.PendingLink
		var sku, owner string
		var qty int
		if err := rows.Scan(&l.ReceiptID, &l.LineNo, &l.LPN, &l.SSCC, &l.ParentSSCC, &sku, &l.GTIN, &qty, &owner, &l.Lot, &l.Expiry, &l.Status, &l.Reason); err != nil {
			return nil, err
		}
		l.SKU, err = rehydrateSKU(sku)
		if err != nil {
			return nil, err
		}
		l.Quantity, err = rehydrateQuantity("pending qty_eaches", qty)
		if err != nil {
			return nil, err
		}
		l.OwnerRef = shared.OwnerRef(owner)
		out = append(out, l)
	}
	return out, rows.Err()
}

func (r *PendingLinkRepo) Remove(ctx context.Context, receiptID string, lineNo int, lpn string) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx,
		`DELETE FROM container_pending_links WHERE receipt_id = $1 AND line_no = $2 AND lpn = $3`, receiptID, lineNo, lpn)
	return err
}

func (r *PendingLinkRepo) Reject(ctx context.Context, receiptID string, lineNo int, lpn, reason string) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		UPDATE container_pending_links SET status = 'REJECTED', reason = $4
		WHERE receipt_id = $1 AND line_no = $2 AND lpn = $3
	`, receiptID, lineNo, lpn, reason)
	return err
}

// PackagingViewRepo is the pgxpool-backed PackagingView (ports.PackagingViewRepo
// and ports.PackagingProjection) over pack_levels (migration 0038).
type PackagingViewRepo struct {
	pool *pgxpool.Pool
}

func NewPackagingViewRepo(pool *pgxpool.Pool) *PackagingViewRepo {
	return &PackagingViewRepo{pool: pool}
}

// ApplyDeclared upserts the level when p.Version is newer than the stored one.
// effective_to is never touched here: only a Retired event end-dates a level.
func (r *PackagingViewRepo) ApplyDeclared(ctx context.Context, p packaging.PackLevel) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO pack_levels (gtin, sku, level, units_per_pack, child_gtin, effective_from, version)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7)
		ON CONFLICT (gtin) DO UPDATE SET
			sku = EXCLUDED.sku, level = EXCLUDED.level, units_per_pack = EXCLUDED.units_per_pack,
			child_gtin = EXCLUDED.child_gtin, effective_from = EXCLUDED.effective_from,
			version = EXCLUDED.version, updated_at = now()
		WHERE pack_levels.version < EXCLUDED.version
	`, p.GTIN.String(), p.SKU.String(), string(p.Level), p.UnitsPerPack, p.ChildGTIN, p.EffectiveFrom, p.Version)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ApplyRetired end-dates gtin when version is not older than the stored one.
func (r *PackagingViewRepo) ApplyRetired(ctx context.Context, gtin string, effectiveTo time.Time, version int64) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		UPDATE pack_levels SET effective_to = $2, version = $3, updated_at = now()
		WHERE gtin = $1 AND version <= $3
	`, gtin, effectiveTo, version)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

const packLevelColumns = `gtin, sku, level, units_per_pack, COALESCE(child_gtin, ''), effective_from, effective_to, version`

func (r *PackagingViewRepo) FindByGTIN(ctx context.Context, gtin string) (*packaging.PackLevel, error) {
	return r.findOne(ctx, `SELECT `+packLevelColumns+` FROM pack_levels WHERE gtin = $1`, gtin)
}

func (r *PackagingViewRepo) FindActive(ctx context.Context, sku shared.SKU, level packaging.Level, at time.Time) (*packaging.PackLevel, error) {
	return r.findOne(ctx, `SELECT `+packLevelColumns+` FROM pack_levels
		WHERE sku = $1 AND level = $2 AND effective_from <= $3 AND (effective_to IS NULL OR effective_to > $3)
		ORDER BY effective_from DESC LIMIT 1`, sku.String(), string(level), at)
}

func (r *PackagingViewRepo) findOne(ctx context.Context, query string, args ...any) (*packaging.PackLevel, error) {
	var gtin, sku, level, child string
	var units int
	var from time.Time
	var to *time.Time
	var version int64
	err := querierFrom(ctx, r.pool).QueryRow(ctx, query, args...).Scan(&gtin, &sku, &level, &units, &child, &from, &to, &version)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	g, err := gs1.ParseGTIN(gtin)
	if err != nil {
		return nil, fmt.Errorf("pack level gtin %q: %w", gtin, err)
	}
	s, err := rehydrateSKU(sku)
	if err != nil {
		return nil, err
	}
	return &packaging.PackLevel{
		GTIN: g, SKU: s, Level: packaging.Level(level), UnitsPerPack: units, ChildGTIN: child,
		EffectiveFrom: from, EffectiveTo: to, Version: version,
	}, nil
}

var (
	_ ports.ReceiptBookingRepo  = (*ReceiptBookingRepo)(nil)
	_ ports.PendingLinkRepo     = (*PendingLinkRepo)(nil)
	_ ports.PackagingViewRepo   = (*PackagingViewRepo)(nil)
	_ ports.PackagingProjection = (*PackagingViewRepo)(nil)
)
