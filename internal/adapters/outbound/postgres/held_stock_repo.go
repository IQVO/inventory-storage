package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// HeldStockRepo is the pgxpool-backed ports.HeldStockRepo over held_stock and
// held_stock_carton_marks (ADR 0052). Every statement runs through
// querierFrom, so inside a UnitOfWork it joins the caller's transaction.
type HeldStockRepo struct {
	pool *pgxpool.Pool
}

func NewHeldStockRepo(pool *pgxpool.Pool) *HeldStockRepo { return &HeldStockRepo{pool: pool} }

// heldColumns is the SELECT list scanHeld reads; the last column aggregates
// the LPNs of the Damaged cartons marked on the unit's receipt line.
const heldColumns = `h.id, h.receipt_id, h.asn_number, h.line_no, h.sku, h.quantity, COALESCE(h.owner_ref, ''), COALESCE(h.sscc, ''),
	h.reason, h.state, COALESCE(h.disposition, ''), h.rework_count, h.held_at, h.decided_at, h.version,
	COALESCE((SELECT array_agg(m.lpn ORDER BY m.marked_at, m.lpn) FROM held_stock_carton_marks m
		WHERE m.receipt_id = h.receipt_id AND m.line_no = h.line_no), '{}')`

// Save inserts a new held unit, or updates an existing one version-guarded
// (ADR 0019): the conflict action's WHERE only fires while the row still
// carries the version the unit was loaded at, so a concurrent decision leaves
// RowsAffected at 0 and the save fails with ErrConcurrentModification. Only
// the decision fields change on update; the source never does.
func (r *HeldStockRepo) Save(ctx context.Context, h *stock.HeldUnit) error {
	var decidedAt *time.Time
	if !h.DecidedAt().IsZero() {
		d := h.DecidedAt().UTC()
		decidedAt = &d
	}
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO held_stock (id, receipt_id, asn_number, line_no, sku, quantity, owner_ref, sscc, reason, state,
			disposition, rework_count, held_at, decided_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), NULLIF($8, ''), $9, $10, NULLIF($11, ''), $12, $13, $14, $15)
		ON CONFLICT (id) DO UPDATE SET
			reason = EXCLUDED.reason, state = EXCLUDED.state, disposition = EXCLUDED.disposition,
			rework_count = EXCLUDED.rework_count, decided_at = EXCLUDED.decided_at,
			version = held_stock.version + 1, updated_at = now()
		WHERE held_stock.version = $15
	`, h.ID(), h.ReceiptID(), h.ASNNumber(), h.LineNo(), h.SKU().String(), h.Quantity().Int(), h.OwnerRef().String(), h.SSCC(),
		string(h.Reason()), string(h.State()), string(h.Disposition()), h.ReworkCount(), h.HeldAt().UTC(), decidedAt, h.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrConcurrentModification
	}
	return nil
}

func (r *HeldStockRepo) FindByID(ctx context.Context, id string) (*stock.HeldUnit, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `SELECT `+heldColumns+` FROM held_stock h WHERE h.id = $1`, id)
	h, err := scanHeld(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return h, err
}

// ListHeld returns the units still HELD matching q in (held_at, id) order.
func (r *HeldStockRepo) ListHeld(ctx context.Context, q ports.HeldStockQuery) ([]*stock.HeldUnit, error) {
	where := []string{"h.state = 'HELD'"}
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if q.Reason != "" {
		add("h.reason = ?", string(q.Reason))
	}
	if q.SKU != "" {
		add("h.sku = ?", q.SKU.String())
	}
	if !q.HeldBefore.IsZero() {
		add("h.held_at < ?", q.HeldBefore.UTC())
	}
	if q.After != nil {
		args = append(args, q.After.HeldAt.UTC(), q.After.ID)
		where = append(where, fmt.Sprintf("(h.held_at, h.id) > ($%d, $%d)", len(args)-1, len(args)))
	}
	sql := `SELECT ` + heldColumns + ` FROM held_stock h WHERE ` + strings.Join(where, " AND ") + ` ORDER BY h.held_at, h.id`
	if q.Limit > 0 {
		args = append(args, q.Limit)
		sql += " LIMIT $" + strconv.Itoa(len(args))
	}
	rows, err := querierFrom(ctx, r.pool).Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*stock.HeldUnit
	for rows.Next() {
		h, err := scanHeld(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// MarkCarton records the LPN of a Damaged carton content on (receipt, line);
// a mark already recorded is left untouched.
func (r *HeldStockRepo) MarkCarton(ctx context.Context, receiptID string, lineNo int, lpn, sscc string) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO held_stock_carton_marks (receipt_id, line_no, lpn, sscc) VALUES ($1, $2, $3, NULLIF($4, ''))
		ON CONFLICT (receipt_id, line_no, lpn) DO NOTHING
	`, receiptID, lineNo, lpn, sscc)
	return err
}

func (r *HeldStockRepo) NextID(context.Context) (string, error) {
	return "hu-" + uuid.NewString(), nil
}

// scanHeld rebuilds a HeldUnit, failing on a row that violates a domain
// invariant instead of embedding a zero value (see rehydrate.go).
func scanHeld(row rowScanner) (*stock.HeldUnit, error) {
	var (
		id, receiptID, asn, skuStr, owner, sscc, reason, state, disposition string
		lineNo, quantity, reworkCount, version                              int
		heldAt                                                              time.Time
		decidedAt                                                           *time.Time
		lpns                                                                []string
	)
	if err := row.Scan(&id, &receiptID, &asn, &lineNo, &skuStr, &quantity, &owner, &sscc, &reason, &state, &disposition,
		&reworkCount, &heldAt, &decidedAt, &version, &lpns); err != nil {
		return nil, err
	}
	sku, err := rehydrateSKU(skuStr)
	if err != nil {
		return nil, fmt.Errorf("held unit %q: %w", id, err)
	}
	qty, err := rehydrateQuantity("quantity", quantity)
	if err != nil {
		return nil, fmt.Errorf("held unit %q: %w", id, err)
	}
	rec := stock.HeldRecord{
		ID: id, Reason: stock.HoldReason(reason), State: stock.State(state), Disposition: stock.Disposition(disposition),
		HeldAt: heldAt.UTC(), ReworkCount: reworkCount, Version: version, LPNs: lpns,
		Source: stock.HeldSource{
			ReceiptID: receiptID, ASNNumber: asn, LineNo: lineNo, SKU: sku, Quantity: qty, OwnerRef: shared.OwnerRef(owner), SSCC: sscc,
		},
	}
	if decidedAt != nil {
		rec.DecidedAt = decidedAt.UTC()
	}
	return stock.RehydrateHeldUnit(rec), nil
}

var _ ports.HeldStockRepo = (*HeldStockRepo)(nil)
