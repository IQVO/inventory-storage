package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// TransferAllocationRepo is the pgxpool-backed implementation of
// ports.TransferAllocationRepo over the transfer_allocations ledger
// table (DB-unique on transfer_line_id — the idempotency anchor).
type TransferAllocationRepo struct {
	pool *pgxpool.Pool
}

func NewTransferAllocationRepo(pool *pgxpool.Pool) *TransferAllocationRepo {
	return &TransferAllocationRepo{pool: pool}
}

// Save inserts one decided ledger row. A row already existing for the
// same transfer_line_id fails with usecases.ErrTransferLineAlreadyDecided
// (unique constraint 23505 mapped) so the use case can route the replay
// to the return-the-original-outcome path. Runs through querierFrom, so
// it joins the caller's UnitOfWork transaction.
func (r *TransferAllocationRepo) Save(ctx context.Context, a *transfer.Allocation) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO transfer_allocations
			(transfer_id, transfer_line_id, origin_site_id, sku, requested_quantity, reservation_id, outcome, rejection_reason, decided_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, NULLIF($8, ''), $9, now(), now())
	`, a.TransferID(), a.TransferLineID(), a.OriginSiteID().String(), a.SKU().String(), a.RequestedQuantity().Int(), a.ReservationID(), string(a.Outcome()), string(a.RejectionReason()), a.DecidedAt())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return usecases.ErrTransferLineAlreadyDecided
		}
		return err
	}
	return nil
}

// FindByTransferLineID returns the decided row for that line, or nil when
// the line has never been decided.
func (r *TransferAllocationRepo) FindByTransferLineID(ctx context.Context, transferLineID string) (*transfer.Allocation, error) {
	var (
		transferID, sku, outcome string
		originSite               string
		requestedQuantity        int
		reservationID, reason    *string
		decidedAt                time.Time
	)
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT transfer_id, origin_site_id, sku, requested_quantity, reservation_id, outcome, rejection_reason, decided_at
		FROM transfer_allocations
		WHERE transfer_line_id = $1
	`, transferLineID).Scan(&transferID, &originSite, &sku, &requestedQuantity, &reservationID, &outcome, &reason, &decidedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	site, err := shared.NewSiteID(originSite)
	if err != nil {
		return nil, err
	}
	skuVO, err := shared.NewSKU(sku)
	if err != nil {
		return nil, err
	}
	qty, err := rehydrateQuantity("requested_quantity", requestedQuantity)
	if err != nil {
		return nil, err
	}
	resID := ""
	if reservationID != nil {
		resID = *reservationID
	}
	reasonStr := ""
	if reason != nil {
		reasonStr = *reason
	}
	return transfer.Rehydrate(transferID, transferLineID, site, skuVO, qty, resID, transfer.Outcome(outcome), transfer.RejectionReason(reasonStr), decidedAt), nil
}

// Compile-time assertion that TransferAllocationRepo satisfies the port.
var _ ports.TransferAllocationRepo = (*TransferAllocationRepo)(nil)
