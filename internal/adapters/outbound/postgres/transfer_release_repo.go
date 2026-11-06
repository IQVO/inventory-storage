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

// TransferReleaseRepo is the pgxpool-backed implementation of
// ports.TransferReleaseRepo over the transfer_allocation_releases ledger
// table (DB-unique on transfer_line_id — the idempotency anchor of the
// release leg, ADR 0051).
type TransferReleaseRepo struct {
	pool *pgxpool.Pool
}

func NewTransferReleaseRepo(pool *pgxpool.Pool) *TransferReleaseRepo {
	return &TransferReleaseRepo{pool: pool}
}

// Save inserts one decided release ledger row. A row already existing
// for the same transfer_line_id fails with
// usecases.ErrTransferReleaseAlreadyDecided (unique constraint 23505
// mapped) so the use case can route the replay to the
// return-the-original-outcome path. Runs through querierFrom, so it
// joins the caller's UnitOfWork transaction.
func (r *TransferReleaseRepo) Save(ctx context.Context, rel *transfer.Release) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO transfer_allocation_releases
			(transfer_id, transfer_line_id, reservation_id, origin_site_id, sku, quantity, outcome, reason, decided_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())
	`, rel.TransferID(), rel.TransferLineID(), rel.ReservationID(), rel.OriginSiteID().String(), rel.SKU().String(), rel.Quantity().Int(), string(rel.Outcome()), rel.Reason(), rel.DecidedAt())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return usecases.ErrTransferReleaseAlreadyDecided
		}
		return err
	}
	return nil
}

// FindByTransferLineID returns the decided release row for that line, or
// nil when the line's allocation has never been released.
func (r *TransferReleaseRepo) FindByTransferLineID(ctx context.Context, transferLineID string) (*transfer.Release, error) {
	var (
		transferID, reservationID, originSite, sku, outcome, reason string
		quantity                                                    int
		decidedAt                                                   time.Time
	)
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT transfer_id, reservation_id, origin_site_id, sku, quantity, outcome, reason, decided_at
		FROM transfer_allocation_releases
		WHERE transfer_line_id = $1
	`, transferLineID).Scan(&transferID, &reservationID, &originSite, &sku, &quantity, &outcome, &reason, &decidedAt)
	if errors.Is(err, pgx.ErrNoRows) {
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
	qty, err := rehydrateQuantity("quantity", quantity)
	if err != nil {
		return nil, err
	}
	return transfer.RehydrateRelease(transferID, transferLineID, reservationID, site, skuVO, qty, transfer.ReleaseOutcome(outcome), reason, decidedAt), nil
}

// Compile-time assertion that TransferReleaseRepo satisfies the port.
var _ ports.TransferReleaseRepo = (*TransferReleaseRepo)(nil)
