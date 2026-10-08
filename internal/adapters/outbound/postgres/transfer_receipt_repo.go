package postgres

import (
	"context"
	"encoding/json"
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

// TransferReceiptRepo is the pgxpool-backed implementation of
// ports.TransferReceiptRepo over the transfer_receipts table (ADR 0031).
// The DB-UNIQUE transfer_line_id is the idempotency anchor for BOTH the
// stage and the stow step; the STAGED->STOWED state guard is enforced by
// the UPDATE's WHERE clause plus the table's CHECK constraints.
type TransferReceiptRepo struct {
	pool *pgxpool.Pool
}

func NewTransferReceiptRepo(pool *pgxpool.Pool) *TransferReceiptRepo {
	return &TransferReceiptRepo{pool: pool}
}

// Save inserts one STAGED receipt. A row already existing for the same
// transfer_line_id fails with usecases.ErrTransferReceiptAlreadyStaged
// (unique constraint 23505 mapped). Runs through querierFrom, so it
// joins the caller's UnitOfWork transaction.
func (r *TransferReceiptRepo) Save(ctx context.Context, receipt *transfer.Receipt) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO transfer_receipts
			(transfer_id, transfer_line_id, destination_site_id, sku, reservation_id, expected_quantity, received_quantity, variance, state, staged_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'STAGED', $9, now(), now())
	`, receipt.TransferID(), receipt.TransferLineID(), receipt.DestinationSiteID().String(), receipt.SKU().String(),
		receipt.ReservationID(), receipt.ExpectedQuantity().Int(), receipt.ReceivedQuantity().Int(), receipt.Variance(), receipt.StagedAt())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return usecases.ErrTransferReceiptAlreadyStaged
		}
		return err
	}
	return nil
}

// stowLegDTO is the JSONB payload of transfer_receipts.stow_legs — an
// adapter-owned DTO (the domain carries no wire tags; see the
// architecture fitness test). snake_case matches the TransferStockStowed
// event's allocations shape.
type stowLegDTO struct {
	StockUnitID string `json:"stock_unit_id"`
	BinID       string `json:"bin_id"`
	Quantity    int    `json:"quantity"`
}

// encodeStowLegs maps the receipt's stow legs onto the JSONB payload.
func encodeStowLegs(legs []transfer.StowLeg) ([]byte, error) {
	if len(legs) == 0 {
		return nil, nil
	}
	out := make([]stowLegDTO, 0, len(legs))
	for _, leg := range legs {
		out = append(out, stowLegDTO{StockUnitID: leg.StockUnitID, BinID: leg.BinID.String(), Quantity: leg.Quantity.Int()})
	}
	return json.Marshal(out)
}

// decodeStowLegs parses the JSONB payload back into domain legs.
func decodeStowLegs(raw []byte) ([]transfer.StowLeg, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var dtos []stowLegDTO
	if err := json.Unmarshal(raw, &dtos); err != nil {
		return nil, err
	}
	legs := make([]transfer.StowLeg, 0, len(dtos))
	for _, dto := range dtos {
		binID, err := shared.NewBinId(dto.BinID)
		if err != nil {
			return nil, err
		}
		qty, err := shared.NewQuantity(dto.Quantity)
		if err != nil {
			return nil, err
		}
		legs = append(legs, transfer.StowLeg{StockUnitID: dto.StockUnitID, BinID: binID, Quantity: qty})
	}
	return legs, nil
}

// SaveStowed moves the receipt to its STOWED terminal state, recording
// the stow legs. The WHERE state = 'STAGED' guard makes a concurrent
// double-stow affect zero rows, which maps to
// usecases.ErrTransferReceiptAlreadyStowed (the loser is routed to the
// replay path by the use case).
func (r *TransferReceiptRepo) SaveStowed(ctx context.Context, receipt *transfer.Receipt) error {
	legs, err := encodeStowLegs(receipt.StowLegs())
	if err != nil {
		return err
	}
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		UPDATE transfer_receipts
		SET state = 'STOWED', stow_legs = $2, stowed_at = $3, updated_at = now()
		WHERE transfer_line_id = $1 AND state = 'STAGED'
	`, receipt.TransferLineID(), legs, receipt.StowedAt())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return usecases.ErrTransferReceiptAlreadyStowed
		}
		return err
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrTransferReceiptAlreadyStowed
	}
	return nil
}

// FindByTransferLineID returns the receipt for that line, or nil when
// no receipt has ever been staged for it.
func (r *TransferReceiptRepo) FindByTransferLineID(ctx context.Context, transferLineID string) (*transfer.Receipt, error) {
	var (
		transferID, sku, reservationID, state string
		destinationSite                       string
		expected, received, variance          int
		stowLegsRaw                           []byte
		stagedAt, stowedAt                    time.Time
		stowedAtOrNull                        *time.Time
	)
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT transfer_id, destination_site_id, sku, reservation_id, expected_quantity, received_quantity, variance, state, stow_legs, staged_at, stowed_at
		FROM transfer_receipts
		WHERE transfer_line_id = $1
	`, transferLineID).Scan(&transferID, &destinationSite, &sku, &reservationID, &expected, &received, &variance, &state, &stowLegsRaw, &stagedAt, &stowedAtOrNull)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	site, err := shared.NewSiteID(destinationSite)
	if err != nil {
		return nil, err
	}
	skuVO, err := shared.NewSKU(sku)
	if err != nil {
		return nil, err
	}
	expectedQty, err := rehydrateQuantity("expected_quantity", expected)
	if err != nil {
		return nil, err
	}
	receivedQty, err := rehydrateQuantity("received_quantity", received)
	if err != nil {
		return nil, err
	}
	if stowedAtOrNull != nil {
		stowedAt = *stowedAtOrNull
	}
	stowLegs, err := decodeStowLegs(stowLegsRaw)
	if err != nil {
		return nil, err
	}
	return transfer.RehydrateReceipt(transferID, transferLineID, site, skuVO, reservationID, expectedQty, receivedQty, variance, transfer.ReceiptState(state), stowLegs, stagedAt, stowedAt)
}

// Compile-time assertion that TransferReceiptRepo satisfies the port.
var _ ports.TransferReceiptRepo = (*TransferReceiptRepo)(nil)

// InventoryExceptionRepo is the pgxpool-backed implementation of
// ports.InventoryExceptionRepo over the inventory_exceptions table (ADR
// 0031). The UNIQUE (transfer_line_id, destination_site_id, sku,
// received_quantity) constraint makes a repeated identical scan a
// benign replay instead of a second quarantine row.
type InventoryExceptionRepo struct {
	pool *pgxpool.Pool
}

func NewInventoryExceptionRepo(pool *pgxpool.Pool) *InventoryExceptionRepo {
	return &InventoryExceptionRepo{pool: pool}
}

func (r *InventoryExceptionRepo) FindByScan(ctx context.Context, transferLineID string, destinationSiteID shared.SiteID, sku shared.SKU, receivedQty shared.Quantity) (*transfer.Exception, error) {
	var (
		transferID, destinationSite, skuStr, kind, detail string
		received                                          int
		createdAt                                         time.Time
	)
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT transfer_id, destination_site_id, sku, received_quantity, kind, detail, created_at
		FROM inventory_exceptions
		WHERE transfer_line_id = $1 AND destination_site_id = $2 AND sku = $3 AND received_quantity = $4
	`, transferLineID, destinationSiteID.String(), sku.String(), receivedQty.Int()).Scan(&transferID, &destinationSite, &skuStr, &received, &kind, &detail, &createdAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	site, err := shared.NewSiteID(destinationSite)
	if err != nil {
		return nil, err
	}
	skuVO, err := shared.NewSKU(skuStr)
	if err != nil {
		return nil, err
	}
	qty, err := rehydrateQuantity("received_quantity", received)
	if err != nil {
		return nil, err
	}
	return rehydrateException(transferID, transferLineID, site, skuVO, qty, kind, detail, createdAt), nil
}

func (r *InventoryExceptionRepo) Save(ctx context.Context, e *transfer.Exception) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO inventory_exceptions
			(transfer_id, transfer_line_id, destination_site_id, sku, received_quantity, kind, detail, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, e.TransferID(), e.TransferLineID(), e.DestinationSiteID().String(), e.SKU().String(), e.ReceivedQuantity().Int(), string(e.Kind()), e.Detail(), e.CreatedAt())
	return err
}

// rehydrateException rebuilds an Exception from its row without
// re-running creation invariants beyond the value objects.
func rehydrateException(transferID, transferLineID string, site shared.SiteID, sku shared.SKU, qty shared.Quantity, kind, detail string, createdAt time.Time) *transfer.Exception {
	return transfer.RehydrateException(transferID, transferLineID, site, sku, qty, transfer.ExceptionKind(kind), detail, createdAt)
}

// Compile-time assertion that InventoryExceptionRepo satisfies the port.
var _ ports.InventoryExceptionRepo = (*InventoryExceptionRepo)(nil)
