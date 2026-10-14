package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// RelocationRepo is the pgxpool-backed ports.RelocationRepo over the
// relocations table (ADR 0039, migration 0036). Save joins the caller's
// UnitOfWork through querierFrom, so the row commits with the move.
type RelocationRepo struct {
	pool *pgxpool.Pool
}

func NewRelocationRepo(pool *pgxpool.Pool) *RelocationRepo {
	return &RelocationRepo{pool: pool}
}

// relocationLegDTO is the JSONB shape of relocations.legs — adapter-owned
// (the domain carries no wire tags).
type relocationLegDTO struct {
	StockUnitID string `json:"stock_unit_id"`
	Quantity    int    `json:"quantity"`
}

// Save inserts the applied relocation. A relocation is immutable: saving the
// same id twice fails on the primary key.
func (r *RelocationRepo) Save(ctx context.Context, rel *relocation.Relocation) error {
	legs := make([]relocationLegDTO, 0, len(rel.Legs()))
	for _, leg := range rel.Legs() {
		legs = append(legs, relocationLegDTO{StockUnitID: leg.StockUnitID, Quantity: leg.Quantity.Int()})
	}
	raw, err := json.Marshal(legs)
	if err != nil {
		return err
	}
	_, err = querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO relocations
			(id, site_code, sku, from_bin, to_bin, quantity, reason, source_ref, legs, destination_stock_unit_id, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, rel.ID(), rel.SiteID().String(), rel.SKU().String(), rel.FromBin().String(), rel.ToBin().String(),
		rel.Quantity().Int(), string(rel.Reason()), rel.SourceRef(), raw, rel.DestinationStockUnitID(), rel.OccurredAt())
	return err
}

// FindByID returns the relocation, or nil when the id is unknown.
func (r *RelocationRepo) FindByID(ctx context.Context, id string) (*relocation.Relocation, error) {
	var (
		site, sku, from, to, reason, sourceRef, destID string
		qty                                            int
		legsRaw                                        []byte
		occurredAt                                     time.Time
	)
	err := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT site_code, sku, from_bin, to_bin, quantity, reason, source_ref, legs, destination_stock_unit_id, occurred_at
		FROM relocations WHERE id = $1
	`, id).Scan(&site, &sku, &from, &to, &qty, &reason, &sourceRef, &legsRaw, &destID, &occurredAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	spec, err := relocationSpec(id, site, sku, from, to, qty, reason, sourceRef)
	if err != nil {
		return nil, fmt.Errorf("relocation %q: %w", id, err)
	}
	legs, err := decodeRelocationLegs(legsRaw)
	if err != nil {
		return nil, fmt.Errorf("relocation %q: %w", id, err)
	}
	return relocation.Rehydrate(spec, legs, destID, occurredAt.UTC()), nil
}

func relocationSpec(id, site, sku, from, to string, qty int, reason, sourceRef string) (relocation.Spec, error) {
	siteID, err := shared.NewSiteID(site)
	if err != nil {
		return relocation.Spec{}, err
	}
	skuVO, err := rehydrateSKU(sku)
	if err != nil {
		return relocation.Spec{}, err
	}
	fromBin, err := rehydrateBinID(from)
	if err != nil {
		return relocation.Spec{}, err
	}
	toBin, err := rehydrateBinID(to)
	if err != nil {
		return relocation.Spec{}, err
	}
	quantity, err := rehydrateQuantity("quantity", qty)
	if err != nil {
		return relocation.Spec{}, err
	}
	return relocation.Spec{
		ID: id, SiteID: siteID, SKU: skuVO, FromBin: fromBin, ToBin: toBin,
		Quantity: quantity, Reason: relocation.Reason(reason), SourceRef: sourceRef,
	}, nil
}

func decodeRelocationLegs(raw []byte) ([]relocation.Leg, error) {
	var dtos []relocationLegDTO
	if err := json.Unmarshal(raw, &dtos); err != nil {
		return nil, err
	}
	legs := make([]relocation.Leg, 0, len(dtos))
	for _, dto := range dtos {
		qty, err := rehydrateQuantity("leg quantity", dto.Quantity)
		if err != nil {
			return nil, err
		}
		legs = append(legs, relocation.Leg{StockUnitID: dto.StockUnitID, Quantity: qty})
	}
	return legs, nil
}

// NextID mints a relocation id.
func (r *RelocationRepo) NextID(_ context.Context) (string, error) {
	return "rel-" + uuid.NewString(), nil
}

// Compile-time assertion that RelocationRepo satisfies the port.
var _ ports.RelocationRepo = (*RelocationRepo)(nil)
