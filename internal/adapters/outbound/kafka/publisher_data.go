package kafka

import (
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// reservationData is the `data` payload shape for StockReserved and
// ReservationRevoked, per CLAUDE.md.
type reservationData struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demand_ref"`
}

// transferAllocatedData is the `data` payload for
// TransferStockAllocated (v1, frozen).
type transferAllocatedData struct {
	TransferID     string                     `json:"transfer_id"`
	TransferLineID string                     `json:"transfer_line_id"`
	OriginSiteID   string                     `json:"origin_site_id"`
	ReservationID  string                     `json:"reservation_id"`
	SKU            string                     `json:"sku"`
	Quantity       int                        `json:"quantity"`
	Allocations    []transferAllocationLegOut `json:"allocations"`
	ExpiresAt      time.Time                  `json:"expires_at"`
}

type transferAllocationLegOut struct {
	StockUnitID string `json:"stock_unit_id"`
	BinID       string `json:"bin_id"`
	Quantity    int    `json:"quantity"`
}

// transferRejectedData is the `data` payload for
// TransferStockAllocationRejected (v1, frozen).
type transferRejectedData struct {
	TransferID        string `json:"transfer_id"`
	TransferLineID    string `json:"transfer_line_id"`
	OriginSiteID      string `json:"origin_site_id"`
	SKU               string `json:"sku"`
	RequestedQuantity int    `json:"requested_quantity"`
	Reason            string `json:"reason"`
}

// receiptStagedData is the `data` payload for TransferReceiptStaged
// (v1, frozen — ADR 0033).
type receiptStagedData struct {
	TransferID        string `json:"transfer_id"`
	TransferLineID    string `json:"transfer_line_id"`
	DestinationSiteID string `json:"destination_site_id"`
	SKU               string `json:"sku"`
	ExpectedQuantity  int    `json:"expected_quantity"`
	ReceivedQuantity  int    `json:"received_quantity"`
	Variance          int    `json:"variance"`
}

// stockStowedData is the `data` payload for TransferStockStowed
// (v1, frozen — ADR 0033).
type stockStowedData struct {
	TransferID        string                     `json:"transfer_id"`
	TransferLineID    string                     `json:"transfer_line_id"`
	DestinationSiteID string                     `json:"destination_site_id"`
	SKU               string                     `json:"sku"`
	ReceivedQuantity  int                        `json:"received_quantity"`
	StowedQuantity    int                        `json:"stowed_quantity"`
	Allocations       []transferAllocationLegOut `json:"allocations"`
}

// stockRelocatedData is the `data` payload for StockRelocated (v1, frozen —
// ADR 0039). source_ref is always present ("" when none).
type stockRelocatedData struct {
	RelocationID string    `json:"relocation_id"`
	SiteCode     string    `json:"site_code"`
	SKU          string    `json:"sku"`
	FromBin      string    `json:"from_bin"`
	ToBin        string    `json:"to_bin"`
	Quantity     int       `json:"quantity"`
	Reason       string    `json:"reason"`
	SourceRef    string    `json:"source_ref"`
	OccurredAt   time.Time `json:"occurred_at"`
}

func newStockRelocatedData(e shared.StockRelocated) stockRelocatedData {
	return stockRelocatedData{
		RelocationID: e.RelocationID,
		SiteCode:     e.SiteID.String(),
		SKU:          e.SKU.String(),
		FromBin:      e.FromBin.String(),
		ToBin:        e.ToBin.String(),
		Quantity:     e.Quantity.Int(),
		Reason:       e.Reason,
		SourceRef:    e.SourceRef,
		OccurredAt:   e.OccurredAt().UTC(),
	}
}

// binStockLevelChangedData is the `data` payload for BinStockLevelChanged
// (v1, frozen — ADR 0040). on_hand and reserved are ABSOLUTE levels after the
// change, version the per-(bin, sku) counter, reason one of STOW | PICK |
// RELOCATE | REPLENISHMENT | COUNT | RESERVE | REVOKE | UNLOCATED.
// pick-face-replenishment decodes exactly these field names.
type binStockLevelChangedData struct {
	SiteCode   string    `json:"site_code"`
	Bin        string    `json:"bin"`
	SKU        string    `json:"sku"`
	OnHand     int       `json:"on_hand"`
	Reserved   int       `json:"reserved"`
	Version    int64     `json:"version"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
	// OwnerRef is present only for owner-segregated stock (ADR 0045); absent
	// means fleet-owned stock and the payload is byte-identical to v1 without it.
	OwnerRef string `json:"owner_ref,omitempty"`
}

func newBinStockLevelChangedData(e shared.BinStockLevelChanged) binStockLevelChangedData {
	return binStockLevelChangedData{
		SiteCode:   e.SiteID.String(),
		Bin:        e.BinID.String(),
		SKU:        e.SKU.String(),
		OnHand:     e.OnHand.Int(),
		Reserved:   e.Reserved.Int(),
		Version:    e.Version,
		Reason:     string(e.Reason),
		OccurredAt: e.OccurredAt().UTC(),
		OwnerRef:   e.OwnerRef.String(),
	}
}

// binLevelKey is the Kafka key and CloudEvents subject of a
// BinStockLevelChanged: "<bin>:<sku>", so every level of one (bin, sku)
// lands on one partition in version order (ADR 0021, ADR 0040).
func binLevelKey(e shared.BinStockLevelChanged) string {
	return e.BinID.String() + ":" + e.SKU.String()
}

// transferLegsOut maps domain allocation legs onto their wire shape.
func transferLegsOut(legs []shared.TransferAllocationLeg) []transferAllocationLegOut {
	out := make([]transferAllocationLegOut, 0, len(legs))
	for _, leg := range legs {
		out = append(out, transferAllocationLegOut{StockUnitID: leg.StockUnitID, BinID: leg.BinID.String(), Quantity: leg.Quantity.Int()})
	}
	return out
}
