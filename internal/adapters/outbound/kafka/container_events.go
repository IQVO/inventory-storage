package kafka

import (
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// containerEntity is the `<entity>` segment of the container events
// (com.warehouse.wms.inventory-storage.container.<Event>, ADR 0045).
const containerEntity = "container"

// The four container events (v1, ADR 0045 and the amending ADR 0046). Every
// optional field is omitted when empty, so a fleet-owned container carries no
// owner_ref. Key = subject = lpn: every fact about one container stays on one
// partition, in order (ADR 0021).

type containerRegisteredData struct {
	LPN          string    `json:"lpn"`
	SKU          string    `json:"sku"`
	GTIN         string    `json:"gtin"`
	UnitsPerPack int       `json:"units_per_pack"`
	QtyEaches    int       `json:"qty_eaches"`
	OwnerRef     string    `json:"owner_ref,omitempty"`
	Lot          string    `json:"lot,omitempty"`
	Expiry       string    `json:"expiry,omitempty"`
	ReceiptID    string    `json:"receipt_id"`
	LineNo       int       `json:"line_no"`
	At           time.Time `json:"at"`
}

type containerStowedData struct {
	LPN       string    `json:"lpn"`
	SKU       string    `json:"sku"`
	QtyEaches int       `json:"qty_eaches"`
	OwnerRef  string    `json:"owner_ref,omitempty"`
	SiteID    string    `json:"site_id"`
	Bin       string    `json:"bin"`
	At        time.Time `json:"at"`
}

type caseBrokenData struct {
	LPN       string    `json:"lpn"`
	SKU       string    `json:"sku"`
	QtyEaches int       `json:"qty_eaches"`
	OwnerRef  string    `json:"owner_ref,omitempty"`
	FromBin   string    `json:"from_bin"`
	LooseBin  string    `json:"loose_bin"`
	At        time.Time `json:"at"`
}

type containerEmptiedData struct {
	LPN      string    `json:"lpn"`
	SKU      string    `json:"sku"`
	OwnerRef string    `json:"owner_ref,omitempty"`
	SiteID   string    `json:"site_id"`
	Reason   string    `json:"reason"`
	At       time.Time `json:"at"`
}

// encodeContainerEvent maps a container event onto its wire data payload,
// Kafka key and entity segment. Any other event is outside this publisher's
// contract (nil data, nil error).
func encodeContainerEvent(event shared.DomainEvent) (data any, key, entity string, err error) {
	switch e := event.(type) {
	case shared.ContainerRegistered:
		return containerRegisteredData{
			LPN: e.LPN, SKU: e.SKU.String(), GTIN: e.GTIN, UnitsPerPack: e.UnitsPerPack, QtyEaches: e.Quantity.Int(),
			OwnerRef: e.OwnerRef.String(), Lot: e.Lot, Expiry: e.Expiry, ReceiptID: e.ReceiptID, LineNo: e.LineNo, At: e.OccurredAt().UTC(),
		}, e.LPN, containerEntity, nil
	case shared.ContainerStowed:
		return containerStowedData{
			LPN: e.LPN, SKU: e.SKU.String(), QtyEaches: e.Quantity.Int(), OwnerRef: e.OwnerRef.String(),
			SiteID: e.SiteID.String(), Bin: e.BinID.String(), At: e.OccurredAt().UTC(),
		}, e.LPN, containerEntity, nil
	case shared.CaseBroken:
		return caseBrokenData{
			LPN: e.LPN, SKU: e.SKU.String(), QtyEaches: e.Quantity.Int(), OwnerRef: e.OwnerRef.String(),
			FromBin: e.FromBin.String(), LooseBin: e.LooseBin.String(), At: e.OccurredAt().UTC(),
		}, e.LPN, containerEntity, nil
	case shared.ContainerEmptied:
		return containerEmptiedData{
			LPN: e.LPN, SKU: e.SKU.String(), OwnerRef: e.OwnerRef.String(), SiteID: e.SiteID.String(), Reason: e.Reason, At: e.OccurredAt().UTC(),
		}, e.LPN, containerEntity, nil
	}
	return nil, "", "", nil
}
