package kafka

import (
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// toteEntity is the `<entity>` segment of the tote events
// (com.warehouse.wms.inventory-storage.tote.<Event>, ADR 0054).
const toteEntity = "tote"

// The three tote events (v1). Key = subject = lpn, so every fact about one
// tote stays on one partition, in order (ADR 0021).

type toteRegisteredData struct {
	LPN string    `json:"lpn"`
	At  time.Time `json:"at"`
}

type toteFilledData struct {
	LPN         string    `json:"lpn"`
	SKU         string    `json:"sku"`
	QtyEaches   int       `json:"qty_eaches"`
	OwnerRef    string    `json:"owner_ref,omitempty"`
	SiteID      string    `json:"site_id"`
	Bin         string    `json:"bin"`
	StockUnitID string    `json:"stock_unit_id"`
	At          time.Time `json:"at"`
}

type toteLineData struct {
	SKU       string `json:"sku"`
	QtyEaches int    `json:"qty_eaches"`
	OwnerRef  string `json:"owner_ref,omitempty"`
}

type toteStowedData struct {
	LPN      string         `json:"lpn"`
	SiteID   string         `json:"site_id"`
	FromBin  string         `json:"from_bin,omitempty"`
	Bin      string         `json:"bin"`
	Contents []toteLineData `json:"contents"`
	At       time.Time      `json:"at"`
}

// encodeToteEvent maps a tote event onto its wire payload, Kafka key and
// entity segment, and hands every other event to the next encoder.
func encodeToteEvent(event shared.DomainEvent) (data any, key, entity string, err error) {
	switch e := event.(type) {
	case shared.ToteRegistered:
		return toteRegisteredData{LPN: e.LPN, At: e.OccurredAt().UTC()}, e.LPN, toteEntity, nil
	case shared.ToteFilled:
		return toteFilledData{
			LPN: e.LPN, SKU: e.SKU.String(), QtyEaches: e.Quantity.Int(), OwnerRef: e.OwnerRef.String(),
			SiteID: e.SiteID.String(), Bin: e.BinID.String(), StockUnitID: e.StockUnitID, At: e.OccurredAt().UTC(),
		}, e.LPN, toteEntity, nil
	case shared.ToteStowed:
		lines := make([]toteLineData, 0, len(e.Contents))
		for _, l := range e.Contents {
			lines = append(lines, toteLineData{SKU: l.SKU.String(), QtyEaches: l.Quantity.Int(), OwnerRef: l.OwnerRef.String()})
		}
		return toteStowedData{
			LPN: e.LPN, SiteID: e.SiteID.String(), FromBin: e.FromBin.String(), Bin: e.ToBin.String(), Contents: lines, At: e.OccurredAt().UTC(),
		}, e.LPN, toteEntity, nil
	}
	return encodeHeldEvent(event)
}
