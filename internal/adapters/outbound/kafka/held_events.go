package kafka

import (
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// The three held-stock events (v1, ADR 0052), on `stock` like the sibling
// receipt/stow facts: com.warehouse.wms.inventory-storage.stock.<Event>. Key =
// subject = held unit id, so the hold and every later decision of one unit
// stay on one partition, in order (ADR 0021). Optional fields are omitted when
// empty, so a fleet-owned hold carries no owner_ref. The analytics topic
// carries the same data byte for byte (heldAnalyticsEvent).

type stockHeldData struct {
	HeldUnitID string    `json:"held_unit_id"`
	SKU        string    `json:"sku"`
	Quantity   int       `json:"quantity"`
	Reason     string    `json:"reason"`
	ReceiptID  string    `json:"receipt_id"`
	ASNNumber  string    `json:"asn_number"`
	LineNo     int       `json:"line_no"`
	OwnerRef   string    `json:"owner_ref,omitempty"`
	SSCC       string    `json:"sscc,omitempty"`
	HeldAt     time.Time `json:"held_at"`
}

type stockReleasedData struct {
	HeldUnitID string    `json:"held_unit_id"`
	SKU        string    `json:"sku"`
	Quantity   int       `json:"quantity"`
	ReceiptID  string    `json:"receipt_id"`
	ASNNumber  string    `json:"asn_number"`
	LineNo     int       `json:"line_no"`
	ReleasedAt time.Time `json:"released_at"`
}

type stockDispositionedData struct {
	HeldUnitID  string    `json:"held_unit_id"`
	SKU         string    `json:"sku"`
	Quantity    int       `json:"quantity"`
	Disposition string    `json:"disposition"`
	Reason      string    `json:"reason"`
	ReceiptID   string    `json:"receipt_id"`
	ASNNumber   string    `json:"asn_number"`
	LineNo      int       `json:"line_no"`
	DecidedAt   time.Time `json:"decided_at"`
}

// heldEventData maps a held-stock event onto its wire data and held unit id;
// any other event is outside the contract (nil data).
func heldEventData(event shared.DomainEvent) (data any, heldUnitID string) {
	switch e := event.(type) {
	case shared.StockHeld:
		return stockHeldData{
			HeldUnitID: e.HeldUnitID, SKU: e.SKU.String(), Quantity: e.Quantity.Int(), Reason: e.Reason, ReceiptID: e.ReceiptID,
			ASNNumber: e.ASNNumber, LineNo: e.LineNo, OwnerRef: e.OwnerRef.String(), SSCC: e.SSCC, HeldAt: e.OccurredAt().UTC(),
		}, e.HeldUnitID
	case shared.StockReleased:
		return stockReleasedData{
			HeldUnitID: e.HeldUnitID, SKU: e.SKU.String(), Quantity: e.Quantity.Int(), ReceiptID: e.ReceiptID,
			ASNNumber: e.ASNNumber, LineNo: e.LineNo, ReleasedAt: e.OccurredAt().UTC(),
		}, e.HeldUnitID
	case shared.StockDispositioned:
		return stockDispositionedData{
			HeldUnitID: e.HeldUnitID, SKU: e.SKU.String(), Quantity: e.Quantity.Int(), Disposition: e.Disposition, Reason: e.Reason,
			ReceiptID: e.ReceiptID, ASNNumber: e.ASNNumber, LineNo: e.LineNo, DecidedAt: e.OccurredAt().UTC(),
		}, e.HeldUnitID
	}
	return nil, ""
}

// encodeHeldEvent is the integration-topic mapping: data, Kafka key, entity.
func encodeHeldEvent(event shared.DomainEvent) (data any, key, entity string, err error) {
	data, id := heldEventData(event)
	if data == nil {
		return nil, "", "", nil
	}
	return data, id, stockEntity, nil
}

// heldAnalyticsEvent is the analytics-topic mapping: the same type, key and
// data as the integration topic. The projector acknowledges the types it does
// not fold, so a release shows in the flow report through the StockReceived it
// raises, and no report changes.
func heldAnalyticsEvent(event shared.DomainEvent) (analyticsEvent, bool) {
	data, id := heldEventData(event)
	if data == nil {
		return analyticsEvent{}, false
	}
	return analyticsEvent{entity: stockEntity, subject: id, key: id, data: data}, true
}
