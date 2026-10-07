// Package http is the inbound chi adapter: DTOs, handlers, routing, and
// domain-error-to-HTTP-status mapping. Domain structs never cross this
// boundary.
package http

type receiveStockRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type stagedReceiptResponse struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	ReceivedAt string `json:"receivedAt"`
}

type stowStockRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	BinID    string `json:"binId"`
}

type stockUnitResponse struct {
	ID       string `json:"id"`
	SKU      string `json:"sku"`
	BinID    string `json:"binId"`
	Quantity int    `json:"quantity"`
	Reserved int    `json:"reserved"`
	State    string `json:"state"`
}

type reserveStockRequest struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demandRef"`
}

type allocationResponse struct {
	StockUnitID string `json:"stockUnitId"`
	// BinID is the pick location (the bin the allocated StockUnit sits
	// in). Omitted only for legacy allocations persisted before it was
	// recorded and that the 0008 migration could not backfill (ADR 0025).
	BinID    string `json:"binId,omitempty"`
	Quantity int    `json:"quantity"`
}

type reservationResponse struct {
	ID          string               `json:"id"`
	SKU         string               `json:"sku"`
	Quantity    int                  `json:"quantity"`
	DemandRef   string               `json:"demandRef"`
	Status      string               `json:"status"`
	Allocations []allocationResponse `json:"allocations"`
	CreatedAt   string               `json:"createdAt"`
	ExpiresAt   string               `json:"expiresAt"`
}

type usableInventoryResponse struct {
	SKU    string `json:"sku"`
	Usable int    `json:"usable"`
}

type cycleCountRequest struct {
	// CountedQuantity is a pointer so an omitted field is distinguishable
	// from an explicit zero count (zero is a valid "empty bin" count;
	// omitting the count entirely is a client mistake and must 400).
	CountedQuantity *int64 `json:"countedQuantity"`
}

type cycleCountResponse struct {
	BinID       string `json:"binId"`
	CountedQty  int    `json:"countedQuantity"`
	SystemQty   int    `json:"systemQuantity"`
	Discrepancy bool   `json:"discrepancy"`
}

type registerBinRequest struct {
	// Capacity is a pointer so an omitted field (400 capacity-required)
	// is distinguishable from an explicit 0 (422 invalid-bin-capacity).
	Capacity *int `json:"capacity"`
}

type binResponse struct {
	BinID     string `json:"binId"`
	Capacity  int    `json:"capacity"`
	Occupied  int    `json:"occupied"`
	Available int    `json:"available"`
}

type productClassificationResponse struct {
	SKU              string   `json:"sku"`
	HandlingTags     []string `json:"handlingTags"`
	TemperatureClass string   `json:"temperatureClass,omitempty"`
	// DOTHazardClass is omitted from the response entirely when
	// unspecified, rather than serialized as 0 — 0 is not a valid class.
	DOTHazardClass *int `json:"dotHazardClass,omitempty"`
}

// problemDetails is the RFC 7807 (Problem Details for HTTP APIs) response
// body used for every error response in this service.
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}
