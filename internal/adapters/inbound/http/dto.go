// Package http is the inbound chi adapter: DTOs, handlers, routing, and
// domain-error-to-HTTP-status mapping. Domain structs never cross this
// boundary.
package http

import (
	"bytes"
	"encoding/json"
	"errors"
)

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
	// LineNo is the optional order line this reservation is for (ADR 0036).
	// A pointer so absent/null (unknown) is distinguishable from an invalid 0.
	LineNo *int `json:"lineNo"`
	// OwnerRef selects whose stock the reservation draws on (ADR 0045); absent
	// means fleet-owned stock only.
	OwnerRef string `json:"ownerRef"`
	// Cases, instead of Quantity, reserves whole sealed cases (ReserveFullCases).
	// casesField keeps absent distinguishable from an invalid 0 and rejects an
	// explicit null (the schema does not declare cases nullable).
	Cases casesField `json:"cases"`
}

// casesField is an optional integer: the zero value is "absent", a JSON null
// is a malformed body.
type casesField struct {
	set bool
	n   int
}

var errCasesNotInteger = errors.New("cases must be an integer")

func (c *casesField) UnmarshalJSON(b []byte) error {
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		return errCasesNotInteger
	}
	if err := json.Unmarshal(b, &c.n); err != nil {
		return errCasesNotInteger
	}
	c.set = true
	return nil
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
	ID        string `json:"id"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demandRef"`
	// OwnerRef is the owner whose stock the reservation holds; omitted for
	// fleet-owned stock (ADR 0045).
	OwnerRef string `json:"ownerRef,omitempty"`
	// LineNo is the order line this reservation is for; omitted when unknown
	// (a reservation made without one, ADR 0036).
	LineNo      *int                 `json:"lineNo,omitempty"`
	Status      string               `json:"status"`
	Allocations []allocationResponse `json:"allocations"`
	CreatedAt   string               `json:"createdAt"`
	ExpiresAt   string               `json:"expiresAt"`
}

type usableInventoryResponse struct {
	SKU string `json:"sku"`
	// OwnerRef is the owner the figures are for; omitted for fleet-owned stock.
	OwnerRef string `json:"ownerRef,omitempty"`
	// Usable is in eaches and counts loose stock and sealed cases alike.
	Usable int `json:"usable"`
	// LooseUsable is the part of usable a quantity reservation can draw on.
	LooseUsable int `json:"looseUsable"`
	// ReservableCases is how many whole sealed cases can still be reserved.
	ReservableCases int `json:"reservableCases"`
}

// --- transfer destination receipt/stow (ADR 0031) ---------------------------

type stageTransferReceiptRequest struct {
	TransferID        string `json:"transferId"`
	DestinationSiteID string `json:"destinationSiteId"`
	SKU               string `json:"sku"`
	ReceivedQuantity  int    `json:"receivedQuantity"`
}

type transferReceiptResponse struct {
	TransferID        string `json:"transferId"`
	TransferLineID    string `json:"transferLineId"`
	DestinationSiteID string `json:"destinationSiteId"`
	SKU               string `json:"sku"`
	ExpectedQuantity  int    `json:"expectedQuantity"`
	ReceivedQuantity  int    `json:"receivedQuantity"`
	Variance          int    `json:"variance"`
	State             string `json:"state"`
	StagedAt          string `json:"stagedAt"`
	StowedAt          string `json:"stowedAt,omitempty"`
	Replay            bool   `json:"replay"`
}

type stowTransferRequest struct {
	Bins []stowBinRequest `json:"bins"`
}

type stowBinRequest struct {
	BinID    string `json:"binId"`
	Quantity int    `json:"quantity"`
}

type stowedAllocationResponse struct {
	StockUnitID string `json:"stockUnitId"`
	BinID       string `json:"binId"`
	Quantity    int    `json:"quantity"`
}

type stowTransferResponse struct {
	transferReceiptResponse
	StowedQuantity int                        `json:"stowedQuantity"`
	Allocations    []stowedAllocationResponse `json:"allocations"`
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

// problemDetails is the RFC 7807 (Problem Details for HTTP APIs) response
// body used for every error response in this service.
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}
