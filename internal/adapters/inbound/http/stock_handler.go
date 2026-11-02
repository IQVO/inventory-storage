package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// handleReceiveStock backs POST /stock-receipts.
func (s *Server) handleReceiveStock(w http.ResponseWriter, r *http.Request) {
	var req receiveStockRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		writeError(w, r, err)
		return
	}
	qty, err := shared.NewPositiveQuantity(req.Quantity)
	if err != nil {
		writeError(w, r, err)
		return
	}

	receipt, err := s.ReceiveStock.Execute(r.Context(), sku, qty)
	if err != nil {
		writeError(w, r, err)
		return
	}

	// 202 Accepted, not 201 Created: a staged receipt has no durable
	// identity of its own (no ID, no GET route) — it is an acknowledgment
	// that stock has been accepted into the building, not yet a queryable
	// resource. The addressable resource (a StockUnit) is created later, at
	// StowStock.
	writeJSON(w, http.StatusAccepted, stagedReceiptResponse{
		SKU:        receipt.SKU.String(),
		Quantity:   receipt.Quantity.Int(),
		ReceivedAt: receipt.ReceivedAt.Format(timeFormat),
	})
}

// handleStowStock backs POST /stowages.
func (s *Server) handleStowStock(w http.ResponseWriter, r *http.Request) {
	var req stowStockRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		writeError(w, r, err)
		return
	}
	qty, err := shared.NewPositiveQuantity(req.Quantity)
	if err != nil {
		writeError(w, r, err)
		return
	}
	binID, err := shared.NewBinId(req.BinID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	unit, err := s.StowStock.Execute(r.Context(), sku, qty, binID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/stock/"+unit.ID())
	writeJSON(w, http.StatusCreated, toStockUnitResponse(unit))
}

func (s *Server) handleGetUsable(w http.ResponseWriter, r *http.Request) {
	skuParam := chi.URLParam(r, "sku")
	sku, err := shared.NewSKU(skuParam)
	if err != nil {
		writeError(w, r, err)
		return
	}

	owner, err := shared.NewOwnerRef(r.URL.Query().Get("ownerRef"))
	if err != nil {
		writeError(w, r, err)
		return
	}

	usable, err := s.GetUsable.ExecuteFor(r.Context(), sku, owner)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, usableInventoryResponse{
		SKU: usable.SKU.String(), OwnerRef: usable.OwnerRef.String(), Usable: usable.Usable.Int(),
		LooseUsable: usable.LooseUsable.Int(), ReservableCases: usable.ReservableCases,
	})
}

// handleRunCycleCount backs POST /bins/{binId}/cycle-counts.
func (s *Server) handleRunCycleCount(w http.ResponseWriter, r *http.Request) {
	binIDParam := chi.URLParam(r, "binId")
	binID, err := shared.NewBinId(binIDParam)
	if err != nil {
		writeError(w, r, err)
		return
	}

	var req cycleCountRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.CountedQuantity == nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"counted-quantity-required", "countedQuantity is required"}, "countedQuantity must be present; an explicit 0 is a valid empty-bin count, omitting it is not", r.URL.Path)
		return
	}
	countedQty, err := shared.NewQuantity(int(*req.CountedQuantity))
	if err != nil {
		writeError(w, r, err)
		return
	}

	result, err := s.RunCycleCount.Execute(r.Context(), binID, countedQty)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, cycleCountResponse{
		BinID:       result.BinID.String(),
		CountedQty:  result.CountedQty.Int(),
		SystemQty:   result.SystemQty.Int(),
		Discrepancy: result.Discrepancy,
	})
}

func toStockUnitResponse(u *stock.StockUnit) stockUnitResponse {
	return stockUnitResponse{
		ID:       u.ID(),
		SKU:      u.SKU().String(),
		BinID:    u.BinID().String(),
		Quantity: u.Quantity().Int(),
		Reserved: u.Reserved().Int(),
		State:    string(u.State()),
	}
}
