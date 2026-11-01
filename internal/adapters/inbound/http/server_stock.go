package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

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

func (s *Server) handleReserveStock(w http.ResponseWriter, r *http.Request) {
	var req reserveStockRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		writeError(w, r, err)
		return
	}
	owner, err := shared.NewOwnerRef(req.OwnerRef)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if req.Cases.set && req.Quantity != 0 {
		writeProblem(w, http.StatusBadRequest, problemInfo{"quantity-and-cases-exclusive", "quantity and cases are mutually exclusive"}, "send either quantity (eaches) or cases (whole sealed cases), not both", r.URL.Path)
		return
	}

	// demandRef is the idempotency key ReserveStock retries on and the key
	// GET /reservations?demandRef= looks up by — an empty one would make
	// every such reservation unreachable, so reject it like the GET does.
	if req.DemandRef == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-demand-ref", "demandRef is required"}, "demandRef must not be empty", r.URL.Path)
		return
	}

	// lineNo is optional (ADR 0036); when given it must be a line number,
	// i.e. between 1 and 2147483647 (the 32-bit line_no column).
	if req.LineNo != nil && !reservation.ValidLineNo(*req.LineNo) {
		writeProblem(w, http.StatusBadRequest, problemInfo{"invalid-line-no", "lineNo must be between 1 and 2147483647"}, "lineNo must be an integer between 1 and 2147483647 when present", r.URL.Path)
		return
	}

	res, err := s.reserve(r, req, sku, owner)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/reservations/"+res.ID())
	writeJSON(w, http.StatusCreated, toReservationResponse(res))
}

// reserve runs the quantity reservation, or the whole-case one when the body
// carries `cases` (ADR 0045).
func (s *Server) reserve(r *http.Request, req reserveStockRequest, sku shared.SKU, owner shared.OwnerRef) (*reservation.Reservation, error) {
	if req.Cases.set {
		return s.ReserveFullCases.Execute(r.Context(), usecases.ReserveFullCasesCommand{
			SKU: sku, Cases: req.Cases.n, DemandRef: req.DemandRef, LineNo: req.LineNo, OwnerRef: owner,
		})
	}
	qty, err := shared.NewPositiveQuantity(req.Quantity)
	if err != nil {
		return nil, err
	}
	return s.ReserveStock.ExecuteFor(r.Context(), usecases.ReserveRequest{
		SKU: sku, Quantity: qty, DemandRef: req.DemandRef, LineNo: req.LineNo, OwnerRef: owner,
	})
}

func (s *Server) handleRevokeReservation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.RevokeReservation.Execute(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleGetReservationsByDemandRef backs GET /reservations?demandRef=<ref>.
// demandRef is required (400 if missing/empty) since this is a lookup-by-key
// endpoint, not a list-all — there is no meaningful "give me everything"
// response here.
func (s *Server) handleGetReservationsByDemandRef(w http.ResponseWriter, r *http.Request) {
	demandRef := r.URL.Query().Get("demandRef")
	if demandRef == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-demand-ref", "demandRef query parameter is required"}, "demandRef query parameter must not be empty", r.URL.Path)
		return
	}

	reservations, err := s.GetReservationsByDemandRef.Execute(r.Context(), demandRef)
	if err != nil {
		writeError(w, r, err)
		return
	}

	responses := make([]reservationResponse, 0, len(reservations))
	for _, res := range reservations {
		responses = append(responses, toReservationResponse(res))
	}
	writeJSON(w, http.StatusOK, responses)
}

func (s *Server) handleConfirmPick(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.ConfirmPick.Execute(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
