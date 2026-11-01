package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/http/page"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

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
// response here. The answer is one keyset page, {items, nextCursor}, ordered
// by (created_at, id); `demandRef` is repeated on every page request.
func (s *Server) handleGetReservationsByDemandRef(w http.ResponseWriter, r *http.Request) {
	demandRef := r.URL.Query().Get("demandRef")
	if demandRef == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-demand-ref", "demandRef query parameter is required"}, "demandRef query parameter must not be empty", r.URL.Path)
		return
	}
	limit, pos, ok := pageParams(w, r)
	if !ok {
		return
	}
	after, err := reservationAfter(pos)
	if err != nil {
		writeInvalidCursor(w, r)
		return
	}

	reservations, err := s.GetReservationsByDemandRef.Page(r.Context(), demandRef, after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, pageOf(reservations, limit, reservationPosition, toReservationResponse))
}

// handleConfirmPick backs POST /reservations/{id}/pick-confirmations.
func (s *Server) handleConfirmPick(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.ConfirmPick.Execute(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func toReservationResponse(res *reservation.Reservation) reservationResponse {
	allocations := make([]allocationResponse, 0, len(res.Allocations()))
	for _, a := range res.Allocations() {
		allocations = append(allocations, allocationResponse{StockUnitID: a.StockUnitID, BinID: a.BinID.String(), Quantity: a.Quantity.Int()})
	}
	return reservationResponse{
		ID:          res.ID(),
		SKU:         res.SKU().String(),
		Quantity:    res.Quantity().Int(),
		DemandRef:   res.DemandRef(),
		OwnerRef:    res.OwnerRef().String(),
		LineNo:      res.LineNo(),
		Status:      string(res.Status()),
		Allocations: allocations,
		CreatedAt:   res.CreatedAt().Format(timeFormat),
		ExpiresAt:   res.ExpiresAt().Format(timeFormat),
	}
}
