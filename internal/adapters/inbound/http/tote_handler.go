package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/http/page"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// --- totes (ADR 0054) --------------------------------------------------------

type fillToteRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	OwnerRef string `json:"ownerRef"`
}

type stowToteRequest struct {
	Bin string `json:"bin"`
}

type toteContentResponse struct {
	StockUnitID string `json:"stockUnitId"`
	SKU         string `json:"sku"`
	OwnerRef    string `json:"ownerRef,omitempty"`
	Quantity    int    `json:"quantity"`
	Reserved    int    `json:"reserved"`
}

type toteResponse struct {
	LPN      string                `json:"lpn"`
	SiteID   string                `json:"siteId,omitempty"`
	BinID    string                `json:"binId,omitempty"`
	Contents []toteContentResponse `json:"contents"`
}

type toteSummaryResponse struct {
	LPN    string `json:"lpn"`
	SiteID string `json:"siteId,omitempty"`
	BinID  string `json:"binId,omitempty"`
}

func toToteResponse(t *container.Tote, units []*stock.StockUnit) toteResponse {
	contents := make([]toteContentResponse, 0, len(units))
	for _, u := range units {
		contents = append(contents, toteContentResponse{StockUnitID: u.ID(), SKU: u.SKU().String(), OwnerRef: u.OwnerRef().String(),
			Quantity: u.Quantity().Int(), Reserved: u.Reserved().Int()})
	}
	return toteResponse{LPN: t.LPN(), SiteID: t.SiteID().String(), BinID: t.BinID().String(), Contents: contents}
}

func toToteSummary(t *container.Tote) toteSummaryResponse {
	return toteSummaryResponse{LPN: t.LPN(), SiteID: t.SiteID().String(), BinID: t.BinID().String()}
}

// handleRegisterTote backs PUT /totes/{lpn}: 201 when the tote was created,
// 200 when it already existed; both answer the tote with its contents.
func (s *Server) handleRegisterTote(w http.ResponseWriter, r *http.Request) {
	lpn := chi.URLParam(r, "lpn")
	_, created, err := s.RegisterTote.Execute(r.Context(), lpn)
	if err != nil {
		writeError(w, r, err)
		return
	}
	view, err := s.GetTote.Execute(r.Context(), lpn)
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toToteResponse(view.Tote, view.Contents))
}

// handleGetTote backs GET /totes/{lpn}.
func (s *Server) handleGetTote(w http.ResponseWriter, r *http.Request) {
	view, err := s.GetTote.Execute(r.Context(), chi.URLParam(r, "lpn"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toToteResponse(view.Tote, view.Contents))
}

// handleFillTote backs POST /totes/{lpn}/fills: scan the item, scan the tote.
// 201 with the tote and its contents.
func (s *Server) handleFillTote(w http.ResponseWriter, r *http.Request) {
	var req fillToteRequest
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
	lpn := chi.URLParam(r, "lpn")
	cmd := usecases.FillToteCommand{LPN: lpn, SKU: sku, Quantity: shared.Quantity(req.Quantity), OwnerRef: owner}
	if _, err := s.FillTote.Execute(r.Context(), cmd); err != nil {
		writeError(w, r, err)
		return
	}
	s.writeTote(w, r, lpn, http.StatusCreated)
}

// handleStowTote backs POST /totes/{lpn}/stowages: scan the tote, scan the
// location. 200 with the tote and its contents.
func (s *Server) handleStowTote(w http.ResponseWriter, r *http.Request) {
	var req stowToteRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	bin, err := shared.NewBinId(req.Bin)
	if err != nil {
		writeError(w, r, err)
		return
	}
	lpn := chi.URLParam(r, "lpn")
	if _, err := s.StowTote.Execute(r.Context(), lpn, bin); err != nil {
		writeError(w, r, err)
		return
	}
	s.writeTote(w, r, lpn, http.StatusOK)
}

func (s *Server) writeTote(w http.ResponseWriter, r *http.Request, lpn string, status int) {
	view, err := s.GetTote.Execute(r.Context(), lpn)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, status, toToteResponse(view.Tote, view.Contents))
}

// handleListBinTotes backs GET /bins/{binId}/totes: one keyset page in LPN
// order, like GET /bins/{binId}/containers.
func (s *Server) handleListBinTotes(w http.ResponseWriter, r *http.Request) {
	bin, err := shared.NewBinId(chi.URLParam(r, "binId"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	limit, pos, ok := pageParams(w, r)
	if !ok {
		return
	}
	if pos != nil && pos.Sort != pos.ID {
		writeInvalidCursor(w, r)
		return
	}
	afterLPN := ""
	if pos != nil {
		afterLPN = pos.ID
	}
	totes, err := s.ListBinTotes.Page(r.Context(), bin, afterLPN, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, pageOf(totes, limit, totePosition, toToteSummary))
}

func totePosition(t *container.Tote) page.Position {
	return page.Position{Sort: t.LPN(), ID: t.LPN()}
}
