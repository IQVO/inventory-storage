package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/http/page"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// --- containers / sealed cases (ADR 0045) ----------------------------------------

type stowContainerRequest struct {
	Bin string `json:"bin"`
}

type breakCaseRequest struct {
	LooseBin string `json:"looseBin"`
}

type containerResponse struct {
	LPN          string `json:"lpn"`
	SKU          string `json:"sku"`
	GTIN         string `json:"gtin"`
	UnitsPerPack int    `json:"unitsPerPack"`
	QtyEaches    int    `json:"qtyEaches"`
	OwnerRef     string `json:"ownerRef,omitempty"`
	SiteID       string `json:"siteId,omitempty"`
	BinID        string `json:"binId,omitempty"`
	State        string `json:"state"`
	Lot          string `json:"lot,omitempty"`
	Expiry       string `json:"expiry,omitempty"`
	StockUnitID  string `json:"stockUnitId,omitempty"`
	ReceiptID    string `json:"receiptId"`
	LineNo       int    `json:"lineNo"`
}

func toContainerResponse(c *container.Container) containerResponse {
	return containerResponse{
		LPN: c.LPN(), SKU: c.SKU().String(), GTIN: c.GTIN(), UnitsPerPack: c.UnitsPerPack(), QtyEaches: c.Quantity().Int(),
		OwnerRef: c.OwnerRef().String(), SiteID: c.SiteID().String(), BinID: c.BinID().String(), State: string(c.State()),
		Lot: c.Lot(), Expiry: c.Expiry(), StockUnitID: c.StockUnitID(), ReceiptID: c.ReceiptID(), LineNo: c.LineNo(),
	}
}

// handleStowContainer backs POST /containers/{lpn}/stowages: scan the LPN, scan the
// location. 200 with the container.
func (s *Server) handleStowContainer(w http.ResponseWriter, r *http.Request) {
	var req stowContainerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	bin, err := shared.NewBinId(req.Bin)
	if err != nil {
		writeError(w, r, err)
		return
	}
	c, err := s.StowContainer.Execute(r.Context(), chi.URLParam(r, "lpn"), bin)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toContainerResponse(c))
}

// handleBreakCase backs POST /containers/{lpn}/case-breaks: open the sealed case and
// move its eaches to loose stock in looseBin. 200 with the (now EMPTY) container.
func (s *Server) handleBreakCase(w http.ResponseWriter, r *http.Request) {
	var req breakCaseRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	bin, err := shared.NewBinId(req.LooseBin)
	if err != nil {
		writeError(w, r, err)
		return
	}
	c, err := s.BreakCase.Execute(r.Context(), chi.URLParam(r, "lpn"), bin)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toContainerResponse(c))
}

// handleGetContainer backs GET /containers/{lpn}.
func (s *Server) handleGetContainer(w http.ResponseWriter, r *http.Request) {
	c, err := s.GetContainer.Execute(r.Context(), chi.URLParam(r, "lpn"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toContainerResponse(c))
}

// handleListBinContainers backs GET /bins/{binId}/containers: one keyset page,
// {items, nextCursor}, in LPN order. The LPN is unique, so the cursor carries
// it as both the sort key and the id.
func (s *Server) handleListBinContainers(w http.ResponseWriter, r *http.Request) {
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
	containers, err := s.ListBinContainers.Page(r.Context(), bin, afterLPN, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, pageOf(containers, limit, containerPosition, toContainerResponse))
}

// containerPosition is the keyset position of a container in
// GET /bins/{binId}/containers: its LPN, which is unique.
func containerPosition(c *container.Container) page.Position {
	return page.Position{Sort: c.LPN(), ID: c.LPN()}
}
