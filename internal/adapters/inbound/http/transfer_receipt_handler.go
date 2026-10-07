package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// handleStageTransferReceipt backs POST /transfers/{transferLineId}/receipt
// (ADR 0031): a destination scan counted goods against a transfer line.
// A recognized ALLOCATED line answers 201 (or 200 on an idempotent
// replay) with the STAGED receipt — NO usable stock moved yet. An
// unrecognized scan answers 422 with the quarantine problem AND the
// exception's coordinates as RFC 7807 extension members: the scan was
// recorded as an inventory exception, never as stock.
func (s *Server) handleStageTransferReceipt(w http.ResponseWriter, r *http.Request) {
	lineID := chi.URLParam(r, "transferLineId")

	var req stageTransferReceiptRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.TransferID == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"transfer-id-required", "transferId is required"}, "transferId must not be empty", r.URL.Path)
		return
	}

	destSite, err := shared.NewSiteID(req.DestinationSiteID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		writeError(w, r, err)
		return
	}
	receivedQty, err := shared.NewPositiveQuantity(req.ReceivedQuantity)
	if err != nil {
		writeError(w, r, err)
		return
	}

	res, err := s.StageTransferReceipt.Execute(r.Context(), usecases.ReceiptCommand{
		TransferID:        req.TransferID,
		TransferLineID:    lineID,
		DestinationSiteID: destSite,
		SKU:               sku,
		ReceivedQuantity:  receivedQty,
	})
	if err != nil {
		if res != nil && res.Quarantined() {
			// The scan is quarantined: report the explicit problem with
			// the exception's coordinates so an operator can resolve it.
			writeQuarantineProblem(w, r, err, res.Exception)
			return
		}
		writeError(w, r, err)
		return
	}

	status := http.StatusCreated
	if res.Replay {
		status = http.StatusOK
	}
	writeJSON(w, status, toTransferReceiptResponse(res.Receipt, res.Replay))
}

// handleStowTransferStock backs POST /transfers/{transferLineId}/stow
// (ADR 0031): place a STAGED receipt's goods into destination bins.
// Answers 200 with the STOWED receipt and the created allocations —
// this is the only path that raises the destination site's usable
// stock. An idempotent replay answers 200 with the ORIGINAL outcome and
// creates nothing new.
func (s *Server) handleStowTransferStock(w http.ResponseWriter, r *http.Request) {
	lineID := chi.URLParam(r, "transferLineId")

	var req stowTransferRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Bins) == 0 {
		writeProblem(w, http.StatusBadRequest, problemInfo{"bins-required", "bins is required"}, "bins must contain at least one placement", r.URL.Path)
		return
	}

	bins := make([]usecases.StowBin, 0, len(req.Bins))
	for _, b := range req.Bins {
		binID, err := shared.NewBinId(b.BinID)
		if err != nil {
			writeError(w, r, shared.ErrEmptyBinID)
			return
		}
		qty, err := shared.NewPositiveQuantity(b.Quantity)
		if err != nil {
			writeError(w, r, err)
			return
		}
		bins = append(bins, usecases.StowBin{BinID: binID, Quantity: qty})
	}

	res, err := s.StowTransferStock.Execute(r.Context(), lineID, bins)
	if err != nil {
		writeError(w, r, err)
		return
	}

	allocations := make([]stowedAllocationResponse, 0, len(res.StockUnits))
	stowed := 0
	for i, unit := range res.StockUnits {
		leg := res.Receipt.StowLegs()[i]
		allocations = append(allocations, stowedAllocationResponse{
			StockUnitID: unit.ID(),
			BinID:       leg.BinID.String(),
			Quantity:    leg.Quantity.Int(),
		})
		stowed += leg.Quantity.Int()
	}
	writeJSON(w, http.StatusOK, stowTransferResponse{
		transferReceiptResponse: toTransferReceiptResponse(res.Receipt, res.Replay),
		StowedQuantity:          stowed,
		Allocations:             allocations,
	})
}

// toTransferReceiptResponse maps the domain receipt onto its wire form.
func toTransferReceiptResponse(receipt *transfer.Receipt, replay bool) transferReceiptResponse {
	resp := transferReceiptResponse{
		TransferID:        receipt.TransferID(),
		TransferLineID:    receipt.TransferLineID(),
		DestinationSiteID: receipt.DestinationSiteID().String(),
		SKU:               receipt.SKU().String(),
		ExpectedQuantity:  receipt.ExpectedQuantity().Int(),
		ReceivedQuantity:  receipt.ReceivedQuantity().Int(),
		Variance:          receipt.Variance(),
		State:             string(receipt.State()),
		StagedAt:          receipt.StagedAt().Format(timeFormat),
		Replay:            replay,
	}
	if receipt.State() == transfer.ReceiptStowed {
		resp.StowedAt = receipt.StowedAt().Format(timeFormat)
	}
	return resp
}

// quarantinedScanProblem is the RFC 7807 body for a quarantined
// destination scan: the standard problem members plus an `exception`
// extension object carrying the quarantine row's coordinates.
type quarantinedScanProblem struct {
	Type      string        `json:"type"`
	Title     string        `json:"title"`
	Status    int           `json:"status"`
	Detail    string        `json:"detail"`
	Instance  string        `json:"instance"`
	Exception exceptionBody `json:"exception"`
}

type exceptionBody struct {
	Kind              string `json:"kind"`
	TransferID        string `json:"transferId"`
	TransferLineID    string `json:"transferLineId"`
	DestinationSiteID string `json:"destinationSiteId"`
	SKU               string `json:"sku"`
	ReceivedQuantity  int    `json:"receivedQuantity"`
}

// writeQuarantineProblem answers a quarantined scan with 422 and the
// explicit problem + exception coordinates, so an operator can find the
// quarantine row without guessing.
func writeQuarantineProblem(w http.ResponseWriter, r *http.Request, problem error, exc *transfer.Exception) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusUnprocessableEntity)
	_ = json.NewEncoder(w).Encode(quarantinedScanProblem{
		Type:     problemBaseURI + "transfer-scan-quarantined",
		Title:    "Destination scan quarantined",
		Status:   http.StatusUnprocessableEntity,
		Detail:   problem.Error(),
		Instance: r.URL.Path,
		Exception: exceptionBody{
			Kind:              string(exc.Kind()),
			TransferID:        exc.TransferID(),
			TransferLineID:    exc.TransferLineID(),
			DestinationSiteID: exc.DestinationSiteID().String(),
			SKU:               exc.SKU().String(),
			ReceivedQuantity:  exc.ReceivedQuantity().Int(),
		},
	})
}
