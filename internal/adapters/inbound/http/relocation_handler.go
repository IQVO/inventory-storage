package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// --- intra-site relocation (ADR 0039) ------------------------------------------

type relocateStockRequest struct {
	SiteCode string `json:"siteCode"`
	SKU      string `json:"sku"`
	FromBin  string `json:"fromBin"`
	ToBin    string `json:"toBin"`
	Quantity int    `json:"quantity"`
	// Reason is optional; absent or empty means MANUAL.
	Reason    string `json:"reason"`
	SourceRef string `json:"sourceRef"`
}

type relocationLegResponse struct {
	StockUnitID string `json:"stockUnitId"`
	Quantity    int    `json:"quantity"`
}

type relocationResponse struct {
	ID                     string                  `json:"id"`
	SiteCode               string                  `json:"siteCode"`
	SKU                    string                  `json:"sku"`
	FromBin                string                  `json:"fromBin"`
	ToBin                  string                  `json:"toBin"`
	Quantity               int                     `json:"quantity"`
	Reason                 string                  `json:"reason"`
	SourceRef              string                  `json:"sourceRef"`
	DestinationStockUnitID string                  `json:"destinationStockUnitId"`
	Legs                   []relocationLegResponse `json:"legs"`
	OccurredAt             string                  `json:"occurredAt"`
}

func toRelocationResponse(r *relocation.Relocation) relocationResponse {
	legs := make([]relocationLegResponse, 0, len(r.Legs()))
	for _, leg := range r.Legs() {
		legs = append(legs, relocationLegResponse{StockUnitID: leg.StockUnitID, Quantity: leg.Quantity.Int()})
	}
	return relocationResponse{
		ID:                     r.ID(),
		SiteCode:               r.SiteID().String(),
		SKU:                    r.SKU().String(),
		FromBin:                r.FromBin().String(),
		ToBin:                  r.ToBin().String(),
		Quantity:               r.Quantity().Int(),
		Reason:                 string(r.Reason()),
		SourceRef:              r.SourceRef(),
		DestinationStockUnitID: r.DestinationStockUnitID(),
		Legs:                   legs,
		OccurredAt:             r.OccurredAt().UTC().Format(timeFormat),
	}
}

// handleRelocateStock backs POST /relocations: move a quantity of one SKU
// from one bin to another bin of the same site. 201 with the relocation and
// Location: /relocations/{id}.
func (s *Server) handleRelocateStock(w http.ResponseWriter, r *http.Request) {
	var req relocateStockRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cmd, err := relocateCommand(req)
	if err != nil {
		writeError(w, r, err)
		return
	}
	rel, err := s.RelocateStock.Execute(r.Context(), cmd)
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/relocations/"+rel.ID())
	writeJSON(w, http.StatusCreated, toRelocationResponse(rel))
}

// relocateCommand validates the body's value objects. The site is required
// on REST (only the RELOCATE-task path resolves it from the source bin).
func relocateCommand(req relocateStockRequest) (usecases.RelocateStockCommand, error) {
	site, err := shared.NewSiteID(req.SiteCode)
	if err != nil {
		return usecases.RelocateStockCommand{}, err
	}
	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		return usecases.RelocateStockCommand{}, err
	}
	from, err := shared.NewBinId(req.FromBin)
	if err != nil {
		return usecases.RelocateStockCommand{}, err
	}
	to, err := shared.NewBinId(req.ToBin)
	if err != nil {
		return usecases.RelocateStockCommand{}, err
	}
	qty, err := shared.NewPositiveQuantity(req.Quantity)
	if err != nil {
		return usecases.RelocateStockCommand{}, err
	}
	reason := relocation.ReasonManual
	if req.Reason != "" {
		if reason, err = relocation.ParseReason(req.Reason); err != nil {
			return usecases.RelocateStockCommand{}, err
		}
	}
	return usecases.RelocateStockCommand{
		SiteID: site, SKU: sku, FromBin: from, ToBin: to, Quantity: qty, Reason: reason, SourceRef: req.SourceRef,
	}, nil
}

// handleGetRelocation backs GET /relocations/{id}.
func (s *Server) handleGetRelocation(w http.ResponseWriter, r *http.Request) {
	rel, err := s.GetRelocation.Execute(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toRelocationResponse(rel))
}
