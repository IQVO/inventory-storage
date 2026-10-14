package mcp

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/inventory-storage/internal/domain/relocation"
)

// errRelocationsNotWired is returned when the composition root did not wire
// the relocation read (Deps.GetRelocation is nil).
var errRelocationsNotWired = errors.New("relocation lookups are not configured on this server")

// --- get_relocation (read, ADR 0039) ------------------------------------------

type getRelocationInput struct {
	RelocationId string `json:"relocationId" jsonschema:"the id of an applied intra-site relocation (the relocation_id of a StockRelocated event, or the id POST /relocations returned)"`
}

// relocationView is the tool-boundary DTO of one relocation: what moved,
// between which bins of which site, why, and which StockUnits it touched.
type relocationView struct {
	RelocationId           string              `json:"relocationId"`
	SiteCode               string              `json:"siteCode"`
	SKU                    string              `json:"sku"`
	FromBin                string              `json:"fromBin"`
	ToBin                  string              `json:"toBin"`
	Quantity               int                 `json:"quantity"`
	Reason                 string              `json:"reason"`
	SourceRef              string              `json:"sourceRef"`
	DestinationStockUnitId string              `json:"destinationStockUnitId"`
	Legs                   []relocationLegView `json:"legs"`
	OccurredAt             string              `json:"occurredAt"`
}

// relocationLegView is one source StockUnit's contribution to the move.
type relocationLegView struct {
	StockUnitId string `json:"stockUnitId"`
	Quantity    int    `json:"quantity"`
}

func toRelocationView(r *relocation.Relocation) relocationView {
	legs := make([]relocationLegView, 0, len(r.Legs()))
	for _, leg := range r.Legs() {
		legs = append(legs, relocationLegView{StockUnitId: leg.StockUnitID, Quantity: leg.Quantity.Int()})
	}
	return relocationView{
		RelocationId:           r.ID(),
		SiteCode:               r.SiteID().String(),
		SKU:                    r.SKU().String(),
		FromBin:                r.FromBin().String(),
		ToBin:                  r.ToBin().String(),
		Quantity:               r.Quantity().Int(),
		Reason:                 string(r.Reason()),
		SourceRef:              r.SourceRef(),
		DestinationStockUnitId: r.DestinationStockUnitID(),
		Legs:                   legs,
		OccurredAt:             r.OccurredAt().UTC().Format(time.RFC3339),
	}
}

func (d Deps) getRelocation(ctx context.Context, in getRelocationInput) (relocationView, error) {
	id := strings.TrimSpace(in.RelocationId)
	if id == "" {
		return relocationView{}, errors.New("relocationId is required")
	}
	if d.GetRelocation == nil {
		return relocationView{}, errRelocationsNotWired
	}
	rel, err := d.GetRelocation.Execute(ctx, id)
	if err != nil {
		// usecases.ErrRelocationNotFound surfaces unchanged as the tool error.
		return relocationView{}, err
	}
	return toRelocationView(rel), nil
}

// registerRelocationTool adds the read-only get_relocation tool.
func (d Deps) registerRelocationTool(server *mcp.Server) {
	addTool(server, &mcp.Tool{
		Name:        "get_relocation",
		Description: "Return one applied intra-site stock relocation by id: SKU, quantity, source and destination bin of one site, reason (SLOTTING, CONSOLIDATION, MANUAL), source reference (e.g. the slot-plan move), the source StockUnits drawn and the destination StockUnit created. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, d.getRelocation)
}
