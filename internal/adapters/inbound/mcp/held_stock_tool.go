package mcp

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// errHeldStockNotWired is returned when the composition root did not wire the
// held-stock read (Deps.ListHeldStock is nil).
var errHeldStockNotWired = errors.New("held-stock lookups are not configured on this server")

// --- list_held_stock (read, authority L1, ADR 0052) -----------------------------

type listHeldStockInput struct {
	Reason         string `json:"reason,omitempty" jsonschema:"only units held for this reason: DAMAGED, QC_PENDING, SUSPECT or OTHER"`
	SKU            string `json:"sku,omitempty" jsonschema:"only units of this SKU"`
	OlderThanHours *int   `json:"olderThanHours,omitempty" jsonschema:"only units held for MORE than this many hours (the ageing filter)"`
}

type listHeldStockOutput struct {
	Items []heldUnitView `json:"items"`
}

// heldUnitView is the tool-boundary DTO of one held unit.
type heldUnitView struct {
	ID          string   `json:"id"`
	SKU         string   `json:"sku"`
	Quantity    int      `json:"quantity"`
	Reason      string   `json:"reason"`
	State       string   `json:"state"`
	ReceiptID   string   `json:"receiptId"`
	ASNNumber   string   `json:"asnNumber,omitempty"`
	LineNo      int      `json:"lineNo"`
	OwnerRef    string   `json:"ownerRef,omitempty"`
	SSCC        string   `json:"sscc,omitempty"`
	LPNs        []string `json:"lpns"`
	HeldAt      string   `json:"heldAt"`
	ReworkCount int      `json:"reworkCount"`
}

func toHeldUnitView(h *stock.HeldUnit) heldUnitView {
	lpns := h.LPNs()
	if lpns == nil {
		lpns = []string{}
	}
	return heldUnitView{
		ID: h.ID(), SKU: h.SKU().String(), Quantity: h.Quantity().Int(), Reason: string(h.Reason()), State: string(h.State()),
		ReceiptID: h.ReceiptID(), ASNNumber: h.ASNNumber(), LineNo: h.LineNo(), OwnerRef: h.OwnerRef().String(), SSCC: h.SSCC(),
		LPNs: lpns, HeldAt: h.HeldAt().UTC().Format(time.RFC3339), ReworkCount: h.ReworkCount(),
	}
}

func (d Deps) listHeldStock(ctx context.Context, in listHeldStockInput) (listHeldStockOutput, error) {
	if d.ListHeldStock == nil {
		return listHeldStockOutput{}, errHeldStockNotWired
	}
	var q usecases.ListHeldStockQuery
	if r := strings.TrimSpace(in.Reason); r != "" {
		reason, err := stock.ParseHoldReason(r)
		if err != nil {
			return listHeldStockOutput{}, err
		}
		q.Reason = reason
	}
	if s := strings.TrimSpace(in.SKU); s != "" {
		sku, err := shared.NewSKU(s)
		if err != nil {
			return listHeldStockOutput{}, err
		}
		q.SKU = sku
	}
	if in.OlderThanHours != nil {
		if *in.OlderThanHours < 0 {
			return listHeldStockOutput{}, errors.New("olderThanHours must not be negative")
		}
		q.OlderThan = time.Duration(*in.OlderThanHours) * time.Hour
	}
	units, err := d.ListHeldStock.Execute(ctx, q)
	if err != nil {
		return listHeldStockOutput{}, err
	}
	out := listHeldStockOutput{Items: make([]heldUnitView, 0, len(units))}
	for _, h := range units {
		out.Items = append(out.Items, toHeldUnitView(h))
	}
	return out, nil
}

// registerHeldStockTool adds the read-only list_held_stock tool. There is NO
// write tool: a disposition stays REST-only (ADR 0052, governance charter).
func (d Deps) registerHeldStockTool(server *mcp.Server) {
	addTool(server, &mcp.Tool{
		Name:        "list_held_stock",
		Description: "List the stock units held out of usable inventory (damaged or otherwise not-Good receipt lines), oldest first, optionally filtered by reason (DAMAGED, QC_PENDING, SUSPECT, OTHER), SKU and age in hours. A held unit has no usable quantity and can never be reserved; a disposition (RELEASE, RETURN_TO_VENDOR, SCRAP, REWORK) is decided over REST, not here. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, d.listHeldStock)
}
