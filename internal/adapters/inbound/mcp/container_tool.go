package mcp

import (
	"context"
	"errors"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/inventory-storage/internal/domain/container"
)

// errContainersNotWired is returned when the composition root did not wire the
// container read (Deps.GetContainer is nil).
var errContainersNotWired = errors.New("container lookups are not configured on this server")

// --- get_container (read, ADR 0045) -------------------------------------------

type getContainerInput struct {
	Lpn string `json:"lpn" jsonschema:"the license plate of a sealed case: its SSCC, or an internal LPN-<site>-<id> for an unlabelled carton"`
}

// containerView is the tool-boundary DTO of one container.
type containerView struct {
	Lpn          string `json:"lpn"`
	SKU          string `json:"sku"`
	GTIN         string `json:"gtin"`
	UnitsPerPack int    `json:"unitsPerPack"`
	QtyEaches    int    `json:"qtyEaches"`
	OwnerRef     string `json:"ownerRef,omitempty"`
	SiteId       string `json:"siteId,omitempty"`
	BinId        string `json:"binId,omitempty"`
	State        string `json:"state"`
	Lot          string `json:"lot,omitempty"`
	Expiry       string `json:"expiry,omitempty"`
	StockUnitId  string `json:"stockUnitId,omitempty"`
	ReceiptId    string `json:"receiptId"`
	LineNo       int    `json:"lineNo"`
}

func toContainerView(c *container.Container) containerView {
	return containerView{
		Lpn: c.LPN(), SKU: c.SKU().String(), GTIN: c.GTIN(), UnitsPerPack: c.UnitsPerPack(), QtyEaches: c.Quantity().Int(),
		OwnerRef: c.OwnerRef().String(), SiteId: c.SiteID().String(), BinId: c.BinID().String(), State: string(c.State()),
		Lot: c.Lot(), Expiry: c.Expiry(), StockUnitId: c.StockUnitID(), ReceiptId: c.ReceiptID(), LineNo: c.LineNo(),
	}
}

func (d Deps) getContainer(ctx context.Context, in getContainerInput) (containerView, error) {
	lpn := strings.TrimSpace(in.Lpn)
	if lpn == "" {
		return containerView{}, errors.New("lpn is required")
	}
	if d.GetContainer == nil {
		return containerView{}, errContainersNotWired
	}
	c, err := d.GetContainer.Execute(ctx, lpn)
	if err != nil {
		// usecases.ErrContainerNotFound surfaces unchanged as the tool error.
		return containerView{}, err
	}
	return toContainerView(c), nil
}

// registerContainerTool adds the read-only get_container tool.
func (d Deps) registerContainerTool(server *mcp.Server) {
	addTool(server, &mcp.Tool{
		Name:        "get_container",
		Description: "Return one sealed-case container by its LPN: SKU, case GTIN, units per pack, quantity in eaches, owner, site, bin, state (SEALED, OPEN or EMPTY), lot, expiry and the receipt line it came from. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, d.getContainer)
}
