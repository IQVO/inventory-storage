package mcp_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

const (
	mcpRelFrom = shared.BinId("WH1-STOR-AMB-A01-01-01-A")
	mcpRelTo   = shared.BinId("WH1-STOR-AMB-A02-01-01-A")
)

// newRelocationToolServer applies one real relocation (4 of 10 units of
// SKU-1, SLOTTING) over in-memory repos and serves the MCP tools over them.
// It returns the server URL and the relocation's id.
func newRelocationToolServer(t *testing.T, wire bool) (string, string) {
	t.Helper()
	ctx := context.Background()
	stockRepo := memory.NewStockRepo()
	locations := memory.NewLocationRepo()
	relocations := memory.NewRelocationRepo()
	for _, id := range []shared.BinId{mcpRelFrom, mcpRelTo} {
		bin, _ := location.NewBin(id, 50)
		if id == mcpRelFrom {
			_ = bin.Occupy(10)
		}
		if err := locations.Save(ctx, bin); err != nil {
			t.Fatalf("save bin: %v", err)
		}
	}
	unit, _ := stock.NewStockUnit("su-src", "SKU-1", mcpRelFrom, 10)
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}
	relocate := &usecases.RelocateStock{
		Stock: stockRepo, Locations: locations, Relocations: relocations,
		Events: events.NewBufferedPublisher(), Clock: memory.NewFixedClock(time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)),
	}
	rel, err := relocate.Execute(ctx, usecases.RelocateStockCommand{
		SKU: "SKU-1", FromBin: mcpRelFrom, ToBin: mcpRelTo, Quantity: 4,
		Reason: relocation.ReasonSlotting, SourceRef: "plan-9#3",
	})
	if err != nil {
		t.Fatalf("relocate: %v", err)
	}

	deps := inboundmcp.Deps{GetUsable: &usecases.GetUsable{Stock: stockRepo}, Stock: stockRepo}
	if wire {
		deps.GetRelocation = &usecases.GetRelocation{Relocations: relocations}
	}
	httpSrv := httptest.NewServer(inboundmcp.Handler(inboundmcp.NewServer(deps)))
	t.Cleanup(httpSrv.Close)
	return httpSrv.URL, rel.ID()
}

func callGetRelocation(t *testing.T, url, id string) *sdk.CallToolResult {
	t.Helper()
	res, err := connect(t, url).CallTool(context.Background(), &sdk.CallToolParams{
		Name: "get_relocation", Arguments: map[string]any{"relocationId": id},
	})
	if err != nil {
		t.Fatalf("call get_relocation: %v", err)
	}
	return res
}

func toolErrorText(res *sdk.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if text, ok := c.(*sdk.TextContent); ok {
			b.WriteString(text.Text)
		}
	}
	return b.String()
}

func TestGetRelocation_ReturnsTheAppliedMove(t *testing.T) {
	url, id := newRelocationToolServer(t, true)
	res := callGetRelocation(t, url, id)
	if res.IsError {
		t.Fatalf("tool error: %s", toolErrorText(res))
	}
	got, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content = %T", res.StructuredContent)
	}
	want := map[string]any{
		"relocationId": id, "siteCode": "WH1", "sku": "SKU-1",
		"fromBin": string(mcpRelFrom), "toBin": string(mcpRelTo), "quantity": float64(4),
		"reason": "SLOTTING", "sourceRef": "plan-9#3", "occurredAt": "2026-10-13T12:00:00Z",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if dest, _ := got["destinationStockUnitId"].(string); dest == "" || dest == "su-src" {
		t.Errorf("destinationStockUnitId = %q, want the new destination unit", dest)
	}
	legs, _ := got["legs"].([]any)
	if len(legs) != 1 {
		t.Fatalf("legs = %v, want one source leg", got["legs"])
	}
	leg := legs[0].(map[string]any)
	if leg["stockUnitId"] != "su-src" || leg["quantity"] != float64(4) {
		t.Errorf("leg = %v, want su-src x4", leg)
	}
}

func TestGetRelocation_UnknownIdIsAToolError(t *testing.T) {
	url, _ := newRelocationToolServer(t, true)
	res := callGetRelocation(t, url, "rel-does-not-exist")
	if !res.IsError || !strings.Contains(toolErrorText(res), usecases.ErrRelocationNotFound.Error()) {
		t.Fatalf("result = %+v, want the relocation-not-found tool error", res)
	}
}

func TestGetRelocation_BlankIdIsAToolError(t *testing.T) {
	url, _ := newRelocationToolServer(t, true)
	res := callGetRelocation(t, url, "  ")
	if !res.IsError || !strings.Contains(toolErrorText(res), "relocationId is required") {
		t.Fatalf("result = %+v, want the required-id tool error", res)
	}
}

func TestGetRelocation_UnwiredServerAnswersNotConfigured(t *testing.T) {
	url, id := newRelocationToolServer(t, false)
	res := callGetRelocation(t, url, id)
	if !res.IsError || !strings.Contains(toolErrorText(res), "not configured") {
		t.Fatalf("result = %+v, want the not-configured tool error", res)
	}
}
