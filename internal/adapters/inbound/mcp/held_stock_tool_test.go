package mcp_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// newHeldToolServer holds three units (2 DAMAGED, 1 QC_PENDING; held 50h, 30h
// and 2h before the clock) and serves list_held_stock over them. wire == false
// leaves Deps.ListHeldStock nil.
func newHeldToolServer(t *testing.T, wire bool) string {
	t.Helper()
	ctx := context.Background()
	held := memory.NewHeldStockRepo()
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	for i, u := range []struct {
		sku    string
		qty    int
		reason stock.HoldReason
		age    time.Duration
	}{
		{"SKU-A", 1, stock.ReasonDamaged, 50 * time.Hour},
		{"SKU-B", 2, stock.ReasonQCPending, 30 * time.Hour},
		{"SKU-A", 3, stock.ReasonDamaged, 2 * time.Hour},
	} {
		id, _ := held.NextID(ctx)
		h, err := stock.NewHeldUnit(id, stock.HeldSource{
			ReceiptID: "rcpt-1", ASNNumber: "ASN-1", LineNo: i + 1, SKU: shared.SKU(u.sku), Quantity: shared.Quantity(u.qty),
		}, u.reason, now.Add(-u.age))
		if err != nil {
			t.Fatal(err)
		}
		if err := held.Save(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	deps := inboundmcp.Deps{}
	if wire {
		deps.ListHeldStock = &usecases.ListHeldStock{Held: held, Clock: memory.NewFixedClock(now)}
	}
	srv := httptest.NewServer(inboundmcp.Handler(inboundmcp.NewServer(deps)))
	t.Cleanup(srv.Close)
	return srv.URL
}

func callListHeld(t *testing.T, url string, args map[string]any) *sdk.CallToolResult {
	t.Helper()
	res, err := connect(t, url).CallTool(context.Background(), &sdk.CallToolParams{Name: "list_held_stock", Arguments: args})
	if err != nil {
		t.Fatalf("call list_held_stock: %v", err)
	}
	return res
}

func heldItems(t *testing.T, res *sdk.CallToolResult) []map[string]any {
	t.Helper()
	if res.IsError {
		t.Fatalf("tool error: %s", toolErrorText(res))
	}
	got, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("structured content = %T", res.StructuredContent)
	}
	raw, _ := got["items"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		out = append(out, r.(map[string]any))
	}
	return out
}

func TestListHeldStock_ListsOldestFirstWithItsFields(t *testing.T) {
	items := heldItems(t, callListHeld(t, newHeldToolServer(t, true), map[string]any{}))
	if len(items) != 3 || items[0]["quantity"] != float64(1) || items[2]["quantity"] != float64(3) {
		t.Fatalf("items = %v, want 3 held units, oldest first", items)
	}
	first := items[0]
	if first["sku"] != "SKU-A" || first["reason"] != "DAMAGED" || first["state"] != "HELD" || first["receiptId"] != "rcpt-1" ||
		first["asnNumber"] != "ASN-1" || first["lineNo"] != float64(1) || first["heldAt"] != "2026-11-07T10:00:00Z" {
		t.Fatalf("first = %v", first)
	}
}

func TestListHeldStock_Filters(t *testing.T) {
	url := newHeldToolServer(t, true)
	for name, tc := range map[string]struct {
		args map[string]any
		want int
	}{
		"by sku":    {map[string]any{"sku": "SKU-A"}, 2},
		"by reason": {map[string]any{"reason": "QC_PENDING"}, 1},
		"by age":    {map[string]any{"olderThanHours": 24}, 2},
		"combined":  {map[string]any{"sku": "SKU-A", "olderThanHours": 24}, 1},
	} {
		if got := len(heldItems(t, callListHeld(t, url, tc.args))); got != tc.want {
			t.Errorf("%s = %d items, want %d", name, got, tc.want)
		}
	}
}

func TestListHeldStock_BadFiltersAreToolErrors(t *testing.T) {
	url := newHeldToolServer(t, true)
	for name, args := range map[string]map[string]any{
		"unknown reason": {"reason": "BROKEN"},
		"negative age":   {"olderThanHours": -1},
	} {
		if res := callListHeld(t, url, args); !res.IsError {
			t.Errorf("%s: want a tool error, got %+v", name, res)
		}
	}
}

func TestListHeldStock_UnwiredServerAnswersNotConfigured(t *testing.T) {
	res := callListHeld(t, newHeldToolServer(t, false), map[string]any{})
	if !res.IsError || !strings.Contains(toolErrorText(res), "not configured") {
		t.Fatalf("result = %+v, want the not-configured tool error", res)
	}
}
