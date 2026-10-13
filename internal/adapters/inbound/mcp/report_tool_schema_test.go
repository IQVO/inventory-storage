package mcp

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// stubReports is a package-internal ReportsClient double for the wire test.
type stubReports struct{ last FlowAccuracyQuery }

func (s *stubReports) GetFlowAccuracy(_ context.Context, q FlowAccuracyQuery) (FlowAccuracyReportView, error) {
	s.last = q
	return FlowAccuracyReportView{}, nil
}

func (s *stubReports) GetFreshness(context.Context) (FreshnessView, error) {
	return FreshnessView{}, nil
}

// connectReportClient serves the real MCP server (with a reports client
// configured) over an in-memory transport and returns the client session.
func connectReportClient(t *testing.T, reports ReportsClient) *sdk.ClientSession {
	t.Helper()
	deps := newHarness(t).deps
	deps.Reports = reports
	server := NewServer(deps)

	client := sdk.NewClient(&sdk.Implementation{Name: "d16", Version: "0"}, nil)
	ct, st := sdk.NewInMemoryTransports()
	ctx := context.Background()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// requiredInputs returns the sorted `required` list of a tool's input schema.
func requiredInputs(t *testing.T, cs *sdk.ClientSession, name string) []string {
	t.Helper()
	list, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	for _, tl := range list.Tools {
		if tl.Name != name {
			continue
		}
		raw, err := json.Marshal(tl.InputSchema)
		if err != nil {
			t.Fatalf("marshal input schema: %v", err)
		}
		var schema struct {
			Required []string `json:"required"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatalf("decode input schema: %v", err)
		}
		sort.Strings(schema.Required)
		return schema.Required
	}
	t.Fatalf("%s not advertised with a reports client configured", name)
	return nil
}

// TestReportTool_OptionalFiltersAreOptionalOnTheWire pins gap fix D16 at the
// protocol level: the advertised input schema of
// get_inventory_flow_accuracy_report requires only from and to, and a call
// that leaves sku, binId and granularity out reaches the handler instead of
// being rejected by the SDK's schema validation.
func TestReportTool_OptionalFiltersAreOptionalOnTheWire(t *testing.T) {
	stub := &stubReports{}
	cs := connectReportClient(t, stub)

	if got := requiredInputs(t, cs, "get_inventory_flow_accuracy_report"); !slices.Equal(got, []string{"from", "to"}) {
		t.Fatalf("required = %v, want [from to] (sku, binId and granularity are optional)", got)
	}

	res, err := cs.CallTool(context.Background(), &sdk.CallToolParams{
		Name:      "get_inventory_flow_accuracy_report",
		Arguments: map[string]any{"from": "2026-03-01T00:00:00Z", "to": "2026-03-02T00:00:00Z"},
	})
	if err != nil {
		t.Fatalf("call without optional filters: %v", err)
	}
	if res.IsError {
		t.Fatalf("call without optional filters returned a tool error: %+v", res.Content)
	}
	if stub.last.From != "2026-03-01T00:00:00Z" || stub.last.SKU != "" || stub.last.BinId != "" || stub.last.Granularity != "" {
		t.Fatalf("forwarded query = %+v", stub.last)
	}
}
