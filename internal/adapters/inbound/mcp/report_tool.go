package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- reports REST views (tool + client boundary) ------------------------------

// FlowAccuracyRowView is one row of the Inventory Flow & Accuracy report as the
// MCP tool returns it and the reports REST client decodes it. Field tags match
// the reports service's JSON so the same struct round-trips both ways.
type FlowAccuracyRowView struct {
	SKU                   string `json:"sku"`
	BinId                 string `json:"binId"`
	HourBucket            string `json:"hourBucket"`
	ReceivedQuantity      int    `json:"receivedQuantity"`
	StowedCount           int    `json:"stowedCount"`
	PickedQuantity        int    `json:"pickedQuantity"`
	ReservationsCreated   int    `json:"reservationsCreated"`
	ReservationsExpired   int    `json:"reservationsExpired"`
	ReservationsRevoked   int    `json:"reservationsRevoked"`
	CycleCountsCompleted  int    `json:"cycleCountsCompleted"`
	DiscrepanciesDetected int    `json:"discrepanciesDetected"`
	UnlocatedCount        int    `json:"unlocatedCount"`
}

// FlowAccuracyReportView is the report body.
type FlowAccuracyReportView struct {
	Rows []FlowAccuracyRowView `json:"rows"`
}

// FreshnessView is the freshness-lag body.
type FreshnessView struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// FlowAccuracyQuery is the filter set passed to the reports REST client.
type FlowAccuracyQuery struct {
	From        string
	To          string
	SKU         string
	BinId       string
	Granularity string
}

// ReportsClient is the narrow port the MCP report tool depends on: a client of
// the inventory-reports REST service. It is an interface so the tool can be
// unit-tested with a fake, and so the curated tool never talks to the
// analytical database directly — it goes through the reports REST surface,
// preserving the single read path (ADR-0011).
type ReportsClient interface {
	GetFlowAccuracy(ctx context.Context, q FlowAccuracyQuery) (FlowAccuracyReportView, error)
	GetFreshness(ctx context.Context) (FreshnessView, error)
}

// --- reports REST client ------------------------------------------------------

// ReportsRESTClient is the HTTP implementation of ReportsClient. Base URL and
// the *http.Client are injected so the composition root controls the target
// and timeouts, and tests can point it at an httptest server.
type ReportsRESTClient struct {
	baseURL string
	http    *http.Client
}

// NewReportsRESTClient constructs a ReportsRESTClient for the reports service
// at baseURL. A nil httpClient falls back to a client with a sane timeout.
func NewReportsRESTClient(baseURL string, httpClient *http.Client) *ReportsRESTClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &ReportsRESTClient{baseURL: baseURL, http: httpClient}
}

// GetFlowAccuracy calls GET /reports/flow-accuracy with q as the query string.
func (c *ReportsRESTClient) GetFlowAccuracy(ctx context.Context, q FlowAccuracyQuery) (FlowAccuracyReportView, error) {
	vals := url.Values{}
	vals.Set("from", q.From)
	vals.Set("to", q.To)
	if q.SKU != "" {
		vals.Set("sku", q.SKU)
	}
	if q.BinId != "" {
		vals.Set("binId", q.BinId)
	}
	if q.Granularity != "" {
		vals.Set("granularity", q.Granularity)
	}
	var out FlowAccuracyReportView
	if err := c.getJSON(ctx, "/reports/flow-accuracy?"+vals.Encode(), &out); err != nil {
		return FlowAccuracyReportView{}, err
	}
	return out, nil
}

// GetFreshness calls GET /reports/flow-accuracy/freshness.
func (c *ReportsRESTClient) GetFreshness(ctx context.Context) (FreshnessView, error) {
	var out FreshnessView
	if err := c.getJSON(ctx, "/reports/flow-accuracy/freshness", &out); err != nil {
		return FreshnessView{}, err
	}
	return out, nil
}

// getJSON performs a GET against baseURL+path and decodes a 2xx JSON body into
// out. A non-2xx response is an error.
func (c *ReportsRESTClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("reports client: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("reports client: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reports client: unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("reports client: decode: %w", err)
	}
	return nil
}

// Compile-time assertion that ReportsRESTClient satisfies the port.
var _ ReportsClient = (*ReportsRESTClient)(nil)

// --- get_inventory_flow_accuracy_report tool ----------------------------------

// FlowAccuracyToolInput is the tool's argument set (untrusted, from a model).
type FlowAccuracyToolInput struct {
	From        string `json:"from" jsonschema:"start of the window, inclusive, RFC3339 (required)"`
	To          string `json:"to" jsonschema:"end of the window, exclusive, RFC3339 (required)"`
	SKU         string `json:"sku,omitempty" jsonschema:"optional SKU filter (flow rows: received, stowed, picked, reserved, unlocated)"`
	BinId       string `json:"binId,omitempty" jsonschema:"optional bin filter (accuracy rows: cycle counts, discrepancies)"`
	Granularity string `json:"granularity,omitempty" jsonschema:"optional time bucket granularity; only 'hour' is supported (the default)"`
}

// getFlowAccuracyReport is the tool handler: it validates the required window,
// delegates to the reports REST client, and returns the report view.
func (d Deps) getFlowAccuracyReport(ctx context.Context, in FlowAccuracyToolInput) (FlowAccuracyReportView, error) {
	return GetFlowAccuracyReportForTest(ctx, d.Reports, in)
}

// GetFlowAccuracyReportForTest is the tool's pure logic, factored out so it can
// be unit-tested with a fake ReportsClient independent of the MCP server
// wiring. It validates from/to and forwards the filters.
func GetFlowAccuracyReportForTest(ctx context.Context, client ReportsClient, in FlowAccuracyToolInput) (FlowAccuracyReportView, error) {
	if client == nil {
		return FlowAccuracyReportView{}, fmt.Errorf("reports client not configured")
	}
	if in.From == "" || in.To == "" {
		return FlowAccuracyReportView{}, fmt.Errorf("from and to are required (RFC3339)")
	}
	return client.GetFlowAccuracy(ctx, FlowAccuracyQuery(in))
}

// registerReportTool adds the curated read-only Inventory Flow & Accuracy
// report tool. It is registered only when a reports client is configured
// (Deps.Reports != nil), so an MCP deployment without the reports service
// simply does not expose it.
func (d Deps) registerReportTool(server *mcp.Server) {
	if d.Reports == nil {
		return
	}
	readOnly := true
	addTool(server, &mcp.Tool{
		Name:        "get_inventory_flow_accuracy_report",
		Description: "Return the Inventory Flow & Accuracy report (received/stowed/picked quantities, reservations created/expired/revoked, cycle-count completions, discrepancies, and unlocated items) for a time window, optionally filtered by SKU or bin. Reads via the inventory-reports REST service.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getFlowAccuracyReport)
}
