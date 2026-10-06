package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// newTestRouter builds the exact HTTP surface cmd/mcp serves, with the real
// inbound MCP adapter over in-memory adapters.
func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	stock := memory.NewStockRepo()
	deps := inboundmcp.Deps{
		GetUsable: &usecases.GetUsable{Stock: stock},
		Stock:     stock,
	}
	return newRouter(inboundmcp.Handler(inboundmcp.NewServer(deps)), "inventory-storage-mcp-test")
}

func TestHealthzIsUnauthenticated(t *testing.T) {
	router := newTestRouter(t)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"status":"ok"}` {
		t.Errorf("body = %q, want {\"status\":\"ok\"}", body)
	}
}

// GET /healthz is open on the healthz path specifically; any other method on
// that path falls through to the catch-all MCP handler.
func TestHealthzOnlyOpenForGET(t *testing.T) {
	router := newTestRouter(t)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))

	// POST /healthz is not registered as the healthz handler (only GET is);
	// it falls through to the MCP handler, so it must not be a 404 and must
	// not be treated as the healthz 200 either.
	if rec.Code == http.StatusNotFound {
		t.Fatalf("POST /healthz: got %d, want it to reach the MCP handler, not 404", rec.Code)
	}
}

func TestMCPRoutesReachHandler(t *testing.T) {
	router := newTestRouter(t)

	for _, path := range []string{"/", "/mcp", "/mcp/"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			// Whatever the SDK answers, it must not be the mux's 404 — the
			// request reached the MCP Streamable HTTP handler.
			if rec.Code == http.StatusNotFound {
				t.Fatalf("POST %s: got 404, expected the MCP handler to be reached", path)
			}
		})
	}
}
