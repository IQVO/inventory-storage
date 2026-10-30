package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inbounda2a "github.com/claudioed/inventory-storage/internal/adapters/inbound/a2a"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

const testAgentURL = "http://inventory-storage-a2a.warehouse-systems.svc.cluster.local:8093/"

// newTestRouter builds the exact HTTP surface cmd/a2a serves, with the real
// inbound A2A adapter over in-memory adapters and the real Agent Card.
func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	deps := inbounda2a.Deps{GetUsable: &usecases.GetUsable{Stock: memory.NewStockRepo()}}
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: "test"})
	return newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card, "inventory-storage-a2a-test")
}

func TestHealthzIsUnauthenticated(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

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

func TestServesTheAgentCardAtTheWellKnownPath(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET agent card: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{`"name":"inventory-storage"`, testAgentURL, `"check_availability"`, `"revoke_reservation"`} {
		if !strings.Contains(body, want) {
			t.Errorf("agent card lacks %s: %s", want, body)
		}
	}
	if strings.Contains(body, "securitySchemes") {
		t.Errorf("agent card declares security schemes, but the surface is unauthenticated: %s", body)
	}
}

// TestBothMountsReachTheJSONRPCHandler pins the fleet's <CTX>_A2A_ENDPOINT
// convention: the endpoint answers at "/" and at "/a2a".
func TestBothMountsReachTheJSONRPCHandler(t *testing.T) {
	h := newTestRouter(t)
	for _, path := range []string{"/", "/a2a"} {
		t.Run(path, func(t *testing.T) {
			// An unknown skill is answered in-band before any use case runs,
			// so a JSON-RPC result proves the request reached the SDK handler
			// at this mount point.
			body := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"data":{"skill":"launch_rocket","arguments":{}}}]}}}`
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("A2A-Version", "1.0")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			raw, _ := io.ReadAll(rec.Body)
			if rec.Code == http.StatusNotFound || !strings.Contains(string(raw), `"jsonrpc"`) {
				t.Fatalf("POST %s: %d %q does not look like a JSON-RPC reply", path, rec.Code, raw)
			}
			if !strings.Contains(string(raw), "unknown-skill") {
				t.Fatalf("POST %s: reply %q lacks the in-band unknown-skill problem", path, raw)
			}
		})
	}
}

// TestHealthzIsGETOnly: only GET /healthz is the probe; POST /healthz is
// not the probe and must not answer 200 {"status":"ok"}.
func TestHealthzIsGETOnly(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if rec.Code == http.StatusOK && strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("POST /healthz answered the probe body")
	}
}
