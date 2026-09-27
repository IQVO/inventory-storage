// Shared harness for the MCP eval suites (E1-E3): one place that builds a
// real Streamable HTTP server over in-memory adapters and connects a real
// SDK client to it, so schema evals, wire conformance evals, and the
// Gherkin behavioral evals all exercise exactly the surface a model host
// would.
package mcp_test

import (
	"context"
	"net/http/httptest"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// evalHarness is a fully wired MCP server over in-memory repos plus the
// client session talking to it, with the knobs the evals assert on.
type evalHarness struct {
	session         *sdk.ClientSession
	server          *httptest.Server
	stock           *memory.StockRepo
	reservations    *memory.ReservationRepo
	publisher       *events.BufferedPublisher
	clock           *memory.FixedClock
	reservationID   string
	lastCallResult  *sdk.CallToolResult
	lastCallErr     error
	lastCallContent string
}

// newEvalDeps builds the read-only tool surface over empty in-memory
// repos — enough for schema and conformance evals that do not seed state.
func newEvalDeps() inboundmcp.Deps {
	stockRepo := memory.NewStockRepo()
	return inboundmcp.Deps{
		GetUsable: &usecases.GetUsable{Stock: stockRepo},
		Stock:     stockRepo,
	}
}

// newEvalHarness seeds the canonical eval state (SKU-A@BIN-1 qty 10, one
// reservation of 4 against it — the same contract server_test.go's seed
// provides) over a real Streamable HTTP server with the write tool wired,
// and connects a client session to it.
func newEvalHarness(t *testing.T) *evalHarness {
	t.Helper()

	h := &evalHarness{}
	h.stock, h.reservations, h.publisher, h.clock, h.reservationID = seed(t)

	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetUsable:         &usecases.GetUsable{Stock: h.stock},
		RevokeReservation: &usecases.RevokeReservation{Stock: h.stock, Reservations: h.reservations, Events: h.publisher, Clock: h.clock},
		Stock:             h.stock,
	})
	h.server = httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(h.server.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: h.server.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("eval harness connect: %v", err)
	}
	h.session = session
	t.Cleanup(func() { _ = session.Close() })
	return h
}

// wireSession builds a real Streamable HTTP server over the given deps and
// connects a client session to it, for evals that do not need seeded
// state.
func wireSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: httpSrv.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("wire session connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// callTool invokes a tool and records the result for the Then steps.
func (h *evalHarness) callTool(ctx context.Context, name string, args map[string]any) error {
	res, err := h.session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	h.lastCallResult, h.lastCallErr = res, err
	h.lastCallContent = ""
	if res != nil {
		for _, c := range res.Content {
			if text, ok := c.(*sdk.TextContent); ok {
				h.lastCallContent += text.Text
			}
		}
	}
	return err
}
