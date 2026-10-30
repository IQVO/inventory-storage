// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// same application-layer use cases the HTTP and MCP adapters use. It is built
// on the official A2A Go SDK (a2aproject/a2a-go) and speaks JSON-RPC 2.0 over
// HTTP, with the Agent Card served at the well-known path.
//
// Per ADR-0048, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state through its own use cases. It is
// not a data-sync channel: facts between contexts still travel exclusively as
// CloudEvents on Kafka (ADR-0004, ADR-0024), including the event a write skill
// raises through the transactional outbox (ADR-0017).
//
// Like the MCP adapter (ADR-0008), this package depends inward on the
// application layer (use cases) and the domain only, never on an outbound
// adapter. The composition root (cmd/a2a) wires concrete repositories into the
// use cases. Domain structs never leak across the message boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the same use cases the HTTP and MCP adapters use; the adapter
// never constructs an outbound adapter itself.
type Deps struct {
	// GetUsable is the existing read-model use case, reused unchanged: how much
	// of a SKU is immediately available to fulfil (fleet-owned stock).
	GetUsable *usecases.GetUsable
	// RevokeReservation is the existing write use case, reused unchanged. Its
	// domain semantics (revocation returns quantity to usable, a missing or
	// already-resolved reservation is a clean typed error) make a delegated
	// revocation safe by construction: it never strands or destroys stock.
	RevokeReservation *usecases.RevokeReservation
}

// Card builds this agent's Agent Card (ADR-0048). The surface is two curated,
// goal-shaped skills, derived from the MCP tools check_availability and
// revoke_reservation (the context's MCP charter already offers exactly this
// read and this write; nothing is widened), not one skill per endpoint.
// check_availability answers "how much of this SKU can I promise";
// revoke_reservation is the single state-changing delegation. The card
// declares no security schemes on purpose: this surface is unauthenticated by
// the same fleet-wide decision as REST and MCP (ADR-0015); schemes are added
// here, not in a redesign, if that decision is ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "inventory-storage",
		Description: "Inventory & Storage agent for the Warehouse Management System: answers how much of a SKU is usable (on-hand minus active reservations and held or unlocated stock, which is what actually constrains a release) and revokes a reservation so its quantity returns to usable. It reports and releases stock from this context's own ledger; it never stows, receives, picks or moves stock.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/inventory-storage",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillCheckAvailability,
				Name:        "Usable availability of a SKU",
				Description: "Read the usable quantity of one SKU: on-hand across all its bins minus active reservations and held or unlocated stock, counted for fleet-owned stock. Usable, not total on-hand, is what constrains a release. Changes nothing. Unknown SKUs answer 0.",
				Tags:        []string{"inventory", "availability", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"check_availability","arguments":{"sku":"SKU-1"}}`,
				},
			},
			{
				ID:          skillRevokeReservation,
				Name:        "Revoke a reservation",
				Description: "Revokes an active reservation, returning its bound quantity to usable inventory so a failed physical delivery never strands an order, and publishes ReservationRevoked (plus the bin stock-level facts) through the transactional outbox. Rejected as a problem answer, with nothing written, when the reservation is unknown or already resolved (revoked, confirmed or expired). NOT idempotent: a second call on the same reservation is a 409, so a caller may treat that as already done.",
				Tags:        []string{"inventory", "reservation", "write"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"revoke_reservation","arguments":{"reservationId":"res-123"}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are transactional request/response work, so
// there is no long-running Task lifecycle to track in this first A2A
// surface. The response carries the caller's contextId (and taskId when the
// client referenced one) so a delegating saga can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(deps.answer(ctx, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler: the only transport this agent
// ships (ADR-0048), mirroring ADR-0008's "Streamable HTTP only" minimalism.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
