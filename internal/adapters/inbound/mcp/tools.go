package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for MCP tool spans.
const tracerName = "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"

// Deps is everything the MCP tools need, injected by the composition root.
// It carries the same use cases the HTTP adapter uses plus the narrow read
// port; the adapter never constructs an outbound adapter itself.
type Deps struct {
	// GetUsable is the existing read-model use case, reused unchanged. It
	// answers "how much of a SKU is immediately available to fulfil".
	GetUsable *usecases.GetUsable
	// RevokeReservation is the existing write use case, reused unchanged. Its
	// domain semantics (revocation returns quantity to usable, and revoking a
	// missing reservation is a clean typed error) make a model-invoked
	// revocation safe by construction — it never strands or destroys stock.
	RevokeReservation *usecases.RevokeReservation
	// Stock is the read-only query port for the bin-occupancy diagnostic.
	Stock StockQueries
	// Reports is the optional client of the inventory-reports REST service.
	// When non-nil, the curated read-only Inventory Flow & Accuracy report
	// tool is registered; when nil, that tool is simply not exposed. It is a
	// narrow port so the analytical read model is reached only through the
	// reports REST surface, never the analytical database directly (ADR-0011).
	Reports ReportsClient
	// GetRelocation backs the read-only get_relocation tool (ADR 0039): the
	// same use case as GET /relocations/{id}. The tool is always advertised;
	// nil makes it answer a clean "not configured" tool error.
	GetRelocation *usecases.GetRelocation
	// GetContainer backs the read-only get_container tool (ADR 0045): the same
	// use case as GET /containers/{lpn}. The tool is always advertised; nil
	// makes it answer a clean "not configured" tool error.
	GetContainer *usecases.GetContainer
}

// --- check_availability -------------------------------------------------------

type checkAvailabilityInput struct {
	SKU string `json:"sku" jsonschema:"the stock-keeping unit to check usable availability for"`
}

type checkAvailabilityOutput struct {
	SKU    string `json:"sku"`
	Usable int    `json:"usable"`
}

func (d Deps) checkAvailability(ctx context.Context, in checkAvailabilityInput) (checkAvailabilityOutput, error) {
	sku, err := shared.NewSKU(in.SKU)
	if err != nil {
		return checkAvailabilityOutput{}, err
	}
	usable, err := d.GetUsable.Execute(ctx, sku)
	if err != nil {
		return checkAvailabilityOutput{}, err
	}
	return checkAvailabilityOutput{SKU: usable.SKU.String(), Usable: usable.Usable.Int()}, nil
}

// --- get_bin_occupancy --------------------------------------------------------

type binOccupancyInput struct {
	BinId string `json:"binId" jsonschema:"the coded bin/location slot to inspect"`
}

func (d Deps) getBinOccupancy(ctx context.Context, in binOccupancyInput) (binOccupancy, error) {
	binID, err := shared.NewBinId(in.BinId)
	if err != nil {
		return binOccupancy{}, err
	}
	units, err := d.Stock.FindByBin(ctx, binID)
	if err != nil {
		return binOccupancy{}, err
	}
	return toBinOccupancy(binID.String(), units), nil
}

// --- revoke_reservation (write) -----------------------------------------------

type revokeReservationInput struct {
	ReservationId string `json:"reservationId" jsonschema:"the id of the reservation to revoke; its quantity is returned to usable"`
}

type revokeReservationOutput struct {
	ReservationId string `json:"reservationId"`
	Revoked       bool   `json:"revoked"`
}

func (d Deps) revokeReservation(ctx context.Context, in revokeReservationInput) (revokeReservationOutput, error) {
	if in.ReservationId == "" {
		return revokeReservationOutput{}, fmt.Errorf("reservationId is required")
	}
	err := d.RevokeReservation.Execute(ctx, in.ReservationId)
	if err != nil {
		// The use case's domain errors (reservation not found, stock unit
		// not found, already revoked) surface unchanged as the tool error.
		// Revocation is revocable-by-design: it returns quantity to usable
		// rather than consuming or destroying stock, so a mistaken model
		// call is recoverable.
		return revokeReservationOutput{}, err
	}
	return revokeReservationOutput{ReservationId: in.ReservationId, Revoked: true}, nil
}

// --- registration -------------------------------------------------------------

// registerTools adds every tool to the server, each wrapped so its handler
// runs inside an OTel span named "mcp.tool <name>".
func (d Deps) registerTools(server *mcp.Server) {
	readOnly := true

	addTool(server, &mcp.Tool{
		Name:        "check_availability",
		Description: "Return the usable quantity for a SKU: on-hand across all its bins minus active reservations and held/unlocated stock. Usable, not total, is what constrains a release.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.checkAvailability)

	addTool(server, &mcp.Tool{
		Name:        "get_bin_occupancy",
		Description: "Return what a single bin holds: its total on-hand, reserved, and usable quantities, plus a per-StockUnit breakdown (SKU, quantities, state).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getBinOccupancy)

	// Read-only relocation lookup (ADR 0039).
	d.registerRelocationTool(server)

	// Read-only container lookup (ADR 0045).
	d.registerContainerTool(server)

	// Write tool: revokes a reservation, returning its quantity to usable.
	// Annotated destructive (non-read-only) so a host can see it changes
	// state before letting a model call it. Revocation is revocable by
	// design — it never strands or consumes stock — which bounds the risk
	// of a mistaken call.
	destructive := true
	notIdempotent := false
	addTool(server, &mcp.Tool{
		Name:        "revoke_reservation",
		Description: "Revoke a reservation, returning its bound quantity to usable inventory so a failed physical delivery never strands an order. Rejected if the reservation is not found or already revoked.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive, IdempotentHint: notIdempotent},
	}, d.revokeReservation)

	// Curated read-only analytics report tool, registered only when a reports
	// client is wired (Deps.Reports != nil).
	d.registerReportTool(server)
}

// addTool registers one tool. It centralises the cross-cutting concerns
// every tool shares: a span per call and mapping a handler error onto the
// span before returning it.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		var zero Out
		ctx, span := otel.Tracer(tracerName).Start(ctx, "mcp.tool "+tool.Name,
			trace.WithAttributes(
				attribute.String("mcp.tool.name", tool.Name),
			),
		)
		defer span.End()

		out, err := handle(ctx, in)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, zero, err
		}
		return nil, out, nil
	})
}
