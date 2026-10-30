package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/inventory-storage/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card). They keep the names of
// the MCP tools they are derived from.
const (
	skillCheckAvailability = "check_availability"
	skillRevokeReservation = "revoke_reservation"
)

// skillRequest is the wire contract of every skill call: the first message part
// carries (as a Data part, or a Text part holding the same JSON) the skill id
// and its arguments. Strict on purpose: an agent client is machine-to-machine,
// so unknown fields are rejected, never ignored. (Go's encoding/json matches
// field names case-insensitively, so "SKU" still reaches sku; only genuinely
// unknown names are refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// Per-skill argument shapes.
type checkAvailabilityArgs struct {
	SKU string `json:"sku"`
}

type revokeReservationArgs struct {
	ReservationId string `json:"reservationId"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: a message-only agent, so a skill run
// is exactly one request mapped to one answer (ADR-0048).
func (d Deps) answer(ctx context.Context, ec *a2asrv.ExecutorContext) a2a.Event {
	req, p := parseSkillRequest(ec)
	if p != nil {
		return reply(ec, envelope{Problem: p}, p.summary())
	}
	if req.Skill != skillCheckAvailability && req.Skill != skillRevokeReservation {
		// No span: the skill name is caller-controlled, so spanning unknown
		// names would let an unauthenticated caller mint span names.
		p = problemUnknownSkill(req.Skill)
		return reply(ec, envelope{Problem: p}, p.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	res, p := d.run(ctx, req)
	if p != nil {
		span.SetStatus(codes.Error, p.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: p}, p.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: &res.data}, res.summary)
}

// run dispatches one validated request to its skill.
func (d Deps) run(ctx context.Context, req skillRequest) (skillResult, *problem) {
	if req.Skill == skillCheckAvailability {
		return d.checkAvailability(ctx, req.Arguments)
	}
	return d.revokeReservation(ctx, req.Arguments)
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first part. A
// Data part (or a Text part holding the same JSON object) is accepted; anything
// else is a request-shape problem, answered, not raised, because a malformed
// delegation is a business answer for the calling agent, not a transport
// failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields
// and trailing data. Absent arguments decode as the empty object so each
// skill's own required-field checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// checkAvailability is the read skill: the usable quantity of one SKU, through
// the same GetUsable use case (and the same SKU validation) as the MCP tool of
// the same name and GET /inventory/{sku}/usable.
func (d Deps) checkAvailability(ctx context.Context, rawArgs json.RawMessage) (skillResult, *problem) {
	var args checkAvailabilityArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return skillResult{}, problemInvalidArgument(fmt.Errorf("arguments: %w", err))
	}
	sku, err := shared.NewSKU(args.SKU)
	if err != nil {
		return skillResult{}, problemFromUseCase(err)
	}
	usable, err := d.GetUsable.Execute(ctx, sku)
	if err != nil {
		return skillResult{}, problemFromUseCase(err)
	}
	dto := availability{SKU: usable.SKU.String(), Usable: usable.Usable.Int()}
	return skillResult{
		data:    skillDTO{Availability: &dto},
		summary: fmt.Sprintf("%s: %d usable", dto.SKU, dto.Usable),
	}, nil
}

// revokeReservation is the single state-changing delegation: it revokes a
// reservation through the same use case REST and MCP use, so the
// revocable-reservation invariants and the transactional outbox behave
// identically regardless of caller.
func (d Deps) revokeReservation(ctx context.Context, rawArgs json.RawMessage) (skillResult, *problem) {
	var args revokeReservationArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return skillResult{}, problemInvalidArgument(fmt.Errorf("arguments: %w", err))
	}
	if strings.TrimSpace(args.ReservationId) == "" {
		return skillResult{}, problemInvalidArgument(errors.New("reservationId is required"))
	}
	if err := d.RevokeReservation.Execute(ctx, args.ReservationId); err != nil {
		return skillResult{}, problemFromUseCase(err)
	}
	dto := reservationRevoked{ReservationId: args.ReservationId, Revoked: true}
	return skillResult{
		data:    skillDTO{ReservationRevoked: &dto},
		summary: fmt.Sprintf("revoked reservation %s; its quantity is back in usable", dto.ReservationId),
	}, nil
}
