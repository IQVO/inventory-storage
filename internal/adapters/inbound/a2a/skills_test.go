package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// harness builds Deps over in-memory repos, seeding stock directly and
// reservations through the real ReserveStock use case with a fixed clock, the
// same approach as the MCP adapter's harness.
type harness struct {
	deps    Deps
	stock   *memory.StockRepo
	reserve *usecases.ReserveStock
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	stockRepo := memory.NewStockRepo()
	reservationRepo := memory.NewReservationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(base)
	return &harness{
		deps: Deps{
			GetUsable:         &usecases.GetUsable{Stock: stockRepo},
			RevokeReservation: &usecases.RevokeReservation{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
		},
		stock:   stockRepo,
		reserve: &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock},
	}
}

func (h *harness) seedStock(t *testing.T, id, sku, bin string, qty int) {
	t.Helper()
	q, err := shared.NewPositiveQuantity(qty)
	if err != nil {
		t.Fatal(err)
	}
	unit, err := stock.NewStockUnit(id, shared.SKU(sku), shared.BinId(bin), q)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.stock.Save(context.Background(), unit); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) seedReservation(t *testing.T, sku string, qty int) string {
	t.Helper()
	q, err := shared.NewPositiveQuantity(qty)
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.reserve.Execute(context.Background(), shared.SKU(sku), q, "demand-1")
	if err != nil {
		t.Fatal(err)
	}
	return res.ID()
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  *struct {
		Availability *struct {
			SKU    string `json:"sku"`
			Usable int    `json:"usable"`
		} `json:"availability"`
		ReservationRevoked *struct {
			ReservationId string `json:"reservationId"`
			Revoked       bool   `json:"revoked"`
		} `json:"reservationRevoked"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct {
	client *a2aclient.Client
}

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// call sends one message part and returns the decoded envelope and the
// one-line text summary of the agent's reply.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	return env, string(text)
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemTypeBase+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
	if env.Data != nil {
		t.Fatalf("a problem answer must carry no data, got %+v", env.Data)
	}
}

func TestWireCheckAvailabilityReadsUsable(t *testing.T) {
	h := newHarness(t)
	h.seedStock(t, "su-1", "SKU-1", "BIN-1", 10)
	h.seedStock(t, "su-2", "SKU-1", "BIN-2", 5)
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillCheckAvailability, map[string]any{"sku": "SKU-1"})
	a := env.Data.Availability
	if env.Problem != nil || env.Skill != skillCheckAvailability || a == nil || a.SKU != "SKU-1" || a.Usable != 15 {
		t.Fatalf("envelope = %+v", env)
	}
	if text != "SKU-1: 15 usable" {
		t.Fatalf("summary = %q", text)
	}

	// An unknown SKU is a clean zero, exactly as on the MCP tool and REST.
	env, _ = w.skill(t, skillCheckAvailability, map[string]any{"sku": "NOPE"})
	if a := env.Data.Availability; a == nil || a.Usable != 0 {
		t.Fatalf("unknown SKU = %+v, want usable 0", env)
	}
}

// TestWireRevokeReturnsQuantityToUsable proves the delegated write really
// happened: the read skill sees the quantity come back.
func TestWireRevokeReturnsQuantityToUsable(t *testing.T) {
	h := newHarness(t)
	h.seedStock(t, "su-1", "SKU-1", "BIN-1", 10)
	id := h.seedReservation(t, "SKU-1", 4)
	w := newWire(t, h.deps)

	env, _ := w.skill(t, skillCheckAvailability, map[string]any{"sku": "SKU-1"})
	if a := env.Data.Availability; a == nil || a.Usable != 6 {
		t.Fatalf("before revoke usable = %+v, want 6", env)
	}

	env, text := w.skill(t, skillRevokeReservation, map[string]any{"reservationId": id})
	r := env.Data.ReservationRevoked
	if env.Problem != nil || env.Skill != skillRevokeReservation || r == nil || r.ReservationId != id || !r.Revoked {
		t.Fatalf("envelope = %+v", env)
	}
	if text != "revoked reservation "+id+"; its quantity is back in usable" {
		t.Fatalf("summary = %q", text)
	}

	env, _ = w.skill(t, skillCheckAvailability, map[string]any{"sku": "SKU-1"})
	if a := env.Data.Availability; a == nil || a.Usable != 10 {
		t.Fatalf("after revoke usable = %+v, want 10 back", env)
	}
}

// TestWireDomainRejectionsAreAnsweredNotRaised: the REST adapter's status
// classification, delivered in-band.
func TestWireDomainRejectionsAreAnsweredNotRaised(t *testing.T) {
	h := newHarness(t)
	h.seedStock(t, "su-1", "SKU-1", "BIN-1", 10)
	id := h.seedReservation(t, "SKU-1", 4)
	w := newWire(t, h.deps)

	env, _ := w.skill(t, skillRevokeReservation, map[string]any{"reservationId": "ghost"})
	wantProblem(t, env, http.StatusNotFound, "reservation-not-found")

	if env, _ = w.skill(t, skillRevokeReservation, map[string]any{"reservationId": id}); env.Problem != nil {
		t.Fatalf("first revoke: %+v", env.Problem)
	}
	env, _ = w.skill(t, skillRevokeReservation, map[string]any{"reservationId": id})
	wantProblem(t, env, http.StatusConflict, "reservation-already-resolved")

	env, _ = w.skill(t, skillCheckAvailability, map[string]any{"sku": ""})
	wantProblem(t, env, http.StatusBadRequest, "empty-sku")
	env, _ = w.skill(t, skillCheckAvailability, map[string]any{})
	wantProblem(t, env, http.StatusBadRequest, "empty-sku")
}

func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	cases := map[string]struct {
		skill string
		args  map[string]any
		slug  string
	}{
		"check: unknown argument":     {skillCheckAvailability, map[string]any{"sku": "SKU-1", "binId": "BIN-1"}, "invalid-argument"},
		"check: typo'd argument":      {skillCheckAvailability, map[string]any{"skuu": "SKU-1"}, "invalid-argument"},
		"check: wrong type":           {skillCheckAvailability, map[string]any{"sku": 7}, "invalid-argument"},
		"revoke: missing id":          {skillRevokeReservation, map[string]any{}, "invalid-argument"},
		"revoke: blank id":            {skillRevokeReservation, map[string]any{"reservationId": "   "}, "invalid-argument"},
		"revoke: unknown argument":    {skillRevokeReservation, map[string]any{"reservationId": "r1", "force": true}, "invalid-argument"},
		"revoke: typo'd argument":     {skillRevokeReservation, map[string]any{"reservationID": "r1", "reason": "x"}, "invalid-argument"},
		"revoke: wrong type":          {skillRevokeReservation, map[string]any{"reservationId": 12}, "invalid-argument"},
		"revoke: id and extra object": {skillRevokeReservation, map[string]any{"reservationId": "r1", "extra": map[string]any{}}, "invalid-argument"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, _ := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, http.StatusBadRequest, tc.slug)
		})
	}
}

// TestWireNoSkillRunsOnRejectedArguments: a refused argument shape must never
// reach the write use case (the strict decoder is the guard).
func TestWireNoSkillRunsOnRejectedArguments(t *testing.T) {
	h := newHarness(t)
	h.seedStock(t, "su-1", "SKU-1", "BIN-1", 10)
	id := h.seedReservation(t, "SKU-1", 4)
	w := newWire(t, h.deps)

	env, _ := w.skill(t, skillRevokeReservation, map[string]any{"reservationId": id, "dryRun": true})
	wantProblem(t, env, http.StatusBadRequest, "invalid-argument")

	env, _ = w.skill(t, skillCheckAvailability, map[string]any{"sku": "SKU-1"})
	if a := env.Data.Availability; a == nil || a.Usable != 6 {
		t.Fatalf("usable = %+v, want 6: a rejected request must not have revoked the reservation", env)
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	h := newHarness(t)
	h.seedStock(t, "su-1", "SKU-1", "BIN-1", 3)
	w := newWire(t, h.deps)

	env, _ := w.call(t, a2a.NewTextPart("not json at all"))
	wantProblem(t, env, http.StatusBadRequest, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}))
	wantProblem(t, env, http.StatusBadRequest, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": skillCheckAvailability, "arguments": map[string]any{"sku": "SKU-1"}, "extra": 1}))
	wantProblem(t, env, http.StatusBadRequest, "malformed-skill-request")

	// Trailing data after the JSON document is refused too.
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"check_availability","arguments":{"sku":"SKU-1"}} {"skill":"revoke_reservation"}`))
	wantProblem(t, env, http.StatusBadRequest, "malformed-skill-request")

	env, _ = w.skill(t, "launch_rocket", map[string]any{})
	wantProblem(t, env, http.StatusBadRequest, "unknown-skill")
	if env.Skill != "" {
		t.Fatalf("an unknown skill must not be echoed as the envelope skill, got %q", env.Skill)
	}

	// A text part holding the same JSON is accepted (documented contract).
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"check_availability","arguments":{"sku":"SKU-1"}}`))
	if env.Problem != nil || env.Data == nil || env.Data.Availability == nil || env.Data.Availability.Usable != 3 {
		t.Fatalf("a text part holding the skill JSON must be accepted, got %+v", env)
	}
}

// TestCardIsServedAndDialable proves discovery end to end: the card handler
// serves the card at the well-known path and a client built from the FETCHED
// card reaches the agent.
func TestCardIsServedAndDialable(t *testing.T) {
	h := newHarness(t)
	h.seedStock(t, "su-1", "SKU-1", "BIN-1", 3)
	mux := http.NewServeMux()
	mux.Handle("/", Handler(NewExecutor(h.deps)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(Card(Config{AgentURL: srv.URL + "/", Version: "test"})))

	res, err := http.Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("card fetch = %v / %v", res, err)
	}
	defer func() { _ = res.Body.Close() }()
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	client, err := a2aclient.NewFromCard(context.Background(), &card)
	if err != nil {
		t.Fatal(err)
	}
	w := &wire{client: client}
	env, _ := w.skill(t, skillCheckAvailability, map[string]any{"sku": "SKU-1"})
	if env.Data == nil || env.Data.Availability == nil {
		t.Fatalf("a client built from the served card could not call the agent: %+v", env)
	}
}

// ---- the REST-parity problem classification --------------------------------

// TestProblemClassificationMatchesTheRESTAdapter pins, per typed error, the
// status the REST adapter's statusFor gives it (404 not found, 400 bad request,
// 409 business rule, 500 unknown), including through wrapping.
func TestProblemClassificationMatchesTheRESTAdapter(t *testing.T) {
	cases := []struct {
		err    error
		status int
		slug   string
	}{
		{usecases.ErrReservationNotFound, 404, "reservation-not-found"},
		{usecases.ErrStockUnitNotFound, 404, "stock-unit-not-found"},
		{shared.ErrEmptySKU, 400, "empty-sku"},
		{reservation.ErrAlreadyResolved, 409, "reservation-already-resolved"},
		{reservation.ErrExpired, 409, "reservation-expired"},
		{stock.ErrInsufficientReserved, 409, "insufficient-reserved"},
		{location.ErrReleaseExceedsOccupancy, 409, "release-exceeds-occupancy"},
		{usecases.ErrConcurrentModification, 409, "concurrent-modification"},
		{fmt.Errorf("wrapped: %w", usecases.ErrConcurrentModification), 409, "concurrent-modification"},
	}
	for _, tc := range cases {
		t.Run(tc.slug, func(t *testing.T) {
			p := problemFromUseCase(tc.err)
			if p.Status != tc.status || p.Type != problemTypeBase+tc.slug || p.Title == "" || p.Detail == "" {
				t.Fatalf("problem = %+v, want %d %s", p, tc.status, tc.slug)
			}
		})
	}
}

// TestInternalFailuresAreAnsweredWithoutLeakingTheCause: an unrecognised
// failure (say, a database error naming a DSN) is a 500 problem whose detail is
// adapter-owned text, never the raw error.
func TestInternalFailuresAreAnsweredWithoutLeakingTheCause(t *testing.T) {
	p := problemFromUseCase(errors.New(`dial tcp 10.0.0.9:5432: password authentication failed for user "inventory"`))
	if p.Status != http.StatusInternalServerError || p.Type != problemTypeBase+"internal-error" {
		t.Fatalf("problem = %+v, want 500 internal-error", p)
	}
	for _, s := range []string{p.Detail, p.Title, p.summary()} {
		if strings.Contains(s, "10.0.0.9") || strings.Contains(s, "password") || strings.Contains(s, "inventory\"") {
			t.Fatalf("the 500 answer leaks the underlying error: %q", s)
		}
	}
}

// ---- observability ----------------------------------------------------------

// TestEverySkillRunsInASpanCarryingItsId pins the OTel contract mirrored from
// the MCP adapter's mcp.tool span, and that an unknown (caller-controlled)
// skill name never mints a span.
func TestEverySkillRunsInASpanCarryingItsId(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	h := newHarness(t)
	h.seedStock(t, "su-1", "SKU-1", "BIN-1", 3)
	w := newWire(t, h.deps)
	w.skill(t, skillCheckAvailability, map[string]any{"sku": "SKU-1"})
	w.skill(t, skillRevokeReservation, map[string]any{"reservationId": "ghost"})
	w.skill(t, "launch_rocket", map[string]any{})

	got := map[string]string{}
	for _, s := range recorder.Ended() {
		for _, kv := range s.Attributes() {
			if string(kv.Key) == "a2a.skill" {
				got[s.Name()] = kv.Value.AsString()
			}
		}
		if strings.Contains(s.Name(), "launch_rocket") {
			t.Errorf("an unknown skill minted the span %q", s.Name())
		}
	}
	for _, id := range []string{skillCheckAvailability, skillRevokeReservation} {
		if got["a2a.skill "+id] != id {
			t.Errorf("no span %q with a2a.skill=%q; spans = %v", "a2a.skill "+id, id, got)
		}
	}
}
