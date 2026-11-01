package http

import (
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// Server holds every use case the HTTP adapter depends on.
type Server struct {
	ReceiveStock               *usecases.ReceiveStock
	StowStock                  *usecases.StowStock
	ReserveStock               *usecases.ReserveStock
	RevokeReservation          *usecases.RevokeReservation
	ConfirmPick                *usecases.ConfirmPick
	GetUsable                  *usecases.GetUsable
	GetReservationsByDemandRef *usecases.GetReservationsByDemandRef
	RunCycleCount              *usecases.RunCycleCount
	// RegisterBin backs PUT /bins/{binId}: declarative, idempotent bin
	// registration/resize by inventory control (ADR 0025).
	RegisterBin *usecases.RegisterBin
	// GetBin backs GET /bins/{binId}: capacity/occupancy read.
	GetBin *usecases.GetBin
	// StageTransferReceipt backs POST /transfers/{transferLineId}/receipt
	// (ADR 0031): destination scan -> STAGED receipt or quarantine.
	StageTransferReceipt *usecases.StageTransferReceipt
	// StowTransferStock backs POST /transfers/{transferLineId}/stow
	// (ADR 0031): STAGED receipt -> destination StockUnits (the only
	// path that raises destination usable).
	StowTransferStock *usecases.StowTransferStock
	// RelocateStock backs POST /relocations (ADR 0039): intra-site move of
	// one SKU between two bins, behind RequireIdempotencyKey.
	RelocateStock *usecases.RelocateStock
	// GetRelocation backs GET /relocations/{id}.
	GetRelocation *usecases.GetRelocation
	// StowContainer, BreakCase, GetContainer and ListBinContainers back the
	// container endpoints (ADR 0045); ReserveFullCases serves POST
	// /reservations when the body asks for whole cases.
	StowContainer     *usecases.StowContainer
	BreakCase         *usecases.BreakCase
	GetContainer      *usecases.GetContainer
	ListBinContainers *usecases.ListBinContainers
	ReserveFullCases  *usecases.ReserveFullCases
	// Classifications backs the deprecated read-only GET endpoint: the
	// local copy of product-master's classifications (ADR 0034). There is
	// no dedicated use case because the read is a direct, no-invariant
	// repo lookup.
	Classifications ports.ProductClassificationRepo
	// IdempotencyPool, when non-nil, wires RequireIdempotencyKey onto
	// POST /stock/receive and POST /reservations (see idempotency.go) —
	// this service's two true resource-creation endpoints (a StagedReceipt
	// acknowledgment and a Reservation, both with server-generated
	// identity). A nil pool means "no transactional Postgres backing
	// wired" (in-memory dev/test configuration) — the idempotency
	// middleware needs a real pgxpool.Pool to begin its own transaction,
	// so it is simply not applied in that case, exactly this codebase's
	// existing convention for every other optional Postgres-backed
	// capability (UnitOfWork, the outbox relay).
	IdempotencyPool *pgxpool.Pool
	// Readiness backs GET /readyz (ADR-0020 §graceful shutdown):
	// flipped to not-ready as the FIRST step of shutdown, so a
	// Kubernetes readinessProbe can stop routing new traffic before
	// the HTTP server itself stops accepting connections. A nil
	// Readiness (every existing test/caller that predates this field)
	// leaves /readyz always reporting ready.
	Readiness *Readiness
}

// DefaultServiceName labels this service's spans and metrics when the caller
// does not supply one. It matches the OTel resource's service.name.
const DefaultServiceName = "inventory-storage"

// defaultCORSAllowedOrigins is the local-dev default for CORS_ALLOWED_ORIGINS:
// the warehouse-console shell (localhost:5173) and this service's own future
// MFE remote dev origin (localhost:5182). Overridable via the env var
// (comma-separated) for other environments.
const defaultCORSAllowedOrigins = "http://localhost:5173,http://localhost:5182"

// NewRouter builds the chi router for every endpoint in CLAUDE.md's REST API.
// A nil logger defaults to slog.Default(); an empty serviceName defaults to
// DefaultServiceName.
//
// Middleware order matters here. otelchi runs before RequestLogger so the
// request context already carries a span by the time a line is logged, which
// is what lets the telemetry slog handler stamp trace_id/span_id onto it.
// WithChiRoutes resolves the route pattern up front, so spans are named
// "/reservations/{id}" rather than one distinct name per reservation id.
func NewRouter(s *Server, logger *slog.Logger, serviceName string, opts ...RouterOption) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = DefaultServiceName
	}
	var cfg routerConfig
	for _, o := range opts {
		o(&cfg)
	}

	r := chi.NewRouter()

	// An unmatched path (for example a container lpn outside [A-Za-z0-9._-])
	// answers RFC 7807 like every other error, not chi's text/plain 404.
	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		writeProblem(w, http.StatusNotFound, problemInfo{"route-not-found", "No such resource"}, "no route matches "+req.Method+" "+req.URL.Path, req.URL.Path)
	})
	metricCfg := otelchimetric.NewBaseConfig(serviceName)

	// rejectNUL is first: a NUL (U+0000) in the path, query or body is a
	// client error before any other middleware (including the
	// Idempotency-Key reservation) or Postgres text parameter sees it.
	r.Use(rejectNUL)
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	// Emits http.server.request.duration (seconds) per OTel HTTP semantic
	// conventions; no hand-rolled histogram needed.
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)
	// Allows browser SPAs on a different origin (e.g. the warehouse-console
	// shell, or this service's own MFE remote) to call this API directly.
	// CORS_ALLOWED_ORIGINS is comma-separated, defaulting to the two local
	// dev origins. No credentials are needed — this API has no cookie-based
	// auth, so the browser doesn't need cross-origin credentialed requests.
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsAllowedOrigins(),
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		AllowedHeaders:   []string{"Accept", "Content-Type", "Authorization", "Idempotency-Key"},
		AllowCredentials: false,
	}))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	// POST /stock/receive and POST /reservations are route-scoped (r.With,
	// not r.Use) behind RequireIdempotencyKey — they are the two mutating
	// endpoints that create a NEW resource with a server-generated id
	// (a StagedReceipt acknowledgment, a Reservation), so a lost response
	// and a client retry would otherwise create a duplicate. The other
	// mutating routes act on a caller-supplied {id}/{binId}/{sku} and are
	// lower priority for v1 (see the ADR). IdempotencyPool nil (in-memory
	// dev/test configuration, no transactional Postgres backing) skips
	// the middleware entirely, mirroring every other optional
	// Postgres-backed capability's nil convention in this repo.
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/stock/receive", s.handleReceiveStock)
	} else {
		r.Post("/stock/receive", s.handleReceiveStock)
	}
	r.Post("/stock/stow", s.handleStowStock)
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/reservations", s.handleReserveStock)
	} else {
		r.Post("/reservations", s.handleReserveStock)
	}
	r.Get("/reservations", s.handleGetReservationsByDemandRef)
	r.Delete("/reservations/{id}", s.handleRevokeReservation)
	r.Post("/reservations/{id}/confirm-pick", s.handleConfirmPick)
	r.Get("/inventory/{sku}/usable", s.handleGetUsable)
	r.Put("/bins/{binId}", s.handleRegisterBin)
	r.Get("/bins/{binId}", s.handleGetBin)
	r.Post("/bins/{binId}/cycle-count", s.handleRunCycleCount)

	// The two destination transfer-custody endpoints (ADR 0031), both
	// behind RequireIdempotencyKey when a transactional pool is wired:
	// a stage creates a receipt (server-correlated state) and a stow
	// creates destination StockUnits, so a lost response plus a client
	// retry must never double-apply. The use cases are idempotent on
	// transfer_line_id at the DATABASE level too — the middleware is
	// defense in depth that also replays the original HTTP response.
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/transfers/{transferLineId}/receipt", s.handleStageTransferReceipt)
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/transfers/{transferLineId}/stow", s.handleStowTransferStock)
	} else {
		r.Post("/transfers/{transferLineId}/receipt", s.handleStageTransferReceipt)
		r.Post("/transfers/{transferLineId}/stow", s.handleStowTransferStock)
	}

	// Intra-site relocation (ADR 0039): POST creates a relocation with a
	// server-generated id and moves stock, so it sits behind
	// RequireIdempotencyKey exactly like the other creating POSTs.
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/relocations", s.handleRelocateStock)
	} else {
		r.Post("/relocations", s.handleRelocateStock)
	}
	r.Get("/relocations/{id}", s.handleGetRelocation)

	// Containers (ADR 0045). The custom-method suffix is part of the path
	// ("/containers/{lpn}:stow"); an LPN is [A-Za-z0-9._-] so it never holds
	// a colon. Stow and break act on a caller-supplied lpn and are naturally
	// idempotent (a repeat stow is a no-op, a repeat break is 409); the
	// Idempotency-Key middleware (ADR 0018) is defence in depth that also
	// replays the original response.
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/containers/{lpn:[A-Za-z0-9._-]+}:stow", s.handleStowContainer)
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/containers/{lpn:[A-Za-z0-9._-]+}:break", s.handleBreakCase)
	} else {
		r.Post("/containers/{lpn:[A-Za-z0-9._-]+}:stow", s.handleStowContainer)
		r.Post("/containers/{lpn:[A-Za-z0-9._-]+}:break", s.handleBreakCase)
	}
	r.Get("/containers/{lpn:[A-Za-z0-9._-]+}", s.handleGetContainer)
	r.Get("/bins/{binId}/containers", s.handleListBinContainers)

	return r
}

// RouterOption customises NewRouter without widening its signature for
// every caller. No options remain currently — the fleet REST identity
// middleware that used to be the sole RouterOption was removed; this stays
// as an empty extension point rather than breaking NewRouter's/
// NewReportsRouter's signature.
type RouterOption func(*routerConfig)

type routerConfig struct{}

// corsAllowedOrigins reads CORS_ALLOWED_ORIGINS (comma-separated), falling
// back to defaultCORSAllowedOrigins for local dev when unset/empty.
func corsAllowedOrigins() []string {
	raw := os.Getenv("CORS_ALLOWED_ORIGINS")
	if raw == "" {
		raw = defaultCORSAllowedOrigins
	}
	origins := strings.Split(raw, ",")
	for i, o := range origins {
		origins[i] = strings.TrimSpace(o)
	}
	return origins
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
