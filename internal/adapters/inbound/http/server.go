package http

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
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
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
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
	ClassifyProduct            *usecases.ClassifyProduct
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
	// Classifications backs the read-only GET endpoint. It is the same
	// port ClassifyProduct writes through; there is no dedicated
	// "GetProductClassification" use case because the read is a direct,
	// no-invariant repo lookup — consistent with how GetUsable is the
	// only use case that reads without also writing.
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
	metricCfg := otelchimetric.NewBaseConfig(serviceName)

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
	r.Put("/products/{sku}/classification", s.handleClassifyProduct)
	r.Get("/products/{sku}/classification", s.handleGetProductClassification)

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

func (s *Server) handleReceiveStock(w http.ResponseWriter, r *http.Request) {
	var req receiveStockRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		writeError(w, r, err)
		return
	}
	qty, err := shared.NewPositiveQuantity(req.Quantity)
	if err != nil {
		writeError(w, r, err)
		return
	}

	receipt, err := s.ReceiveStock.Execute(r.Context(), sku, qty)
	if err != nil {
		writeError(w, r, err)
		return
	}

	// 202 Accepted, not 201 Created: a staged receipt has no durable
	// identity of its own (no ID, no GET route) — it is an acknowledgment
	// that stock has been accepted into the building, not yet a queryable
	// resource. The addressable resource (a StockUnit) is created later, at
	// StowStock.
	writeJSON(w, http.StatusAccepted, stagedReceiptResponse{
		SKU:        receipt.SKU.String(),
		Quantity:   receipt.Quantity.Int(),
		ReceivedAt: receipt.ReceivedAt.Format(timeFormat),
	})
}

func (s *Server) handleStowStock(w http.ResponseWriter, r *http.Request) {
	var req stowStockRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		writeError(w, r, err)
		return
	}
	qty, err := shared.NewPositiveQuantity(req.Quantity)
	if err != nil {
		writeError(w, r, err)
		return
	}
	binID, err := shared.NewBinId(req.BinID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	unit, err := s.StowStock.Execute(r.Context(), sku, qty, binID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/stock/"+unit.ID())
	writeJSON(w, http.StatusCreated, toStockUnitResponse(unit))
}

func (s *Server) handleReserveStock(w http.ResponseWriter, r *http.Request) {
	var req reserveStockRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	sku, err := shared.NewSKU(req.SKU)
	if err != nil {
		writeError(w, r, err)
		return
	}
	qty, err := shared.NewPositiveQuantity(req.Quantity)
	if err != nil {
		writeError(w, r, err)
		return
	}

	// demandRef is the idempotency key ReserveStock retries on and the key
	// GET /reservations?demandRef= looks up by — an empty one would make
	// every such reservation unreachable, so reject it like the GET does.
	if req.DemandRef == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-demand-ref", "demandRef is required"}, "demandRef must not be empty", r.URL.Path)
		return
	}

	res, err := s.ReserveStock.Execute(r.Context(), sku, qty, req.DemandRef)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/reservations/"+res.ID())
	writeJSON(w, http.StatusCreated, toReservationResponse(res))
}

func (s *Server) handleRevokeReservation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.RevokeReservation.Execute(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleGetReservationsByDemandRef backs GET /reservations?demandRef=<ref>.
// demandRef is required (400 if missing/empty) since this is a lookup-by-key
// endpoint, not a list-all — there is no meaningful "give me everything"
// response here.
func (s *Server) handleGetReservationsByDemandRef(w http.ResponseWriter, r *http.Request) {
	demandRef := r.URL.Query().Get("demandRef")
	if demandRef == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-demand-ref", "demandRef query parameter is required"}, "demandRef query parameter must not be empty", r.URL.Path)
		return
	}

	reservations, err := s.GetReservationsByDemandRef.Execute(r.Context(), demandRef)
	if err != nil {
		writeError(w, r, err)
		return
	}

	responses := make([]reservationResponse, 0, len(reservations))
	for _, res := range reservations {
		responses = append(responses, toReservationResponse(res))
	}
	writeJSON(w, http.StatusOK, responses)
}

func (s *Server) handleConfirmPick(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.ConfirmPick.Execute(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetUsable(w http.ResponseWriter, r *http.Request) {
	skuParam := chi.URLParam(r, "sku")
	sku, err := shared.NewSKU(skuParam)
	if err != nil {
		writeError(w, r, err)
		return
	}

	usable, err := s.GetUsable.Execute(r.Context(), sku)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, usableInventoryResponse{SKU: usable.SKU.String(), Usable: usable.Usable.Int()})
}

func (s *Server) handleRunCycleCount(w http.ResponseWriter, r *http.Request) {
	binIDParam := chi.URLParam(r, "binId")
	binID, err := shared.NewBinId(binIDParam)
	if err != nil {
		writeError(w, r, err)
		return
	}

	var req cycleCountRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.CountedQuantity == nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"counted-quantity-required", "countedQuantity is required"}, "countedQuantity must be present; an explicit 0 is a valid empty-bin count, omitting it is not", r.URL.Path)
		return
	}
	countedQty, err := shared.NewQuantity(int(*req.CountedQuantity))
	if err != nil {
		writeError(w, r, err)
		return
	}

	result, err := s.RunCycleCount.Execute(r.Context(), binID, countedQty)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, cycleCountResponse{
		BinID:       result.BinID.String(),
		CountedQty:  result.CountedQty.Int(),
		SystemQty:   result.SystemQty.Int(),
		Discrepancy: result.Discrepancy,
	})
}

// handleRegisterBin backs PUT /bins/{binId}. It is idempotent and
// declarative: the body states the desired capacity and the response is
// 201 when the bin was created, 200 when it already existed (unchanged or
// resized). A capacity below current occupancy is a 409
// (capacity-below-occupancy).
func (s *Server) handleRegisterBin(w http.ResponseWriter, r *http.Request) {
	binID, err := shared.NewBinId(chi.URLParam(r, "binId"))
	if err != nil {
		writeError(w, r, err)
		return
	}

	var req registerBinRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Capacity == nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"capacity-required", "capacity is required"}, "capacity must be present in the request body", r.URL.Path)
		return
	}
	if *req.Capacity > maxBinCapacity {
		writeProblem(w, http.StatusUnprocessableEntity, problemInfo{"capacity-out-of-range", "Bin capacity is out of range"}, fmt.Sprintf("capacity must not exceed %d", maxBinCapacity), r.URL.Path)
		return
	}
	capacity, err := shared.NewQuantity(*req.Capacity)
	if err != nil {
		writeError(w, r, err)
		return
	}

	bin, outcome, err := s.RegisterBin.Execute(r.Context(), binID, capacity)
	if err != nil {
		writeError(w, r, err)
		return
	}

	status := http.StatusOK
	if outcome == usecases.BinCreated {
		w.Header().Set("Location", "/bins/"+bin.ID().String())
		status = http.StatusCreated
	}
	writeJSON(w, status, toBinResponse(bin))
}

// maxBinCapacity is the largest capacity the wire contract (int32) and the
// bins.capacity INTEGER column can hold.
const maxBinCapacity = math.MaxInt32

// handleGetBin backs GET /bins/{binId}: the bin's capacity, occupancy and
// remaining space, or 404 bin-not-found.
func (s *Server) handleGetBin(w http.ResponseWriter, r *http.Request) {
	binID, err := shared.NewBinId(chi.URLParam(r, "binId"))
	if err != nil {
		writeError(w, r, err)
		return
	}

	bin, err := s.GetBin.Execute(r.Context(), binID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBinResponse(bin))
}

func (s *Server) handleClassifyProduct(w http.ResponseWriter, r *http.Request) {
	skuParam := chi.URLParam(r, "sku")
	sku, err := shared.NewSKU(skuParam)
	if err != nil {
		writeError(w, r, err)
		return
	}

	var req classifyProductRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	tags := make([]product.HandlingTag, 0, len(req.HandlingTags))
	for _, raw := range req.HandlingTags {
		tag, err := product.ParseHandlingTag(raw)
		if err != nil {
			writeError(w, r, err)
			return
		}
		tags = append(tags, tag)
	}

	var temperatureClass product.TemperatureClass
	if req.TemperatureClass != "" {
		temperatureClass, err = product.ParseTemperatureClass(req.TemperatureClass)
		if err != nil {
			writeError(w, r, err)
			return
		}
	}

	var dotHazardClass product.DOTHazardClass
	if req.DOTHazardClass != nil {
		dotHazardClass, err = product.ParseDOTHazardClass(*req.DOTHazardClass)
		if err != nil {
			writeError(w, r, err)
			return
		}
	}

	existing, err := s.Classifications.FindBySKU(r.Context(), sku)
	if err != nil {
		writeError(w, r, err)
		return
	}

	c, err := s.ClassifyProduct.Execute(r.Context(), sku, tags, temperatureClass, dotHazardClass)
	if err != nil {
		writeError(w, r, err)
		return
	}

	// 201 Created for a first-time classification (this SKU had none
	// before this call), 200 OK when replacing an existing one — the same
	// create-vs-replace distinction PUT semantics call for.
	status := http.StatusOK
	if existing == nil {
		status = http.StatusCreated
	}
	writeJSON(w, status, toProductClassificationResponse(c))
}

func (s *Server) handleGetProductClassification(w http.ResponseWriter, r *http.Request) {
	skuParam := chi.URLParam(r, "sku")
	sku, err := shared.NewSKU(skuParam)
	if err != nil {
		writeError(w, r, err)
		return
	}

	c, err := s.Classifications.FindBySKU(r.Context(), sku)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if c == nil {
		writeError(w, r, usecases.ErrProductClassificationNotFound)
		return
	}

	writeJSON(w, http.StatusOK, toProductClassificationResponse(c))
}

const timeFormat = "2006-01-02T15:04:05Z07:00"

func toStockUnitResponse(u *stock.StockUnit) stockUnitResponse {
	return stockUnitResponse{
		ID:       u.ID(),
		SKU:      u.SKU().String(),
		BinID:    u.BinID().String(),
		Quantity: u.Quantity().Int(),
		Reserved: u.Reserved().Int(),
		State:    string(u.State()),
	}
}

func toReservationResponse(res *reservation.Reservation) reservationResponse {
	allocations := make([]allocationResponse, 0, len(res.Allocations()))
	for _, a := range res.Allocations() {
		allocations = append(allocations, allocationResponse{StockUnitID: a.StockUnitID, BinID: a.BinID.String(), Quantity: a.Quantity.Int()})
	}
	return reservationResponse{
		ID:          res.ID(),
		SKU:         res.SKU().String(),
		Quantity:    res.Quantity().Int(),
		DemandRef:   res.DemandRef(),
		Status:      string(res.Status()),
		Allocations: allocations,
		CreatedAt:   res.CreatedAt().Format(timeFormat),
		ExpiresAt:   res.ExpiresAt().Format(timeFormat),
	}
}

func toBinResponse(b *location.Bin) binResponse {
	return binResponse{
		BinID:     b.ID().String(),
		Capacity:  b.Capacity().Int(),
		Occupied:  b.Occupied().Int(),
		Available: b.Available().Int(),
	}
}

func toProductClassificationResponse(c *product.ProductClassification) productClassificationResponse {
	tags := c.HandlingTags()
	handlingTags := make([]string, 0, len(tags))
	for _, tag := range tags {
		handlingTags = append(handlingTags, string(tag))
	}
	var dotHazardClass *int
	if c.DOTHazardClass() != product.DOTHazardClassUnspecified {
		v := int(c.DOTHazardClass())
		dotHazardClass = &v
	}
	return productClassificationResponse{
		SKU:              c.SKU().String(),
		HandlingTags:     handlingTags,
		TemperatureClass: string(c.TemperatureClass()),
		DOTHazardClass:   dotHazardClass,
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dest any) bool {
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-request-body", "The request body is not valid JSON"}, err.Error(), r.URL.Path)
		return false
	}
	return true
}

// writeError writes a domain/application error as an RFC 7807
// (application/problem+json) response. statusFor's status-code mapping is
// unchanged; this only decides the body shape and Content-Type.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, statusFor(err), problemFor(err), err.Error(), r.URL.Path)
}

func writeProblem(w http.ResponseWriter, status int, info problemInfo, detail, instance string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemDetails{
		Type:     problemBaseURI + info.slug,
		Title:    info.title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
