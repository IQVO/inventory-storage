package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/inventory-storage/internal/analytics/report"
)

// ReportsHandlers is the inbound HTTP adapter for the inventory-storage
// "Inventory Flow & Accuracy" data product's READER. It depends only on the
// read-model port (report.ReportStore); it never touches the OLTP use cases or
// the writer.
type ReportsHandlers struct {
	Store report.ReportStore
}

// flowAccuracyRowDTO is the wire shape of one report row. It is a dedicated
// DTO so the read-model struct (report.Row) never leaks onto the API.
type flowAccuracyRowDTO struct {
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

// flowAccuracyReportDTO is the wire shape of a report response.
type flowAccuracyReportDTO struct {
	Rows []flowAccuracyRowDTO `json:"rows"`
}

// freshnessDTO is the wire shape of the freshness-lag response.
type freshnessDTO struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// GetFlowAccuracy serves GET /reports/flow-accuracy. from and to (RFC3339) are
// required; sku, binId and granularity are optional (granularity defaults to
// hour).
func (h *ReportsHandlers) GetFlowAccuracy(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from, ok := parseRequiredTime(w, r, q.Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseRequiredTime(w, r, q.Get("to"), "to")
	if !ok {
		return
	}

	granularity := report.GranularityHour
	if g := q.Get("granularity"); g != "" {
		if g != string(report.GranularityHour) {
			writeReportBadRequest(w, r, "granularity must be 'hour'")
			return
		}
		granularity = report.Granularity(g)
	}

	rep, err := h.Store.Query(r.Context(), report.ReportQuery{
		From:        from,
		To:          to,
		SKU:         q.Get("sku"),
		BinId:       q.Get("binId"),
		Granularity: granularity,
	})
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}

	dto := flowAccuracyReportDTO{Rows: make([]flowAccuracyRowDTO, 0, len(rep.Rows))}
	for _, row := range rep.Rows {
		dto.Rows = append(dto.Rows, flowAccuracyRowDTO{
			SKU:                   row.Key.SKU,
			BinId:                 row.Key.BinId,
			HourBucket:            row.Key.HourBucket.UTC().Format(time.RFC3339),
			ReceivedQuantity:      row.ReceivedQuantity,
			StowedCount:           row.StowedCount,
			PickedQuantity:        row.PickedQuantity,
			ReservationsCreated:   row.ReservationsCreated,
			ReservationsExpired:   row.ReservationsExpired,
			ReservationsRevoked:   row.ReservationsRevoked,
			CycleCountsCompleted:  row.CycleCountsCompleted,
			DiscrepanciesDetected: row.DiscrepanciesDetected,
			UnlocatedCount:        row.UnlocatedCount,
		})
	}
	writeJSON(w, http.StatusOK, dto)
}

// GetFreshness serves GET /reports/flow-accuracy/freshness.
func (h *ReportsHandlers) GetFreshness(w http.ResponseWriter, r *http.Request) {
	lag, err := h.Store.FreshnessLag(r.Context())
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, freshnessDTO{LagSeconds: lag.Seconds()})
}

// GetReportsHealthz serves GET /healthz for the reports service.
func (h *ReportsHandlers) GetReportsHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// parseRequiredTime parses an RFC3339 timestamp, writing an RFC 7807 400 and
// returning ok=false when it is missing or malformed.
func parseRequiredTime(w http.ResponseWriter, r *http.Request, raw, name string) (time.Time, bool) {
	if raw == "" {
		writeReportBadRequest(w, r, "query parameter '"+name+"' is required (RFC3339)")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeReportBadRequest(w, r, "query parameter '"+name+"' must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return t, true
}

// writeReportBadRequest writes the reports service's RFC 7807 400.
func writeReportBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest,
		problemInfo{"invalid-report-query", "The report query is malformed or missing a required parameter"},
		detail, r.URL.Path)
}

// writeReportInternal writes the reports service's RFC 7807 500.
func writeReportInternal(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, http.StatusInternalServerError,
		problemInfo{"report-store-error", "The report could not be served"},
		err.Error(), r.URL.Path)
}

// NewReportsRouter builds the chi router for the inventory-reports reader
// service. A nil logger falls back to slog.Default(); an empty serviceName
// falls back to DefaultServiceName.
func NewReportsRouter(h *ReportsHandlers, logger *slog.Logger, serviceName string, opts ...RouterOption) http.Handler {
	var cfg routerConfig
	for _, o := range opts {
		o(&cfg)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = DefaultServiceName
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	// ADR-0016 Tier 1: http.server.request.duration and
	// http.server.active_requests, same middleware order as NewRouter.
	metricCfg := otelchimetric.NewBaseConfig(serviceName)
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(otelchimetric.NewServerActiveRequests(metricCfg))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.GetReportsHealthz)
	r.Get("/reports/flow-accuracy", h.GetFlowAccuracy)
	r.Get("/reports/flow-accuracy/freshness", h.GetFreshness)

	return r
}
