package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/http/page"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// --- held stock and disposition (ADR 0052) -------------------------------------

// The 400 problems of the held-stock filters.
var (
	problemInvalidHoldReasonFilter = problemInfo{"invalid-hold-reason", "reason must be DAMAGED, QC_PENDING, SUSPECT or OTHER"}
	problemInvalidOlderThanHours   = problemInfo{"invalid-older-than-hours", "olderThanHours must be a non-negative integer"}
)

type dispositionRequest struct {
	Disposition string `json:"disposition"`
	// Reason is the new hold reason of a REWORK; absent means OTHER.
	Reason string `json:"reason"`
}

type heldUnitResponse struct {
	ID          string   `json:"id"`
	SKU         string   `json:"sku"`
	Quantity    int      `json:"quantity"`
	Reason      string   `json:"reason"`
	State       string   `json:"state"`
	ReceiptID   string   `json:"receiptId"`
	ASNNumber   string   `json:"asnNumber,omitempty"`
	LineNo      int      `json:"lineNo"`
	OwnerRef    string   `json:"ownerRef,omitempty"`
	SSCC        string   `json:"sscc,omitempty"`
	LPNs        []string `json:"lpns"`
	HeldAt      string   `json:"heldAt"`
	Disposition string   `json:"disposition,omitempty"`
	DecidedAt   string   `json:"decidedAt,omitempty"`
	ReworkCount int      `json:"reworkCount"`
}

func toHeldUnitResponse(h *stock.HeldUnit) heldUnitResponse {
	lpns := h.LPNs()
	if lpns == nil {
		lpns = []string{}
	}
	out := heldUnitResponse{
		ID: h.ID(), SKU: h.SKU().String(), Quantity: h.Quantity().Int(), Reason: string(h.Reason()), State: string(h.State()),
		ReceiptID: h.ReceiptID(), ASNNumber: h.ASNNumber(), LineNo: h.LineNo(), OwnerRef: h.OwnerRef().String(), SSCC: h.SSCC(),
		LPNs: lpns, HeldAt: h.HeldAt().UTC().Format(timeFormat), Disposition: string(h.Disposition()), ReworkCount: h.ReworkCount(),
	}
	if !h.DecidedAt().IsZero() {
		out.DecidedAt = h.DecidedAt().UTC().Format(timeFormat)
	}
	return out
}

// heldPosition is the keyset position of a held unit in GET /held-stock: its
// hold instant (UTC, RFC 3339 with nanoseconds) and id.
func heldPosition(h *stock.HeldUnit) page.Position {
	return page.Position{Sort: h.HeldAt().UTC().Format(time.RFC3339Nano), ID: h.ID()}
}

// heldAfter turns a decoded cursor into the repository's keyset position; a
// cursor whose sort key is not a timestamp is invalid.
func heldAfter(pos *page.Position) (*stock.HeldPageAfter, error) {
	if pos == nil {
		return nil, nil
	}
	heldAt, err := time.Parse(time.RFC3339Nano, pos.Sort)
	if err != nil {
		return nil, page.ErrInvalidCursor
	}
	return &stock.HeldPageAfter{HeldAt: heldAt, ID: pos.ID}, nil
}

// heldQuery reads the reason, sku and olderThanHours filters. On a bad value
// it has already written the 400 problem and reports ok == false.
func heldQuery(w http.ResponseWriter, r *http.Request) (q usecases.ListHeldStockQuery, ok bool) {
	if v := r.URL.Query().Get("reason"); v != "" {
		reason, err := stock.ParseHoldReason(v)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemInvalidHoldReasonFilter, err.Error(), r.URL.Path)
			return q, false
		}
		q.Reason = reason
	}
	if v := r.URL.Query().Get("sku"); v != "" {
		sku, err := shared.NewSKU(v)
		if err != nil {
			writeError(w, r, err)
			return q, false
		}
		q.SKU = sku
	}
	if v := r.URL.Query().Get("olderThanHours"); v != "" {
		hours, err := strconv.Atoi(v)
		if err != nil || hours < 0 {
			writeProblem(w, http.StatusBadRequest, problemInvalidOlderThanHours, "olderThanHours is not a non-negative integer: "+v, r.URL.Path)
			return q, false
		}
		q.OlderThan = time.Duration(hours) * time.Hour
	}
	return q, true
}

// handleListHeldStock backs GET /held-stock: one keyset page, {items,
// nextCursor}, of the units still held, oldest first, filtered by reason, sku
// and age.
func (s *Server) handleListHeldStock(w http.ResponseWriter, r *http.Request) {
	limit, pos, ok := pageParams(w, r, "reason", "sku", "olderThanHours")
	if !ok {
		return
	}
	after, err := heldAfter(pos)
	if err != nil {
		writeInvalidCursor(w, r)
		return
	}
	q, ok := heldQuery(w, r)
	if !ok {
		return
	}
	q.After, q.Limit = after, page.FetchSize(limit)
	units, err := s.ListHeldStock.Execute(r.Context(), q)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, pageOf(units, limit, heldPosition, toHeldUnitResponse))
}

// handleGetHeldUnit backs GET /held-stock/{id}: a unit still held or already
// settled (its disposition is the audit trail).
func (s *Server) handleGetHeldUnit(w http.ResponseWriter, r *http.Request) {
	h, err := s.ListHeldStock.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toHeldUnitResponse(h))
}

// handlePostDisposition backs POST /held-stock/{id}/dispositions: decide what
// happens to a held unit. 200 with the unit (REMOVED for RELEASE,
// RETURN_TO_VENDOR and SCRAP, still HELD for REWORK).
func (s *Server) handlePostDisposition(w http.ResponseWriter, r *http.Request) {
	var req dispositionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	disposition, err := stock.ParseDisposition(req.Disposition)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var reason stock.HoldReason
	if req.Reason != "" {
		if reason, err = stock.ParseHoldReason(req.Reason); err != nil {
			writeError(w, r, err)
			return
		}
	}
	h, err := s.DecideHeldDisposition.Execute(r.Context(), usecases.DecideHeldDispositionCommand{
		HeldUnitID: chi.URLParam(r, "id"), Disposition: disposition, Reason: reason,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toHeldUnitResponse(h))
}
