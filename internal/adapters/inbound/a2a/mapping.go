package a2a

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// problemTypeBase is the REST adapter's problem namespace (ADR-0005), reused
// so a delegating agent sees the very same `type` URI a REST caller would.
const problemTypeBase = "https://errors.inventory-storage.warehouse-systems.dev/"

// problem is the RFC 7807-shaped rejection an agent client receives instead of
// a result: domain rejections and request-shape failures are business
// answers, not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: http.StatusBadRequest}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: http.StatusBadRequest}
}

func problemInvalidArgument(err error) *problem {
	return &problem{Type: problemTypeBase + "invalid-argument", Title: "Invalid skill argument", Detail: err.Error(), Status: http.StatusBadRequest}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id the
// answer belongs to ("" when the request named no known skill).
type envelope struct {
	Skill   string    `json:"skill"`
	Data    *skillDTO `json:"data,omitempty"`
	Problem *problem  `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field, keeping the envelope
// flat and versioned as one contract. Domain types never cross the boundary.
type skillDTO struct {
	Availability       *availability       `json:"availability,omitempty"`
	ReservationRevoked *reservationRevoked `json:"reservationRevoked,omitempty"`
}

// availability is the read DTO of check_availability, the same two fields the
// MCP tool of that name returns.
type availability struct {
	SKU    string `json:"sku"`
	Usable int    `json:"usable"`
}

// reservationRevoked is the write DTO of revoke_reservation, the same two
// fields the MCP tool of that name returns.
type reservationRevoked struct {
	ReservationId string `json:"reservationId"`
	Revoked       bool   `json:"revoked"`
}

// skillResult is a successful skill run: its DTO and the one-line summary for
// the reply's text part.
type skillResult struct {
	data    skillDTO
	summary string
}

// problemClass pins one typed error to its status and RFC 7807 (slug, title),
// copied from the REST adapter's statusFor/problemCatalog for the errors the
// two skills' use cases can raise, so an agent sees the outcome a REST caller
// would. Order matters: the first entry whose target the error wraps wins.
type problemClass struct {
	target error
	status int
	slug   string
	title  string
}

func problemCatalog() []problemClass {
	return []problemClass{
		{usecases.ErrReservationNotFound, http.StatusNotFound, "reservation-not-found", "Reservation not found"},
		{usecases.ErrStockUnitNotFound, http.StatusNotFound, "stock-unit-not-found", "Stock unit not found"},
		{shared.ErrEmptySKU, http.StatusBadRequest, "empty-sku", "SKU must not be empty"},
		{reservation.ErrAlreadyResolved, http.StatusConflict, "reservation-already-resolved", "Reservation is already resolved"},
		{reservation.ErrExpired, http.StatusConflict, "reservation-expired", "Reservation has expired"},
		{stock.ErrInsufficientReserved, http.StatusConflict, "insufficient-reserved", "Pick quantity exceeds reserved quantity"},
		{location.ErrReleaseExceedsOccupancy, http.StatusConflict, "release-exceeds-occupancy", "Release exceeds bin occupancy"},
		{usecases.ErrConcurrentModification, http.StatusConflict, "concurrent-modification", "The resource was modified by another request; re-fetch the latest version and retry"},
	}
}

// problemFromUseCase maps a use case error to a problem answer. Domain
// rejections are EXPECTED business outcomes and are answered as RFC 7807-shaped
// problems (ADR-0005); anything unrecognised is an internal failure, still
// answered as a problem (status 500) so the calling agent always receives a
// structured, machine-readable result. The 500 detail is adapter-owned: the
// raw error is logged, never echoed to the caller.
func problemFromUseCase(err error) *problem {
	for _, c := range problemCatalog() {
		if errors.Is(err, c.target) {
			return &problem{Type: problemTypeBase + c.slug, Title: c.title, Detail: err.Error(), Status: c.status}
		}
	}
	slog.Error("a2a skill failed", "error", err)
	return &problem{Type: problemTypeBase + "internal-error", Title: "An unexpected internal error occurred", Detail: "the skill failed; see server logs", Status: http.StatusInternalServerError}
}
