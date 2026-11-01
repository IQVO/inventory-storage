package http

import (
	"encoding/json"
	"net/http"

	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

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
		OwnerRef:    res.OwnerRef().String(),
		LineNo:      res.LineNo(),
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
