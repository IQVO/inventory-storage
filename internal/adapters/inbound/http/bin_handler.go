package http

import (
	"fmt"
	"math"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

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

func toBinResponse(b *location.Bin) binResponse {
	return binResponse{
		BinID:     b.ID().String(),
		Capacity:  b.Capacity().Int(),
		Occupied:  b.Occupied().Int(),
		Available: b.Available().Int(),
	}
}
