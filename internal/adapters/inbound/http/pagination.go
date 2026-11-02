package http

import (
	"net/http"
	"time"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/http/page"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
)

// The 400 problems of a paginated collection GET (REST API guideline 3.2, 3.4).
var (
	problemInvalidLimit  = problemInfo{"invalid-limit", "limit must be an integer between 1 and 500"}
	problemInvalidCursor = problemInfo{"invalid-cursor", "cursor is not a valid page token"}
	problemUnknownParam  = problemInfo{"unknown-query-parameter", "the collection does not declare this query parameter"}
)

// pageParams reads `limit` and `cursor`; filters lists the other query parameters the
// collection declares, and any further one is a 400 (a retired paging parameter such as
// `after` must fail loudly, not read page one forever). On a bad value it has already
// written the 400 problem and reports ok == false.
func pageParams(w http.ResponseWriter, r *http.Request, filters ...string) (limit int, pos *page.Position, ok bool) {
	if name := page.Unknown(r, filters...); name != "" {
		writeProblem(w, http.StatusBadRequest, problemUnknownParam, "unknown query parameter: "+name, r.URL.Path)
		return 0, nil, false
	}
	limit, err := page.Limit(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidLimit, err.Error(), r.URL.Path)
		return 0, nil, false
	}
	pos, err = page.After(r)
	if err != nil {
		writeInvalidCursor(w, r)
		return 0, nil, false
	}
	return limit, pos, true
}

func writeInvalidCursor(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, http.StatusBadRequest, problemInvalidCursor, page.ErrInvalidCursor.Error(), r.URL.Path)
}

// pageOf maps one page of rows (fetched with page.FetchSize(limit)) to its
// response: the first `limit` rows through toDTO, plus the cursor after the
// last one when more rows exist.
func pageOf[T, D any](rows []T, limit int, at func(T) page.Position, toDTO func(T) D) page.Page[D] {
	pg := page.Of(rows, limit, at)
	out := page.Page[D]{Items: make([]D, 0, len(pg.Items)), NextCursor: pg.NextCursor}
	for _, row := range pg.Items {
		out.Items = append(out.Items, toDTO(row))
	}
	return out
}

// reservationPosition is the keyset position of a reservation in
// GET /reservations: its creation instant (UTC, RFC 3339 with nanoseconds) and id.
func reservationPosition(res *reservation.Reservation) page.Position {
	return page.Position{Sort: res.CreatedAt().UTC().Format(time.RFC3339Nano), ID: res.ID()}
}

// reservationAfter turns a decoded cursor into the repository's keyset
// position; a cursor whose sort key is not a timestamp is invalid.
func reservationAfter(pos *page.Position) (*reservation.PageAfter, error) {
	if pos == nil {
		return nil, nil
	}
	createdAt, err := time.Parse(time.RFC3339Nano, pos.Sort)
	if err != nil {
		return nil, page.ErrInvalidCursor
	}
	return &reservation.PageAfter{CreatedAt: createdAt, ID: pos.ID}, nil
}
