// Package page is the fleet reference for REST keyset pagination (REST API guideline section 3).
// Copy this file verbatim into the service (e.g. internal/adapters/inbound/http/page/page.go) and
// change only the package path. Do not invent a second cursor format.
package page

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

const (
	// DefaultLimit and MaxLimit are the fleet-wide bounds for the `limit` query parameter.
	DefaultLimit = 100
	MaxLimit     = 500
)

// ErrInvalidLimit and ErrInvalidCursor map to 400 problems `invalid-limit` and `invalid-cursor`.
var (
	ErrInvalidLimit  = errors.New("limit must be an integer between 1 and 500")
	ErrInvalidCursor = errors.New("cursor is not a valid page token")
)

// Page is the ONLY collection response shape: { "items": [...], "nextCursor": "..." }.
// NextCursor is omitted on the last page. Items is never null.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// Position is the keyset a cursor points after: the last returned row's sort key and id.
// Repositories order by (Sort, ID) so ties never skip or repeat a row.
type Position struct {
	Sort string `json:"s"`
	ID   string `json:"i"`
}

// Limit parses `limit` from the query string. Absent -> DefaultLimit.
func Limit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return DefaultLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > MaxLimit {
		return 0, ErrInvalidLimit
	}
	return n, nil
}

// After parses `cursor` from the query string. Absent -> nil (first page).
func After(r *http.Request) (*Position, error) {
	raw := r.URL.Query().Get("cursor")
	if raw == "" {
		return nil, nil
	}
	return Decode(raw)
}

// Encode makes the opaque cursor for a position.
func Encode(p Position) string {
	b, _ := json.Marshal(p) // a struct of two strings cannot fail to marshal
	return base64.RawURLEncoding.EncodeToString(b)
}

// Decode parses an opaque cursor; any malformed value is ErrInvalidCursor.
func Decode(s string) (*Position, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var p Position
	if err := json.Unmarshal(b, &p); err != nil || p.ID == "" {
		return nil, ErrInvalidCursor
	}
	return &p, nil
}

// Of builds the response page from rows fetched with `limit+1` (ask the repository for
// FetchSize(limit) rows). When more than `limit` rows came back there is a next page and
// the cursor points after the last row that is returned.
func Of[T any](rows []T, limit int, at func(T) Position) Page[T] {
	if rows == nil {
		rows = []T{}
	}
	if len(rows) <= limit {
		return Page[T]{Items: rows}
	}
	rows = rows[:limit]
	return Page[T]{Items: rows, NextCursor: Encode(at(rows[len(rows)-1]))}
}

// FetchSize is how many rows a repository must request to know whether another page exists.
func FetchSize(limit int) int { return limit + 1 }
