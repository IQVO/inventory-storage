package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/http/page"
	"github.com/claudioed/inventory-storage/internal/domain/container"
)

// The REST API guideline cutover (ADR 0049) at the HTTP boundary: the retired
// routes are gone, and the two collection GETs are keyset-paginated.

// TestRetiredRoutesAreGone: every pre-guideline route answers 404 or 405. No
// alias survives the hard cutover.
func TestRetiredRoutesAreGone(t *testing.T) {
	ts := newTestServer()
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/stock/receive"},
		{http.MethodPost, "/stock/stow"},
		{http.MethodPost, "/reservations/res-1/confirm-pick"},
		{http.MethodPost, "/bins/A-1-1/cycle-count"},
		{http.MethodPost, "/transfers/tl-77-1/stow"},
		{http.MethodPost, "/containers/" + cLPN + ":stow"},
		{http.MethodPost, "/containers/" + cLPN + ":break"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := ts.do(t, tc.method, tc.path, map[string]any{})
			if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s = %d (%s), want 404 or 405", tc.method, tc.path, rec.Code, rec.Body.String())
			}
		})
	}
}

// pageBody is the collection envelope of guideline section 3.3.
type pageBody struct {
	Items      []map[string]any `json:"items"`
	NextCursor string           `json:"nextCursor"`
}

// walkPages follows nextCursor from `path` (which already carries its filters
// and a limit) until it is absent and returns the `key` of every item in the
// order served. Every page but the last must be full.
func walkPages(t *testing.T, do func(path string) *httptest.ResponseRecorder, path string, limit int, key string) []string {
	t.Helper()
	var got []string
	cursor := ""
	for n := 0; ; n++ {
		if n > 50 {
			t.Fatalf("walk of %s did not terminate", path)
		}
		next := path
		if cursor != "" {
			next += "&cursor=" + url.QueryEscape(cursor)
		}
		rec := do(next)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", next, rec.Code, rec.Body.String())
		}
		var body pageBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("GET %s: body is not a page: %v: %s", next, err, rec.Body.String())
		}
		for _, item := range body.Items {
			id, _ := item[key].(string)
			got = append(got, id)
		}
		if body.NextCursor == "" {
			return got
		}
		if len(body.Items) != limit {
			t.Fatalf("GET %s: a page with a nextCursor must be full (%d items, limit %d)", next, len(body.Items), limit)
		}
		cursor = body.NextCursor
	}
}

func assertWalk(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("walk returned %d rows, want %d: %v", len(got), len(want), got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("row %q served twice: %v", id, got)
		}
		seen[id] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("row %q was skipped: %v", id, got)
		}
	}
	if !slices.IsSorted(got) {
		t.Fatalf("rows are not in (sort key, id) order: %v", got)
	}
}

// Seven reservations of one demandRef share the fixed clock's createdAt, so
// every row ties on the sort key and only the id tiebreak keeps the walk exact.
func TestGetReservationsByDemandRef_WalksEveryPageWithoutDuplicatesOrSkips(t *testing.T) {
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 100)
	if rec := ts.do(t, http.MethodPost, "/stowages", map[string]any{"sku": "SKU-1", "quantity": 100, "binId": "A-1-1"}); rec.Code != http.StatusCreated {
		t.Fatalf("stow = %d: %s", rec.Code, rec.Body.String())
	}
	var want []string
	for line := 1; line <= 7; line++ {
		rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 1, "demandRef": "order-7", "lineNo": line})
		if rec.Code != http.StatusCreated {
			t.Fatalf("reserve line %d = %d: %s", line, rec.Code, rec.Body.String())
		}
		var res struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		want = append(want, res.ID)
	}
	slices.Sort(want)
	// another demand must never leak into the walk
	if rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 1, "demandRef": "order-8", "lineNo": 1}); rec.Code != http.StatusCreated {
		t.Fatalf("reserve other = %d: %s", rec.Code, rec.Body.String())
	}
	do := func(path string) *httptest.ResponseRecorder { return ts.do(t, http.MethodGet, path, nil) }

	assertWalk(t, walkPages(t, do, "/reservations?demandRef=order-7&limit=3", 3, "id"), want)
	// exactly `limit` rows is one page and no nextCursor, not an empty page after it
	assertWalk(t, walkPages(t, do, "/reservations?demandRef=order-7&limit=7", 7, "id"), want)
	assertWalk(t, walkPages(t, do, "/reservations?demandRef=order-7&limit=1", 1, "id"), want)
}

func TestListBinContainers_WalksEveryPageWithoutDuplicatesOrSkips(t *testing.T) {
	cs := newContainerServer(t)
	var want []string
	for i := 0; i < 7; i++ {
		lpn := fmt.Sprintf("LPN-T%02d", i)
		c, err := container.Register(container.RegisterSpec{
			LPN: lpn, SKU: "SKU-1", GTIN: "24006381333935", UnitsPerPack: 12, Quantity: 12, OwnerRef: "seller-a",
			Lot: "L1", Expiry: "2027-03-01", ReceiptID: "R-1", LineNo: i + 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cs.containers.Add(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		if rec := cs.do(t, http.MethodPost, "/containers/"+lpn+"/stowages", map[string]any{"bin": cCaseBin}); rec.Code != http.StatusOK {
			t.Fatalf("stow %s = %d: %s", lpn, rec.Code, rec.Body.String())
		}
		want = append(want, lpn)
	}
	// the container seeded by newContainerServer is not stowed: it has no bin and is not listed
	do := func(path string) *httptest.ResponseRecorder { return cs.do(t, http.MethodGet, path, nil) }

	assertWalk(t, walkPages(t, do, "/bins/"+cCaseBin+"/containers?limit=3", 3, "lpn"), want)
	assertWalk(t, walkPages(t, do, "/bins/"+cCaseBin+"/containers?limit=7", 7, "lpn"), want)
	assertWalk(t, walkPages(t, do, "/bins/"+cCaseBin+"/containers?limit=1", 1, "lpn"), want)
}

// A bad limit is 400 invalid-limit and a cursor that does not decode (or whose
// sort key is not this list's) is 400 invalid-cursor, and any undeclared query parameter is
// 400 unknown-query-parameter, on both collections.
func TestCollections_RejectBadLimitAndCursor(t *testing.T) {
	cs := newContainerServer(t)
	for _, list := range []struct {
		name, base string
		foreign    page.Position // decodes, but is not a position of this list
	}{
		{"reservations", "/reservations?demandRef=order-1", page.Position{Sort: "not-a-timestamp", ID: "res-1"}},
		{"bin containers", "/bins/" + cCaseBin + "/containers?", page.Position{Sort: "LPN-A", ID: "LPN-B"}},
	} {
		sep := "&"
		if strings.HasSuffix(list.base, "?") {
			sep = ""
		}
		for _, tc := range []struct{ query, slug string }{
			{"limit=0", "invalid-limit"},
			{"limit=501", "invalid-limit"},
			{"limit=-1", "invalid-limit"},
			{"limit=abc", "invalid-limit"},
			{"cursor=" + url.QueryEscape("not base64 !!"), "invalid-cursor"},
			{"cursor=" + page.Encode(page.Position{Sort: "x"}), "invalid-cursor"}, // no id
			{"cursor=" + page.Encode(list.foreign), "invalid-cursor"},
			// A retired paging parameter fails loudly instead of reading page one forever.
			{"after=anything", "unknown-query-parameter"},
			{"offset=10", "unknown-query-parameter"},
			{"nextToken=abc", "unknown-query-parameter"},
		} {
			t.Run(list.name+" "+tc.query, func(t *testing.T) {
				rec := cs.do(t, http.MethodGet, list.base+sep+tc.query, nil)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d (%s), want 400", rec.Code, rec.Body.String())
				}
				if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
					t.Fatalf("Content-Type = %q, want application/problem+json", ct)
				}
				var problem struct {
					Type string `json:"type"`
				}
				_ = json.Unmarshal(rec.Body.Bytes(), &problem)
				if !strings.HasSuffix(problem.Type, "/"+tc.slug) {
					t.Fatalf("problem type = %q, want .../%s", problem.Type, tc.slug)
				}
			})
		}
	}
}
