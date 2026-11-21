package http

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/inbound/http/page"
)

// GET /bins/{binId}/totes is one keyset page in LPN order (REST guideline 3),
// like GET /bins/{binId}/containers. Pins the 400s schemathesis.toml widens
// positive_data_acceptance for (listBinTotes).

func stowThreeTotes(t *testing.T, srv http.Handler) {
	t.Helper()
	for i, lpn := range []string{"TOTE-3", "TOTE-1", "TOTE-2"} {
		do(t, srv, http.MethodPut, "/totes/"+lpn, "")
		mustStatus(t, doKeyed(t, srv, "/totes/"+lpn+"/stowages", `{"bin":"WH1-AUTO-ASRS-ASRS1-00-00-S"}`, "k"+string(rune('a'+i))), http.StatusOK)
	}
}

type totePageBody struct {
	Items []struct {
		LPN   string `json:"lpn"`
		BinID string `json:"binId"`
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
}

func TestListBinTotesPagesInLPNOrder(t *testing.T) {
	srv := newToteTestServer(t)
	stowThreeTotes(t, srv)
	var first, second totePageBody
	resp := do(t, srv, http.MethodGet, "/bins/WH1-AUTO-ASRS-ASRS1-00-00-S/totes?limit=2", "")
	decode(t, resp, &first)
	if resp.StatusCode != http.StatusOK || len(first.Items) != 2 || first.Items[0].LPN != "TOTE-1" ||
		first.Items[1].LPN != "TOTE-2" || first.Items[0].BinID != "WH1-AUTO-ASRS-ASRS1-00-00-S" || first.NextCursor == "" {
		t.Fatalf("first page = %d %+v", resp.StatusCode, first)
	}
	decode(t, do(t, srv, http.MethodGet, "/bins/WH1-AUTO-ASRS-ASRS1-00-00-S/totes?limit=2&cursor="+url.QueryEscape(first.NextCursor), ""), &second)
	if len(second.Items) != 1 || second.Items[0].LPN != "TOTE-3" || second.NextCursor != "" {
		t.Fatalf("second page = %+v", second)
	}
}

func TestListBinTotesRejectsBadLimitAndCursor(t *testing.T) {
	srv := newToteTestServer(t)
	base := "/bins/WH1-AUTO-ASRS-ASRS1-00-00-S/totes?"
	for query, slug := range map[string]string{
		"limit=0":   "invalid-limit",
		"limit=501": "invalid-limit",
		"cursor=" + url.QueryEscape("not base64 !!"):                         "invalid-cursor",
		"cursor=" + page.Encode(page.Position{Sort: "TOTE-1", ID: "TOTE-2"}): "invalid-cursor", // not a position of this list
		"after=TOTE-1": "unknown-query-parameter",
	} {
		assertProblem(t, do(t, srv, http.MethodGet, base+query, ""), http.StatusBadRequest, slug)
	}
	assertProblem(t, do(t, srv, http.MethodGet, "/bins/NOPE/totes", ""), http.StatusNotFound, "bin-not-found")
}
