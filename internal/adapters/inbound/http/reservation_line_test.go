package http_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

type lineReservation struct {
	ID        string `json:"id"`
	DemandRef string `json:"demandRef"`
	LineNo    *int   `json:"lineNo"`
}

func seededLineServer(t *testing.T) testServer {
	t.Helper()
	ts := newTestServer()
	ts.seedBin(t, "A-1-1", 30)
	ts.do(t, http.MethodPost, "/stock/stow", map[string]any{"sku": "SKU-1", "quantity": 30, "binId": "A-1-1"})
	return ts
}

// POST /reservations accepts lineNo (decision 18, ADR 0036) and the create
// response, GET by demandRef and the stored aggregate all carry it.
func TestReserveStock_Endpoint_LineNo_StoredAndReturned(t *testing.T) {
	ts := seededLineServer(t)

	rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 2, "demandRef": "order-1", "lineNo": 2})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var created lineReservation
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.LineNo == nil || *created.LineNo != 2 {
		t.Fatalf("create response lineNo = %v, want 2: %s", created.LineNo, rec.Body.String())
	}

	get := ts.do(t, http.MethodGet, "/reservations?demandRef=order-1", nil)
	var list []lineReservation
	if err := json.Unmarshal(get.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list) != 1 || list[0].LineNo == nil || *list[0].LineNo != 2 {
		t.Fatalf("GET by demandRef = %s, want one reservation with lineNo 2", get.Body.String())
	}
}

// Without lineNo the field is omitted from the response (unknown), not null
// and not 0, and the request is accepted exactly as before.
func TestReserveStock_Endpoint_NoLineNo_OmitsTheField(t *testing.T) {
	ts := seededLineServer(t)

	rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 2, "demandRef": "order-legacy"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "lineNo") {
		t.Fatalf("response must omit lineNo when unknown, got %s", rec.Body.String())
	}
	get := ts.do(t, http.MethodGet, "/reservations?demandRef=order-legacy", nil)
	if strings.Contains(get.Body.String(), "lineNo") {
		t.Fatalf("GET must omit lineNo when unknown, got %s", get.Body.String())
	}
}

// An explicit null is the same as absent.
func TestReserveStock_Endpoint_NullLineNo_IsUnknown(t *testing.T) {
	ts := seededLineServer(t)

	rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 2, "demandRef": "order-null", "lineNo": nil})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "lineNo") {
		t.Fatalf("response must omit lineNo, got %s", rec.Body.String())
	}
}

// 0 and negatives are a 400 (invalid-line-no) and reserve nothing.
func TestReserveStock_Endpoint_NonPositiveLineNo_Rejected(t *testing.T) {
	for _, n := range []int{0, -1, -7} {
		ts := seededLineServer(t)
		rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 2, "demandRef": "order-bad", "lineNo": n})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("lineNo %d: expected 400, got %d: %s", n, rec.Code, rec.Body.String())
		}
		assertProblemDetails(t, rec, http.StatusBadRequest, "invalid-line-no", "/reservations")

		usable := ts.do(t, http.MethodGet, "/inventory/SKU-1/usable", nil)
		if !strings.Contains(usable.Body.String(), `"usable":30`) {
			t.Fatalf("lineNo %d: a rejected reservation must not reserve stock, got %s", n, usable.Body.String())
		}
	}
}

// A non-integer lineNo is a malformed body (the existing 400).
func TestReserveStock_Endpoint_NonIntegerLineNo_IsMalformed(t *testing.T) {
	ts := seededLineServer(t)
	for _, v := range []any{"two", 1.5, true} {
		rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 2, "demandRef": "order-bad", "lineNo": v})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("lineNo %v: expected 400, got %d: %s", v, rec.Code, rec.Body.String())
		}
		assertProblemDetails(t, rec, http.StatusBadRequest, "malformed-request-body", "/reservations")
	}
}

// Two lines of one order with the same SKU and quantity are two reservations
// once the client says which line each is for; resending a line returns the
// same reservation (the application-level replay guard).
func TestReserveStock_Endpoint_SameSKUAndQuantityOnTwoLines(t *testing.T) {
	ts := seededLineServer(t)
	post := func(line int) lineReservation {
		rec := ts.do(t, http.MethodPost, "/reservations", map[string]any{"sku": "SKU-1", "quantity": 2, "demandRef": "order-two", "lineNo": line})
		if rec.Code != http.StatusCreated {
			t.Fatalf("line %d: expected 201, got %d: %s", line, rec.Code, rec.Body.String())
		}
		var r lineReservation
		_ = json.Unmarshal(rec.Body.Bytes(), &r)
		return r
	}
	l1, l2, l1again := post(1), post(2), post(1)
	if l1.ID == l2.ID {
		t.Fatal("line 2 was answered with line 1's reservation")
	}
	if l1again.ID != l1.ID {
		t.Fatalf("resending line 1 created %s, want the first reservation %s", l1again.ID, l1.ID)
	}
	usable := ts.do(t, http.MethodGet, "/inventory/SKU-1/usable", nil)
	if !strings.Contains(usable.Body.String(), `"usable":26`) {
		t.Fatalf("two lines of 2 must hold 4 units, got %s", usable.Body.String())
	}
}
