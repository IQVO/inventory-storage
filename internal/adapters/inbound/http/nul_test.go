package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// next records whether the wrapped handler ran and what body it saw, so the
// tests prove both the rejection AND that a clean request is passed through
// intact (the middleware reads the body, so it must put it back).
func nulProbe(ran *bool, seen *string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*ran = true
		b, _ := io.ReadAll(r.Body)
		*seen = string(b)
		w.WriteHeader(http.StatusNoContent)
	})
}

func TestRejectNUL_Rejects(t *testing.T) {
	cases := []struct {
		name       string
		target     string
		body       string
		wantStatus int
		wantType   string
	}{
		{"raw NUL byte in the body", "/stock", "{\"sku\":\"A\x00B\"}", http.StatusBadRequest, "malformed-request-body"},
		{"json escape \\u0000 in the body", "/stock", `{"sku":"A\u0000B"}`, http.StatusBadRequest, "malformed-request-body"},
		{"escape after an even run of backslashes", "/stock", `{"sku":"A\\\u0000B"}`, http.StatusBadRequest, "malformed-request-body"},
		{"percent-encoded NUL in the query", "/stock?sku=A%00B", "", http.StatusBadRequest, "malformed-request-body"},
		{"percent-encoded NUL in the path", "/stock/A%00B", "", http.StatusNotFound, "not-found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran bool
			var seen string
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tc.body))
			rejectNUL(nulProbe(&ran, &seen)).ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if ran {
				t.Fatal("the wrapped handler ran: the NUL must be rejected before any other middleware")
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("Content-Type = %q, want application/problem+json", ct)
			}
			if !strings.HasSuffix(problemTypeOf(t, rec.Body.String()), tc.wantType) {
				t.Fatalf("problem type = %s, want it to end in %s", rec.Body.String(), tc.wantType)
			}
		})
	}
}

func TestRejectNUL_PassesCleanRequestsWithTheBodyIntact(t *testing.T) {
	cases := []struct {
		name   string
		target string
		body   string
	}{
		{"plain json body", "/stock", `{"sku":"AB"}`},
		{"escaped backslash followed by the text u0000 is not a NUL", "/stock", `{"sku":"A\\u0000B"}`},
		{"no body", "/stock", ""},
		{"clean query", "/stock?sku=AB", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran bool
			var seen string
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(tc.body))
			rejectNUL(nulProbe(&ran, &seen)).ServeHTTP(rec, req)

			if !ran || rec.Code != http.StatusNoContent {
				t.Fatalf("ran=%v status=%d, want the handler to run and answer 204", ran, rec.Code)
			}
			if seen != tc.body {
				t.Fatalf("handler saw body %q, want it unchanged %q", seen, tc.body)
			}
		})
	}
}

func TestContainsNUL(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"abc", false},
		{"a\x00b", true},
		{`\u0000`, true},
		{`x\u0000`, true},
		{`\\u0000`, false},
		{`\\\u0000`, true},
		{`\u0000 and \\u0000`, true},
		{`\\u0000 and \\u0000`, false},
		{`\u0001`, false},
	}
	for _, tc := range cases {
		if got := containsNUL([]byte(tc.in)); got != tc.want {
			t.Errorf("containsNUL(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// problemTypeOf extracts the "type" member of a problem+json body without
// depending on the exact struct, so the test only pins the slug.
func problemTypeOf(t *testing.T, body string) string {
	t.Helper()
	const key = `"type":"`
	i := strings.Index(body, key)
	if i < 0 {
		t.Fatalf("no type member in %s", body)
	}
	rest := body[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		t.Fatalf("unterminated type member in %s", body)
	}
	return rest[:j]
}
