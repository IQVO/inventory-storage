package http

import (
	"net/http"
	"strings"
	"testing"
)

// The route pattern admits any length of [A-Za-z0-9._-]; container.NewTote
// refuses more than 64 characters. That refusal is the request's own fault:
// 400 invalid-lpn (FINAL), never a 500 (found by the Schemathesis contract
// test: PUT /totes/<65 zeros>).
func TestPutToteWithAnOverlongLPNIsInvalidLPN(t *testing.T) {
	srv := newToteTestServer(t)
	assertProblem(t, do(t, srv, http.MethodPut, "/totes/"+strings.Repeat("0", 65), ""), http.StatusBadRequest, "invalid-lpn")
	mustStatus(t, do(t, srv, http.MethodPut, "/totes/"+strings.Repeat("0", 64), ""), http.StatusCreated)
}
