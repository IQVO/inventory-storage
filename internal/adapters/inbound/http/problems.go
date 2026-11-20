package http

import "net/http"

// registered collects the problem types declared with register (problems.go). The typed domain
// errors are not here: they are the entries of problemCatalog (errors.go), their status is
// statusFor's.
var registered []problemType

// register declares a problem type that is not a typed domain error: it records the entry for
// allProblemTypes and returns the problemInfo to answer it with. The status is the one the call
// sites answer; allProblemTypes' tests use it to drive the real error writer.
func register(slug, title string, status int, class replayClass) problemInfo {
	info := problemInfo{slug, title, class}
	registered = append(registered, problemType{Slug: slug, Status: status, Class: class, info: info})
	return info
}

// The problem types that are not a typed domain error (those are the entries of problemCatalog in
// errors.go). Each is declared with register, which takes the replay class, so none can exist
// without one (replay_class_test.go scans for a problemInfo literal anywhere else).
//
// Request validation is FINAL: the answer is a function of the request alone. A refusal that
// depends on state, a dependency or the server itself is RETRYABLE. The one OUTCOME is the
// quarantined destination scan, which commits the inventory exception before it is refused.
var (
	problemTransferIDRequired      = register("transfer-id-required", "transferId is required", http.StatusBadRequest, classFinal)
	problemQuantityNotPositive     = register("quantity-not-positive", "quantity must be positive", http.StatusBadRequest, classFinal)
	problemBinsRequired            = register("bins-required", "bins is required", http.StatusBadRequest, classFinal)
	problemCountedQuantityRequired = register("counted-quantity-required", "countedQuantity is required", http.StatusBadRequest, classFinal)
	problemCapacityRequired        = register("capacity-required", "capacity is required", http.StatusBadRequest, classFinal)
	problemCapacityOutOfRange      = register("capacity-out-of-range", "Bin capacity is out of range", http.StatusUnprocessableEntity, classFinal)
	problemQuantityAndCases        = register("quantity-and-cases-exclusive", "quantity and cases are mutually exclusive", http.StatusBadRequest, classFinal)
	problemMissingDemandRef        = register("missing-demand-ref", "demandRef is required", http.StatusBadRequest, classFinal)
	problemMissingDemandRefQuery   = register("missing-demand-ref", "demandRef query parameter is required", http.StatusBadRequest, classFinal)
	problemInvalidLineNo           = register("invalid-line-no", "lineNo must be between 1 and 2147483647", http.StatusBadRequest, classFinal)

	problemMalformedJSON  = register(slugMalformedRequestBody, "The request body is not valid JSON", http.StatusBadRequest, classFinal)
	problemMalformedQuery = register(slugMalformedRequestBody, "Malformed request body", http.StatusBadRequest, classFinal)
	problemBodyUnreadable = register(slugMalformedRequestBody, "The request body could not be read", http.StatusBadRequest, classFinal)
	problemBodyHasNUL     = register(slugMalformedRequestBody, "The request body contains a NUL character", http.StatusBadRequest, classFinal)

	problemIdempotencyKeyRequired = register("idempotency-key-required", "Idempotency-Key header is required", http.StatusBadRequest, classFinal)
	problemIdempotencyKeyInvalid  = register("idempotency-key-invalid", "Idempotency-Key header is invalid", http.StatusBadRequest, classFinal)
	// problemIdempotencyKeyReused is the store's own answer to a key seen with another request; it
	// is never stored (no handler ran, no key row is written by the request that gets it).
	problemIdempotencyKeyReused = register("idempotency-key-reused", "Idempotency-Key was already used with a different request", http.StatusConflict, classFinal)

	problemRouteNotFound = register("route-not-found", "No such resource", http.StatusNotFound, classFinal)
	problemNotFound      = register("not-found", "Resource not found", http.StatusNotFound, classFinal)

	// The 400 problems of the read side (collections, held-stock filters, reports): no keyed route
	// answers them, they carry the class of the question "is it a function of the request alone".
	problemInvalidHoldReasonFilter = register("invalid-hold-reason", "reason must be DAMAGED, QC_PENDING, SUSPECT or OTHER", http.StatusBadRequest, classFinal)
	problemInvalidOlderThanHours   = register("invalid-older-than-hours", "olderThanHours must be a non-negative integer", http.StatusBadRequest, classFinal)
	problemInvalidLimit            = register("invalid-limit", "limit must be an integer between 1 and 500", http.StatusBadRequest, classFinal)
	problemInvalidCursor           = register("invalid-cursor", "cursor is not a valid page token", http.StatusBadRequest, classFinal)
	problemUnknownParam            = register("unknown-query-parameter", "the collection does not declare this query parameter", http.StatusBadRequest, classFinal)
	problemInvalidReportQuery      = register("invalid-report-query", "The report query is malformed or missing a required parameter", http.StatusBadRequest, classFinal)

	// Server-side answers: never stored (a 5xx), so RETRYABLE.
	problemInternalError         = register("internal-error", "An unexpected internal error occurred", http.StatusInternalServerError, classRetryable)
	problemReportStoreError      = register("report-store-error", "The report could not be served", http.StatusInternalServerError, classRetryable)
	problemDependencyUnavailable = register("dependency-unavailable", "A runtime dependency is unavailable", http.StatusServiceUnavailable, classRetryable)

	// problemTransferScanQuarantined answers a destination scan of a transfer line this service
	// never allocated: the scan is recorded as an inventory exception (committed) and refused.
	problemTransferScanQuarantined = register("transfer-scan-quarantined", "Destination scan quarantined", http.StatusUnprocessableEntity, classOutcome)
)
