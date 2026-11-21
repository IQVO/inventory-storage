//go:build integration

package http_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/pgtx"
)

// poisonTx simulates what a mapped domain conflict does to the middleware's
// transaction: a statement fails on the server (a unique violation, like a
// lost race on a unique index), the repository translates it to a typed
// error, and the handler answers 4xx -- but the transaction is ABORTED from
// then on. The middleware must still record and return that truthful 4xx.
func poisonTx(t *testing.T, r *http.Request) {
	t.Helper()
	tx, ok := pgtx.TxFrom(r.Context())
	if !ok {
		t.Error("the middleware did not bind a transaction into the request context")
		return
	}
	for i := 0; i < 2; i++ {
		if _, err := tx.Exec(r.Context(), `INSERT INTO idempotency_keys (key, method, path, request_hash) VALUES ('dup', 'm', 'p', 'h')`); err != nil {
			return // the second insert aborts the transaction (23505)
		}
	}
	t.Error("the duplicate insert did not abort the transaction")
}

// refusingHandler poisons the transaction like a lost unique-index race and then answers the problem
// type (slug, status) through the PRODUCTION error writer, so the answer carries its replay class.
func refusingHandler(t *testing.T, calls *atomic.Int64, slug string, status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		poisonTx(t, r)
		inboundhttp.RefuseForTest(w, r, slug, status)
	})
}

func keyedRequest(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/reservations", nil)
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	return req
}

// A FINAL refusal over an aborted transaction answers its own status (not 500), is committed, and
// replays without ever re-running the handler.
func TestIdempotency_AFinalRefusalFromAnAbortedTransactionCommitsTheOutcome(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	mw := inboundhttp.RequireIdempotencyKey(pool)(refusingHandler(t, &calls, "zero-quantity", http.StatusUnprocessableEntity))

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, keyedRequest("k-poison"))
	if first.Code != http.StatusUnprocessableEntity {
		t.Fatalf("first = %d %s, want the truthful 422", first.Code, first.Body.String())
	}

	var statusCode int
	if err := pool.QueryRow(t.Context(), `SELECT status_code FROM idempotency_keys WHERE key = 'k-poison'`).Scan(&statusCode); err != nil {
		t.Fatalf("the FINAL outcome was not committed: %v", err)
	}
	if statusCode != http.StatusUnprocessableEntity {
		t.Fatalf("cached status = %d", statusCode)
	}

	replay := httptest.NewRecorder()
	mw.ServeHTTP(replay, keyedRequest("k-poison"))
	if replay.Code != http.StatusUnprocessableEntity || replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q, want the cached 422", replay.Code, replay.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s, want %s", replay.Body.String(), first.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1 (replay must not re-run it)", calls.Load())
	}
}

// A RETRYABLE refusal over an aborted transaction is released like any other: the transaction rolls
// back (an aborted one can), the client gets the truthful 409, no row is left and the key retries.
func TestIdempotency_ARetryableRefusalFromAnAbortedTransactionIsReleased(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	mw := inboundhttp.RequireIdempotencyKey(pool)(refusingHandler(t, &calls, "concurrent-modification", http.StatusConflict))

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, keyedRequest("k-retryable"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the truthful 409", first.Code, first.Body.String())
	}
	if n := poisonQueryInt(t, pool, `SELECT count(*) FROM idempotency_keys WHERE key = 'k-retryable'`); n != 0 {
		t.Fatalf("a RETRYABLE refusal left %d idempotency rows", n)
	}
	second := httptest.NewRecorder()
	mw.ServeHTTP(second, keyedRequest("k-retryable"))
	if second.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" || calls.Load() != 2 {
		t.Fatalf("retry replayed=%q calls=%d, want a fresh execution (2 calls)", second.Header().Get(inboundhttp.IdempotencyReplayedHeader), calls.Load())
	}
}

// An OUTCOME refusal over an aborted transaction is a lie: its writes are gone, so it is neither
// returned nor stored (500) and the same key retries, exactly like a 2xx behind a failed release.
func TestIdempotency_AnOutcomeRefusalWhoseWritesAreLostIsNeverStored(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	mw := inboundhttp.RequireIdempotencyKey(pool)(refusingHandler(t, &calls, "reservation-already-resolved", http.StatusConflict))

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, keyedRequest("k-outcome-lost"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d %s, want 500: the OUTCOME's writes never committed", first.Code, first.Body.String())
	}
	if n := poisonQueryInt(t, pool, `SELECT count(*) FROM idempotency_keys WHERE key = 'k-outcome-lost'`); n != 0 {
		t.Fatalf("a lost OUTCOME left %d idempotency rows: the key could not retry", n)
	}
}

// A 2xx over an aborted transaction is a lie: 500, nothing cached, and the
// same key retries the real work.
func TestIdempotency_A2xxFromAnAbortedTransactionIsNeverCached(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			poisonTx(t, r) // only the first attempt dies inside the transaction
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mw := inboundhttp.RequireIdempotencyKey(pool)(handler)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, keyedRequest("k-lie"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d %s, want 500 (the writes never happened)", first.Code, first.Body.String())
	}
	if n := poisonQueryInt(t, pool, `SELECT count(*) FROM idempotency_keys WHERE key = 'k-lie'`); n != 0 {
		t.Fatalf("a failed success left %d idempotency rows: the key could not retry", n)
	}

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, keyedRequest("k-lie"))
	if second.Code != http.StatusCreated {
		t.Fatalf("retry = %d %s, want the real work to succeed", second.Code, second.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (the first attempt was discarded)", calls.Load())
	}
}

func poisonQueryInt(t *testing.T, pool *pgxpool.Pool, sql string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
