//go:build integration

// Integration tests for the transactional Idempotency-Key middleware
// (docs/docs/adr/0018-idempotency-key-middleware.md) against a real
// Postgres 16, through the REAL chi router (inboundhttp.NewRouter) over
// real net/http requests — not the middleware's internals in isolation.
// Testcontainers-only: the package's TestMain boots one disposable Postgres
// and each test gets a private database cloned from a migrated template
// (testdb_integration_test.go); never reads DATABASE_URL or hardcodes
// localhost, so CI cannot silently skip this contract. Ported from order-management PR #105 (ADR
// 0023) and extended to cover BOTH of this service's protected routes:
// POST /stock/receive (handleReceiveStock) and POST /reservations
// (handleReserveStock).
package http_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// countingPublisher wraps a real ports.EventPublisher and counts how many
// times Publish is actually invoked, keyed by event name — used by the
// POST /stock/receive concurrency scenario as the "was the real handler
// only actually executed once" proxy, since a StagedReceipt has no
// persisted table row of its own to count (see ReceiveStock's doc
// comment: it is an acknowledgment, not a durable aggregate).
type countingPublisher struct {
	mu    sync.Mutex
	inner ports.EventPublisher
	count int
}

func (p *countingPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	p.mu.Lock()
	p.count++
	p.mu.Unlock()
	return p.inner.Publish(ctx, event)
}

func (p *countingPublisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

type fixedIdempotencyClock time.Time

func (c fixedIdempotencyClock) Now() time.Time { return time.Time(c) }

// idempotencyFixture wires the REAL chi router (inboundhttp.NewRouter)
// with Postgres-backed repos + UnitOfWork, so POST /stock/receive and
// POST /reservations' full request cycles — idempotency bookkeeping, the
// use case's own work, the outbox-free log-publisher call — run through
// the exact same transaction-join mechanism production uses
// (internal/pgtx via postgres.UnitOfWork.Execute).
type idempotencyFixture struct {
	router    http.Handler
	pool      *pgxpool.Pool
	publisher *countingPublisher
}

func newIdempotencyFixture(t *testing.T, pool *pgxpool.Pool) idempotencyFixture {
	t.Helper()
	stockRepo := postgres.NewStockRepo(pool)
	reservationRepo := postgres.NewReservationRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	logPub := events.NewLogPublisher(slog.New(slog.NewTextHandler(io.Discard, nil)))
	publisher := &countingPublisher{inner: logPub}
	clock := fixedIdempotencyClock(time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC))

	server := &inboundhttp.Server{
		ReceiveStock: &usecases.ReceiveStock{Events: publisher, Clock: clock, UnitOfWork: uow},
		ReserveStock: &usecases.ReserveStock{
			Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow,
		},
		IdempotencyPool: pool,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return idempotencyFixture{router: inboundhttp.NewRouter(server, logger, ""), pool: pool, publisher: publisher}
}

// seedStock gives sku usable quantity on a fresh bin, via the same repos
// the router itself uses — a direct-to-Postgres fixture setup step, not a
// call through the HTTP layer (mirrors this fleet's established pattern
// for arranging pre-conditions ahead of a probe under test).
func seedStock(t *testing.T, pool *pgxpool.Pool, sku string, qty int) {
	t.Helper()
	ctx := context.Background()
	locationRepo := postgres.NewLocationRepo(pool)
	stockRepo := postgres.NewStockRepo(pool)

	binID, err := shared.NewBinId("IDEMP-BIN-" + sku)
	if err != nil {
		t.Fatalf("build bin id: %v", err)
	}
	bin, err := location.NewBin(binID, mustIdempQty(t, qty+1))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := locationRepo.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}

	skuVO, err := shared.NewSKU(sku)
	if err != nil {
		t.Fatalf("build sku: %v", err)
	}
	id, err := stockRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("next stock unit id: %v", err)
	}
	unit, err := stock.NewStockUnit(id, skuVO, binID, mustIdempQty(t, qty))
	if err != nil {
		t.Fatalf("build stock unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save stock unit: %v", err)
	}
}

func mustIdempQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("build quantity: %v", err)
	}
	return q
}

func countReservationRows(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM reservations").Scan(&n); err != nil {
		t.Fatalf("count reservations: %v", err)
	}
	return n
}

func idempotencyRowOutcome(t *testing.T, pool *pgxpool.Pool, key string) (storedHash string, statusCode *int, body []byte) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		"SELECT request_hash, status_code, response_body FROM idempotency_keys WHERE key = $1", key,
	).Scan(&storedHash, &statusCode, &body); err != nil {
		t.Fatalf("read idempotency row: %v", err)
	}
	return storedHash, statusCode, body
}

func problemSlug(t *testing.T, body []byte) string {
	t.Helper()
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &problem); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, body)
	}
	idx := strings.LastIndex(problem.Type, "/")
	if idx == -1 {
		return problem.Type
	}
	return problem.Type[idx+1:]
}

// ---------------------------------------------------------------------
// POST /stock/receive
// ---------------------------------------------------------------------

const receiveBody = `{"sku":"SKU-RECEIVE-1","quantity":10}`

func doReceive(router http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/stock/receive", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// (a) fresh key -> 202, outcome recorded verbatim.
func TestIdempotency_Receive_FreshKey_RecordsOutcome(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)

	rec := doReceive(fx.router, "recv-fresh-1", receiveBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := fx.publisher.Count(); got != 1 {
		t.Fatalf("publish count = %d, want 1", got)
	}

	_, statusCode, body := idempotencyRowOutcome(t, pool, "recv-fresh-1")
	if statusCode == nil || *statusCode != http.StatusAccepted {
		t.Fatalf("stored status_code = %v, want 202", statusCode)
	}
	if string(body) != rec.Body.String() {
		t.Fatalf("stored response_body does not match what was returned to the caller")
	}
}

// (b) replay: same key + identical body -> byte-identical response, the
// real handler (and its Publish call) never runs a second time.
func TestIdempotency_Receive_Replay_ReturnsIdenticalResponseNoDoublePublish(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)

	first := doReceive(fx.router, "recv-replay-1", receiveBody)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first call status = %d, want 202 (body: %s)", first.Code, first.Body.String())
	}
	second := doReceive(fx.router, "recv-replay-1", receiveBody)
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs:\nfirst:  %s\nsecond: %s", first.Body.String(), second.Body.String())
	}
	if got := fx.publisher.Count(); got != 1 {
		t.Fatalf("publish count after replay = %d, want exactly 1 (no re-execution)", got)
	}
}

// (c) same key + different body -> 422.
func TestIdempotency_Receive_SameKeyDifferentBody_Returns422(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)

	first := doReceive(fx.router, "recv-mismatch-1", receiveBody)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first call status = %d, want 202 (body: %s)", first.Code, first.Body.String())
	}

	second := doReceive(fx.router, "recv-mismatch-1", `{"sku":"SKU-RECEIVE-2","quantity":5}`)
	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body: %s)", second.Code, second.Body.String())
	}
	if slug := problemSlug(t, second.Body.Bytes()); slug != "idempotency-key-reused" {
		t.Fatalf("problem.type slug = %q, want idempotency-key-reused", slug)
	}
	if got := fx.publisher.Count(); got != 1 {
		t.Fatalf("publish count = %d, want 1 (mismatched retry must not re-execute)", got)
	}
}

// (d) no key header -> 400.
func TestIdempotency_Receive_NoHeader_Returns400(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)

	rec := doReceive(fx.router, "", receiveBody)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	if slug := problemSlug(t, rec.Body.Bytes()); slug != "idempotency-key-required" {
		t.Fatalf("problem.type slug = %q, want idempotency-key-required", slug)
	}
	if got := fx.publisher.Count(); got != 0 {
		t.Fatalf("publish count = %d, want 0 (handler must never run without the header)", got)
	}
}

// (e) concurrency: N real goroutines, same key + body, real HTTP through
// the real router and the real Postgres unique-index lock — not a
// sequential simulation. Exactly one real Publish call happens.
func TestIdempotency_Receive_Concurrent_ExactlyOnePublish(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)

	const n = 5
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = doReceive(fx.router, "recv-concurrent-1", receiveBody)
		}(i)
	}
	wg.Wait()

	for i, rec := range results {
		if rec.Code != http.StatusAccepted {
			t.Fatalf("goroutine %d status = %d, want 202 (body: %s)", i, rec.Code, rec.Body.String())
		}
		if rec.Body.String() != results[0].Body.String() {
			t.Fatalf("goroutine %d body differs from goroutine 0", i)
		}
	}
	if got := fx.publisher.Count(); got != 1 {
		t.Fatalf("publish count after %d concurrent identical requests = %d, want exactly 1", n, got)
	}
}

// (f) a business validation error (zero quantity) is cached and replayed
// verbatim on retry, not re-validated.
func TestIdempotency_Receive_BusinessErrorResponse_IsCachedAndReplayed(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)

	invalidBody := `{"sku":"SKU-RECEIVE-BAD","quantity":0}`

	first := doReceive(fx.router, "recv-business-error-1", invalidBody)
	if first.Code != http.StatusUnprocessableEntity {
		t.Fatalf("first call status = %d, want 422 (body: %s)", first.Code, first.Body.String())
	}

	_, statusCode, _ := idempotencyRowOutcome(t, pool, "recv-business-error-1")
	if statusCode == nil || *statusCode != http.StatusUnprocessableEntity {
		t.Fatalf("stored status_code = %v, want 422 — the business error response must be cached", statusCode)
	}

	second := doReceive(fx.router, "recv-business-error-1", invalidBody)
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d (cached error response)", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs from the cached error response")
	}
	if got := fx.publisher.Count(); got != 0 {
		t.Fatalf("publish count = %d, want 0 (invalid receive must never publish, either time)", got)
	}
}

// ---------------------------------------------------------------------
// POST /reservations
// ---------------------------------------------------------------------

const reserveBodyTmpl = `{"sku":"%s","quantity":6,"demandRef":"%s"}`

func doReserve(router http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/reservations", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// (a) fresh key + valid body -> 201, reservation persisted, outcome
// recorded verbatim.
func TestIdempotency_Reserve_FreshKey_CreatesReservationAndRecordsOutcome(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-A", 100)
	body := sprintfReserve("SKU-RESERVE-A", "order-a1")

	rec := doReserve(fx.router, "res-fresh-1", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := countReservationRows(t, pool); got != 1 {
		t.Fatalf("reservations rows = %d, want 1", got)
	}

	_, statusCode, body2 := idempotencyRowOutcome(t, pool, "res-fresh-1")
	if statusCode == nil || *statusCode != http.StatusCreated {
		t.Fatalf("stored status_code = %v, want 201", statusCode)
	}
	if string(body2) != rec.Body.String() {
		t.Fatalf("stored response_body does not match what was returned to the caller")
	}
}

// (b) replay: same key + identical body -> byte-identical response, no
// duplicate reservation.
func TestIdempotency_Reserve_Replay_ReturnsIdenticalResponseNoDuplicate(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-B", 100)
	body := sprintfReserve("SKU-RESERVE-B", "order-b1")

	first := doReserve(fx.router, "res-replay-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first call status = %d, want 201 (body: %s)", first.Code, first.Body.String())
	}
	second := doReserve(fx.router, "res-replay-1", body)
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs:\nfirst:  %s\nsecond: %s", first.Body.String(), second.Body.String())
	}
	if second.Header().Get("Location") != first.Header().Get("Location") {
		t.Fatalf("replay Location = %q, want %q", second.Header().Get("Location"), first.Header().Get("Location"))
	}
	if got := countReservationRows(t, pool); got != 1 {
		t.Fatalf("reservations rows after replay = %d, want exactly 1 (no duplicate reservation)", got)
	}
}

// (c) same key + different body -> 422.
func TestIdempotency_Reserve_SameKeyDifferentBody_Returns422(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-C", 100)

	first := doReserve(fx.router, "res-mismatch-1", sprintfReserve("SKU-RESERVE-C", "order-c1"))
	if first.Code != http.StatusCreated {
		t.Fatalf("first call status = %d, want 201 (body: %s)", first.Code, first.Body.String())
	}

	second := doReserve(fx.router, "res-mismatch-1", sprintfReserve("SKU-RESERVE-C", "order-c2"))
	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body: %s)", second.Code, second.Body.String())
	}
	if slug := problemSlug(t, second.Body.Bytes()); slug != "idempotency-key-reused" {
		t.Fatalf("problem.type slug = %q, want idempotency-key-reused", slug)
	}
	if got := countReservationRows(t, pool); got != 1 {
		t.Fatalf("reservations rows = %d, want 1 (the mismatched retry must not create a second reservation)", got)
	}
}

// (d) no key header -> 400.
func TestIdempotency_Reserve_NoHeader_Returns400(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-D", 100)

	rec := doReserve(fx.router, "", sprintfReserve("SKU-RESERVE-D", "order-d1"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	if slug := problemSlug(t, rec.Body.Bytes()); slug != "idempotency-key-required" {
		t.Fatalf("problem.type slug = %q, want idempotency-key-required", slug)
	}
	if got := countReservationRows(t, pool); got != 0 {
		t.Fatalf("reservations rows = %d, want 0 (no reservation should be created without the header)", got)
	}
}

// (e) concurrency: N real goroutines, same key + body, real HTTP through
// the real router and the real Postgres unique-index lock — exactly one
// reservation is created despite N racing identical requests.
func TestIdempotency_Reserve_Concurrent_ExactlyOneReservationCreated(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-E", 100)
	body := sprintfReserve("SKU-RESERVE-E", "order-e1")

	const n = 5
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = doReserve(fx.router, "res-concurrent-1", body)
		}(i)
	}
	wg.Wait()

	for i, rec := range results {
		if rec.Code != http.StatusCreated {
			t.Fatalf("goroutine %d status = %d, want 201 (body: %s)", i, rec.Code, rec.Body.String())
		}
		if rec.Body.String() != results[0].Body.String() {
			t.Fatalf("goroutine %d body differs from goroutine 0:\n%d: %s\n0: %s", i, i, rec.Body.String(), results[0].Body.String())
		}
	}
	if got := countReservationRows(t, pool); got != 1 {
		t.Fatalf("reservations rows after %d concurrent identical requests = %d, want exactly 1", n, got)
	}
}

// (f) a business validation error (reserve quantity exceeding usable
// inventory) is cached and replayed on retry, not re-decided.
func TestIdempotency_Reserve_BusinessErrorResponse_IsCachedAndReplayed(t *testing.T) {
	pool := idempotencyDB(t)
	fx := newIdempotencyFixture(t, pool)
	seedStock(t, pool, "SKU-RESERVE-F", 5) // only 5 usable

	overReserveBody := `{"sku":"SKU-RESERVE-F","quantity":999,"demandRef":"order-f1"}`

	first := doReserve(fx.router, "res-business-error-1", overReserveBody)
	if first.Code != http.StatusConflict {
		t.Fatalf("first call status = %d, want 409 (body: %s)", first.Code, first.Body.String())
	}

	_, statusCode, _ := idempotencyRowOutcome(t, pool, "res-business-error-1")
	if statusCode == nil || *statusCode != http.StatusConflict {
		t.Fatalf("stored status_code = %v, want 409 — the business error response must be cached", statusCode)
	}

	second := doReserve(fx.router, "res-business-error-1", overReserveBody)
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d (cached error response)", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs from the cached error response")
	}
	if got := countReservationRows(t, pool); got != 0 {
		t.Fatalf("reservations rows = %d, want 0 (the over-reserve was never persisted, either time)", got)
	}
}

func sprintfReserve(sku, demandRef string) string {
	return `{"sku":"` + sku + `","quantity":6,"demandRef":"` + demandRef + `"}`
}
