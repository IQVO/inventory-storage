//go:build integration

// The REST API guideline's idempotency contract (docs/docs/adr/0049) for EVERY
// POST route of this service, through the real chi router
// (inboundhttp.NewRouter) over a real Postgres 16 (testcontainers, see
// testdb_integration_test.go) with the real UnitOfWork and the real
// transactional outbox:
//
//   - every POST route is behind the middleware (a missing key is 400
//     idempotency-key-required, a malformed one 400 idempotency-key-invalid);
//   - the previously unkeyed POST /stowages, /reservations/{id}/pick-confirmations
//     and /bins/{binId}/cycle-counts: fresh key, byte-identical replay with
//     Idempotency-Replayed: true, key reuse 409, N concurrent requests with one
//     key give exactly one aggregate change, a 4xx is cached and replayed;
//   - the key row, the aggregate rows and the outbox rows commit together, and a
//     5xx (or a panic) rolls ALL of them back, so the retry executes again;
//   - a use case that fails half-way leaves none of its writes behind even
//     though the 4xx answer is cached (the savepoint of a joined unit of work).
package http_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// outboxStubEncoder gives every event exactly one outbox row, so a test counts
// the rows an operation (or its replay) wrote.
type outboxStubEncoder struct{}

func (outboxStubEncoder) Encode(_ context.Context, event shared.DomainEvent) ([]kafka.Encoded, error) {
	return []kafka.Encoded{{Topic: "test.outbox", EventType: event.EventName(), Value: []byte(`{}`)}}, nil
}

// faultyPublisher wraps the real outbox publisher and can fail or panic ONCE,
// on the next Publish, to prove a half-finished use case leaves nothing behind.
type faultyPublisher struct {
	inner *postgres.OutboxPublisher
	mu    sync.Mutex
	fail  error
	boom  bool
}

func (p *faultyPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	p.mu.Lock()
	fail, boom := p.fail, p.boom
	p.fail, p.boom = nil, false
	p.mu.Unlock()
	if boom {
		panic("injected panic after the aggregate was written")
	}
	if fail != nil {
		return fail
	}
	return p.inner.Publish(ctx, event)
}

func (p *faultyPublisher) failNext(err error) { p.mu.Lock(); p.fail = err; p.mu.Unlock() }
func (p *faultyPublisher) panicNext()         { p.mu.Lock(); p.boom = true; p.mu.Unlock() }

type routesFixture struct {
	router    http.Handler
	pool      *pgxpool.Pool
	publisher *faultyPublisher
}

func newRoutesFixture(t *testing.T) routesFixture {
	t.Helper()
	pool := idempotencyDB(t)
	stockRepo := postgres.NewStockRepo(pool)
	locationRepo := postgres.NewLocationRepo(pool)
	reservationRepo := postgres.NewReservationRepo(pool)
	containerRepo := postgres.NewContainerRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	publisher := &faultyPublisher{inner: postgres.NewOutboxPublisher(pool, outboxStubEncoder{})}
	clock := fixedIdempotencyClock(time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC))

	reserve := &usecases.ReserveStock{Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow}
	server := &inboundhttp.Server{
		ReceiveStock: &usecases.ReceiveStock{Events: publisher, Clock: clock, UnitOfWork: uow},
		StowStock:    &usecases.StowStock{Stock: stockRepo, Locations: locationRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		ReserveStock: reserve,
		ConfirmPick: &usecases.ConfirmPick{
			Stock: stockRepo, Locations: locationRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow,
		},
		RunCycleCount: &usecases.RunCycleCount{Stock: stockRepo, Events: publisher, Clock: clock, UnitOfWork: uow},
		GetReservationsByDemandRef: &usecases.GetReservationsByDemandRef{
			Stock: stockRepo, Reservations: reservationRepo, Events: publisher, Clock: clock, UnitOfWork: uow,
		},
		ListBinContainers: &usecases.ListBinContainers{Containers: containerRepo, Locations: locationRepo},
		IdempotencyPool:   pool,
	}
	router := inboundhttp.NewRouter(server, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	return routesFixture{router: router, pool: pool, publisher: publisher}
}

func (f routesFixture) post(path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

func (f routesFixture) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (f routesFixture) keyRows(t *testing.T, key string) int {
	return f.count(t, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key)
}

func (f routesFixture) outboxRows(t *testing.T) int {
	return f.count(t, `SELECT count(*) FROM outbox_events`)
}

func (f routesFixture) seedBin(t *testing.T, id string, capacity int) {
	t.Helper()
	binID, err := shared.NewBinId(id)
	if err != nil {
		t.Fatal(err)
	}
	bin, err := location.NewBin(binID, mustIdempQty(t, capacity))
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewLocationRepo(f.pool).Save(context.Background(), bin); err != nil {
		t.Fatalf("seed bin: %v", err)
	}
}

func assertReplay(t *testing.T, first, second *httptest.ResponseRecorder) {
	t.Helper()
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("replay differs:\nfirst:  %d %s\nsecond: %d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	for _, h := range []string{"Content-Type", "Location"} {
		if second.Header().Get(h) != first.Header().Get(h) {
			t.Fatalf("replay %s = %q, want %q", h, second.Header().Get(h), first.Header().Get(h))
		}
	}
	if got := first.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("the first execution must not carry %s, got %q", inboundhttp.IdempotencyReplayedHeader, got)
	}
	if got := second.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "true" {
		t.Fatalf("the replay must carry %s: true, got %q", inboundhttp.IdempotencyReplayedHeader, got)
	}
	if got := second.Header().Get("Idempotent-Replayed"); got != "" {
		t.Fatalf("the retired Idempotent-Replayed spelling must never be emitted, got %q", got)
	}
}

// Every POST route is behind the middleware. The routes are enumerated from the
// live router, so a POST route added later without the middleware fails here.
func TestIdempotency_EveryPostRouteRequiresAValidKey(t *testing.T) {
	fx := newRoutesFixture(t)
	braces := regexp.MustCompile(`\{[^}]*\}`)

	var posts []string
	err := chi.Walk(fx.router.(chi.Routes), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if method == http.MethodPost {
			posts = append(posts, braces.ReplaceAllString(route, "x"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/stock-receipts", "/stowages", "/reservations", "/reservations/x/pick-confirmations", "/bins/x/cycle-counts",
		"/transfers/x/receipt", "/transfers/x/stowages", "/relocations", "/containers/x/stowages", "/containers/x/case-breaks",
	}
	if len(posts) != len(want) {
		t.Fatalf("POST routes = %v, want exactly %v", posts, want)
	}
	for _, w := range want {
		found := false
		for _, p := range posts {
			found = found || p == w
		}
		if !found {
			t.Fatalf("POST route %s is missing from %v", w, posts)
		}
	}

	for _, path := range posts {
		rec := fx.post(path, "", `{}`)
		if rec.Code != http.StatusBadRequest || problemSlug(t, rec.Body.Bytes()) != "idempotency-key-required" {
			t.Errorf("POST %s without a key = %d %s, want 400 idempotency-key-required", path, rec.Code, rec.Body.String())
		}
		for _, bad := range []string{strings.Repeat("k", 129), "bad\x01key", "clé"} {
			rec := fx.post(path, bad, `{}`)
			if rec.Code != http.StatusBadRequest || problemSlug(t, rec.Body.Bytes()) != "idempotency-key-invalid" {
				t.Errorf("POST %s with key %q = %d %s, want 400 idempotency-key-invalid", path, bad, rec.Code, rec.Body.String())
			}
		}
	}
	if n := fx.count(t, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("idempotency_keys rows after refusals = %d, want 0", n)
	}
	// 1 and 128 characters are both fine.
	for _, ok := range []string{"k", strings.Repeat("k", 128)} {
		if rec := fx.post("/stock-receipts", ok, `{"sku":"SKU-LEN","quantity":1}`); rec.Code != http.StatusAccepted {
			t.Fatalf("key of %d chars = %d %s, want 202", len(ok), rec.Code, rec.Body.String())
		}
	}
}

// The fingerprint is method + path + body: the same key on another route is key
// reuse even when the body bytes are identical.
func TestIdempotency_SameKeyOnAnotherPathIs409(t *testing.T) {
	fx := newRoutesFixture(t)
	body := `{"sku":"SKU-PATH","quantity":3}`
	if first := fx.post("/stock-receipts", "path-key", body); first.Code != http.StatusAccepted {
		t.Fatalf("first = %d %s", first.Code, first.Body.String())
	}
	second := fx.post("/stowages", "path-key", body)
	if second.Code != http.StatusConflict || problemSlug(t, second.Body.Bytes()) != "idempotency-key-reused" {
		t.Fatalf("same key, other path = %d %s, want 409 idempotency-key-reused", second.Code, second.Body.String())
	}
	if second.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("a key-reuse refusal is not a replay")
	}
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 0 {
		t.Fatalf("stock_units = %d, want 0 (the refused request must not execute)", n)
	}
}

// ---------------------------------------------------------------------
// POST /stowages
// ---------------------------------------------------------------------

const stowBody = `{"sku":"SKU-STOW-1","quantity":4,"binId":"STOW-BIN-1"}`

func TestIdempotency_Stow_FreshKeyThenReplayThenReuse(t *testing.T) {
	fx := newRoutesFixture(t)
	fx.seedBin(t, "STOW-BIN-1", 100)

	first := fx.post("/stowages", "stow-key-1", stowBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first = %d %s, want 201", first.Code, first.Body.String())
	}
	// The key row, the stock unit, the bin occupancy and the outbox rows are all there.
	if n := fx.keyRows(t, "stow-key-1"); n != 1 {
		t.Fatalf("key rows = %d, want 1", n)
	}
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 1 {
		t.Fatalf("stock_units = %d, want 1", n)
	}
	if n := fx.count(t, `SELECT occupied FROM bins WHERE id = 'STOW-BIN-1'`); n != 4 {
		t.Fatalf("bin occupied = %d, want 4", n)
	}
	outbox := fx.outboxRows(t)
	if outbox != 2 {
		t.Fatalf("outbox rows = %d, want 2 (ItemStowed + LocationRecorded)", outbox)
	}

	second := fx.post("/stowages", "stow-key-1", stowBody)
	assertReplay(t, first, second)
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 1 {
		t.Fatalf("stock_units after replay = %d, want 1", n)
	}
	if n := fx.count(t, `SELECT occupied FROM bins WHERE id = 'STOW-BIN-1'`); n != 4 {
		t.Fatalf("bin occupied after replay = %d, want 4 (stowed once)", n)
	}
	if n := fx.outboxRows(t); n != outbox {
		t.Fatalf("outbox rows after replay = %d, want %d", n, outbox)
	}

	other := fx.post("/stowages", "stow-key-1", `{"sku":"SKU-STOW-1","quantity":5,"binId":"STOW-BIN-1"}`)
	if other.Code != http.StatusConflict || problemSlug(t, other.Body.Bytes()) != "idempotency-key-reused" {
		t.Fatalf("same key, other body = %d %s, want 409 idempotency-key-reused", other.Code, other.Body.String())
	}
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 1 {
		t.Fatalf("stock_units after reuse = %d, want 1", n)
	}
}

func TestIdempotency_Stow_Concurrent_ExactlyOneStockUnit(t *testing.T) {
	fx := newRoutesFixture(t)
	fx.seedBin(t, "STOW-BIN-1", 100)

	const n = 5
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = fx.post("/stowages", "stow-concurrent", stowBody)
		}(i)
	}
	wg.Wait()

	replays := 0
	for i, rec := range results {
		if rec.Code != http.StatusCreated || rec.Body.String() != results[0].Body.String() {
			t.Fatalf("request %d = %d %s, want the one 201 answer %s", i, rec.Code, rec.Body.String(), results[0].Body.String())
		}
		if rec.Header().Get(inboundhttp.IdempotencyReplayedHeader) == "true" {
			replays++
		}
	}
	if replays != n-1 {
		t.Fatalf("replayed answers = %d, want %d (exactly one request executed)", replays, n-1)
	}
	if got := fx.count(t, `SELECT count(*) FROM stock_units`); got != 1 {
		t.Fatalf("stock_units after %d concurrent identical requests = %d, want exactly 1", n, got)
	}
	if got := fx.count(t, `SELECT occupied FROM bins WHERE id = 'STOW-BIN-1'`); got != 4 {
		t.Fatalf("bin occupied = %d, want 4", got)
	}
}

func TestIdempotency_Stow_4xxIsCachedAndReplayed(t *testing.T) {
	fx := newRoutesFixture(t)

	body := `{"sku":"SKU-STOW-1","quantity":4,"binId":"NO-SUCH-BIN"}`
	first := fx.post("/stowages", "stow-404", body)
	if first.Code != http.StatusNotFound {
		t.Fatalf("first = %d %s, want 404 bin-not-found", first.Code, first.Body.String())
	}
	var stored int
	if err := fx.pool.QueryRow(context.Background(), `SELECT status_code FROM idempotency_keys WHERE key = 'stow-404'`).Scan(&stored); err != nil || stored != 404 {
		t.Fatalf("stored status = %d (%v), want the cached 404", stored, err)
	}
	assertReplay(t, first, fx.post("/stowages", "stow-404", body))
}

func TestIdempotency_5xxIsNotStoredAndTheRetryExecutes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*faultyPublisher)
	}{
		{"error", func(p *faultyPublisher) { p.failNext(errors.New("injected infrastructure failure")) }},
		{"panic", func(p *faultyPublisher) { p.panicNext() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newRoutesFixture(t)
			fx.seedBin(t, "STOW-BIN-1", 100)

			tc.break_(fx.publisher)
			failed := fx.post("/stowages", "stow-5xx", stowBody)
			if failed.Code != http.StatusInternalServerError {
				t.Fatalf("failed attempt = %d %s, want 500", failed.Code, failed.Body.String())
			}
			// Nothing of the attempt survives: no key row, no stock unit, no occupancy, no outbox.
			if n := fx.keyRows(t, "stow-5xx"); n != 0 {
				t.Fatalf("key rows after a 5xx = %d, want 0 (a 5xx is never stored)", n)
			}
			if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 0 {
				t.Fatalf("stock_units after a 5xx = %d, want 0", n)
			}
			if n := fx.count(t, `SELECT occupied FROM bins WHERE id = 'STOW-BIN-1'`); n != 0 {
				t.Fatalf("bin occupied after a 5xx = %d, want 0", n)
			}
			if n := fx.outboxRows(t); n != 0 {
				t.Fatalf("outbox rows after a 5xx = %d, want 0", n)
			}

			retry := fx.post("/stowages", "stow-5xx", stowBody)
			if retry.Code != http.StatusCreated || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
				t.Fatalf("retry = %d %s (replayed=%q), want a fresh 201", retry.Code, retry.Body.String(), retry.Header().Get(inboundhttp.IdempotencyReplayedHeader))
			}
			if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 1 {
				t.Fatalf("stock_units after the retry = %d, want 1", n)
			}
		})
	}
}

// A use case that fails half-way (the aggregate is written, the event is not)
// must leave none of its writes behind even though the 4xx answer is cached:
// the joined unit of work runs in a savepoint of the middleware's transaction.
func TestIdempotency_FailedUseCaseLeavesNoPartialWrites(t *testing.T) {
	fx := newRoutesFixture(t)
	fx.seedBin(t, "STOW-BIN-1", 100)

	fx.publisher.failNext(usecases.ErrConcurrentModification)
	first := fx.post("/stowages", "stow-partial", stowBody)
	if first.Code != http.StatusConflict || problemSlug(t, first.Body.Bytes()) != "concurrent-modification" {
		t.Fatalf("first = %d %s, want 409 concurrent-modification", first.Code, first.Body.String())
	}
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 0 {
		t.Fatalf("stock_units = %d, want 0: the unit written before the failure must be rolled back", n)
	}
	if n := fx.count(t, `SELECT occupied FROM bins WHERE id = 'STOW-BIN-1'`); n != 0 {
		t.Fatalf("bin occupied = %d, want 0: the occupancy written before the failure must be rolled back", n)
	}
	if n := fx.outboxRows(t); n != 0 {
		t.Fatalf("outbox rows = %d, want 0", n)
	}
	// The deterministic answer is cached: the same key replays the 409, nothing is retried.
	assertReplay(t, first, fx.post("/stowages", "stow-partial", stowBody))
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 0 {
		t.Fatalf("stock_units after the replay = %d, want 0", n)
	}
}

// ---------------------------------------------------------------------
// POST /reservations/{id}/pick-confirmations
// ---------------------------------------------------------------------

// occupyBin marks the bin seedStock created for sku as holding qty units. seedStock writes the
// stock unit only; a pick confirmation RELEASES that quantity from the bin, so the bin must
// really hold it or the release is refused as release-exceeds-occupancy.
func occupyBin(t *testing.T, pool *pgxpool.Pool, sku string, qty int) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `UPDATE bins SET occupied = $2 WHERE id = $1`, "IDEMP-BIN-"+sku, qty); err != nil {
		t.Fatalf("occupy bin: %v", err)
	}
}

func (f routesFixture) reserveOne(t *testing.T, sku, demandRef string) string {
	t.Helper()
	rec := f.post("/reservations", "seed-"+demandRef, `{"sku":"`+sku+`","quantity":6,"demandRef":"`+demandRef+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("seed reservation = %d %s", rec.Code, rec.Body.String())
	}
	loc := rec.Header().Get("Location")
	return strings.TrimPrefix(loc, "/reservations/")
}

func TestIdempotency_ConfirmPick_ReplayConfirmsOnce(t *testing.T) {
	fx := newRoutesFixture(t)
	seedStock(t, fx.pool, "SKU-PICK-1", 10)
	occupyBin(t, fx.pool, "SKU-PICK-1", 10)
	id := fx.reserveOne(t, "SKU-PICK-1", "order-pick-1")
	path := "/reservations/" + id + "/pick-confirmations"
	before := fx.outboxRows(t)

	first := fx.post(path, "pick-key-1", "")
	if first.Code != http.StatusNoContent {
		t.Fatalf("first = %d %s, want 204", first.Code, first.Body.String())
	}
	if n := fx.count(t, `SELECT count(*) FROM reservations WHERE id = $1 AND status = 'CONFIRMED'`, id); n != 1 {
		t.Fatalf("confirmed reservations = %d, want 1", n)
	}
	if n := fx.count(t, `SELECT quantity FROM stock_units WHERE sku = 'SKU-PICK-1'`); n != 4 {
		t.Fatalf("stock unit quantity = %d, want 4 (10 - 6 picked once)", n)
	}
	picked := fx.outboxRows(t) - before
	if picked != 1 {
		t.Fatalf("outbox rows written by the pick = %d, want 1 (StockPicked)", picked)
	}

	second := fx.post(path, "pick-key-1", "")
	assertReplay(t, first, second)
	if n := fx.count(t, `SELECT quantity FROM stock_units WHERE sku = 'SKU-PICK-1'`); n != 4 {
		t.Fatalf("stock unit quantity after replay = %d, want still 4", n)
	}
	if n := fx.outboxRows(t) - before; n != picked {
		t.Fatalf("outbox rows after replay = %d, want %d", n, picked)
	}

	// A NEW key on an already confirmed reservation is a business 409, distinguishable
	// from key reuse by its problem type.
	again := fx.post(path, "pick-key-2", "")
	if again.Code != http.StatusConflict || problemSlug(t, again.Body.Bytes()) == "idempotency-key-reused" {
		t.Fatalf("second confirmation with a new key = %d %s, want a business 409", again.Code, again.Body.String())
	}
	// The same key on another reservation id is key reuse.
	reused := fx.post("/reservations/res-other/pick-confirmations", "pick-key-1", "")
	if reused.Code != http.StatusConflict || problemSlug(t, reused.Body.Bytes()) != "idempotency-key-reused" {
		t.Fatalf("same key, other path = %d %s, want 409 idempotency-key-reused", reused.Code, reused.Body.String())
	}
}

func TestIdempotency_ConfirmPick_Concurrent_PicksOnce(t *testing.T) {
	fx := newRoutesFixture(t)
	seedStock(t, fx.pool, "SKU-PICK-2", 10)
	occupyBin(t, fx.pool, "SKU-PICK-2", 10)
	id := fx.reserveOne(t, "SKU-PICK-2", "order-pick-2")
	path := "/reservations/" + id + "/pick-confirmations"
	before := fx.outboxRows(t)

	const n = 5
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = fx.post(path, "pick-concurrent", "")
		}(i)
	}
	wg.Wait()
	for i, rec := range results {
		if rec.Code != http.StatusNoContent {
			t.Fatalf("request %d = %d %s, want 204", i, rec.Code, rec.Body.String())
		}
	}
	if got := fx.count(t, `SELECT quantity FROM stock_units WHERE sku = 'SKU-PICK-2'`); got != 4 {
		t.Fatalf("stock unit quantity = %d, want 4 (picked exactly once)", got)
	}
	if got := fx.outboxRows(t) - before; got != 1 {
		t.Fatalf("StockPicked outbox rows = %d, want exactly 1", got)
	}
}

func TestIdempotency_ConfirmPick_4xxIsCachedAndReplayed(t *testing.T) {
	fx := newRoutesFixture(t)
	first := fx.post("/reservations/res-unknown/pick-confirmations", "pick-404", "")
	if first.Code != http.StatusNotFound || problemSlug(t, first.Body.Bytes()) != "reservation-not-found" {
		t.Fatalf("first = %d %s, want 404 reservation-not-found", first.Code, first.Body.String())
	}
	assertReplay(t, first, fx.post("/reservations/res-unknown/pick-confirmations", "pick-404", ""))
}

// ---------------------------------------------------------------------
// POST /bins/{binId}/cycle-counts
// ---------------------------------------------------------------------

func TestIdempotency_CycleCount_ReplayCountsOnce(t *testing.T) {
	fx := newRoutesFixture(t)
	seedStock(t, fx.pool, "SKU-CC-1", 10)
	path := "/bins/IDEMP-BIN-SKU-CC-1/cycle-counts"
	body := `{"countedQuantity":7}`

	first := fx.post(path, "cc-key-1", body)
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"discrepancy":true`) {
		t.Fatalf("first = %d %s, want 200 with a discrepancy", first.Code, first.Body.String())
	}
	if n := fx.count(t, `SELECT count(*) FROM stock_units WHERE sku = 'SKU-CC-1' AND state = 'UNLOCATED'`); n != 1 {
		t.Fatalf("unlocated units = %d, want 1", n)
	}
	events := fx.outboxRows(t)
	if events != 3 {
		t.Fatalf("outbox rows = %d, want 3 (DiscrepancyDetected, ItemUnlocated, CycleCountCompleted)", events)
	}

	second := fx.post(path, "cc-key-1", body)
	assertReplay(t, first, second)
	if n := fx.outboxRows(t); n != events {
		t.Fatalf("outbox rows after replay = %d, want %d (a recount would have published again)", n, events)
	}

	other := fx.post(path, "cc-key-1", `{"countedQuantity":9}`)
	if other.Code != http.StatusConflict || problemSlug(t, other.Body.Bytes()) != "idempotency-key-reused" {
		t.Fatalf("same key, other body = %d %s, want 409 idempotency-key-reused", other.Code, other.Body.String())
	}
}

func TestIdempotency_CycleCount_Concurrent_CountsOnce(t *testing.T) {
	fx := newRoutesFixture(t)
	seedStock(t, fx.pool, "SKU-CC-2", 10)
	path := "/bins/IDEMP-BIN-SKU-CC-2/cycle-counts"

	const n = 5
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = fx.post(path, "cc-concurrent", `{"countedQuantity":7}`)
		}(i)
	}
	wg.Wait()
	for i, rec := range results {
		if rec.Code != http.StatusOK || rec.Body.String() != results[0].Body.String() {
			t.Fatalf("request %d = %d %s", i, rec.Code, rec.Body.String())
		}
	}
	if got := fx.outboxRows(t); got != 3 {
		t.Fatalf("outbox rows after %d concurrent identical counts = %d, want exactly 3", n, got)
	}
}

func TestIdempotency_CycleCount_4xxIsCachedAndReplayed(t *testing.T) {
	fx := newRoutesFixture(t)
	path := "/bins/IDEMP-BIN-SKU-CC-3/cycle-counts"
	first := fx.post(path, "cc-400", `{}`)
	if first.Code != http.StatusBadRequest || problemSlug(t, first.Body.Bytes()) != "counted-quantity-required" {
		t.Fatalf("first = %d %s, want 400 counted-quantity-required", first.Code, first.Body.String())
	}
	assertReplay(t, first, fx.post(path, "cc-400", `{}`))
	if !bytes.Contains(first.Body.Bytes(), []byte("counted-quantity-required")) {
		t.Fatalf("unexpected body %s", first.Body.String())
	}
}
