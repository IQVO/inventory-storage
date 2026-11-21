//go:build integration

// The replay-class rule (docs/docs/adr/0053-idempotency-replay-classes.md) through the REAL chi
// router over a real Postgres 16: a refusal that nothing committed and that depends on state that
// can change (RETRYABLE) is NOT stored, so the retry under the same Idempotency-Key executes; a
// refusal that committed state (OUTCOME) is stored together with its writes and replayed.
package http_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/inventory-storage/internal/adapters/inbound/http"
)

// steppedClock is a clock a test moves forward between two requests.
type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppedClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func assertNotReplayed(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if got := rec.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("a fresh execution carries %s: %q", inboundhttp.IdempotencyReplayedHeader, got)
	}
}

// waitForLockWaiter blocks until some session of the test database waits on a row lock: the
// loser of the race is then provably queued behind the winner's uncommitted write.
func (f routesFixture) waitForLockWaiter(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if f.count(t, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no session ever waited on a row lock: the race did not happen")
}

// RETRYABLE: two stows race for the same bin row; the loser answers 409 concurrent-modification.
// Its key row is gone (nothing is stored), the retry under the SAME key executes, succeeds, and
// only then is stored and replayed.
func TestReplayClass_ConcurrentModificationLoserIsReleasedAndItsRetryExecutes(t *testing.T) {
	fx := newRoutesFixture(t)
	fx.seedBin(t, "STOW-BIN-1", 100)

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fx.publisher.gate = func() { once.Do(func() { close(reached); <-release }) }

	winnerBody := `{"sku":"SKU-RACE-A","quantity":4,"binId":"STOW-BIN-1"}`
	loserBody := `{"sku":"SKU-RACE-B","quantity":5,"binId":"STOW-BIN-1"}`

	var winner, loser *httptest.ResponseRecorder
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); winner = fx.post("/stowages", "race-winner", winnerBody) }()
	<-reached // the winner has written the bin row and not committed
	wg.Add(1)
	go func() { defer wg.Done(); loser = fx.post("/stowages", "race-loser", loserBody) }()
	fx.waitForLockWaiter(t)
	close(release)
	wg.Wait()

	if winner.Code != http.StatusCreated {
		t.Fatalf("winner = %d %s, want 201", winner.Code, winner.Body.String())
	}
	if loser.Code != http.StatusConflict || problemSlug(t, loser.Body.Bytes()) != "concurrent-modification" {
		t.Fatalf("loser = %d %s, want 409 concurrent-modification", loser.Code, loser.Body.String())
	}
	if n := fx.keyRows(t, "race-loser"); n != 0 {
		t.Fatalf("key rows of the loser = %d, want 0: a RETRYABLE refusal is not stored", n)
	}
	if n := fx.keyRows(t, "race-winner"); n != 1 {
		t.Fatalf("key rows of the winner = %d, want 1", n)
	}
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 1 {
		t.Fatalf("stock_units after the race = %d, want 1 (the winner's)", n)
	}

	retry := fx.post("/stowages", "race-loser", loserBody)
	if retry.Code != http.StatusCreated {
		t.Fatalf("retry under the same key = %d %s, want 201: the retry must execute", retry.Code, retry.Body.String())
	}
	assertNotReplayed(t, retry)
	if n := fx.keyRows(t, "race-loser"); n != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1", n)
	}
	assertReplay(t, retry, fx.post("/stowages", "race-loser", loserBody))
	if n := fx.count(t, `SELECT count(*) FROM stock_units`); n != 2 {
		t.Fatalf("stock_units after the retry and its replay = %d, want 2", n)
	}
	if n := fx.count(t, `SELECT occupied FROM bins WHERE id = 'STOW-BIN-1'`); n != 9 {
		t.Fatalf("bin occupied = %d, want 9 (4 + 5, each once)", n)
	}
}

// RETRYABLE: a referenced fact not known yet. The bin is registered after the first attempt; the
// retry under the same key must see it instead of replaying the 404.
func TestReplayClass_UnknownBinIsReleasedSoTheRetryFindsTheBinOnceItIsKnown(t *testing.T) {
	fx := newRoutesFixture(t)

	first := fx.post("/stowages", "stow-late-bin", stowBody)
	if first.Code != http.StatusNotFound || problemSlug(t, first.Body.Bytes()) != "bin-not-found" {
		t.Fatalf("first = %d %s, want 404 bin-not-found", first.Code, first.Body.String())
	}
	if n := fx.keyRows(t, "stow-late-bin"); n != 0 {
		t.Fatalf("key rows after a RETRYABLE 404 = %d, want 0", n)
	}

	fx.seedBin(t, "STOW-BIN-1", 100)
	retry := fx.post("/stowages", "stow-late-bin", stowBody)
	if retry.Code != http.StatusCreated {
		t.Fatalf("retry = %d %s, want 201 now the bin is known", retry.Code, retry.Body.String())
	}
	assertNotReplayed(t, retry)
	assertReplay(t, retry, fx.post("/stowages", "stow-late-bin", stowBody))

	// A different body under a released key is a NEW request, never idempotency-key-reused.
	other := fx.post("/stowages", "stow-late-bin-2", `{"sku":"SKU-STOW-2","quantity":1,"binId":"NO-SUCH-BIN"}`)
	if other.Code != http.StatusNotFound {
		t.Fatalf("setup: %d", other.Code)
	}
	again := fx.post("/stowages", "stow-late-bin-2", stowBody)
	if again.Code != http.StatusCreated {
		t.Fatalf("a different body under a released key = %d %s, want a new request (201), not idempotency-key-reused", again.Code, again.Body.String())
	}
}

// OUTCOME: the reservation timed out; the pick confirmation discovers it, COMMITS the expiry (the
// reservation EXPIRED, its quantity back to usable, ReservationExpired in the outbox) and then
// refuses with 409 reservation-already-resolved (confirm_pick.go: expireIfDue, then Confirm). The
// refusal is stored with its writes, and the retry replays it without recording anything again.
func TestReplayClass_PickConfirmationOfATimedOutReservationCommitsTheExpiryAndReplays(t *testing.T) {
	clock := &steppedClock{now: time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC)}
	fx := newRoutesFixtureAt(t, clock)
	seedStock(t, fx.pool, "SKU-PICK-OUT", 10)
	occupyBin(t, fx.pool, "SKU-PICK-OUT", 10)
	id := fx.reserveOne(t, "SKU-PICK-OUT", "order-pick-out")
	path := "/reservations/" + id + "/pick-confirmations"
	if n := fx.count(t, `SELECT reserved FROM stock_units WHERE sku = 'SKU-PICK-OUT'`); n != 6 {
		t.Fatalf("reserved before the timeout = %d, want 6", n)
	}
	outboxBefore := fx.outboxRows(t)

	clock.Advance(31 * time.Minute) // past the 30 minute default timeout
	first := fx.post(path, "pick-out-1", "")
	if first.Code != http.StatusConflict || problemSlug(t, first.Body.Bytes()) != "reservation-already-resolved" {
		t.Fatalf("first = %d %s, want 409 reservation-already-resolved", first.Code, first.Body.String())
	}
	if n := fx.count(t, `SELECT count(*) FROM reservations WHERE id = $1 AND status = 'EXPIRED'`, id); n != 1 {
		t.Fatalf("expired reservations after the refusal = %d, want 1: the expiry must be committed", n)
	}
	if n := fx.count(t, `SELECT reserved FROM stock_units WHERE sku = 'SKU-PICK-OUT'`); n != 0 {
		t.Fatalf("reserved after the refusal = %d, want 0: the quantity went back to usable", n)
	}
	expiredEvents := fx.outboxRows(t) - outboxBefore
	if expiredEvents < 1 {
		t.Fatalf("outbox rows written by the refused confirmation = %d, want the ReservationExpired event", expiredEvents)
	}
	if n := fx.keyRows(t, "pick-out-1"); n != 1 {
		t.Fatalf("key rows = %d, want 1: an OUTCOME refusal is stored with its writes", n)
	}

	assertReplay(t, first, fx.post(path, "pick-out-1", ""))
	if n := fx.outboxRows(t) - outboxBefore; n != expiredEvents {
		t.Fatalf("outbox rows after the replay = %d, want still %d: a replay records nothing new", n, expiredEvents)
	}
}

// OUTCOME: a destination scan of a transfer line this service never allocated is QUARANTINED: the
// inventory exception is committed (stage_transfer_receipt.go: quarantine inside atomically, the
// problem returned after the commit) and the scan is refused with 422 transfer-scan-quarantined.
// The refusal is stored with the exception row; the retry replays it and records nothing again.
func TestReplayClass_QuarantinedScanCommitsTheExceptionAndReplays(t *testing.T) {
	fx := newRoutesFixture(t)
	body := `{"transferId":"T-1","destinationSiteId":"SITE-B","sku":"SKU-Q","receivedQuantity":3}`
	path := "/transfers/LINE-UNKNOWN/receipt"

	first := fx.post(path, "scan-1", body)
	if first.Code != http.StatusUnprocessableEntity || problemSlug(t, first.Body.Bytes()) != "transfer-scan-quarantined" {
		t.Fatalf("first = %d %s, want 422 transfer-scan-quarantined", first.Code, first.Body.String())
	}
	if n := fx.count(t, `SELECT count(*) FROM inventory_exceptions`); n != 1 {
		t.Fatalf("inventory_exceptions after the refusal = %d, want 1: the quarantine is committed", n)
	}
	if n := fx.keyRows(t, "scan-1"); n != 1 {
		t.Fatalf("key rows = %d, want 1", n)
	}
	outbox := fx.outboxRows(t)

	assertReplay(t, first, fx.post(path, "scan-1", body))
	if n := fx.count(t, `SELECT count(*) FROM inventory_exceptions`); n != 1 {
		t.Fatalf("inventory_exceptions after the replay = %d, want still 1", n)
	}
	if n := fx.outboxRows(t); n != outbox {
		t.Fatalf("outbox rows after the replay = %d, want %d", n, outbox)
	}
}
