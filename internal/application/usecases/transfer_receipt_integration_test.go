//go:build integration

// Destination transfer receipt custody (ADR 0031, Phase 3) end-to-end
// proof, on the SAME shared testcontainers Postgres+Kafka the transfer
// allocation set boots (TestTransferMain owns the lifecycle). The
// scenarios required by the Phase 3 contract:
//
//  1. stage -> TransferReceiptStaged in the outbox AND on the real
//     Kafka topic (via the outbox relay), with the SIGNED variance;
//  2. stow -> destination site usable +N EXACTLY ONCE; a replayed stow
//     creates no second StockUnit and no second event;
//  3. unknown transfer_line_id -> an inventory_exceptions row, no
//     receipt, no stock change, no availability-raising event;
//  4. forced outbox failure rolls EVERYTHING back (receipt stays
//     absent, stock untouched).
package usecases_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// wireReceiptHarness builds the use cases over the shared env's pool.
func wireReceiptHarness(t *testing.T, env *transferTestEnv) (stage *usecases.StageTransferReceipt, stow *usecases.StowTransferStock) {
	t.Helper()
	receipts := postgres.NewTransferReceiptRepo(env.pool)
	exceptions := postgres.NewInventoryExceptionRepo(env.pool)
	uow := postgres.NewUnitOfWork(env.pool)

	encoder := kafka.NewPublisher(nil, postgres.NewReservationRepo(env.pool))
	publisher := postgres.NewOutboxPublisher(env.pool, encoder)

	stage = &usecases.StageTransferReceipt{
		Transfers:  postgres.NewTransferAllocationRepo(env.pool),
		Receipts:   receipts,
		Exceptions: exceptions,
		Events:     publisher,
		Clock:      memory.SystemClock{},
		UnitOfWork: uow,
	}
	stow = &usecases.StowTransferStock{
		Receipts:   receipts,
		Stock:      postgres.NewStockRepo(env.pool),
		Locations:  postgres.NewLocationRepo(env.pool),
		Events:     publisher,
		Clock:      memory.SystemClock{},
		UnitOfWork: uow,
	}
	return stage, stow
}

// seedDestinationBin registers a bin AT a site through the real repos.
func seedDestinationBin(t *testing.T, env *transferTestEnv, binID, site string, capacity int) {
	t.Helper()
	id, _ := shared.NewBinId(binID)
	base, err := location.NewBin(id, shared.Quantity(capacity))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	atSite := location.RehydrateBinAtSite(id, base.Capacity(), base.Occupied(), mustSiteID(t, site), base.Version())
	if err := postgres.NewLocationRepo(env.pool).Save(context.Background(), atSite); err != nil {
		t.Fatalf("save bin: %v", err)
	}
}

// outboxCountFor counts outbox rows of the given event type.
func outboxCountFor(t *testing.T, env *transferTestEnv, eventType string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE event_type = $1`, eventType,
	).Scan(&n); err != nil {
		t.Fatalf("count outbox %s: %v", eventType, err)
	}
	return n
}

const (
	stagedEventType = "com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged"
	stowedEventType = "com.warehouse.wms.inventory-storage.stock.TransferStockStowed"
)

// containsEventType reports whether a structured CloudEvents JSON value
// carries the full type string (cheap substring check on the envelope).
func containsEventType(value []byte, wantType string) bool {
	return len(value) > 0 && stringContains(string(value), `"type":"`+wantType+`"`)
}

func stringContains(haystack, needle string) bool {
	return len(needle) > 0 && strings.Contains(haystack, needle)
}

// relayOnce drains the outbox ONCE through the real OutboxRelay and
// RelaySink onto brokers. Every pending row publishes (earlier tests'
// rows on the shared database included — the consumer filters by type),
// which is exactly what the production relay loop does.
func relayOnce(t *testing.T, env *transferTestEnv, brokers []string) {
	t.Helper()
	sink := kafka.NewRelaySink(brokers)
	defer func() { _ = sink.Close() }()
	relay := postgres.NewOutboxRelay(env.pool, sink)
	if _, err := relay.RelayOnce(context.Background()); err != nil {
		t.Fatalf("relay once: %v", err)
	}
}

// readEventFromTopic consumes messages from topic until one of the
// wanted CloudEvents type arrives, returning its raw value. A fresh
// unique group replay-reads the topic from the beginning.
func readEventFromTopic(t *testing.T, brokers []string, topic, wantType string) string {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     "receipt-itest-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		MinBytes:    1,
		MaxBytes:    1 << 20,
		MaxWait:     200 * time.Millisecond,
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = r.Close() }()

	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s on %s", wantType, topic)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		msg, err := r.FetchMessage(ctx)
		cancel()
		if err != nil {
			continue
		}
		if containsEventType(msg.Value, wantType) && string(msg.Key) == "tl-recv-1" {
			return string(msg.Value)
		}
	}
}

// Scenario 1+2: stage -> event in outbox AND on Kafka; stow ->
// destination usable +5 exactly once; replay adds nothing.
func TestIntegration_TransferReceipt_StageThenStow_RaisesDestinationUsableOnce(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()
	stage, stow := wireReceiptHarness(t, env)

	site := mustSiteID(t, "SITE-RECV-A")
	sku := mustSKU(t, "SKU-RECV")
	seedAllocatedLineForReceipt(t, env, "tr-recv", "tl-recv-1", site, sku, 6)
	seedDestinationBin(t, env, "BIN-RECV-1", "SITE-RECV-DEST", 20)
	seedDestinationBin(t, env, "BIN-RECV-2", "SITE-RECV-DEST", 20)

	dest := mustSiteID(t, "SITE-RECV-DEST")

	// Stage: counted 5 of the promised 6 — a SHORT receipt, variance -1.
	res, err := stage.Execute(ctx, usecases.ReceiptCommand{
		TransferID: "tr-recv", TransferLineID: "tl-recv-1", DestinationSiteID: dest, SKU: sku, ReceivedQuantity: mustQty(t, 5),
	})
	if err != nil || res.Quarantined() {
		t.Fatalf("stage: res=%v err=%v", res, err)
	}
	if res.Receipt.Variance() != -1 {
		t.Fatalf("variance = %d, want -1 (short by one, signed)", res.Receipt.Variance())
	}
	if got := outboxCountFor(t, env, stagedEventType); got != 1 {
		t.Fatalf("staged outbox rows = %d, want exactly 1", got)
	}

	// The event is durable in the OUTBOX and carries the signed variance
	// in its payload (verified byte-exact by the golden test). Drain the
	// outbox through the REAL relay sink onto the REAL broker and prove
	// the message lands on warehouse.inventory.events, keyed by the
	// transfer line id.
	var stagedValue []byte
	if err := env.pool.QueryRow(ctx,
		`SELECT value FROM outbox_events WHERE event_type = $1 LIMIT 1`, stagedEventType,
	).Scan(&stagedValue); err != nil {
		t.Fatalf("read staged outbox value: %v", err)
	}
	if !containsEventType(stagedValue, stagedEventType) {
		t.Fatalf("outbox value does not carry the staged type: %s", stagedValue)
	}
	if !stringContains(string(stagedValue), `"variance":-1`) {
		t.Fatalf("staged event must carry the signed variance: %s", stagedValue)
	}

	eventsTopic := "warehouse.inventory.events"
	createTransferTopic(t, env.brokers[0], eventsTopic, 4)
	relayOnce(t, env, env.brokers)
	consumed := readEventFromTopic(t, env.brokers, eventsTopic, stagedEventType)
	if !stringContains(consumed, `"transfer_line_id":"tl-recv-1"`) || !stringContains(consumed, `"variance":-1`) {
		t.Fatalf("staged event on the broker must carry the line id and signed variance: %s", consumed)
	}

	// Stow the RECEIVED 5 across two destination bins.
	stowRes, err := stow.Execute(ctx, "tl-recv-1", []usecases.StowBin{
		{BinID: mustBinIDForReceipt(t, "BIN-RECV-1"), Quantity: mustQty(t, 3)},
		{BinID: mustBinIDForReceipt(t, "BIN-RECV-2"), Quantity: mustQty(t, 2)},
	})
	if err != nil {
		t.Fatalf("stow: %v", err)
	}
	if len(stowRes.StockUnits) != 2 {
		t.Fatalf("stow created %d units, want 2", len(stowRes.StockUnits))
	}

	// Destination usable rose by exactly 5 (the RECEIVED qty, not the
	// expected 6 — the variance stays on the receipt, never absorbed).
	var usable int
	if err := env.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(quantity - reserved), 0) FROM stock_units WHERE site_id = $1 AND sku = $2
	`, "SITE-RECV-DEST", "SKU-RECV").Scan(&usable); err != nil {
		t.Fatalf("sum destination usable: %v", err)
	}
	if usable != 5 {
		t.Fatalf("destination usable = %d, want exactly 5", usable)
	}

	if got := outboxCountFor(t, env, stowedEventType); got != 1 {
		t.Fatalf("stowed outbox rows = %d, want exactly 1", got)
	}

	// REPLAY both steps: identical stage, identical stow.
	replayStage, err := stage.Execute(ctx, usecases.ReceiptCommand{
		TransferID: "tr-recv", TransferLineID: "tl-recv-1", DestinationSiteID: dest, SKU: sku, ReceivedQuantity: mustQty(t, 5),
	})
	if err != nil || !replayStage.Replay {
		t.Fatalf("replay stage must return the original: res=%v err=%v", replayStage, err)
	}
	replayStow, err := stow.Execute(ctx, "tl-recv-1", []usecases.StowBin{
		{BinID: mustBinIDForReceipt(t, "BIN-RECV-1"), Quantity: mustQty(t, 3)},
		{BinID: mustBinIDForReceipt(t, "BIN-RECV-2"), Quantity: mustQty(t, 2)},
	})
	if err != nil || !replayStow.Replay {
		t.Fatalf("replay stow must return the original: res=%v err=%v", replayStow, err)
	}

	// Still exactly 2 units, exactly 5 usable, exactly one event each.
	var unitRows int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM stock_units WHERE site_id = $1 AND sku = $2`, "SITE-RECV-DEST", "SKU-RECV",
	).Scan(&unitRows); err != nil {
		t.Fatalf("count destination units: %v", err)
	}
	if unitRows != 2 {
		t.Fatalf("destination units after replay = %d, want exactly 2 (no second StockUnit)", unitRows)
	}
	if err := env.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(quantity - reserved), 0) FROM stock_units WHERE site_id = $1 AND sku = $2
	`, "SITE-RECV-DEST", "SKU-RECV").Scan(&usable); err != nil {
		t.Fatalf("re-sum destination usable: %v", err)
	}
	if usable != 5 {
		t.Fatalf("destination usable after replay = %d, want still exactly 5", usable)
	}
	if got := outboxCountFor(t, env, stagedEventType); got != 1 {
		t.Fatalf("staged events after replay = %d, want exactly 1", got)
	}
	if got := outboxCountFor(t, env, stowedEventType); got != 1 {
		t.Fatalf("stowed events after replay = %d, want exactly 1", got)
	}
}

// Scenario 3: an unknown transfer_line_id is quarantined — exception
// row, no receipt, no stock change, no availability-raising event.
func TestIntegration_TransferReceipt_UnknownLineIsQuarantined(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()
	stage, _ := wireReceiptHarness(t, env)

	outboxStagedBefore := outboxCountFor(t, env, stagedEventType)

	res, err := stage.Execute(ctx, usecases.ReceiptCommand{
		TransferID: "tr-ghost", TransferLineID: "tl-ghost-9", DestinationSiteID: mustSiteID(t, "SITE-RECV-DEST"),
		SKU: mustSKU(t, "SKU-GHOST"), ReceivedQuantity: mustQty(t, 4),
	})
	if err == nil {
		t.Fatal("an unknown line must surface an explicit problem")
	}
	if res == nil || !res.Quarantined() || res.Exception.Kind() != transfer.ExceptionUnknownTransfer {
		t.Fatalf("res = %+v, want an UNKNOWN_TRANSFER quarantine", res)
	}

	// The exception row exists exactly once; a repeat scan is a replay.
	var excRows int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM inventory_exceptions WHERE transfer_line_id = $1`, "tl-ghost-9").Scan(&excRows); err != nil {
		t.Fatalf("count exceptions: %v", err)
	}
	if excRows != 1 {
		t.Fatalf("exception rows = %d, want 1", excRows)
	}
	res2, err2 := stage.Execute(ctx, usecases.ReceiptCommand{
		TransferID: "tr-ghost", TransferLineID: "tl-ghost-9", DestinationSiteID: mustSiteID(t, "SITE-RECV-DEST"),
		SKU: mustSKU(t, "SKU-GHOST"), ReceivedQuantity: mustQty(t, 4),
	})
	if err2 == nil || res2 == nil || !res2.Quarantined() || !res2.Replay {
		t.Fatalf("repeat quarantine must be a replay: res=%v err=%v", res2, err2)
	}
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM inventory_exceptions WHERE transfer_line_id = $1`, "tl-ghost-9").Scan(&excRows); err != nil {
		t.Fatalf("recount exceptions: %v", err)
	}
	if excRows != 1 {
		t.Fatalf("exception rows after repeat = %d, want still 1", excRows)
	}

	// No receipt, no stock, no new availability-raising event.
	var receiptRows int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM transfer_receipts WHERE transfer_line_id = $1`, "tl-ghost-9").Scan(&receiptRows); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	if receiptRows != 0 {
		t.Fatalf("receipt rows = %d, want 0", receiptRows)
	}
	var ghostUnits int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM stock_units WHERE sku = $1`, "SKU-GHOST").Scan(&ghostUnits); err != nil {
		t.Fatalf("count ghost units: %v", err)
	}
	if ghostUnits != 0 {
		t.Fatalf("ghost units = %d, want 0 (quarantine raises no stock)", ghostUnits)
	}
	if got := outboxCountFor(t, env, stagedEventType); got != outboxStagedBefore {
		t.Fatalf("staged events = %d, want unchanged %d (quarantine publishes nothing)", got, outboxStagedBefore)
	}
}

// Scenario 4: a forced outbox failure mid-stow rolls EVERYTHING back —
// no stock rows, the receipt stays STAGED (retriable), no outbox row.
func TestIntegration_TransferReceipt_OutboxFailureRollsReceiptAndStockBack(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()

	receipts := postgres.NewTransferReceiptRepo(env.pool)
	transfers := postgres.NewTransferAllocationRepo(env.pool)
	exceptions := postgres.NewInventoryExceptionRepo(env.pool)
	uow := postgres.NewUnitOfWork(env.pool)

	site := mustSiteID(t, "SITE-RECV-B")
	sku := mustSKU(t, "SKU-RB2")
	seedAllocatedLineForReceipt(t, env, "tr-rb2", "tl-rb2", site, sku, 5)
	seedDestinationBin(t, env, "BIN-RB2", "SITE-RECV-DEST2", 20)

	// Stage with a WORKING publisher first.
	workingEncoder := kafka.NewPublisher(nil, postgres.NewReservationRepo(env.pool))
	stageOK := &usecases.StageTransferReceipt{
		Transfers: transfers, Receipts: receipts, Exceptions: exceptions,
		Events: postgres.NewOutboxPublisher(env.pool, workingEncoder),
		Clock:  memory.SystemClock{}, UnitOfWork: uow,
	}
	if _, err := stageOK.Execute(ctx, usecases.ReceiptCommand{
		TransferID: "tr-rb2", TransferLineID: "tl-rb2", DestinationSiteID: mustSiteID(t, "SITE-RECV-DEST2"),
		SKU: sku, ReceivedQuantity: mustQty(t, 5),
	}); err != nil {
		t.Fatalf("stage: %v", err)
	}

	outboxStowedBefore := outboxCountFor(t, env, stowedEventType)
	var stockBefore int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM stock_units WHERE sku = $1`, "SKU-RB2").Scan(&stockBefore); err != nil {
		t.Fatalf("count stock before: %v", err)
	}

	// Stow with a FAILING encoder: the outbox insert is the transaction's
	// last write, so its failure must unwind the stock saves and the
	// receipt transition too.
	failing := &usecases.StowTransferStock{
		Receipts:   receipts,
		Stock:      postgres.NewStockRepo(env.pool),
		Locations:  postgres.NewLocationRepo(env.pool),
		Events:     postgres.NewOutboxPublisher(env.pool, failingTransferEncoder{}),
		Clock:      memory.SystemClock{},
		UnitOfWork: uow,
	}
	if _, err := failing.Execute(ctx, "tl-rb2", []usecases.StowBin{
		{BinID: mustBinIDForReceipt(t, "BIN-RB2"), Quantity: mustQty(t, 5)},
	}); err == nil {
		t.Fatal("expected the outbox failure to surface as an error")
	}

	// Stock: nothing survived.
	var stockAfter int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM stock_units WHERE sku = $1`, "SKU-RB2").Scan(&stockAfter); err != nil {
		t.Fatalf("count stock after: %v", err)
	}
	if stockAfter != stockBefore {
		t.Fatalf("stock rows after rollback = %d, want %d", stockAfter, stockBefore)
	}
	// Receipt: still STAGED — the transition was rolled back with everything else.
	var state string
	if err := env.pool.QueryRow(ctx, `SELECT state FROM transfer_receipts WHERE transfer_line_id = $1`, "tl-rb2").Scan(&state); err != nil {
		t.Fatalf("read receipt state: %v", err)
	}
	if state != "STAGED" {
		t.Fatalf("receipt state after rollback = %s, want STAGED (retriable)", state)
	}
	// Outbox: no stowed row was queued.
	if got := outboxCountFor(t, env, stowedEventType); got != outboxStowedBefore {
		t.Fatalf("stowed outbox rows after rollback = %d, want unchanged %d", got, outboxStowedBefore)
	}

	// And a RETRY with a working publisher completes the stow exactly
	// once — proving the rollback left the flow genuinely retriable.
	stowOK := &usecases.StowTransferStock{
		Receipts:   receipts,
		Stock:      postgres.NewStockRepo(env.pool),
		Locations:  postgres.NewLocationRepo(env.pool),
		Events:     postgres.NewOutboxPublisher(env.pool, workingEncoder),
		Clock:      memory.SystemClock{},
		UnitOfWork: uow,
	}
	if _, err := stowOK.Execute(ctx, "tl-rb2", []usecases.StowBin{
		{BinID: mustBinIDForReceipt(t, "BIN-RB2"), Quantity: mustQty(t, 5)},
	}); err != nil {
		t.Fatalf("retry stow: %v", err)
	}
	var usable int
	if err := env.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(quantity - reserved), 0) FROM stock_units WHERE site_id = $1 AND sku = $2
	`, "SITE-RECV-DEST2", "SKU-RB2").Scan(&usable); err != nil {
		t.Fatalf("sum retry usable: %v", err)
	}
	if usable != 5 {
		t.Fatalf("destination usable after retry = %d, want 5", usable)
	}
}

// seedAllocatedLineForReceipt writes an ALLOCATED ledger row directly.
func seedAllocatedLineForReceipt(t *testing.T, env *transferTestEnv, transferID, lineID string, originSite shared.SiteID, sku shared.SKU, qty int) {
	t.Helper()
	allocated, err := transfer.NewAllocated(transferID, lineID, originSite, sku, shared.Quantity(qty), "res-"+lineID, time.Now().UTC())
	if err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	if err := postgres.NewTransferAllocationRepo(env.pool).Save(context.Background(), allocated); err != nil {
		t.Fatalf("seed allocation save: %v", err)
	}
}

func mustBinIDForReceipt(t *testing.T, raw string) shared.BinId {
	t.Helper()
	id, err := shared.NewBinId(raw)
	if err != nil {
		t.Fatalf("NewBinId(%q): %v", raw, err)
	}
	return id
}
