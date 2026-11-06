//go:build integration

// Transfer allocation RELEASE (cancel-after-allocation compensation, ADR
// 0050) end-to-end proof, on the same REAL infrastructure the allocation
// scenarios boot (shared testcontainers Postgres + Kafka, TestMain-owned):
//
//  1. allocate, then release => the reservation is REVOKED and its
//     quantity returns to usable EXACTLY ONCE, the release ledger holds
//     RELEASED, and exactly one TransferStockAllocationReleased fact is
//     queued in the outbox;
//  2. a replayed release command (new CloudEvents id, same line) adds no
//     second ledger row and republishes no second event;
//  3. a release for a line this service never decided is committed past
//     with no ledger row and no event (never retried forever).
//
// Settling is observed via the LEDGERS and the OUTBOX (the transaction's
// durable outcomes), not via consumer-group offsets.
package usecases_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// publishTransferReleaseCommand writes one
// TransferAllocationReleaseRequested CloudEvent onto the command topic,
// keyed by transfer_line_id, exactly as network-inventory-planning
// (ADR 0050) would.
func publishTransferReleaseCommand(t *testing.T, topic string, brokers []string, id, transferID, lineID, reservationID, site, sku string, qty int, reason string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"transfer_id":      transferID,
		"transfer_line_id": lineID,
		"reservation_id":   reservationID,
		"origin_site_id":   site,
		"sku":              sku,
		"quantity":         qty,
		"reason":           reason,
	})
	value, _ := json.Marshal(map[string]any{
		"specversion":     "1.0",
		"id":              id,
		"source":          "/warehouse/network-inventory-planning",
		"type":            "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationReleaseRequested",
		"subject":         lineID,
		"time":            "2026-11-05T12:00:00Z",
		"datacontenttype": "application/json",
		"dataschema":      "urn:warehouse:network-inventory-planning:events:TransferAllocationReleaseRequested:v1",
		"data":            json.RawMessage(payload),
	})

	w := kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		BatchTimeout: 50 * time.Millisecond,
	}
	defer func() { _ = w.Close() }()
	msg := kafkago.Message{
		Key:     []byte(lineID),
		Value:   value,
		Headers: []kafkago.Header{{Key: "content-type", Value: []byte("application/cloudevents+json; charset=UTF-8")}},
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := w.WriteMessages(context.Background(), msg)
		if err == nil {
			return
		}
		if !errors.Is(err, kafkago.UnknownTopicOrPartition) || time.Now().After(deadline) {
			t.Fatalf("publish release command %s: %v", id, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// releaseLedgerRow returns the decided release outcome and reason for a
// line, "" while undecided. Tolerant of pgx.ErrNoRows because it doubles
// as the poll condition inside runConsumerUntil.
func releaseLedgerRow(t *testing.T, env *transferTestEnv, lineID string) (string, string) {
	t.Helper()
	var outcome, reason *string
	err := env.pool.QueryRow(context.Background(),
		`SELECT outcome, reason FROM transfer_allocation_releases WHERE transfer_line_id = $1`, lineID,
	).Scan(&outcome, &reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ""
	}
	if err != nil {
		t.Fatalf("read release ledger %s: %v", lineID, err)
	}
	if outcome == nil {
		return "", ""
	}
	return *outcome, *reason
}

// reservationStatus reads one reservation's status straight from the DB.
func reservationStatus(t *testing.T, env *transferTestEnv, reservationID string) string {
	t.Helper()
	var status string
	if err := env.pool.QueryRow(context.Background(),
		`SELECT status FROM reservations WHERE id = $1`, reservationID,
	).Scan(&status); err != nil {
		t.Fatalf("read reservation %s: %v", reservationID, err)
	}
	return status
}

// Scenario 1+2: allocate, release, replay. The revoke returns the
// quantity to usable exactly once; the replay produces one ledger
// outcome and no second event.
func TestIntegration_TransferRelease_RevokesOnceAndReplayAddsNothing(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()

	run := uniqueRun()
	unitID, sku, transferID, lineID := "su-rel-"+run, "SKU-REL-"+run, "tr-rel-"+run, "tl-rel-"+run
	siteA, _ := shared.NewSiteID("SITE-A")
	seedTransferStock(t, env, unitID, sku, siteA, 10)

	group := "transfer-itest-rel-" + run
	consumer := wireTransferConsumer(t, env, group)

	// Allocate 6 of 10 through the real command path.
	publishTransferCommand(t, env.topic, env.brokers, "ce-alloc-"+run, transferID, lineID, "SITE-A", sku, 6)
	runConsumerUntil(t, consumer, func() bool { return transferLedgerOutcome(t, env, lineID) == "ALLOCATED" })
	if got := usableAt(t, env, unitID); got != 4 {
		t.Fatalf("usable after allocation = %d, want 4", got)
	}

	// The reservation the release command must name, from the ledger.
	var reservationID string
	if err := env.pool.QueryRow(ctx,
		`SELECT reservation_id FROM transfer_allocations WHERE transfer_line_id = $1`, lineID,
	).Scan(&reservationID); err != nil {
		t.Fatalf("read reservation id: %v", err)
	}

	// Release it (cancel-after-allocation), then replay the SAME command
	// under a NEW CloudEvents id (a planning retry).
	publishTransferReleaseCommand(t, env.topic, env.brokers, "ce-rel-1-"+run, transferID, lineID, reservationID, "SITE-A", sku, 6, "transfer cancelled")
	publishTransferReleaseCommand(t, env.topic, env.brokers, "ce-rel-2-"+run, transferID, lineID, reservationID, "SITE-A", sku, 6, "transfer cancelled")

	runConsumerUntil(t, consumer, func() bool {
		outcome, _ := releaseLedgerRow(t, env, lineID)
		return outcome != "" && groupLag(env, group) == 0
	})

	// Ledger: exactly one release row, RELEASED, carrying the reason.
	outcome, reason := releaseLedgerRow(t, env, lineID)
	if outcome != "RELEASED" || reason != "transfer cancelled" {
		t.Fatalf("release ledger = %s/%s, want RELEASED/transfer cancelled", outcome, reason)
	}
	var releaseRows int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM transfer_allocation_releases WHERE transfer_line_id = $1`, lineID,
	).Scan(&releaseRows); err != nil {
		t.Fatalf("count release ledger: %v", err)
	}
	if releaseRows != 1 {
		t.Fatalf("release ledger rows = %d, want exactly 1", releaseRows)
	}

	// The revoke returned the hold to usable EXACTLY ONCE (10, not 16).
	if got := usableAt(t, env, unitID); got != 10 {
		t.Fatalf("usable after release = %d, want 10 (exactly one revoke)", got)
	}

	// The reservation is REVOKED.
	if got := reservationStatus(t, env, reservationID); got != "REVOKED" {
		t.Fatalf("reservation status = %s, want REVOKED", got)
	}

	// Exactly ONE TransferStockAllocationReleased fact for the line:
	// the replay republished nothing.
	if n := outboxEventsForLine(t, env, "TransferStockAllocationReleased", lineID); n != 1 {
		t.Fatalf("TransferStockAllocationReleased outbox rows = %d, want exactly 1", n)
	}
}

// Scenario 3: a release command for a line this service never decided is
// a deterministic refusal — committed past, no ledger row, no event, no
// endless retry.
func TestIntegration_TransferRelease_UnknownLineCommittedPastNoEvent(t *testing.T) {
	env := transferSharedEnv(t)
	ctx := context.Background()

	run := uniqueRun()
	lineID := "tl-unknown-" + run

	group := "transfer-itest-relunk-" + run
	consumer := wireTransferConsumer(t, env, group)

	publishTransferReleaseCommand(t, env.topic, env.brokers, "ce-unk-"+run, "tr-unknown-"+run, lineID, "res-unknown", "SITE-A", "SKU-UNK-"+run, 1, "cancel")

	// Settled = the group committed past the poison message.
	runConsumerUntil(t, consumer, func() bool { return groupLag(env, group) == 0 })

	var releaseRows int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM transfer_allocation_releases WHERE transfer_line_id = $1`, lineID,
	).Scan(&releaseRows); err != nil {
		t.Fatalf("count release ledger: %v", err)
	}
	if releaseRows != 0 {
		t.Fatalf("release ledger rows = %d, want 0 (refusal writes nothing)", releaseRows)
	}
	if n := outboxEventsForLine(t, env, "TransferStockAllocationReleased", lineID); n != 0 {
		t.Fatalf("released events for unknown line = %d, want 0", n)
	}
}
