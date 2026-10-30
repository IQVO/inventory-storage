//go:build integration

package main

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inbounda2a "github.com/claudioed/inventory-storage/internal/adapters/inbound/a2a"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// a2aCall sends one skill request over the real JSON-RPC wire and returns the
// decoded data part of the agent's reply.
func a2aCall(t *testing.T, client *a2aclient.Client, skill string, args map[string]any) map[string]any {
	t.Helper()
	resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args})),
	})
	if err != nil {
		t.Fatalf("SendMessage %s: %v", skill, err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) == 0 {
		t.Fatalf("reply = %T, want a message", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	env, ok := data.Value.(map[string]any)
	if !ok {
		t.Fatalf("reply data is %T, want the envelope object", data.Value)
	}
	return env
}

// TestA2ARevokeReservation_WritesUnpublishedOutboxRowsOnBothTopics is the
// end-to-end proof that the delegated write leaves through the transactional
// outbox like the REST/MCP writes do: it boots a real Postgres (testcontainers),
// wires cmd/a2a's production adapters, calls the revoke_reservation skill over
// the A2A JSON-RPC wire, and asserts the outbox holds an UNPUBLISHED row for
// BOTH the integration and the analytics topic. The relay stays in
// cmd/inventory, so rows (not Kafka messages) are the contract of this binary
// (ADR-0017). A rejected delegation (unknown reservation) stores nothing.
func TestA2ARevokeReservation_WritesUnpublishedOutboxRowsOnBothTopics(t *testing.T) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	ad, err := buildAdapters(ctx, url, url, migrationsDirForTest(t), quietLogger())
	if err != nil {
		t.Fatalf("buildAdapters: %v", err)
	}
	t.Cleanup(ad.close)

	// A second plain pool for seeding the bin FK and for assertions.
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open assertion pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `INSERT INTO bins (id, capacity, occupied) VALUES ('BIN-A2A-1', 100, 10)`); err != nil {
		t.Fatalf("seed bin: %v", err)
	}

	// Seed one stowed unit and a reservation against it. The reservation is
	// created with a throwaway publisher so the outbox starts empty and the
	// assertions below only see what the A2A skill itself publishes.
	q10, _ := shared.NewPositiveQuantity(10)
	unit, err := stock.NewStockUnit("su-a2a-1", shared.SKU("SKU-A2A"), shared.BinId("BIN-A2A-1"), q10)
	if err != nil {
		t.Fatalf("new stock unit: %v", err)
	}
	if err := ad.stock.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}
	reserve := &usecases.ReserveStock{
		Stock: ad.stock, Reservations: ad.reservations,
		Events: events.NewBufferedPublisher(), Clock: memory.SystemClock{},
	}
	q4, _ := shared.NewPositiveQuantity(4)
	res, err := reserve.Execute(ctx, shared.SKU("SKU-A2A"), q4, "demand-a2a-1")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	executor := inbounda2a.NewExecutor(buildDeps(ad, memory.SystemClock{}, nil))
	srv := httptest.NewServer(inbounda2a.Handler(executor))
	t.Cleanup(srv.Close)
	client, err := a2aclient.NewFromCard(ctx, inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/", Version: "itest"}))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}

	// A catalogue/domain-invalid delegation is answered in-band and stores nothing.
	env := a2aCall(t, client, "revoke_reservation", map[string]any{"reservationId": "ghost"})
	if p, _ := env["problem"].(map[string]any); p == nil || p["status"] != float64(404) {
		t.Fatalf("revoking an unknown reservation = %v, want an in-band 404 problem", env)
	}
	var rejected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&rejected); err != nil || rejected != 0 {
		t.Fatalf("outbox rows after a rejected delegation = %d (%v), want 0", rejected, err)
	}

	env = a2aCall(t, client, "revoke_reservation", map[string]any{"reservationId": res.ID()})
	if env["problem"] != nil {
		t.Fatalf("revoke_reservation answered a problem: %v", env["problem"])
	}

	dbRows, err := pool.Query(ctx, `SELECT topic, event_type, published_at IS NOT NULL FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer dbRows.Close()

	const wantType = "com.warehouse.wms.inventory-storage.reservation.ReservationRevoked"
	eventTypeByTopic := map[string]string{}
	total := 0
	for dbRows.Next() {
		var topic, eventType string
		var published bool
		if err := dbRows.Scan(&topic, &eventType, &published); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}
		total++
		if published {
			t.Errorf("outbox row %s on %s is already published; this binary must not run the relay", eventType, topic)
		}
		eventTypeByTopic[topic] = eventType
	}
	if err := dbRows.Err(); err != nil {
		t.Fatalf("iterate outbox: %v", err)
	}
	for _, topic := range []string{kafkaadapter.Topic, kafkaadapter.AnalyticsTopic} {
		if got := eventTypeByTopic[topic]; got != wantType {
			t.Errorf("topic %s: outbox event_type = %q, want %q", topic, got, wantType)
		}
	}
	if total != 2 {
		t.Errorf("outbox rows = %d, want exactly 2 (one per topic)", total)
	}
}
