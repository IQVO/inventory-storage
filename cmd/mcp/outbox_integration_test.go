//go:build integration

package main

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	kafkaadapter "github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// TestMCPRevokeReservation_WritesOutboxRowsOnBothTopics is the end-to-end
// regression for the audit finding that MCP-initiated revocations never
// reached Kafka (LogPublisher + no UnitOfWork). It boots a real Postgres
// (testcontainers), wires cmd/mcp's production adapters with
// EVENT_PUBLISHER=kafka, calls the revoke_reservation tool over MCP
// Streamable HTTP, and asserts that the transactional outbox now holds an
// unpublished row for BOTH the integration and the analytics topic. The
// relay stays in cmd/inventory, so rows (not Kafka messages) are the
// contract of this binary (ADR-0017).
func TestMCPRevokeReservation_WritesOutboxRowsOnBothTopics(t *testing.T) {
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

	ad, err := buildAdapters(ctx, url, url, migrationsDirForTest(t), "kafka", nil, quietLogger())
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
	if _, err := pool.Exec(ctx, `INSERT INTO bins (id, capacity, occupied) VALUES ('BIN-MCP-1', 100, 10)`); err != nil {
		t.Fatalf("seed bin: %v", err)
	}

	// Seed one stowed unit and a reservation against it. The reservation is
	// created with a throwaway publisher so the outbox starts empty and the
	// assertions below only see what the MCP tool itself publishes.
	q10, _ := shared.NewPositiveQuantity(10)
	unit, err := stock.NewStockUnit("su-mcp-1", shared.SKU("SKU-MCP"), shared.BinId("BIN-MCP-1"), q10)
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
	res, err := reserve.Execute(ctx, shared.SKU("SKU-MCP"), q4, "demand-mcp-1")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	srv := httptest.NewServer(newRouter(inboundmcp.Handler(inboundmcp.NewServer(buildDeps(ad, memory.SystemClock{}, nil))), "inventory-storage-mcp-itest"))
	t.Cleanup(srv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "itest-client", Version: "0.0.1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	out, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name:      "revoke_reservation",
		Arguments: map[string]any{"reservationId": res.ID()},
	})
	if err != nil {
		t.Fatalf("call revoke_reservation: %v", err)
	}
	if out.IsError {
		t.Fatalf("revoke_reservation returned a tool error: %+v", out.Content)
	}

	// Assert on the outbox.
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
