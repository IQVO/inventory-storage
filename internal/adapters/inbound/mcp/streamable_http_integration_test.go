//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: mcp.Handler(server) mounted on an httptest.Server, driven
// by the SDK's own client (mcp.NewClient + StreamableClientTransport), with
// the REAL Postgres-backed use cases behind it — exactly the deployment
// shape cmd/mcp serves (ADR-0008: Streamable HTTP only, unauthenticated per
// ADR-0015). This proves the wire contract (initialize, tools/list,
// tools/call) end-to-end against real infrastructure, not the tool handlers
// over in-memory repos (server_test.go already covers that).
//
// Every registered tool is driven: the two reads (check_availability,
// get_bin_occupancy), the write (revoke_reservation) and the curated report
// tool (get_inventory_flow_accuracy_report, wired through the REAL
// ReportsRESTClient against an httptest stand-in for inventory-reports).
// Domain rejections (unknown/already-revoked reservation, empty SKU/bin id,
// missing report window) must come back as TOOL errors (res.IsError), never
// transport errors.
//
// Postgres comes from testcontainers: one container per package run, one
// private database per test. TestMain migrates a TEMPLATE database once and
// each test clones it (CREATE DATABASE ... WITH TEMPLATE, a file-level copy:
// milliseconds), so tests are fully isolated with no TRUNCATE bookkeeping.
// Never an external DATABASE_URL, never t.Skip.
package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundmcp "github.com/claudioed/inventory-storage/internal/adapters/inbound/mcp"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/events"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/adapters/outbound/postgres"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template — the same harness wes-work-planning's MCP suite pioneered.
//
// Never an external DATABASE_URL, never t.Skip.
const mcpItTemplate = "mcp_it_template"

var (
	mcpItBaseURL   string // connection URL of the container's default database
	mcpItDBSeq     atomic.Uint64
	mcpItContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	code := runTests(m)
	if mcpItContainer != nil {
		if err := testcontainers.TerminateContainer(mcpItContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_mcp_it"),
		tcpostgres.WithUsername("inventory"),
		tcpostgres.WithPassword("inventory"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	mcpItContainer = container

	var err2 error
	mcpItBaseURL, err2 = container.ConnectionString(ctx, "sslmode=disable")
	if err2 != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err2)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := mcpItCreateDatabase(ctx, mcpItTemplate); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(mcpItWithDB(mcpItBaseURL, mcpItTemplate), mcpItMigrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// mcpItMigrationsDir resolves the repo's OLTP migrations directory relative
// to this file (mirroring migrationsDirForUsecases), so the suite does not
// depend on the working directory it was invoked from.
func mcpItMigrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
}

// mcpItWithDB rewrites the path of a connection URL to the named database.
func mcpItWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// mcpItCreateDatabase creates an empty database inside the shared container.
func mcpItCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, mcpItBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// mcpItMigratedDB hands the test a connection URL to its own private
// database, cloned from the migrated template. Cloning is a file-level
// copy, so it costs milliseconds and the test's writes never leak into
// another test.
func mcpItMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_it_%d", mcpItDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), mcpItBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, mcpItTemplate)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return mcpItWithDB(mcpItBaseURL, name)
}

// mcpItQty builds a Quantity or fails the test.
func mcpItQty(t *testing.T, v int) shared.Quantity {
	t.Helper()
	q, err := shared.NewQuantity(v)
	if err != nil {
		t.Fatalf("quantity %d: %v", v, err)
	}
	return q
}

// mcpItStack is the wired production stack served over Streamable HTTP:
// real Postgres repos + UnitOfWork, the real use cases, the real MCP server
// and handler, plus a connected SDK client session. Seeded state: one bin,
// one StockUnit of 10, and one ACTIVE reservation of 4 against it, so
// check_availability answers 6 and revoke_reservation has real work to do.
type mcpItStack struct {
	session         *sdkmcp.ClientSession
	pool            *pgxpool.Pool
	publisher       *events.BufferedPublisher
	sku             shared.SKU
	binID           shared.BinId
	unitID          string
	resID           string
	lastReportQuery *atomic.Value
}

// newMCPItStack builds the stack on a fresh private database and connects
// an SDK client to it, exactly the way a model host would.
func newMCPItStack(t *testing.T) *mcpItStack {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, mcpItMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	locations := postgres.NewLocationRepo(pool)
	stockRepo := postgres.NewStockRepo(pool)
	reservations := postgres.NewReservationRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	// Seed: one bin, one StockUnit of 10, one ACTIVE reservation of 4 —
	// all through the real Postgres repos.
	run := time.Now().UnixNano()
	binID, err := shared.NewBinId(fmt.Sprintf("BIN-IT-MCP-%d", run))
	if err != nil {
		t.Fatalf("bin id: %v", err)
	}
	bin, err := location.NewBin(binID, mcpItQty(t, 20))
	if err != nil {
		t.Fatalf("build bin: %v", err)
	}
	if err := locations.Save(ctx, bin); err != nil {
		t.Fatalf("save bin: %v", err)
	}
	sku, err := shared.NewSKU(fmt.Sprintf("SKU-IT-MCP-%d", run))
	if err != nil {
		t.Fatalf("sku: %v", err)
	}
	unitID, err := stockRepo.NextID(ctx)
	if err != nil {
		t.Fatalf("next stock unit id: %v", err)
	}
	unit, err := stock.NewStockUnit(unitID, sku, binID, mcpItQty(t, 10))
	if err != nil {
		t.Fatalf("build unit: %v", err)
	}
	if err := stockRepo.Save(ctx, unit); err != nil {
		t.Fatalf("save unit: %v", err)
	}

	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
	reserve := &usecases.ReserveStock{
		Stock: stockRepo, Reservations: reservations,
		Events: publisher, Clock: clock, UnitOfWork: uow,
	}
	res, err := reserve.Execute(ctx, sku, mcpItQty(t, 4), fmt.Sprintf("it-mcp-demand-%d", run))
	if err != nil {
		t.Fatalf("seed reserve: %v", err)
	}

	// The curated report tool is wired through the REAL ReportsRESTClient,
	// pointed at an httptest stand-in for the inventory-reports service, so
	// the full tool -> client -> REST -> DTO path runs. The stub echoes the
	// query it received into the row's hourBucket field so the test can
	// prove the tool's filters round-tripped through the query string.
	var lastReportQuery atomic.Value
	reportsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/reports/flow-accuracy/freshness":
			_ = json.NewEncoder(w).Encode(map[string]any{"lagSeconds": 1.5})
		case "/reports/flow-accuracy":
			lastReportQuery.Store(r.URL.RawQuery)
			_ = json.NewEncoder(w).Encode(map[string]any{"rows": []any{map[string]any{
				"sku": r.URL.Query().Get("sku"), "binId": "", "hourBucket": "2026-10-09T12:00:00Z",
				"receivedQuantity": 10, "stowedCount": 1, "pickedQuantity": 4,
				"reservationsCreated": 1, "reservationsRevoked": 1,
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(reportsSrv.Close)

	deps := inboundmcp.Deps{
		GetUsable: &usecases.GetUsable{Stock: stockRepo},
		RevokeReservation: &usecases.RevokeReservation{
			Stock: stockRepo, Reservations: reservations, Events: publisher,
			Clock: clock, UnitOfWork: uow,
		},
		Stock:   stockRepo,
		Reports: inboundmcp.NewReportsRESTClient(reportsSrv.URL, nil),
	}

	server := inboundmcp.NewServer(deps)
	hs := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-integration-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpItStack{
		session: session, pool: pool, publisher: publisher,
		sku: sku, binID: binID, unitID: unitID, resID: res.ID(),
		lastReportQuery: &lastReportQuery,
	}
}

// callTool drives tools/call and fails the test on a transport-level error,
// so a test only ever has to assert on res.IsError (tool errors) — the
// distinction the wire contract is about.
func (s *mcpItStack) callTool(t *testing.T, name string, args map[string]any) *sdkmcp.CallToolResult {
	t.Helper()
	res, err := s.session.CallTool(context.Background(), &sdkmcp.CallToolParams{
		Name: name, Arguments: args,
	})
	if err != nil {
		t.Fatalf("tools/call %s: transport error (must be a tool error instead): %v", name, err)
	}
	return res
}

// structured returns the call's structured content as a map.
func (s *mcpItStack) structured(t *testing.T, res *sdkmcp.CallToolResult) map[string]any {
	t.Helper()
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("no structured content: %+v", res.StructuredContent)
	}
	return m
}

// TestMCPIntegration_ListToolsExposesEveryRegisteredTool proves tools/list
// advertises the full registry — the two reads, the write, and the curated
// report tool — with the annotations a host needs to gate writes.
func TestMCPIntegration_ListToolsExposesEveryRegisteredTool(t *testing.T) {
	s := newMCPItStack(t)

	list, err := s.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := map[string]*sdkmcp.Tool{}
	for _, tool := range list.Tools {
		names[tool.Name] = tool
	}
	for _, want := range []string{
		"check_availability", "get_bin_occupancy",
		"revoke_reservation", "get_inventory_flow_accuracy_report",
	} {
		if _, ok := names[want]; !ok {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}

	// The write tool must be annotated non-read-only (and destructive) so a
	// host can gate it before letting a model call it.
	revoke := names["revoke_reservation"]
	if revoke.Annotations == nil || revoke.Annotations.ReadOnlyHint {
		t.Fatal("revoke_reservation must carry ReadOnlyHint=false")
	}
	if revoke.Annotations.DestructiveHint == nil || !*revoke.Annotations.DestructiveHint {
		t.Fatal("revoke_reservation must carry DestructiveHint=true")
	}
	// The reads stay annotated read-only.
	for _, name := range []string{"check_availability", "get_bin_occupancy", "get_inventory_flow_accuracy_report"} {
		if a := names[name].Annotations; a == nil || !a.ReadOnlyHint {
			t.Fatalf("%s must carry ReadOnlyHint=true", name)
		}
	}
}

// TestMCPIntegration_CheckAvailabilityReadsThroughPostgres drives the read
// tool over the real repos: 10 on-hand minus the seeded reservation of 4
// answers 6; a SKU with no stock answers 0 (not an error).
func TestMCPIntegration_CheckAvailabilityReadsThroughPostgres(t *testing.T) {
	s := newMCPItStack(t)

	res := s.callTool(t, "check_availability", map[string]any{"sku": s.sku.String()})
	if res.IsError {
		t.Fatalf("check_availability returned a tool error: %+v", res.Content)
	}
	if usable := s.structured(t, res)["usable"].(float64); usable != 6 {
		t.Fatalf("usable = %v, want 6 (10 on-hand minus 4 reserved)", usable)
	}

	unknown, err := shared.NewSKU(fmt.Sprintf("SKU-UNKNOWN-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("sku: %v", err)
	}
	empty := s.callTool(t, "check_availability", map[string]any{"sku": unknown.String()})
	if empty.IsError {
		t.Fatalf("check_availability for an unstocked SKU must answer 0, got a tool error: %+v", empty.Content)
	}
	if usable := s.structured(t, empty)["usable"].(float64); usable != 0 {
		t.Fatalf("usable for unstocked SKU = %v, want 0", usable)
	}
}

// TestMCPIntegration_GetBinOccupancyReadsThroughPostgres drives the bin
// diagnostic over the real FindByBin read: on-hand 10, reserved 4, usable 6
// and the unit's state, exactly what the mapping folds from the seeded row.
func TestMCPIntegration_GetBinOccupancyReadsThroughPostgres(t *testing.T) {
	s := newMCPItStack(t)

	res := s.callTool(t, "get_bin_occupancy", map[string]any{"binId": s.binID.String()})
	if res.IsError {
		t.Fatalf("get_bin_occupancy returned a tool error: %+v", res.Content)
	}
	m := s.structured(t, res)
	if m["binId"] != s.binID.String() {
		t.Fatalf("binId = %v, want %s", m["binId"], s.binID)
	}
	if m["unitCount"].(float64) != 1 {
		t.Fatalf("unitCount = %v, want 1", m["unitCount"])
	}
	if m["onHand"].(float64) != 10 || m["reserved"].(float64) != 4 || m["usable"].(float64) != 6 {
		t.Fatalf("occupancy = onHand %v / reserved %v / usable %v, want 10/4/6", m["onHand"], m["reserved"], m["usable"])
	}
	lines, ok := m["lines"].([]any)
	if !ok || len(lines) != 1 {
		t.Fatalf("lines = %v, want one per-unit line", m["lines"])
	}
	line := lines[0].(map[string]any)
	if line["sku"] != s.sku.String() || line["state"] != string(stock.StateReserved) {
		t.Fatalf("line = %v, want sku %s state %s", line, s.sku, stock.StateReserved)
	}
}

// TestMCPIntegration_RevokeReservationRoundTrip drives the WRITE tool
// end-to-end: the revocation must land in Postgres (reservation REVOKED,
// the reserved quantity returned to usable) and publish ReservationRevoked,
// and a second revocation must surface as a tool error (already resolved).
func TestMCPIntegration_RevokeReservationRoundTrip(t *testing.T) {
	s := newMCPItStack(t)
	ctx := context.Background()

	res := s.callTool(t, "revoke_reservation", map[string]any{"reservationId": s.resID})
	if res.IsError {
		t.Fatalf("revoke_reservation returned a tool error: %+v", res.Content)
	}
	if m := s.structured(t, res); m["revoked"] != true || m["reservationId"] != s.resID {
		t.Fatalf("structured = %v, want {revoked:true, reservationId:%s}", m, s.resID)
	}

	// The write really persisted through the real stack.
	refetched, err := postgres.NewReservationRepo(s.pool).FindByID(ctx, s.resID)
	if err != nil || refetched == nil {
		t.Fatalf("re-find reservation: %v", err)
	}
	if refetched.Status() != reservation.StatusRevoked {
		t.Fatalf("reservation status = %s, want REVOKED", refetched.Status())
	}
	unit, err := postgres.NewStockRepo(s.pool).FindByID(ctx, s.unitID)
	if err != nil || unit == nil {
		t.Fatalf("re-find stock unit: %v", err)
	}
	if unit.Reserved().Int() != 0 {
		t.Fatalf("reserved after revoke = %d, want 0 (quantity returned to usable)", unit.Reserved().Int())
	}

	// The publisher saw ReservationRevoked through the real use case.
	var saw bool
	for _, e := range s.publisher.Events() {
		if e.EventName() == "ReservationRevoked" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("revoke_reservation must publish ReservationRevoked, got %v", s.publisher.Events())
	}

	// The read model agrees: usable is back to 10.
	after := s.callTool(t, "check_availability", map[string]any{"sku": s.sku.String()})
	if after.IsError {
		t.Fatalf("check_availability after revoke: %+v", after.Content)
	}
	if usable := s.structured(t, after)["usable"].(float64); usable != 10 {
		t.Fatalf("usable after revoke = %v, want 10", usable)
	}

	// A second revocation is a domain rejection and must surface as a TOOL
	// error, not a transport error.
	again := s.callTool(t, "revoke_reservation", map[string]any{"reservationId": s.resID})
	if !again.IsError {
		t.Fatal("revoking an already-revoked reservation must surface a tool error")
	}
}

// TestMCPIntegration_RevokeUnknownReservationIsAToolError proves the domain
// rejection for a missing reservation crosses the wire as a tool error the
// model can read, never a transport-level failure.
func TestMCPIntegration_RevokeUnknownReservationIsAToolError(t *testing.T) {
	s := newMCPItStack(t)

	res := s.callTool(t, "revoke_reservation", map[string]any{
		"reservationId": fmt.Sprintf("res-missing-%d", time.Now().UnixNano()),
	})
	if !res.IsError {
		t.Fatal("revoking an unknown reservation must surface a tool error, not success")
	}
}

// TestMCPIntegration_InvalidInputSurfacesToolErrors: empty value objects
// (SKU, BinId) are rejected by the domain constructors and must come back
// as tool errors, not transport errors.
func TestMCPIntegration_InvalidInputSurfacesToolErrors(t *testing.T) {
	s := newMCPItStack(t)

	if res := s.callTool(t, "check_availability", map[string]any{"sku": ""}); !res.IsError {
		t.Fatal("empty sku must surface a tool error")
	}
	if res := s.callTool(t, "get_bin_occupancy", map[string]any{"binId": ""}); !res.IsError {
		t.Fatal("empty binId must surface a tool error")
	}
}

// TestMCPIntegration_ReportToolRoundTripThroughReportsREST drives the
// curated report tool through the REAL ReportsRESTClient: a full argument
// set returns the report rows (with the SKU filter round-tripped through
// the query string), and a missing window bound is a tool error.
func TestMCPIntegration_ReportToolRoundTripThroughReportsREST(t *testing.T) {
	s := newMCPItStack(t)

	// Every input property is required by the tool's schema (empty string
	// means "no filter"), exactly the shape a model host sends.
	res := s.callTool(t, "get_inventory_flow_accuracy_report", map[string]any{
		"from": "2026-10-09T10:00:00Z", "to": "2026-10-09T12:00:00Z",
		"sku": s.sku.String(), "binId": "", "granularity": "hour",
	})
	if res.IsError {
		t.Fatalf("get_inventory_flow_accuracy_report returned a tool error: %+v", res.Content)
	}
	m := s.structured(t, res)
	rows, ok := m["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("rows = %v, want the reports service's single row", m["rows"])
	}
	row := rows[0].(map[string]any)
	if row["sku"] != s.sku.String() || row["receivedQuantity"].(float64) != 10 {
		t.Fatalf("row = %v, want sku %s receivedQuantity 10", row, s.sku)
	}
	// The SKU filter really crossed the REST boundary.
	if q, _ := s.lastReportQuery.Load().(string); !strings.Contains(q, "sku="+s.sku.String()) {
		t.Fatalf("reports query = %q, want the sku filter forwarded", q)
	}

	// Missing required window bound: the schema rejects it before the
	// handler runs — a tool error, never a transport error.
	bad := s.callTool(t, "get_inventory_flow_accuracy_report", map[string]any{
		"from": "2026-10-09T10:00:00Z", "to": "",
		"sku": "", "binId": "", "granularity": "hour",
	})
	if !bad.IsError {
		t.Fatal("empty 'to' must surface a tool error")
	}
}
