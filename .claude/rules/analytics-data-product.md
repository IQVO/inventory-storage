---
paths:
  - "internal/adapters/outbound/analyticsstore/**"
  - "internal/**/analytics*/**"
---

# Analytics data product (ADR-0011)

Additive read side built from this service's OWN domain events. The OLTP
domain/application layers are NOT modified and must NOT import the
analytics store (`internal/architecture/architecture_test.go` arch-test
enforces this). `internal/analytics/report/` depends on nothing.

- Events are fanned to a SEPARATE topic `warehouse.inventory.analytics` by a
  new outbound adapter; the integration topic/publisher (see
  `integration-events.md`) are untouched. Selected by `EVENT_PUBLISHER=kafka`
  (fan-out alongside the integration publisher).
- Separate analytical Postgres (`ANALYTICS_DATABASE_URL`), own migrations
  (`migrations/analytics/`), read-only reader role.
- Three processes:
  - `cmd/inventory` (OLTP)
  - `cmd/inventory-projector` (the ONLY writer; consumes the analytics topic
    from FirstOffset, CloudEvents 1.0 only, dispatches on the full `type`,
    idempotent on the CloudEvents `id`; non-CloudEvents are WARN-logged and skipped)
  - `cmd/inventory-reports` (read-only reader, `GET /reports/...`). Report
    exposed via the MCP server too (`cmd/mcp`, ADR-0008).
- Report: **Inventory Flow & Accuracy**, keyed per SKU/bin × hour
  (received/picked quantity, stow/reservation/cycle-count/discrepancy
  counts).
- `GET /reports/.../freshness` reports projection lag.

## MCP server (ADR-0008)

`cmd/mcp` ships in the same image as `/app/mcp` and is deployed by the Helm
chart as a separate Deployment + ClusterIP Service named `<release>-mcp`,
gated on `mcp.enabled` (off by default). Serves MCP over Streamable HTTP at
both `/` and `/mcp` on port `8090` (`mcp.service.port` → `mcp.httpAddr`),
and answers `GET /healthz`.
