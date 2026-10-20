---
id: tools
title: MCP tools
sidebar_label: MCP tools
description: Every tool, resource and prompt the cmd/mcp server exposes, with input fields, read-only/write annotations, routes and a captured tools/list.
---

# MCP tools

`cmd/mcp` is a separate binary that serves the
[Model Context Protocol](https://modelcontextprotocol.io) over **Streamable
HTTP** only (`github.com/modelcontextprotocol/go-sdk` v1.8.0), implementation
name `inventory-storage-mcp` version `1.0.0`
([ADR 0008](../adr/0008-mcp-inbound-adapter.md)). It is unauthenticated, like the
REST API. Governance rules are in the [MCP governance charter](./governance-charter.md);
configuration in [Configuration](../operations/configuration.md#cmdmcp--the-mcp-server).

## Routes

`cmd/mcp/main.go` (`newRouter`) mounts the SDK handler three times so clients
can use either base path. None of these routes are in `apis/openapi.yaml`.

| Route | Methods | Served by |
| --- | --- | --- |
| `/mcp` | `POST` (JSON-RPC requests), `GET` (server stream), `DELETE` (end session) | Streamable HTTP handler |
| `/mcp/*` | same | Streamable HTTP handler |
| `/*` | same — catch-all, so `http://host:8090/` works as the endpoint | Streamable HTTP handler |
| `/healthz` | `GET` → `200 {"status":"ok"}` | the router itself (probes) |

The first `initialize` returns an `Mcp-Session-Id` header that every later
request must echo. Sessions live in process memory, which is why the chart runs
a single replica. Requests are traced by otelchi and timed by
`http.server.request.duration` / `http.server.active_requests`
([Observability](../operations/observability.md)).

## Tools

| Tool | Annotation | Input (all `string`) | Output | Use case |
| --- | --- | --- | --- | --- |
| `check_availability` | read-only | `sku` — the stock-keeping unit to check usable availability for | `{sku, usable}` | `GetUsable` |
| `get_bin_occupancy` | read-only | `binId` — the coded bin/location slot to inspect | `{binId, unitCount, onHand, reserved, usable, lines[]}`; each line `{stockUnitId, sku, onHand, reserved, usable, state}` | `StockRepo.FindByBin` (read model) |
| `get_relocation` | read-only | `relocationId` — the id of an applied intra-site relocation | `{relocationId, siteCode, sku, fromBin, toBin, quantity, reason, sourceRef, destinationStockUnitId, legs[], occurredAt}`; each leg `{stockUnitId, quantity}` | `GetRelocation` ([ADR 0039](../adr/0039-intra-site-relocation-stockrelocated-cloudevents.md)) |
| `revoke_reservation` | **write**, destructive, not idempotent | `reservationId` — the reservation to revoke; its quantity returns to usable | `{reservationId, revoked: true}` | `RevokeReservation` (emits `ReservationRevoked`, counts `inventory.reservations{outcome=revoked}`) |
| `get_inventory_flow_accuracy_report` | read-only; registered **only** when `REPORTS_BASE_URL` is set | `from` (RFC 3339, inclusive), `to` (RFC 3339, exclusive), `sku` (optional filter), `binId` (optional filter), `granularity` (optional; only `hour`, the default) | `{rows[]}` — same row shape as `GET /reports/flow-accuracy` | HTTP call to `cmd/inventory-reports` (10 s timeout) |

Errors come back as a tool result with `isError: true` and the use-case error
text (for example `reservation not found`), not as a JSON-RPC error. An empty
`sku` / `binId` fails the value-object constructor; `from`/`to` missing fails
with `from and to are required (RFC3339)`.

`get_bin_occupancy` cannot tell an unknown bin from an empty one: both return
zero units. `cmd/mcp` reads the same Postgres as `cmd/inventory`
(`DATABASE_URL` is required; there is no in-memory store), so it sees exactly
what the REST API wrote.

## Resources and prompts

| Kind | Name | Detail |
| --- | --- | --- |
| Resource template | `usable inventory by SKU` | URI `inventory://{sku}/usable`, `application/json`, same content as `check_availability`. |
| Prompt | `triage_low_stock` | No arguments. Returns the SOP: call `check_availability`, inspect bins with `get_bin_occupancy`, revoke only reservations positively identified as failed, escalate otherwise (`internal/adapters/inbound/mcp/prompts.go`). |

## Captured `tools/list`

Taken from a local run of the binary made while it still had an in-memory mode
(`MCP_ADDR=:18190`, `REPORTS_BASE_URL` set so the report tool registers); the
tool list does not depend on the data store:

```json
{"name": "check_availability",
 "inputSchema": {"type": "object", "properties": {"sku": {"type": "string"}},
                 "required": ["sku"], "additionalProperties": false},
 "annotations": {"readOnlyHint": true, "idempotentHint": false}}
{"name": "get_bin_occupancy",
 "inputSchema": {"type": "object", "properties": {"binId": {"type": "string"}},
                 "required": ["binId"], "additionalProperties": false},
 "annotations": {"readOnlyHint": true, "idempotentHint": false}}
{"name": "get_inventory_flow_accuracy_report",
 "inputSchema": {"type": "object",
                 "properties": {"from": {}, "to": {}, "sku": {}, "binId": {}, "granularity": {}},
                 "required": ["from", "to"],
                 "additionalProperties": false},
 "annotations": {"readOnlyHint": true, "idempotentHint": false}}
{"name": "get_relocation",
 "inputSchema": {"type": "object", "properties": {"relocationId": {"type": "string"}},
                 "required": ["relocationId"], "additionalProperties": false},
 "annotations": {"readOnlyHint": true, "idempotentHint": false}}
{"name": "revoke_reservation",
 "inputSchema": {"type": "object", "properties": {"reservationId": {"type": "string"}},
                 "required": ["reservationId"], "additionalProperties": false},
 "annotations": {"readOnlyHint": false, "destructiveHint": true, "idempotentHint": false}}
```

(Descriptions trimmed.) Note the report tool's schema: the SDK marks every
struct field without `omitempty` as required, so `sku`, `binId` and
`granularity` carry `omitempty` and only `from` and `to` are required (gap fix
D16; `TestReportTool_OptionalFiltersAreOptionalOnTheWire` pins it, and the
fitness test `TestMCPOptionalToolInputsCarryOmitempty` fails on any input field
described as optional without `omitempty`). An omitted `granularity` means
`hour`.

## Calling it with curl

```bash
go run ./cmd/mcp   # listens on :8090

curl -si localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
# -> Mcp-Session-Id: <id>

curl -s localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Mcp-Session-Id: <id>' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"check_availability","arguments":{"sku":"SKU-1"}}}'
# event: message
# data: {"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"{\"sku\":\"SKU-1\",\"usable\":0}"}],"structuredContent":{"sku":"SKU-1","usable":0}}}
```

Every tool call opens a span `mcp.tool <name>` with attribute `mcp.tool.name`.
The tool set is pinned by `testdata/tool_registry.golden` and exercised by the
E1–E3 evals ([Testing](../development/testing.md#mcp-evals)).
