---
id: 0008-mcp-inbound-adapter
slug: /adr/0008-mcp-inbound-adapter
title: 0008. Model Context Protocol as an inbound adapter, not a new service
sidebar_label: 8. MCP inbound adapter
sidebar_position: 8
description: "Expose this bounded context to the AI ecosystem via an MCP server built as a second driving adapter over the existing use cases — Streamable HTTP, official Go SDK, curated intent-level tools (the original static bearer-key auth was superseded by ADR-0015: MCP is unauthenticated)."
---

# 0008. Model Context Protocol as an inbound adapter, not a new service

## Status

**Accepted — auth, bearer-key, OAuth-seam and `mcp.readKey`/`mcp.readWriteKey`
sections superseded by [ADR-0015](./0015-remove-rest-identity-layer.md).** MCP
is **unauthenticated by decision**: `auth.go`, the bearer middleware and the
chart's key Secret no longer exist, and the sections below that describe them
are kept as history only. The pilot implementation is `fulfillment-execution` (ADR-0008
there); this ADR records the same decision for `inventory-storage`, adapted to
this context's use cases and tools.

**Addendum 2026-09-07 — deployable.** `cmd/mcp` is now built into the service
image as `/app/mcp` and deployed by the Helm chart as its own Deployment +
ClusterIP Service (`<release>-mcp`, port 8090) behind `mcp.enabled` (off by
default) *(originally with bearer keys from a chart-rendered Secret,
`mcp.readKey` / `mcp.readWriteKey` — removed by ADR-0015)*. The binary now serves an unauthenticated
`GET /healthz` for the probes and mounts the MCP endpoint at both `/` and
`/mcp`. Made deployable to the `warehouse` kind cluster on 2026-09-07; the
cluster rollout itself is a `warehouse-infra` change (`mcp.enabled=true` plus
generated keys per service, and ops-agent's `INVENTORY_STORAGE_MCP_ENDPOINT`
pointed at `http://inventory-storage-mcp.warehouse-systems.svc.cluster.local:8090/mcp`).

## Context

The platform is being connected to the AI ecosystem (Claude, Cursor, ChatGPT,
agent frameworks). The interoperability standard those clients speak is the
**Model Context Protocol (MCP)**: a client discovers a server's *tools*
(model-callable functions), *resources* (read-only context), and *prompts*
(reusable templates), then an LLM decides which to call.

The forces:

- **There is already a clean action surface.** Every capability of this service
  is an application-layer **use case** (`internal/application/usecases`), one
  struct per use case, reached through ports. The `chi` HTTP adapter is a thin
  driving adapter over exactly those use cases. An AI client needs the same
  actions the HTTP client already has — chiefly "how much of this SKU is
  usable?" and "release this failed reservation".
- **The domain must not learn about MCP.** ADR-0001's dependency rule is
  load-bearing: domain depends on nothing, application depends on domain,
  adapters depend inward. A protocol whose shape is set by an external LLM
  ecosystem is precisely the kind of concern that must stay in an adapter.
- **MCP has an idiomatic Go path now.** The official **MCP Go SDK**
  (`github.com/modelcontextprotocol/go-sdk`) is a Tier-1 SDK. Building the
  server in Go keeps it in the same language, module, and quality gate as the
  rest of the service — no Python sidecar, no second toolchain.
- **The spec is versioned aggressively.** Revisions in 2025-06, 2025-11, and
  2026-07 have already deprecated features (`roots`, `sampling`, `logging` —
  SEP-2577). Whatever is built will need to track a moving contract.
- **Tools are model-controlled and can act.** Unlike an HTTP client driven by
  code we wrote, an LLM chooses *when* to call a tool and *with what arguments*.
  The spec's own guidance is emphatic: curate a small set of intent-level
  tools, treat tool invocation as requiring host consent, and guard
  state-changing tools most heavily.
- **This context has exactly one safe write to expose.** `RevokeReservation`
  is *revocable by design* — it returns bound quantity to usable so a failed
  physical delivery never strands an order (ADR-0003). It neither consumes nor
  destroys stock, which makes it the one state change appropriate for an
  autonomous caller. The genuinely destructive operations here (stow, pick,
  cycle-count reconciliation) are deliberately **not** exposed as tools.
- **This is an internal, non-user-facing deployment.** The servers run inside
  the `warehouse` kind cluster for agent and developer use, not on the public
  internet for end users. The MCP authorization spec permits a static bearer
  token for exactly this case; full OAuth 2.1 is required only when a server
  faces real end users.

## Decision

**We will expose this bounded context to the AI ecosystem through an MCP server
built as a second driving adapter over the existing use cases — leaving the
domain and application layers untouched.**

### The adapter, mirroring the HTTP one

A new `internal/adapters/inbound/mcp/` sits beside `internal/adapters/inbound/http/`:

```
internal/adapters/inbound/mcp/
  server.go      MCP Server wiring (Go SDK), capability registration
  tools.go       intent-level tool handlers -> call use cases
  resources.go   read-model resources (scoped, not bulk)
  prompts.go     workflow prompts (operational SOPs)
  auth.go        (removed by ADR-0015 — was the bearer-key auth middleware)
  report_tool.go curated read-only report tool (ADR-0011)
  mapping.go     tool I/O DTOs + a narrow read-only query port
```

It depends inward on `application` exactly as the HTTP adapter does. No MCP type
appears in `internal/domain/**` or `internal/application/**`. The tool handlers
call the **same** use case structs the HTTP handlers call — never a parallel
code path, never the domain directly. Where the adapter needs a read beyond a
use case (bin occupancy), it declares a narrow `StockQueries` port satisfied by
the existing `StockRepo`; `cmd/mcp` injects the concrete repo, so the adapter
still never imports an outbound package.

### A separate `cmd/mcp` binary

The MCP server ships as its own composition root, `cmd/mcp/main.go`, reusing the
same repositories, ports, and telemetry wiring as `cmd/inventory`. Two
deployables from one module: the HTTP service and the MCP server. This isolates
blast radius, lets the two scale independently, and keeps least-privilege clean
(the MCP process can be given a narrower footprint).

### Streamable HTTP only

The single supported transport is **Streamable HTTP**. Sessions are
**stateful**: `inbound/mcp.Handler` calls
`mcp.NewStreamableHTTPHandler(getServer, nil)` with nil options, so the SDK
keeps an in-memory session per `Mcp-Session-Id` in each process (which is why
the chart does not autoscale `cmd/mcp` and a second replica needs session
affinity). We do not ship stdio builds; local desktop-client use goes through the
same HTTP endpoint. One transport is one thing to secure, trace, and test.

### Curated, intent-level tools — not one tool per endpoint

Tools are designed around decisions an agent makes, not around REST endpoints.
Mechanically wrapping all eight HTTP routes would overwhelm the model — the
documented number-one MCP anti-pattern. The surface for this context:

- `check_availability` (read) — usable quantity for a SKU, aggregated across
  its bins; wraps the `GetUsable` read model. Usable, not total, is what
  constrains a release.
- `get_bin_occupancy` (read) — what a single bin holds: total on-hand,
  reserved, usable, and a per-StockUnit breakdown; backed by the existing
  `StockRepo.FindByBin` read via the narrow `StockQueries` port.
- `revoke_reservation` (write, annotated destructive) — wraps the
  `RevokeReservation` use case; the operation is revocable by design (returns
  quantity to usable), so a model-invoked release never strands or destroys
  stock. Revoking a missing or already-revoked reservation is a clean typed
  error.

Resources expose the usable read model as a **scoped** context contract
(`inventory://{sku}/usable`), never a database dump. Prompts encode operational
SOPs (e.g. `triage_low_stock`: how to read availability, inspect bin occupancy,
revoke only confirmed-failed reservations, and when to escalate).

### Static bearer-key auth, behind an OAuth-ready seam *(superseded by ADR-0015 — removed)*

*Historical:* `auth.go` validated a per-client API key (from a Kubernetes Secret) on every
request; missing or invalid key returns `401`; the key is never logged. Two key
classes — read-only and read-write — gate the write tool without an IdP. The
middleware is an **interface**, so an OAuth 2.1 resource-server implementation
(short-lived tokens, `.well-known` discovery, no token passthrough) can drop in
later without touching any tool handler.

### Reuse the existing observability

The adapter is instrumented with the same OpenTelemetry setup as the HTTP and
Kafka boundaries: a span per tool call (tool name, required scope, outcome). MCP
calls appear in Jaeger and Grafana next to HTTP requests, continuing the same
distributed traces.

## Consequences

### Easier

- **The domain and application layers do not change at all.** MCP is purely
  additive; the dependency rule (ADR-0001) is preserved and checked by the
  existing arch-go fitness tests (ADR-0006), extended to cover the mcp adapter.
- **One action surface, two protocols.** HTTP and MCP call the same use cases,
  so behaviour — including every invariant — is identical regardless of caller.
- **Model-invoked writes are safe by construction.** `RevokeReservation` is
  revocable by design (ADR-0003): it returns quantity to usable rather than
  consuming stock, and a missing/already-revoked reservation surfaces as a
  clean structured tool error (ADR-0005). The genuinely irreversible operations
  are simply not exposed.
- **It stays in Go, in one quality gate.** The MCP adapter is unit-tested to the
  same ≥90% bar, linted, and CI-gated like every other package.
- ~~**The auth upgrade is contained.**~~ *(moot — auth was removed by ADR-0015.)*

### Harder

- **A second deployable to run and secure.** `cmd/mcp` is another binary, image,
  Helm release, and ingress. The isolation is deliberate but it is real
  operational surface that did not exist before.
- **No auth at all (ADR-0015).** The original static bearer key was removed;
  the MCP server is unauthenticated by decision and must stay in-cluster
  (internal, non-user-facing). It does **not** cover user-facing or
  multi-tenant use. Recording that boundary is the point.
- **The MCP spec is a moving target.** Aggressive versioning and deprecations
  mean the SDK must be pinned (currently `go-sdk` v1.8.0 in `go.mod`) and revisited; features like
  `roots`/`sampling` are already deprecated and must be avoided in favour of
  tool parameters.
- **Tool curation is an ongoing discipline, not a one-time choice.** Nothing in
  the compiler stops a future PR from adding a tool per endpoint. The MCP
  governance charter (`docs/mcp/governance-charter.md`) and its enforcing tests
  exist to hold the line — a tool-count budget (`maxTools = 8`,
  `TestGovernance_ToolCountWithinBudget`) and annotation checks run in CI
  (see ADR-0027); without them the surface degrades.
- **LLM-chosen arguments are untrusted input.** Every tool handler must validate
  its inputs defensively — the caller is a model, not our own code — which is
  stricter than what the HTTP DTO layer assumes.
- **`revoke_reservation` is a state change an autonomous agent can trigger.** It
  is annotated destructive (there is no scope gate any more — ADR-0015), and the
  spec expects host-side consent. Its `ReservationRevoked` event is published
  through the same transactional outbox as the REST path (ADR-0017). Revocation is recoverable (the quantity returns to usable and demand
  can be re-allocated), but the residual risk of an agent revoking *live* demand
  is real; the `triage_low_stock` prompt tells the model to revoke only a
  positively-identified failed reservation.
