---
id: 0012-adopt-mfe-console-architecture
slug: /adr/0012-adopt-mfe-console-architecture
title: 0012. Adopt the fleet's micro-frontend console architecture (ADR-0002 in warehouse-ops-agent)
sidebar_label: 12. Adopt MFE console architecture
sidebar_position: 12
description: This service's own adoption record for warehouse-ops-agent's ADR-0002 -- the inventory-mfe remote, the additive GET /reservations?demandRef= endpoint, and CORS middleware that let this service participate in the fleet-wide operator console without being modified.
---

# 0012. Adopt the fleet's micro-frontend console architecture

## Status

**Accepted.**

## Context

`warehouse-ops-agent`'s
[ADR-0002](https://github.com/IQVO/warehouse-ops-agent/blob/develop/docs/docs/adr/0002-micro-frontend-console-architecture.md)
is the fleet-wide decision: one Module Federation micro-frontend remote per
bounded context, composed at runtime by a separate `warehouse-console` shell,
with the one genuinely cross-cutting screen (Order Lifecycle) backed by a thin
BFF hosted in `warehouse-ops-agent` rather than as a new bounded context. That
record is not this repo's to own or restate — it is owned by
`warehouse-ops-agent`, the fleet's cross-context correlation surface. This ADR
exists only to record **this service's own side of adopting it**: what
`inventory-storage` had to add, and why each piece is additive rather than a
change to this service's existing domain, application, or REST contract.

Three things followed directly from that fleet decision landing:

- **A remote of our own.** Per ADR-0002's "one remote per bounded context, in
  that context's own repo" rule, this service — not `warehouse-console` —
  owns the screen that shows its own data. `web/` (the `inventory-mfe`
  package) already exists in this repo for exactly that reason.
- **The join-key gap ADR-0002 identified for this service specifically.**
  ADR-0002's Context section names `inventory-storage`'s `Reservation` as one
  of three aggregates that "already carry the join key in their own domain
  (`DemandRef`) ... but none exposed a GET endpoint to look up 'everything
  with this key'." `GET /reservations?demandRef=` closes that gap for this
  service (`getReservationsByDemandRef` in `apis/openapi.yaml`,
  `ports.ReservationRepo.FindByDemandRef` in the application layer) — it is
  the console's read side for "what did inventory-storage do for order X,
  line N," consumed both by the BFF's Order Lifecycle fan-out and directly by
  `inventory-mfe`'s own `InventoryScreen`.
- **CORS as new, permanent surface.** ADR-0002's Decision section requires
  each of the four services touched by the console to add `go-chi/cors`
  global middleware, scoped to `CORS_ALLOWED_ORIGINS` (default
  `http://localhost:5173,http://localhost:5182` — the shell and this
  service's own remote's dev port). This service had never needed a browser
  client before; every prior consumer was a server-to-server caller
  (`wes-work-planning`'s Kafka consumer, `facility-layout`'s synchronous
  read). See [`internal/adapters/inbound/http/server.go`](https://github.com/IQVO/inventory-storage/blob/develop/internal/adapters/inbound/http/server.go)'s
  `NewRouter` and `corsAllowedOrigins`.

## Decision

**We adopt ADR-0002 as-is: this service ships `inventory-mfe` as its Module
Federation remote, exposes `GET /reservations?demandRef=` as the additive
read the fleet decision required of it, and adds CORS middleware scoped to
the console's known origins — with no change to this service's existing
domain model, aggregates, or any pre-existing endpoint's contract.**

Concretely, what already exists in this repo because of that adoption:

- **`web/` — the `inventory-mfe` remote.** A Vite + React Module Federation
  remote (`vite --port 5182` in dev), consuming `@warehouse/ui-kit` via
  `file:../../warehouse-ui-kit` for `Card`, `StatusPill`, `DataTable`, and
  `useFetch` — the same shared components every sibling remote uses, so a
  `Reservation.status` or `StockUnit.state` pill renders identically here and
  in `order-mgmt-mfe`. It talks only to this service's own `SERVICE_API_BASE`
  (`INVENTORY_API_BASE` in `web/src/config.ts`) — no new backend surface, no
  call to any other service's API from the browser.
- **`InventoryScreen.tsx`** — this service's own operator screen, per
  ADR-0002's "decisions about what that screen shows belong to that context's
  own team/PR": two independent lookups, both scoped to what this service's
  REST API already exposes — SKU → usable inventory
  (`GET /inventory/{sku}/usable`) and demandRef → reservations
  (`GET /reservations?demandRef=`) — deliberately not a list-all/browse table,
  because no such endpoint exists here.
- **`GET /reservations?demandRef=`** (`getReservationsByDemandRef`) — returns
  every `Reservation` ever raised against a `demandRef`, array-shaped because
  a demandRef can have multiple reservations across its lifetime (revoked,
  then retried). An unknown `demandRef` returns `200` with an empty array,
  not `404`, matching this endpoint's read-model semantics. Side-effect-free;
  no existing use case, aggregate, or endpoint was modified to add it.
- **CORS middleware** in `NewRouter`, `AllowedOrigins` driven by
  `CORS_ALLOWED_ORIGINS` (comma-separated), defaulting to the shell's dev
  origin and this remote's own dev port, `AllowCredentials: false` (this
  service is unauthenticated by decision — ADR-0015 — and uses no cookies, so
  no credentialed cross-origin request is needed). `Idempotency-Key` is in the
  allowed request headers (ADR-0018) for any future MFE write.

What we deliberately did **not** do, consistent with ADR-0002's own scope
boundaries:

- No client-side fan-out to another service from this service's own code —
  `inventory-mfe` never calls `order-management`, `wes-work-planning`, or
  `fulfillment-execution` directly. The Order Lifecycle correlation stays in
  `warehouse-ops-agent`'s BFF, per ADR-0002's Decision.
  `warehouse-ops-agent`'s BFF is the only caller of
  `GET /reservations?demandRef=` from outside this remote.
- No change to `StockUnit`, `Bin/Location`, or `Reservation`'s domain
  invariants, nor to any pre-existing endpoint's request/response shape or
  status codes (see `REST_AUDIT.md` for the full pre-existing contract,
  unchanged by this adoption).
- No shared read model or database access granted to `warehouse-console` or
  any sibling remote — `inventory-mfe` is the only code that reads this
  service's REST API on the browser's behalf.

## Consequences

### Easier

- **This service's operator screen ships and evolves inside this repo.** A
  change to `Reservation`'s shape and the screen that renders it land in the
  same PR, same review, same ≥90% coverage bar as everything else here —
  exactly the ownership alignment ADR-0002 was written to preserve.
- **The join-key gap the fleet decision found is now closed for this
  service specifically**, and closed the same way every other endpoint in
  this repo is built: port → use case → adapter → handler → OpenAPI → tests.
  No mock, no bypass.
- **Visual consistency with the rest of the console is a compile-time
  consumption choice**, not a convention to remember — `@warehouse/ui-kit`'s
  `StatusPill` is what renders this service's own `Reservation`/`StockUnit`
  states, the same package every sibling remote imports.

### Harder

- **CORS is now permanent surface this service must keep current.** A
  forgotten `CORS_ALLOWED_ORIGINS` update when a new remote dev port or a
  real deployed console origin is added is a silent browser-side
  "Failed to fetch," not a loud backend error — the same risk ADR-0002 named
  for all four touched services, now concretely true here too.
  `TestCORS_Preflight_RejectsUnknownOrigin` guards the current default set,
  but a new origin still requires a deliberate change to
  `defaultCORSAllowedOrigins` or the env var in every deployment.
  `TestCORS_Preflight_AllowsDefaultOrigin`
  and `TestCORS_Preflight_AllowsSecondDefaultOrigin` cover the two defaults.
- **`GET /reservations?demandRef=` is now a contract two external callers
  depend on** (`inventory-mfe` directly, and `warehouse-ops-agent`'s BFF
  transitively) rather than an internal-only read — a breaking change to its
  response shape now has consequences outside this repo that did not exist
  before this adoption.
- **`inventory-mfe`'s `file:../../warehouse-ui-kit` dependency** means this
  remote's own build now requires `warehouse-ui-kit` checked out as a sibling
  directory and built first, the same pre-1.0 coupling ADR-0002 accepted
  fleet-wide and explicitly deferred past the first release.
