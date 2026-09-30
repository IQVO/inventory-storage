---
id: 0022-horizontal-autoscaling-and-pgxpool-tuning
slug: /adr/0022-horizontal-autoscaling-and-pgxpool-tuning
title: "22. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning"
sidebar_label: "22. HPA + pgxpool tuning"
sidebar_position: 22
description: "ADR 0022 — Phase 3 (scalability) for inventory-storage: an autoscaling/v2 HorizontalPodAutoscaler per independently-assessed workload (api max 4, analytics-projector max 2, analytics-reports max 3, frontend max 3; mcp explicitly excluded for a real in-memory-session reason), all default-disabled via values.yaml so this PR changes nothing on merge; plus explicit pgxpool.Config MaxConns caps and per-pool statement_timeout values, sized against the shared Postgres instance's real max_connections=100 ceiling and PgBouncer's transaction-pooling front end."
---

# 22. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning

## Status

Accepted — implemented in the same change that introduces this record.
This is Phase 3 (scalability) of the fleet production-readiness plan,
following the reference PR that order-management's
[ADR-0026](https://github.com/claudioed/order-management/pull/110) shipped
for the other ~9 fleet repos in this phase to copy — the same role
order-management's ADR-0025 played for Phase 2's resilience wave. It also
builds directly on `warehouse-infra` PR #43, which put PgBouncer
(transaction-pooling mode) in front of the fleet's single shared Postgres
instance and already re-pointed every service's OLTP `DATABASE_URL` Secret
at it — including this service's.

## Context

Before this change, this chart had exactly one `HorizontalPodAutoscaler`
template, unconditionally targeting the `api` Deployment only, wired to a
flat `autoscaling.enabled/minReplicas/maxReplicas/
targetCPUUtilizationPercentage` block — untested against the other four
Deployments this chart renders (`mcp`, `frontend`, `analytics-projector`,
`analytics-reports`), and never assessed for whether scaling
`analytics-projector` past 1 replica was even safe given its Kafka
consumer-group membership. `replicaCount` was hardcoded to 1 with no HPA
anywhere else in the chart.

Separately, no pool in the codebase set an explicit `pgxpool.Config.
MaxConns`, so every pool — the OLTP pool
(`internal/adapters/outbound/postgres/pool.go`, used by `cmd/inventory` and
`cmd/mcp`) and the two analytics pools
(`internal/adapters/outbound/analyticsstore/pool.go`'s `NewPool` used by
`cmd/inventory-projector`, and `NewReadOnlyPool` used by
`cmd/inventory-reports`) — ran on pgx's library default, `max(4,
runtime.NumCPU())` connections per process. No pool set a
`statement_timeout` either, so a single runaway query (a lock wait, an
unbounded date-range report query) could hold a pooled connection
indefinitely, with nothing to cancel it.

This matters together, not separately: turning on HPA for a Postgres-backed
workload without an explicit, bounded `MaxConns` means the service's real
connection ceiling becomes "however many CPUs the node happens to have,
times however many replicas the HPA happens to have scaled to" — an
unbounded, indirect function of cluster autoscaling and CPU load, not a
number anyone chose. This PR does both together on purpose: HPA without a
bounded `MaxConns` would have been the actual production-readiness gap.

### Finding: shared Postgres, PgBouncer already in front, and the real max_connections

Re-verified, not assumed (this is the fleet-verified state, already
established by `warehouse-infra` PR #43): all 10 backend services share
ONE physical Postgres server instance, each with its own logical
database/role, `max_connections=100` — the unmodified Bitnami chart
default, never deliberately sized for the fleet's real connection demand.
This service is one of up to 10 fleet services drawing from the same
100-connection pool, not a dedicated database it can spend freely.

Unlike order-management's ADR-0026 (written before PgBouncer existed),
this service's OLTP `DATABASE_URL` Secret is **already** re-pointed at
PgBouncer (transaction-pooling mode) — no code or chart change needed here,
per PgBouncer PR #43's design: pgxpool just points at a different
`host:port`, same DSN shape, transparent to `postgres.NewPool`. PgBouncer
absorbs the real server-side connection multiplexing, so a generous
per-service `pgxpool.MaxConns` (this ADR keeps order-management's
reference numbers) is safe: it bounds this *process's* logical connection
count and worst-case blast radius, not the physical Postgres backend count
PgBouncer is now responsible for keeping under `max_connections`.

Mirroring PgBouncer PR #43's reasoning exactly: **analytics DSNs stay
DIRECT against Postgres**, not through PgBouncer. Both analytics
processes (`cmd/inventory-projector`, `cmd/inventory-reports`) hold few,
low-QPS connections each (a single Kafka-consumer writer and a small
read-only reader pool), so there is no pooling benefit, and PgBouncer's
transaction-pooling mode would add session-affinity risk for no gain.

## Decision

### 1. HorizontalPodAutoscaler — one per independently-assessed workload

Assessed each of the five Deployments this chart renders on its own
merits — statefulness, and (for the Kafka consumer) consumer-group-id
convention — rather than blanket-enabling HPA everywhere:

| Deployment | HPA? | min | max | target CPU | Why |
|---|---|---|---|---|---|
| `api` (`cmd/inventory`) | Yes | 1 | 4 | 70% | Stateless OLTP HTTP. Its only per-process cache — the `LOCATION_LOOKUP_MODE=kafka` facility-location cache (`internal/adapters/outbound/facilitycache`) — is fed by a **per-process-UNIQUE** consumer group (`uniqueConsumerGroup()`, `consumer.go`) that rebuilds the cache from a full Kafka replay on every process start, safe to run N independent copies of by design. It has no inbound consumer under a shared group (this service only publishes to Kafka, per AGENTS.md). Nothing here breaks at N>1. |
| `analytics-projector` (`cmd/inventory-projector`) | Yes, capped at **2**, not api's 4 | 1 | 2 | 70% | Its analytics Kafka consumer group, `kafka.AnalyticsConsumerGroup = "inventory-analytics"` (`internal/adapters/inbound/kafka/analytics_consumer.go`), is a **stable, shared** group with no per-instance uniqueness — verified directly in that file — so N replicas legitimately share the analytics topic's partitions via normal Kafka group rebalancing, the same safe shape as order-management's `RepromiseConsumer`/`AnalyticsConsumerGroup`. Capped at 2 rather than left at 4 for two additional reasons, not correctness: (a) the publisher partitions by SKU (`internal/adapters/outbound/kafka/analytics_publisher.go`'s `marshalData`, keyed per ADR-0021), so ordering is only guaranteed per-SKU, and a wide fan-out buys little for what is a lightweight idempotent-upsert workload; (b) every write is already idempotent on `event_id` via `ProcessedEvents.MarkProcessed`, so correctness does not regress at 2, but there is no throughput case yet that justifies more. |
| `analytics-reports` (`cmd/inventory-reports`) | Yes | 1 | 3 | 70% | Stateless read-only REST reader over its own read-only pgxpool (`analyticsstore.NewReadOnlyPool`) — no in-memory state, no Kafka consumption. Same treatment as `api`. |
| `frontend` (nginx-unprivileged serving the built SPA bundle) | Yes | 1 | 3 | 70% | Pure static-asset serving. No server-side session, no per-request state. The most trivially horizontally-scalable workload in this chart. |
| `mcp` (`cmd/mcp`) | **No — deliberately excluded, not just disabled** | — | — | — | `internal/adapters/inbound/mcp/server.go`'s `Handler` wraps `mcp.NewStreamableHTTPHandler` from `github.com/modelcontextprotocol/go-sdk/mcp` (same SDK version, v1.8.0, and wiring shape order-management's ADR-0026 already assessed) — this handler keeps **per-process, in-memory session state** keyed by the MCP protocol's own `Mcp-Session-Id` header (a real multi-request session, not just a TCP/HTTP connection). `charts/inventory-storage/templates/mcp-service.yaml` is a plain `ClusterIP` Service with no `sessionAffinity` configured, so under >1 replica a second request carrying the same `Mcp-Session-Id` (e.g. a `tools/call` following an earlier `initialize`) could land on a different pod than the one that created the session, which has never heard of it and would reject or silently start a new one. Fixing it for real needs either `sessionAffinity: ClientIP` (a partial mitigation only) or wiring the SDK's `StreamableHTTPOptions.EventStore` to a shared/external session store — a real code change, out of scope for this chart-and-pool-tuning PR. `mcp.replicaCount` stays a plain, manually-set value; no `autoscaling.mcp` block exists in `values.yaml` at all. Revisit if/when `cmd/mcp` adopts an external session store or the SDK's stateless mode. |

Every enabled block is namespaced under a single top-level `autoscaling:`
key in `values.yaml`
(`autoscaling.<api|projector|reports|frontend>.{enabled,minReplicas,
maxReplicas,targetCPUUtilizationPercentage}`), **every `enabled` value
defaults to `false`**. This PR makes per-workload HPA possible and
verified-correct; it deliberately does not turn any of it on — the fleet
enables each workload's HPA later, once, as a conscious rollout decision.

**No replicas-vs-HPA fight.** Each Deployment template guards its
`spec.replicas` field with `{{- if not .Values.autoscaling.<x>.enabled
}}` — when a workload's HPA is enabled, its Deployment renders with NO
`replicas` field at all (a hardcoded `replicas:` next to an active HPA
would otherwise fight it on every reconcile, most visibly right after a
`helm upgrade` resets it back to the chart's static value). Verified
directly with `helm template`:

- Default values → 0 `HorizontalPodAutoscaler` resources render, every
  Deployment keeps its static `replicas:` field (5 Deployments: api,
  projector, reports, frontend, mcp, all with `analytics.enabled`/
  `frontend.enabled`/`mcp.enabled` set true for the render).
- All four `autoscaling.*.enabled=true` → exactly 4
  `HorizontalPodAutoscaler` resources render (one per scalable workload,
  `mcp` has none by design), and none of those four Deployments has a
  `replicas:` field — `mcp`'s Deployment still does.
- Only `autoscaling.api.enabled=true` → exactly 1 HPA renders, only the
  `api` Deployment loses its `replicas:` field; `projector`/`reports`/
  `frontend` keep theirs untouched. Mixed enablement is safe and
  independent per workload, as designed.

`helm lint` passes; the chart's own `tests/test_service_selectors.py`
(each Service selects exactly one Deployment) still passes unchanged —
this PR does not touch any Service or selector. `go build`/`go vet`/
`make check`/`make arch-test` all pass unchanged (no Go code path outside
the two pool files is affected by the chart changes).

### 2. pgxpool MaxConns

All three pools now set an explicit `pgxpool.Config.MaxConns` instead of
inheriting the CPU-derived library default (`max(4, runtime.NumCPU())`),
matching order-management's ADR-0026 reference numbers:

| Pool | Used by | `MaxConns` | Reasoning |
|---|---|---|---|
| OLTP (`postgres.NewPool`) | `cmd/inventory` (`api`), `cmd/mcp` (`mcp`) | **10** | `api`'s HPA ceiling of 4 replicas × 10 = 40 connections, ~40% of the shared instance's `max_connections=100` for this ONE of up to 10 fleet services' OLTP path alone — the plan's stated conservative budget. Since PgBouncer (transaction-pooling mode) already sits in front of this DSN, `MaxConns` here bounds this process's own logical connection count and worst-case Postgres-backend blast radius; PgBouncer is responsible for the real server-side connection multiplexing across every service sharing the instance. |
| Analytics writer (`analyticsstore.NewPool`) | `cmd/inventory-projector` | **5** | HPA-scalable to 2 (see the table above), so at that ceiling 2 × 5 = 10 connections. Its writes are single-row upserts against the projection tables, keyed one SKU/bin at a time — a small, flat pool is enough. Connects DIRECTLY to Postgres (no PgBouncer), per PgBouncer PR #43's split. |
| Analytics reader (`analyticsstore.NewReadOnlyPool`) | `cmd/inventory-reports` | **5** (`ReportsMaxConns`) | `reports` IS HPA-scalable (max 3); at that ceiling, 3 × 5 = 15 connections against the analytical database — comfortably inside the shared ceiling alongside the OLTP path's 40. Also DIRECT, not through PgBouncer. |

Worst case across every workload simultaneously at its proposed HPA
maximum (`api` 4 × 10 = 40, `projector` 2 × 5 = 10, `reports` 3 × 5 = 15;
`mcp` has no HPA, assume 2 manually-set replicas × 10 = 20): 40 + 10 + 15
+ 20 = **85** of the shared instance's 100 connections for this ONE
service alone, even at every proposed HPA ceiling simultaneously, with
**HPA still disabled by default today** (at `replicaCount: 1` everywhere
and no HPA enabled, this service's actual usage is `10 (api) + 10 (mcp,
if deployed) + 5 (projector) + 5 (reports)` = at most 30 connections, 30%
of the ceiling). Because the OLTP path (the largest single contributor,
40 at max scale) is routed through PgBouncer, the number of PHYSICAL
Postgres backend connections this service consumes at max scale is far
lower than 85 — PgBouncer's pool multiplexes many client (pgxpool)
connections onto a much smaller number of real server connections in
transaction-pooling mode. The 85-of-100 figure is presented here as the
honest LOGICAL/client-side ceiling this service's own pools could reach,
consistent with order-management's ADR-0026 accounting style, not a claim
about physical backend usage post-PgBouncer — that number depends on
PgBouncer's own `pool_size` configuration (warehouse-infra PR #43),
outside this PR's scope. This is a real, documented residual risk for the
fleet-wide follow-up once more of the other 9 services' own Phase 3 PRs
land, not one this PR can fully close alone.

If/when the fleet turns on multiple workloads' HPA simultaneously and this
budget gets tight in practice at the PgBouncer layer, the next lever is
PgBouncer's own `pool_size`/`max_connections` (a `warehouse-infra`
Terraform/config change) or Postgres's own `max_connections` — both
separate, cross-service decisions, out of scope here.

### 3. statement_timeout

All three pools set `statement_timeout` via `pgxpool.Config.AfterConnect`,
running `SET statement_timeout = '<value>'` on every new physical
connection as it's established (not per-query, so it survives connection
reuse across pooled acquisitions). Values differ per pool because their
query shapes differ, matching order-management's ADR-0026 reference
values exactly:

| Pool | `statement_timeout` | Reasoning |
|---|---|---|
| OLTP (`postgres.StatementTimeout`) | **5s** | Every OLTP query (stow, reserve, release, pick, cycle-count) is a single-aggregate read/write keyed by id, normally low-single-digit milliseconds. 5s is roughly 1000x that — generous headroom for real transient contention (a lock wait behind a concurrent writer) without ever being a normal-path concern, while bounding the absolute worst case tightly since this is the interactive, latency-sensitive path and also the pool with the most connections (40 at max HPA scale) to protect. |
| Analytics writer (`analyticsstore.StatementTimeout`) | **10s** | A Kafka consumer replaying a backlog after a redeploy issues upserts in a tight loop; a transient lock wait here doesn't need to be as tight as an interactive OLTP request. Still bounded — an unbounded query here would wedge a projector replica's only connection pool indefinitely, stalling that replica's share of the analytics pipeline. |
| Analytics reader (`analyticsstore.ReportsStatementTimeout`) | **15s** | The Inventory Flow & Accuracy report aggregates rows across a caller-chosen time range (ADR-0011) — wider than the OLTP side's always-single-aggregate-by-id shape — so it gets more headroom, but still a hard ceiling: a caller-supplied wide date range must not be able to hold a reports connection forever. |

Verified with a real Postgres via testcontainers
(`internal/adapters/outbound/postgres/pool_limits_integration_test.go`,
`-tags=integration`), not a mock and not just reading `pg_settings`:

- `TestNewPool_AppliesStatementTimeoutToNewConnections` — opens a pool
  against a real `postgres:16-alpine` container with a short test-only
  timeout (200ms, via the shared `NewPoolWithLimits` the production
  `NewPool` wraps), confirms `SHOW statement_timeout` reads back `200ms`
  on a freshly acquired connection, then runs `SELECT pg_sleep(2)` and
  asserts Postgres itself cancels it (SQLSTATE 57014, "canceling
  statement due to statement timeout") rather than letting it run the
  full 2s — proving the setting is genuinely enforced server-side, not
  merely set and ignored — and finally confirms the pool is still usable
  afterward (the cancelled statement doesn't poison the connection).
- `TestNewPool_AppliesMaxConns` — acquires exactly `maxConns` connections
  from a pool configured with `MaxConns=2`, then asserts a further
  `Acquire` blocks until `context.DeadlineExceeded`, proving `MaxConns`
  is the pool's real, enforced ceiling rather than advisory.

Both tests pass locally against a real Postgres container.

## Consequences

- HPA is now possible, correct, and independently verified per workload
  for four of this chart's five Deployments — but **off by default
  everywhere**. Merging this PR changes nothing about production replica
  counts; `MaxConns`/`statement_timeout` are the only behavior change
  that takes effect on deploy, and both are conservative relative to
  today's unbounded defaults (they can only reduce, never increase,
  worst-case connection usage and hung-query duration).
- `mcp` remains explicitly un-autoscaled, with the exact reason
  (in-memory session state, no sticky routing) recorded here and in
  `values.yaml`'s comments, so a future contributor doesn't mechanically
  copy `api`'s HPA block onto it without re-solving the session-affinity
  problem first.
- The worst-case 85-of-100-connections LOGICAL figure for this service
  alone, at every proposed HPA ceiling simultaneously, is an honest,
  documented residual risk at the client/pgxpool level — not a solved
  one. The real physical-connection ceiling depends on PgBouncer's own
  `pool_size` (warehouse-infra PR #43), which is worth a fleet-wide
  follow-up (outside this PR's scope) once more of the other services'
  own Phase 3 PRs land, to check the sum across all 10 services against
  PgBouncer's real pooled capacity rather than each service reasoning
  about its own slice in isolation.
- Analytics DSNs (`cmd/inventory-projector`, `cmd/inventory-reports`)
  stay direct against Postgres, not through PgBouncer, mirroring
  PgBouncer PR #43's reasoning: low QPS, few connections each, no
  pooling benefit. If analytics throughput needs ever grow enough to
  warrant pooling those too, that is a separate decision revisiting
  PgBouncer PR #43's split, not implied by this PR.
- A read replica for the analytics/reports read path is explicitly OUT
  OF SCOPE for this PR — not evaluated, not designed, not decided.
  Revisit only once actually needed and confirmed by the person
  requesting it.
- `max_connections=100` itself is an unexamined Bitnami chart default,
  not a value anyone has deliberately sized for this fleet's real
  demand. This ADR treats it as a hard external constraint to work
  within, not something in scope to change.

## References

- order-management [ADR-0026](https://github.com/claudioed/order-management/blob/develop/docs/docs/adr/0026-horizontal-autoscaling-and-pgxpool-tuning.md) ([PR #110](https://github.com/claudioed/order-management/pull/110)) — the fleet's Phase 3 scalability reference this ADR mirrors.
- `warehouse-infra` PR #43 — PgBouncer (transaction-pooling mode) in front
  of the shared Postgres instance; re-points every service's OLTP
  `DATABASE_URL` Secret, leaves analytics DSNs direct.
