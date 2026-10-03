---
id: 0011-analytical-data-product
title: 11. Per-service analytical data product (report) via a separate analytics topic
sidebar_label: 11. Analytical data product
sidebar_position: 11
description: An analytical read model (the "Inventory Flow & Accuracy report") built from this service's own domain events on a dedicated warehouse.inventory.analytics topic, projected into a separate analytical database and served by a read-only reports binary over REST and MCP — a lightweight data mesh with no central data platform.
---

# 11. Per-service analytical data product (the "report")

## Status

**Accepted; envelope superseded by
[ADR-0024](./0024-cloudevents-mandatory-envelope.md).** The "Envelope v1"
wrapper (`event_id` / `event_type` / `occurred_at` / `source` /
`schema_version` / `data`) described below is retired: analytics events are
CloudEvents 1.0 with `dataschema`
`urn:warehouse:inventory-storage:analytics:<EventName>:v1` replacing
`schema_version`, the projector dispatches on the full `type` and dedupes on
the CloudEvents `id`. The rest of this decision (separate topic, projector,
read model) stands.

## Context

The warehouse-systems estate needs a per-service **report** that supports
analytics while each service stays the **OLTP** system of record for its own
bounded context. The requirement, stated deliberately simply: *follow data-mesh
principles, but without standing up a whole data platform.* No central
warehouse, no lake, no shared ETL team.

Inventory & Storage already has everything the analytical side needs as a
substrate:

- Past-tense **domain events** (`StockReceived`, `ItemStowed`,
  `LocationRecorded`, `StockReserved`, `ReservationExpired`,
  `ReservationRevoked`, `StockPicked`, `ItemUnlocated`, `CycleCountCompleted`,
  `DiscrepancyDetected`) raised by the aggregates.
- A Kafka **integration** path (`warehouse.inventory.events`) already carrying
  `StockReserved` and `ReservationRevoked` to downstream consumers, with the
  CloudEvents-like envelope and OTel trace propagation established in
  [ADR-0004](./0004-kafka-integration-events.md).
- The dual inbound-adapter pattern (HTTP + MCP) from
  [ADR-0008](./0008-mcp-inbound-adapter.md).

So the event backbone exists; what is missing is the **analytical read side**.
The forces shaping the decision:

- **The integration contract must not become coupled to reporting.** The report
  needs many more event types than the integration topic exposes, and they
  change on a different cadence. Widening `warehouse.inventory.events` with
  analytics-only event types would risk surprising existing consumers and
  entangle two contracts that should evolve separately.
- **Analytics must never contend with OLTP.** A report query load, a long
  aggregation, or a projection rebuild must not touch the transactional
  database that serves `StowStock` and `ReserveStock`.
- **The service still owns its data as a product.** Data-mesh domain ownership
  means the read side lives in this repo, owned by the same team, with a
  contract, an owner, and a freshness SLA — not shipped off to a central team.
- **No new central platform.** Reuse what the estate already runs: Kafka,
  Postgres, chi, the MCP SDK, the Helm chart.

## Decision

**Inventory & Storage owns an analytical data product built solely from its own
domain events, delivered on a dedicated analytics topic, projected into a
separate analytical database, and served read-only over REST and MCP. Three
processes; one writer.**

### 1. Separate analytics topic

A new outbound adapter publishes the report-input event set to
**`warehouse.inventory.analytics`**, using the shared **Envelope v1** wrapper
(*superseded by ADR-0024: now CloudEvents 1.0, `schema_version` replaced by
`dataschema`*)
(`event_id`, `event_type`, `occurred_at` RFC3339 UTC, `source`,
`schema_version`, `data` snake_case) with a per-`event_type` `data` payload. The
event key is the aggregate id (SKU or BinId as appropriate). The existing
integration publisher and `warehouse.inventory.events` are **left untouched**,
so no existing consumer is affected. In `cmd/inventory` the analytics publisher
is wired alongside the integration publisher via a fan-out/multi-publisher,
selected by `EVENT_PUBLISHER=kafka`. Analytics consumers switch on `event_type`,
dedupe on `event_id`, and ignore unknown types.

Reservation-lifecycle events (`ReservationExpired`, `ReservationRevoked`) carry
only a reservation id, so the analytics publisher enriches them with the
reservation's SKU via a `ReservationRepo` lookup — the same repo-lookup
enrichment the integration publisher already uses for `ReservationRevoked` —
because the report is keyed by SKU. `LocationRecorded` is published-adjacent but
does not move the report; the projector acknowledges it without projecting.

### 2. Separate analytical database

Projections land in a **separate analytical database** with its own credentials
(`ANALYTICS_DATABASE_URL`), its own golang-migrate migration set
(`migrations/analytics/`), and a **read-only role** for the reader. Baseline is
a dedicated `*_analytics` database in the existing Postgres release; the
`ANALYTICS_DATABASE_URL` seam allows promotion to a physically separate instance
later without code changes. The OLTP `DATABASE_URL` database is never opened by
the analytical side.

### 3. Three processes, one writer

- **`cmd/inventory`** — the OLTP binary. Unchanged, except its composition root
  additionally publishes domain events to the analytics topic when
  `EVENT_PUBLISHER=kafka`.
- **`cmd/inventory-projector`** — the analytics **writer**. Consumes
  `warehouse.inventory.analytics` (consumer group `inventory-analytics`,
  `StartOffset = FirstOffset` so a fresh group replays the full history),
  applies idempotent projections (dedupe on `event_id`), and is the **only**
  writer of the analytical database. Runs the analytical migrations on start.
- **`cmd/inventory-reports`** — the **read-only reader**. Opens the analytical
  database with the read-only role and serves `GET /reports/flow-accuracy` and
  `GET /reports/flow-accuracy/freshness`. Never writes, never migrates.

### 4. Served over REST and MCP

The reports binary serves the REST report resource. A curated, read-only MCP
tool (`get_inventory_flow_accuracy_report`) — following the intent-level tool
discipline of [ADR-0008](./0008-mcp-inbound-adapter.md) — calls the reports REST
rather than opening the analytical database itself, so no process touches a
datastore it does not own.

### 5. The report

An **Inventory Flow & Accuracy** read model, keyed per SKU × bin × hour bucket
(the unused dimension is empty for a given event): received quantity, stowed
count, picked quantity, reservations created/expired/revoked, cycle-count
completions, discrepancies detected, and unlocated items per interval. Quantity
is summed where the event carries a `Quantity` value object; counts otherwise.
It is a **projection** from events (consistent with the existing "read models
are projections, not aggregate state" rule), eventually consistent to a
freshness SLA (p95 event-to-report lag < 30s), not real-time.

The analytical read model lives in a new `internal/analytics/report` region
that depends on nothing; the consumer and store adapters depend on it. The OLTP
**domain and application layers are not modified**, and `arch-test` enforces
that they do not import the analytics store.

## Consequences

### Easier

- **The integration contract is untouched**, so widening what analytics
  consumes never risks an integration consumer. Analytics retention is tuned
  independently of the integration topic.
- **Analytics cannot contend with OLTP** — separate database, separate
  connection, read-only reader role. A runaway report query cannot touch
  transactional throughput.
- **The report is rebuilt purely from events** — no dual-write from OLTP, so the
  transactional write path gains no new failure mode. The read model can be
  rebuilt from scratch by replaying the topic.
- **No central platform.** Everything reuses the estate's existing Kafka,
  Postgres, chi, MCP SDK and Helm.
- **Least privilege by construction.** The read-only DB role and the reader's
  `default_transaction_read_only=on` pool make "a report can never corrupt the
  analytical store" a hard guarantee, not a convention.

### Harder

- **One more topic, two more binaries, and a second database** to operate.
  Mitigated by reusing the OLTP Postgres/consumer/publisher scaffolding.
- **Eventual consistency.** The report lags the OLTP truth by the freshness SLA;
  it is not a real-time view. This is the correct data-mesh tradeoff but must be
  communicated to report consumers.
- **The analytics publisher is a second producer path** for the same domain
  events. It re-serializes them under Envelope v1 for the analytics topic; the
  event set it publishes must be kept in step with the report's inputs.
- **First deploy has an empty report** until events flow; historical backfill
  requires replaying `warehouse.inventory.analytics` from earliest into a fresh
  projector, so Kafka retention must cover the desired backfill window.

## References

- [Inventory Flow & Accuracy report contract](../analytics/inventory-flow-accuracy-report.md)
- [ADR-0004 — Kafka integration events](./0004-kafka-integration-events.md)
- [ADR-0008 — MCP inbound adapter](./0008-mcp-inbound-adapter.md)
