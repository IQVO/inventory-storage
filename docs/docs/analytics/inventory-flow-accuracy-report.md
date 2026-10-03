---
id: inventory-flow-accuracy-report
title: Inventory Flow & Accuracy Report
sidebar_label: Flow & Accuracy report
description: The inventory-storage analytical data product — an Inventory Flow & Accuracy read model built from the service's own domain events, served read-only over REST and MCP. Contract, grain, inputs, freshness SLA, and versioning.
---

# Inventory Flow & Accuracy Report

The analytical **data product** owned by Inventory & Storage. It is built
entirely from this service's own domain events (never another service's
database) and served read-only. See
[ADR-0011](../adr/0011-analytical-data-product.md) for the decision.

## Name & owner

- **Report:** Inventory Flow & Accuracy.
- **Owner:** the Inventory & Storage service/team (the same team that owns the
  OLTP write model).

## Grain

One row per **(SKU × bin × hour bucket)**, where `hourBucket` is the UTC hour
the row aggregates. Flow events (received, picked, reserved) carry a SKU but no
bin; accuracy events (cycle count, discrepancy) carry a bin but no SKU; stow and
unlocate carry both. The unused dimension is the empty string for a given event,
so a row is keyed by whichever dimension(s) its events actually carry. Metrics
per row:

| Metric | Meaning | Value |
|---|---|---|
| `receivedQuantity` | Summed quantity of `StockReceived` in the bucket. | quantity |
| `stowedCount` | Count of `ItemStowed` in the bucket. | count |
| `pickedQuantity` | Summed quantity of `StockPicked` in the bucket. | quantity |
| `reservationsCreated` | Count of `StockReserved` in the bucket. | count |
| `reservationsExpired` | Count of `ReservationExpired` in the bucket. | count |
| `reservationsRevoked` | Count of `ReservationRevoked` in the bucket. | count |
| `cycleCountsCompleted` | Count of `CycleCountCompleted` in the bucket. | count |
| `discrepanciesDetected` | Count of `DiscrepancyDetected` in the bucket. | count |
| `unlocatedCount` | Count of `ItemUnlocated` in the bucket. | count |

Quantity metrics use the summed `Quantity` value object the event carries;
everything else is an event count.

## Inputs (analytics topic events)

Consumed from **`warehouse.inventory.analytics`** (the dedicated analytics
topic, separate from the integration topic). Every message is a CloudEvents
1.0 event ([ADR-0024](../adr/0024-cloudevents-mandatory-envelope.md)) with
`dataschema` `urn:warehouse:inventory-storage:analytics:<EventName>:v1`. The
projector dispatches on the full `type`, dedupes on the CloudEvents `id`,
takes the bucket timestamp from `time`, and logs-and-skips (WARN) anything
that is not a valid CloudEvent.

| `type` (`com.warehouse.wms.inventory-storage.` + …) | Contributes | Keyed by |
|---|---|---|
| `stock.StockReceived` | `receivedQuantity` | SKU |
| `stock.ItemStowed` | `stowedCount` | SKU + bin |
| `reservation.StockPicked` | `pickedQuantity` | SKU |
| `reservation.StockReserved` | `reservationsCreated` | SKU |
| `reservation.ReservationExpired` | `reservationsExpired` | SKU (enriched) |
| `reservation.ReservationRevoked` | `reservationsRevoked` | SKU (enriched) |
| `bin.CycleCountCompleted` | `cycleCountsCompleted` | bin |
| `bin.DiscrepancyDetected` | `discrepanciesDetected` | bin |
| `stock.ItemUnlocated` | `unlocatedCount` | SKU + bin |

`sku` is enriched onto reservation-lifecycle events (`ReservationExpired`,
`ReservationRevoked`) by the publisher via a `ReservationRepo` lookup, since
those domain events carry only a reservation id. `LocationRecorded` is published
to the topic but does not move this report; the projector acknowledges it
without projecting.

## Interface

### REST (served by `cmd/inventory-reports`, read-only)

```
GET /reports/flow-accuracy?from=<RFC3339>&to=<RFC3339>&sku=&binId=&granularity=hour
GET /reports/flow-accuracy/freshness
GET /healthz
```

- `from`, `to` — **required**, RFC3339, `[from, to)` compared against `hourBucket`.
- `sku`, `binId` — optional exact-match filters.
- `granularity` — optional, defaults to `hour`.

Response (`200`):

```json
{
  "rows": [
    {
      "sku": "SKU-1",
      "binId": "",
      "hourBucket": "2026-08-26T14:00:00Z",
      "receivedQuantity": 42,
      "stowedCount": 0,
      "pickedQuantity": 7,
      "reservationsCreated": 3,
      "reservationsExpired": 1,
      "reservationsRevoked": 0,
      "cycleCountsCompleted": 0,
      "discrepanciesDetected": 0,
      "unlocatedCount": 0
    }
  ]
}
```

Freshness (`200`):

```json
{ "lagSeconds": 4.2 }
```

Errors use RFC 7807 `application/problem+json`, consistent with the OLTP API
([ADR-0005](../adr/0005-rfc-7807-problem-details.md)).

### MCP (curated, read-only)

Tool **`get_inventory_flow_accuracy_report`** — same filters as the REST
endpoint; it calls the reports REST rather than opening the analytical database.
Exposed by the existing `cmd/mcp` server (Streamable HTTP) when
`REPORTS_BASE_URL` is set, consistent with
[ADR-0008](../adr/0008-mcp-inbound-adapter.md).

## Freshness SLA

- **Definition:** `lagSeconds` = now − age of the most recently applied event.
- **Target:** p95 event-to-report lag **< 30s** under normal load.
- **Exposed:** `GET /reports/flow-accuracy/freshness`.
- On an empty read model the lag is `0` (the `max(occurred_at)` NULL row is read
  as "no events yet", not an error).
- Breaching the SLA is an operational signal (projector lag / consumer down), not
  a correctness bug — the report catches up when the projector does.

## Versioning

- Additive fields (new optional row metric, new query filter) are non-breaking.
- A breaking change to a row's shape or meaning is a new endpoint/tool version.
- The analytics event contract versions independently via the CloudEvents
  `dataschema` (`urn:warehouse:inventory-storage:analytics:<EventName>:v<N>`);
  a breaking payload change is a new `.v2` type plus a new `dataschema`.

## Runbook notes

- **Two processes, one writer.** `cmd/inventory-projector` is the only writer of
  the analytical DB; `cmd/inventory-reports` connects read-only. The OLTP
  `cmd/inventory` never opens the analytical DB.
- **Empty on first deploy.** The report is empty until events flow. To backfill
  history, replay `warehouse.inventory.analytics` from earliest into a fresh
  projector; Kafka retention must cover the desired window.
- **Eventual consistency.** The report is a projection, not a real-time view; it
  meets the freshness SLA, not transactional consistency.
