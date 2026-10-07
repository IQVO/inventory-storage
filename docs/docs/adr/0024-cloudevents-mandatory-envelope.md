---
id: 0024-cloudevents-mandatory-envelope
slug: /adr/0024-cloudevents-mandatory-envelope
title: "24. CloudEvents 1.0 as the mandatory event envelope"
sidebar_label: "24. CloudEvents mandatory envelope"
sidebar_position: 24
description: "ADR 0024 — every Kafka message inventory-storage produces or consumes (integration warehouse.inventory.events, analytics warehouse.inventory.analytics, and facility-layout's warehouse.facility.events) is a CloudEvents 1.0 structured-mode event built with the official sdk-go event package. The legacy flat envelope and the analytics Envelope v1 (schema_version) are removed with no coexistence. Supersedes the envelope parts of ADR 0004 and ADR 0011."
---

# 24. CloudEvents 1.0 as the mandatory event envelope

## Status

**Accepted.** Fleet-wide standard (2026-09-30), applied to this service in
the same change that introduces this record. Supersedes
[ADR 0004](./0004-kafka-integration-events.md)'s flat envelope and the
"Envelope v1" (`schema_version`) of
[ADR 0011](./0011-analytical-data-product.md). Every other decision in those
records (Kafka as transport, a separate analytics topic, the projector) still
stands.

## Context

Until now this service emitted a **flat envelope**
(`event_id` / `event_type` / `occurred_at` / `source` / `data`, with a bare
`event_type` such as `StockReserved`) on `warehouse.inventory.events`, and a
near-identical **Envelope v1** with an extra `schema_version` on
`warehouse.inventory.analytics`. `apis/asyncapi.yaml` meanwhile already
documented the CloudEvents attributes as a *target*, with a caveat that the
wire format was behind. The facility cache consumer matched facility-layout
events on the trailing name of a fully-qualified type and tolerated several
shapes.

Two descriptions of the same contract, a suffix match, and a per-service
`schema_version` field are exactly the drift a shared standard exists to
prevent. The fleet decided to make CloudEvents 1.0 mandatory everywhere at
once, with no dual-write or dual-read period: all services cut over together
and the topics are recreated (see warehouse-infra
`docs/cloudevents-cutover.md`).

## Decision

**Every Kafka message this service writes or reads is a CloudEvents 1.0
event in structured content mode.** No flat envelope, no Envelope v1, no
dual-write, no dual-read, no envelope toggle.

### Encoding

- Kafka protocol binding, **structured mode**: the message value is the JSON
  event format; every produced message carries the header
  `content-type: application/cloudevents+json; charset=UTF-8`.
- Kafka key is unchanged (the aggregate id each publisher already used —
  ADR 0021) and the writer still uses `kafkago.Hash{}`.
- W3C trace context stays in Kafka headers (`traceparent` / `tracestate`),
  not in CloudEvents extensions.
- Events are built, validated and (un)marshalled with
  `github.com/cloudevents/sdk-go/v2/event` (v2.16.2). Transport stays
  `segmentio/kafka-go`; the SDK's protocol/client packages are not used.
- One helper package owns the envelope:
  `internal/adapters/kafka/cloudevents` (`New`, `Decode`,
  `ContentTypeHeader`, `Type`, `DataSchema`). Nothing else hand-builds one.

### Context attributes (all required)

| attribute | value |
| --- | --- |
| `specversion` | `1.0` |
| `id` | UUID v4, minted once in `Encode` and persisted inside the outbox row's value, so every relay redelivery carries the same id. `(source, id)` is the consumer idempotency key. |
| `source` | `/warehouse/inventory-storage` (both topics) |
| `type` | `com.warehouse.wms.inventory-storage.<entity>.<EventName>` |
| `subject` | the aggregate instance id (reservation id, SKU, stock unit id or bin id — see below) |
| `time` | the domain event's occurred-at, UTC, RFC 3339 |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:inventory-storage:<events\|analytics>:<EventName>:v<N>` |

`data` is byte-for-byte the payload each topic carried before; only the
wrapper changed. The analytics `schema_version` field is **removed** —
`dataschema` replaces it.

The same `type` names an occurrence on both topics; `dataschema` names the
payload shape (`...:events:...` vs `...:analytics:...`). A breaking payload
change requires a new `dataschema` version **and** a new `.v2` type,
published as a new event, never a mutation of the old one.

### This service's types

Entity segments are the ones already catalogued in `apis/asyncapi.yaml`:
`stock` (StockUnit), `reservation`, `bin`.

| topic | `type` | `subject` |
| --- | --- | --- |
| `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.reservation.StockReserved` | reservation id |
| `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` | reservation id |
| `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated` | reservation id |
| `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected` | transfer line id |
| `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged` | transfer line id |
| `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.stock.TransferStockStowed` | transfer line id |
| `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.product.ProductClassified` (backfill command only, ADR 0034) | SKU |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.stock.StockReceived` | SKU |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.stock.ItemStowed` | SKU |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | stock unit id |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.reservation.StockReserved` | reservation id |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.reservation.StockPicked` | reservation id |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.reservation.ReservationExpired` | reservation id |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` | reservation id |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.bin.CycleCountCompleted` | bin id |
| `warehouse.inventory.analytics` | `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` | bin id |
| _(none: in-process only, never published)_ | `com.warehouse.wms.inventory-storage.stock.LocationRecorded` | _n/a_ |

`ProductClassified` (`internal/domain/product`), entity segment `product`
(`com.warehouse.wms.inventory-storage.product.ProductClassified`), was
published on both topics by ADR 0031. Since
[ADR 0034](./0034-product-master-owns-classification.md) it is emitted only by
the one-shot `republish-product-classifications` backfill, on
`warehouse.inventory.events` (subject = SKU), and is retired at
product-master ADR 0003 stage E.

### Consumers

1. Decode with `cloudevents.Decode` (SDK unmarshal + `specversion` check +
   `Validate()`). Anything else — the retired flat envelope, bad JSON, a
   missing required attribute — is a deterministic poison message:
   - **facility cache** (`facilitycache.Consumer`): dead-lettered to
     `<topic>.dlq` (its existing ADR 0020 DLQ path) and `observe()` still
     runs, so the FirstOffset replay **readiness gate is never stalled** by
     an invalid message;
   - **analytics projector** (`AnalyticsConsumer`, no DLQ): WARN log with
     topic/partition/offset and commit past it.

   Never crash, never block the partition, never fall back to a flat parse.
2. Dispatch on the **full** `type`, byte-identical to the publisher. Unknown
   types are ignored. This service consumes:
   - `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered`
   - `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned`
   - `com.warehouse.wms.facility-layout.zone.ZoneRegistered`
   - `com.warehouse.wms.product-master.product.ProductClassified` (topic
     `warehouse.product-master.events`, source `/warehouse/product-master`;
     the local classification copy, ADR 0034 — every other product-master
     type on that topic is ignored)
   - its own nine analytics types above (projector).
3. Dedupe on the CloudEvents `id` (the projector's
   `analytics_processed_events.event_id` column is now populated from `id`).
   The facility cache's updates are idempotent upserts/deletes keyed by
   aggregate, so a redelivery re-applies harmlessly.
4. `time`/`subject` come from attributes; the payload from `DataAs`.

### Fleet cross-service contract

These strings must be byte-identical on both sides:

```text
com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered       -> inventory-storage
com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned   -> inventory-storage
com.warehouse.wms.facility-layout.zone.ZoneRegistered                       -> inventory-storage
com.warehouse.wms.product-master.product.ProductClassified                  -> inventory-storage (ADR 0034)
com.warehouse.wms.inventory-storage.product.ProductClassified               -> product-master (legacy importer, backfill only)
com.warehouse.wms.inventory-storage.reservation.StockReserved               -> wes-work-planning
com.warehouse.wms.inventory-storage.reservation.ReservationRevoked          -> wes-work-planning
com.warehouse.wes.order-management.order.OrderAllocated                     -> wes-work-planning
com.warehouse.wes.order-management.order.OrderPartiallyAllocated            -> wes-work-planning
com.warehouse.wes.order-management.order.OrderRepromised                    -> (published contract)
com.warehouse.wes.process-path-management.processpath.ProcessPathCreated    -> fulfillment-execution, wes-work-planning, workforce-management, order-management
com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated    -> same four
com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated-> same four
com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged    -> order-management
com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded     -> workforce-management (and labor-performance itself)
com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted         -> wes-work-planning
com.warehouse.wes.fulfillment-execution.task.TaskCompleted                  -> wes-work-planning, labor-performance
com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed                  -> order-management
com.warehouse.wes.fulfillment-execution.package.PackageManifested           -> order-management
com.warehouse.wes.work-planning.workunit.WorkReleased                       -> fulfillment-execution
com.warehouse.wes.work-planning.workpool.PathCapacityChanged                -> order-management
```

## Consequences

**Positive**

- One envelope, one parser, one documented contract; `apis/asyncapi.yaml`
  now describes the actual wire format with no caveat.
- Full-type dispatch removes the suffix-match ambiguity in the facility
  cache.
- `dataschema` versions the payload per stream without a bespoke field.

**Negative / costs**

- **Breaking wire change, no coexistence.** This service must deploy
  together with the fleet cutover. Before deploying, drain the outbox (rows
  written before this change are pre-encoded in the flat shape), then delete
  and recreate `warehouse.inventory.events`, `warehouse.inventory.analytics`
  and every other `warehouse.*` topic so no flat message remains for a
  FirstOffset replay. A flat message that does survive is dead-lettered or
  skipped, never applied.
- Messages are slightly larger (two more attributes plus the header).

## Verification

- `internal/adapters/kafka/cloudevents` unit tests (builder, decode,
  legacy rejection).
- `internal/adapters/outbound/kafka/golden_test.go`: exact-JSON golden per
  published type on both topics, plus the `content-type` header.
- Legacy-flat-message rejection tests for every consumer (facility cache
  unit + testcontainers integration, analytics projector unit).
- `publisher_integration_test.go` reads the CloudEvents back from a real
  (testcontainers) broker.
