---
title: Events
sidebar_label: Events
description: The asynchronous contract — CloudEvents envelope, type convention, and every message with its data payload.
---

# Events

The asynchronous half of this service's Published Language. The authoritative
source is [`apis/asyncapi.yaml`](https://github.com/claudioed/inventory-storage/blob/main/apis/asyncapi.yaml)
(AsyncAPI 2.6.0), Spectral-linted in CI by the `api-lint` job. Everything on
this page is drawn from that document.

## Channel

| | |
| --- | --- |
| **Topic** | `warehouse.inventory.events` |
| **Protocol** | Kafka |
| **Broker** | `KAFKA_BROKERS`, default `localhost:9092` (the single platform broker deployed by `warehouse-infra`, exposed on the host at that address) |
| **Selected by** | `EVENT_PUBLISHER=kafka` (default is `log`) |
| **Envelope** | CloudEvents 1.0 structured mode, mandatory ([ADR-0024](../adr/0024-cloudevents-mandatory-envelope.md)) |
| **Direction** | Publish only on this channel. (This service separately *consumes* `facility-layout`'s `warehouse.facility.events` for a local location cache — see [Integration](/docs/ecosystem/integration#what-this-service-consumes).) |
| **Primary consumer** | `wes-work-planning`, projecting into `UsableInventoryObserved` by SKU |
| **Default content type** | `application/cloudevents+json` |

## Envelope: CloudEvents 1.0, structured mode (mandatory)

Every message on every topic this service produces or consumes is a
CloudEvents 1.0 event in **structured content mode** — this is the only
envelope ([ADR-0024](../adr/0024-cloudevents-mandatory-envelope.md)). The
Kafka message value is the JSON event; every message carries the header
`content-type: application/cloudevents+json; charset=UTF-8`; the Kafka key is
the aggregate id (ADR-0021); W3C `traceparent`/`tracestate` ride in headers.

| Attribute | Required | Value |
| --- | --- | --- |
| `specversion` | ✅ | Always `"1.0"` |
| `id` | ✅ | UUID v4, minted once per occurrence and persisted in the outbox row, so a redelivery carries the same id. `(source, id)` is the deduplication key. |
| `source` | ✅ | Always `/warehouse/inventory-storage` |
| `type` | ✅ | `com.warehouse.wms.inventory-storage.<entity>.<EventName>`, pinned per message (below) |
| `subject` | ✅ | The aggregate instance the event is about — a reservation id, SKU, stock unit id, or bin id. Never empty. |
| `time` | ✅ | RFC 3339 UTC, taken from the injected `Clock` port — when it occurred *in the domain*, not at publish |
| `datacontenttype` | ✅ | Always `application/json` |
| `dataschema` | ✅ | `urn:warehouse:inventory-storage:<events\|analytics>:<EventName>:v<N>` |

```json
{
  "specversion": "1.0",
  "id": "1f7a4c30-9b2d-4e85-a6c1-7d3f0b5e8a94",
  "source": "/warehouse/inventory-storage",
  "type": "com.warehouse.wms.inventory-storage.reservation.StockReserved",
  "subject": "res-1",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:inventory-storage:events:StockReserved:v1",
  "data": { "sku": "SKU-1", "quantity": 5, "demand_ref": "order-42" }
}
```

The same `type` is used for an occurrence on both the integration and the
analytics topic; `dataschema` names which payload shape `data` carries. A
breaking payload change ships as a new `.v2` type with a new `dataschema`
version — never by mutating an existing one.

## The `type` convention

Platform-wide, shared across the fleet's services:

```text
com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>
```

All lowercase except the final PascalCase event name. For this context:

- `<subdomain>` = `wms` — Warehouse Management System, a Core subdomain;
  "Inventory & Slotting" is WMS-tier in the reference model.
- `<bounded-context>` = `inventory-storage`.
- `<entity>` = the aggregate that raises the event: `stock`, `reservation`, or
  `bin`.

Each concrete event schema pins `type` to a single value with a `const`, so
consumers can discriminate on it safely and a typo fails validation rather than
producing a silently-unrouted message.

## Message catalog

The AsyncAPI document is the **complete domain-event catalog**. Two topics
carry it: `warehouse.inventory.events` (integration — the contract other
services build against) and `warehouse.inventory.analytics` (internal,
consumed only by this service's analytics projector). `LocationRecorded` and
`ProductClassified` are in-process only.

### Reservation entity

| Event | `type` | Integration topic `data` | Analytics topic `data` |
| --- | --- | --- | --- |
| **StockReserved** | `com.warehouse.wms.inventory-storage.reservation.StockReserved` | `sku`, `quantity`, `demand_ref` | `sku`, `reservation_id`, `quantity` |
| **ReservationRevoked** | `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` | `sku`, `quantity`, `demand_ref` | `reservation_id`, `sku` |
| ReservationExpired | `com.warehouse.wms.inventory-storage.reservation.ReservationExpired` | — | `reservation_id`, `sku` (raised on lazy read, see [Domain Events](/docs/ddd/domain-events#lazy-expiry-no-sweeper-resolved-at-the-next-read)) |
| StockPicked | `com.warehouse.wms.inventory-storage.reservation.StockPicked` | — | `sku`, `reservation_id`, `quantity` |

### Stock entity

| Event | `type` | Integration topic `data` | Analytics topic `data` |
| --- | --- | --- | --- |
| StockReceived | `com.warehouse.wms.inventory-storage.stock.StockReceived` | — | `sku`, `quantity` |
| ItemStowed | `com.warehouse.wms.inventory-storage.stock.ItemStowed` | — | `sku`, `bin_id`, `quantity` |
| ItemUnlocated | `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | — | `sku`, `bin_id`, `stock_unit_id`, `quantity` |
| LocationRecorded | `com.warehouse.wms.inventory-storage.stock.LocationRecorded` | — (in-process only) | — |

### Bin entity

| Event | `type` | Integration topic `data` | Analytics topic `data` |
| --- | --- | --- | --- |
| CycleCountCompleted | `com.warehouse.wms.inventory-storage.bin.CycleCountCompleted` | — | `bin_id`, `counted`, `system`, `discrepancy` |
| DiscrepancyDetected | `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` | — | `bin_id`, `counted`, `system` |

`ProductClassified` (entity `product`) is a domain event that is not
published to Kafka; if it ever is, its type will be
`com.warehouse.wms.inventory-storage.product.ProductClassified`.

## The two integration events in full

### StockReserved

Raised by `ReserveStock` when a reservation is successfully created against
*usable* inventory. The binding is revocable and carries a timeout, so a
physical failure downstream never strands the demand.

The reservation id — which the domain event also carries — is surfaced as the
CloudEvents `subject`, not inside `data`.

```json
{
  "specversion": "1.0",
  "id": "1f7a4c30-9b2d-4e85-a6c1-7d3f0b5e8a94",
  "source": "/warehouse/inventory-storage",
  "type": "com.warehouse.wms.inventory-storage.reservation.StockReserved",
  "subject": "res-1",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:inventory-storage:events:StockReserved:v1",
  "data": {
    "sku": "SKU-1",
    "quantity": 5,
    "demand_ref": "order-42"
  }
}
```

**Downstream effect:** `wes-work-planning` *decrements* its observed usable
count for that SKU.

### ReservationRevoked

Raised by `RevokeReservation`. Revocation is the mechanism that keeps a
physical failure — a blocked pod, a lost tote, a chute jam, a short pick — from
stranding an order.

The domain event carries only the reservation id, so the adapter **enriches**
it by looking the reservation up through `ports.ReservationRepo` and emitting
the same `sku` / `quantity` / `demand_ref` shape as `StockReserved`. If the
lookup finds nothing the publish fails rather than emitting a partial payload.

```json
{
  "specversion": "1.0",
  "id": "4b9e2f61-7c3a-4d08-85e2-1a6f9c0d3b72",
  "source": "/warehouse/inventory-storage",
  "type": "com.warehouse.wms.inventory-storage.reservation.ReservationRevoked",
  "subject": "res-1",
  "time": "2026-08-21T22:10:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:inventory-storage:events:ReservationRevoked:v1",
  "data": {
    "sku": "SKU-1",
    "quantity": 5,
    "demand_ref": "order-42"
  }
}
```

**Downstream effect:** `wes-work-planning` *increments* its observed usable
count for that SKU back.

The symmetry is the whole point — the downstream read model is built on the
assumption that reservations come back.

## Notes for consumers

- **Dispatch on the full `type` string** and ignore unknown types. Never match
  on a suffix or a short name.
- **Reject anything that is not a valid CloudEvent** (dead-letter or WARN and
  skip) — never fall back to parsing a legacy flat envelope.
- **Deduplicate on `(source, id)`.** Kafka delivery is at-least-once.
  `wes-work-planning` implements this with a `processed_events` table keyed by
  event id, precisely so a redelivery does not double-decrement its usable
  count.
- **Ordering is per reservation only.** Messages are keyed by reservation id
  and routed with a `Hash` balancer (ADR-0021), so events for one reservation
  arrive in order; there is no ordering across reservations or SKUs.
- **The authoritative answer is always the REST read.** The event stream is a
  convenience projection; when correctness matters, ask this service.
