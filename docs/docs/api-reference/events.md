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
| **Direction** | Publish only on this channel. (This service separately *consumes* `facility-layout`'s `warehouse.facility.events` for a local location cache — see [Integration](/docs/ecosystem/integration#what-this-service-consumes).) |
| **Primary consumer** | `wes-work-planning`, projecting into `UsableInventoryObserved` by SKU |
| **Default content type** | `application/cloudevents+json` |

## Envelope

### The target: CloudEvents 1.0, structured mode

Context attributes carry routing and identity; the business payload lives
entirely under `data`.

| Attribute | Required | Value |
| --- | --- | --- |
| `specversion` | ✅ | Always `"1.0"` |
| `id` | ✅ | UUID v4 for this occurrence. `(source, id)` is the deduplication key. |
| `source` | ✅ | Always `/warehouse/inventory-storage` |
| `type` | ✅ | Reverse-DNS event type, pinned per message (below) |
| `subject` | | The aggregate instance the event is about — a reservation id, a stock unit id, or a bin id |
| `time` | | RFC 3339, taken from the injected `Clock` port — the time it occurred *in the domain*, not at publish |
| `datacontenttype` | | Always `application/json` |

```json
{
  "specversion": "1.0",
  "id": "1f7a4c30-9b2d-4e85-a6c1-7d3f0b5e8a94",
  "source": "/warehouse/inventory-storage",
  "type": "com.warehouse.wms.inventory-storage.reservation.StockReserved",
  "subject": "res-1",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "data": { "sku": "SKU-1", "quantity": 5, "demand_ref": "order-42" }
}
```

### What ships today: the legacy flat envelope

:::caution Two envelopes, honestly documented
`internal/adapters/outbound/kafka/publisher.go` currently emits the **legacy
flat warehouse envelope**, not the CloudEvents attributes above. The `data`
payloads for `StockReserved` and `ReservationRevoked` match the AsyncAPI
document field-for-field; the CloudEvents context attributes describe the
target the platform is standardising on. `apis/asyncapi.yaml` says the same
thing in its own `info.description` — this is a documented gap, not a
surprise.
:::

```json
{
  "event_id": "1f7a4c30-9b2d-4e85-a6c1-7d3f0b5e8a94",
  "event_type": "StockReserved",
  "occurred_at": "2026-08-21T22:00:00Z",
  "source": "inventory-storage",
  "data": { "sku": "SKU-1", "quantity": 5, "demand_ref": "order-42" }
}
```

Mapping between them: `event_id` → `id`, `event_type` → the last segment of
`type`, `occurred_at` → `time`, `source` (`inventory-storage`) →
`/warehouse/inventory-storage`. The reservation id, carried as `subject` in
CloudEvents, is not present in the flat envelope.

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

Each concrete event schema pins `type` to a single value with an `enum`, so
consumers can discriminate on it safely and a typo fails validation rather than
producing a silently-unrouted message.

## Message catalog

The AsyncAPI document is the **complete domain-event catalog**. Only the two
rows marked **Published** actually reach the broker; the rest are raised
in-process and delivered to whichever `ports.EventPublisher` is configured (the
log publisher by default), then dropped by the Kafka adapter's `default`
branch.

:::warning Do not build a consumer against a catalog-only message
Every message below that is not marked **Published** is documented for
completeness. It is not on the wire. `apis/asyncapi.yaml` states this per
message in its own `description`.
:::

### Reservation entity

| Event | `type` | `data` | Status |
| --- | --- | --- | --- |
| **StockReserved** | `com.warehouse.wms.inventory-storage.reservation.StockReserved` | `sku` (string, req), `quantity` (int, req), `demand_ref` (string, req) | ✅ **Published** |
| **ReservationRevoked** | `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` | `sku` (string, req), `quantity` (int, req), `demand_ref` (string, req) | ✅ **Published** |
| ReservationExpired | `com.warehouse.wms.inventory-storage.reservation.ReservationExpired` | `reservation_id` (string, req) | Catalog only for the integration topic — genuinely raised on lazy read and delivered to the analytics topic, see [Domain Events](/docs/ddd/domain-events#lazy-expiry-no-sweeper-resolved-at-the-next-read) |
| StockPicked | `com.warehouse.wms.inventory-storage.reservation.StockPicked` | `reservation_id`, `sku`, `quantity` | Catalog only |

### Stock entity

| Event | `type` | `data` | Status |
| --- | --- | --- | --- |
| StockReceived | `com.warehouse.wms.inventory-storage.stock.StockReceived` | `sku`, `quantity` (≥1) | Catalog only |
| ItemStowed | `com.warehouse.wms.inventory-storage.stock.ItemStowed` | `sku`, `bin_id`, `quantity` | Catalog only |
| LocationRecorded | `com.warehouse.wms.inventory-storage.stock.LocationRecorded` | `stock_unit_id`, `bin_id` | Catalog only |
| ItemUnlocated | `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | `stock_unit_id`, `sku`, `bin_id`, `quantity` | Catalog only |

### Bin entity

| Event | `type` | `data` | Status |
| --- | --- | --- | --- |
| CycleCountCompleted | `com.warehouse.wms.inventory-storage.bin.CycleCountCompleted` | `bin_id`, `counted_qty`, `system_qty`, `discrepancy` (bool) | Catalog only |
| DiscrepancyDetected | `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` | `bin_id`, `counted_qty`, `system_qty` | Catalog only |

## The two published events in full

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

- **Tolerate unknown `type` values.** The catalog grows as more of it is wired
  to the outbound adapter; a consumer that fails closed on an unrecognised type
  will break the first time that happens.
- **Deduplicate on `(source, id)`.** Kafka delivery is at-least-once.
  `wes-work-planning` implements this with a `processed_events` table keyed by
  event id, precisely so a redelivery does not double-decrement its usable
  count.
- **Do not assume ordering across SKUs.** The publisher uses a `LeastBytes`
  balancer with no partition key, so ordering is not guaranteed even per SKU.
  The downstream projection is an increment/decrement counter, which tolerates
  reordering of independent events but is not a substitute for the
  authoritative `GET /inventory/{sku}/usable`.
- **The authoritative answer is always the REST read.** The event stream is a
  convenience projection; when correctness matters, ask this service.
