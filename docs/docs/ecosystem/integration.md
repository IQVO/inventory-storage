---
id: integration
title: Integration Guide
sidebar_label: Integration Guide
description: How to integrate with inventory-storage — topic, envelope, payloads, configuration, and how to verify it end to end.
---

# Integration Guide

Everything a downstream team needs to consume this service, and everything an
operator needs to run it against the shared broker.

## Every edge at a glance

The relationship patterns, code paths on both sides and live/wired status are on
the [context map](./context-map.md); this table is the operational view: how
each edge behaves when the other side fails, as implemented here.

### Upstream (this service consumes or calls)

| Upstream | Protocol and contract | Switch here | When it fails |
| --- | --- | --- | --- |
| facility-layout | Kafka `warehouse.facility.events` (`ZoneRegistered`, `LocationSlotRegistered`, `LocationSlotHandlingUnitDeclared`, `LocationSlotDecommissioned`) → in-process location cache | `LOCATION_LOOKUP_MODE=kafka` | Startup blocks up to 60 s for the replay and fails if it does not finish; malformed facts go to `warehouse.facility.events.dlq`; an empty cache answers `Known=false` (stow allowed, fail-open). |
| facility-layout | REST `GET /locations/{locationCode}/classification` | `LOCATION_LOOKUP_MODE=http` + `FACILITY_LAYOUT_BASE_URL` | 3 attempts per call inside a circuit breaker; an open breaker falls back to permissive (fail-open); a plain error blocks only hazmat / temperature-sensitive SKUs (`409 location-classification-unavailable`). |
| product-master | Kafka `warehouse.product-master.events`, `ProductClassified` → `product_classifications` | `PRODUCT_MASTER_CONSUMER_GROUP` (mandatory) | Transient errors retry the same message without limit; poison is logged and skipped. While the consumer is behind, stows use the last local copy (or none). |
| network-inventory-planning | Kafka `warehouse.network-inventory-planning.events`, `TransferAllocationRequested` (command) | `TRANSFER_ALLOCATION_CONSUMER_GROUP` (mandatory) | Same retry rule; replies (`TransferStockAllocated` / `...Rejected`) are only sent once the command is handled. |
| inbound-receiving | Kafka `warehouse.inbound-receiving.events`, `ReceiptLineReceived` → `ReceiveStock` | `INBOUND_RECEIPT_CONSUMER_GROUP` (mandatory) | Same retry rule; dedupe through `processed_events`. |
| fulfillment-execution | Kafka `warehouse.fulfillment.events`, `TaskCompleted` (PICK, `order_ref`, optional `line_no` → confirm picks; REPLENISH with `request_ref` → replenishment relocation, [ADR 0041](/docs/adr/0041)) | `TASK_COMPLETED_CONSUMER_GROUP` (mandatory) | 5 attempts, then `warehouse.fulfillment.events.dlq`; poison straight to the DLQ. A consumer that dies ends the process non-zero (ADR 0042). |

### Downstream (others consume or call this service)

| Downstream | Protocol and contract | Failure behaviour on this side |
| --- | --- | --- |
| wes-work-planning | Kafka `warehouse.inventory.events`: `StockReserved`, `ReservationRevoked` | Outbox: a broker outage delays events (relay retries the oldest row) but never loses or reorders them per key. |
| network-inventory-planning | Kafka `warehouse.inventory.events`: `TransferStockAllocated`, `TransferStockAllocationRejected`, `TransferReceiptStaged`, `TransferStockStowed` | Same outbox guarantee. |
| slotting-optimization | Kafka `warehouse.inventory.events`: `StockRelocated` ([ADR 0039](/docs/adr/0039)) | Same outbox guarantee. |
| pick-face-replenishment | Kafka `warehouse.inventory.events`: `BinStockLevelChanged` ([ADR 0040](/docs/adr/0040)), `StockRelocated` | Same outbox guarantee. |
| exception-management | Kafka `warehouse.inventory.events`: `bin.DiscrepancyDetected`, `stock.ItemUnlocated` (promoted [ADR 0043](/docs/adr/0043)) | Same outbox guarantee. |
| returns-processing | Kafka `warehouse.inventory.events`: `stock.ItemStowed` (promoted [ADR 0043](/docs/adr/0043)) | Same outbox guarantee. |
| order-management | REST `POST /reservations` (`Idempotency-Key`), `DELETE /reservations/{id}` | Synchronous: `409 insufficient-usable` / `concurrent-modification`, `422 idempotency-key-reused`. A retried POST with the same key and body replays the stored response. |
| network-fulfillment | REST `GET /inventory/{sku}/usable` | Read-only; no side effects. |
| warehouse-ops-agent | REST `GET /reservations?demandRef=`; reports REST `GET /reports/flow-accuracy`, `/reports/flow-accuracy/freshness`; MCP `check_availability`, `get_bin_occupancy` ([MCP tools](../mcp/tools.md)) | Read-only. MCP sessions are per process; reports reflect the projector's lag. |
| warehouse-console | the inventory micro-frontend (`web/`) calls this REST API through Kong; CORS allows `CORS_ALLOWED_ORIGINS` | Standard RFC 7807 problems. |
| inventory-projector (this repo) | Kafka `warehouse.inventory.analytics` | Analytics only; invalid CloudEvents are skipped, no DLQ. |

Absent on purpose: no edge to workforce-management (Separate Ways), and no
outbound REST or MCP call from this service to any sibling except the optional
facility-layout lookup. Every route, topic and group is also on the
[Runbook](../operations/runbook.md#kafka).

## What this service publishes

| | |
| --- | --- |
| **Topic** | `warehouse.inventory.events` |
| **Events** | `StockReserved`, `ReservationRevoked`, the transfer replies and facts (ADR 0030, ADR 0033), `StockRelocated` (ADR 0039), `BinStockLevelChanged` (ADR 0040) and the promoted exception facts `bin.DiscrepancyDetected`, `stock.ItemUnlocated`, `stock.ItemStowed` (byte-identical copies of the analytics feed, [ADR 0043](/docs/adr/0043)); the legacy `ProductClassified` was removed at stage E (2026-10-17) |
| **Client library** | `github.com/segmentio/kafka-go` |
| **Balancer** | `Hash` (message key = reservation id, [ADR 0021](/docs/adr/0021-kafka-producer-partition-key)), `AllowAutoTopicCreation: true` |
| **Consumers today** | `wes-work-planning` (`StockReserved`, `ReservationRevoked`); `network-inventory-planning` (the transfer replies); `slotting-optimization` (`StockRelocated`); `pick-face-replenishment` (`BinStockLevelChanged`, `StockRelocated`); `exception-management` (`DiscrepancyDetected`, `ItemUnlocated`); `returns-processing` (`ItemStowed`) |

## What this service consumes

Operator and sibling commands against this service's own aggregates arrive as
explicit HTTP commands against [the REST API](/docs/api-reference), which runs
this service's own invariants before anything is written. The exceptions are
event-driven: transfer allocation commands (ADR 0030, see the README), pick
confirmation from `TaskCompleted` (below), and product classification, which
this service no longer authors (below).

**One Kafka topic, read into a local read model (ADR 0013).** `StowStock`
needs the target bin's zone attributes when the SKU being stowed carries the
`Hazmat` or `TemperatureSensitive` handling tag, to enforce placement rules
(hazmat-rated zone, matching temperature class — see
[ADR 0009](/docs/adr/0009-product-classification-as-sku-master-data) for the
fail-open/fail-closed asymmetry). With `LOCATION_LOOKUP_MODE=kafka` those
attributes come from `internal/adapters/outbound/facilitycache`, which
replays `facility-layout`'s `warehouse.facility.events` from the earliest
offset into memory on every start, under a unique per-process consumer group,
and blocks startup until the replay is complete (up to 60s). See
[ADR 0013](/docs/adr/0013-location-classification-via-facility-events).
Messages are CloudEvents 1.0; the consumer dispatches on these exact `type`
strings ([ADR-0024](../adr/0024-cloudevents-mandatory-envelope.md)):

- `com.warehouse.wms.facility-layout.zone.ZoneRegistered`
- `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered`
- `com.warehouse.wms.facility-layout.locationslot.LocationSlotHandlingUnitDeclared` (the bin handling unit, ADR 0047)
- `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned`

Any other type is ignored; a message that is not a valid CloudEvent (e.g. the
retired flat envelope) is dead-lettered to `warehouse.facility.events.dlq`
and the replay — and the readiness gate — moves past it.

| Env var | Default | Purpose |
| --- | --- | --- |
| `LOCATION_LOOKUP_MODE` | *(none, required)* | `kafka`: Kafka-fed local cache (what the `warehouse-infra` cluster runs). `http`: synchronous `GET /locations/{locationCode}/classification` on facility-layout per stow — the rollback for `kafka`. Unset and `permissive` are boot errors since [ADR 0042](/docs/adr/0042): a lookup that answers `Known=false` for every location validates nothing (the 2026-10-06 "kept permissive" decision of ADR 0013/0020 no longer applies). |
| `FACILITY_LAYOUT_BASE_URL` | *(unset)* | Base URL for the `http` mode client, e.g. `http://facility-layout:80`; required in that mode. |
| `KAFKA_BROKERS` | *(none, required)* | Required by every mode and every binary except `cmd/mcp`; the process exits at boot if it is missing. |

**A second Kafka topic, picks confirmed from an event (ADR 0035, per line ADR 0036).** The
physical decrement of reserved stock is not an HTTP command in production:
this service always consumes
`com.warehouse.wes.fulfillment-execution.task.TaskCompleted` from
`warehouse.fulfillment.events` under the stable group
`TASK_COMPLETED_CONSUMER_GROUP` (mandatory; the chart default is
`inventory-storage-confirm-pick`). For
a `PICK` task (one per order line) with the additive optional `order_ref` and `line_no`
it confirms exactly the ACTIVE reservation of (`order_ref`, `line_no`) through the
existing `ConfirmPick` use case, in one transaction with the CloudEvents-id claim
([ADR 0036](/docs/adr/0036)); without a `line_no`, or for reservations that have
none, it falls back to counting the order's picks (`order_pick_progress`) and
confirming every ACTIVE reservation of the order when the **last** one completes
(ADR 0035); a redelivery changes
nothing, an expired reservation is skipped and counted
(`inventory.pick_confirmations`), and short picks are not modelled. Poison and
exhausted messages go to `warehouse.fulfillment.events.dlq`. See
[ADR 0035](/docs/adr/0035).

| Env var | Default | Purpose |
| --- | --- | --- |
| `TASK_COMPLETED_CONSUMER_GROUP` | *(none, required)* | Stable consumer group id (chart default `inventory-storage-confirm-pick`). The consumer always runs; `StockPicked` leaves through the outbox. |
| `TASK_COMPLETED_CONSUMER_MODE` | *(unset)* | Legacy guard: unset or `kafka`; `off` or anything else is a boot error. |

**A third Kafka topic, product-master's classifications (ADR 0034).**
product-master owns SKU product classification. This service always consumes
(under the mandatory `PRODUCT_MASTER_CONSUMER_GROUP`)
`com.warehouse.wms.product-master.product.ProductClassified` from
`warehouse.product-master.events` (key = SKU) and keeps a version-guarded
local copy in `product_classifications`: `ApplyProductClassification` claims
the CloudEvents `id` in `processed_events` and upserts the row in one
transaction, applying a message only when its `version` is greater than the
stored one. Every other product-master type is committed past; a message that
is not a valid CloudEvent, or breaks the classification invariants, is logged
and committed past. The copy is what `StowStock` reads for its placement and
DOT segregation rules. `PUT` and `GET /products/{sku}/classification` were
removed at stage E (2026-10-17): the router registers neither, so both answer
`404`. See [ADR 0034](/docs/adr/0034).

| Env var | Default | Purpose |
| --- | --- | --- |
| `PRODUCT_MASTER_CONSUMER_GROUP` | *(none, required)* | Stable consumer group id (chart default `inventory-storage-product-master`). The consumer always runs. |

**A fourth Kafka topic, inbound-receiving's receipts (ADR 0037).**
inbound-receiving owns ASNs, dock appointments and receipts. This service always
consumes (under the mandatory `INBOUND_RECEIPT_CONSUMER_GROUP`)
`com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived` from
`warehouse.inbound-receiving.events` (key = ASN number). A `condition=Good`
line runs the existing `ReceiveStock` use case, in one transaction with the
CloudEvents-id claim in `processed_events`: the same `StockReceived` as
`POST /stock/receive`, the quantity staged and not yet usable, stow still the RF
action. A `Damaged` line is not booked in v1 (INFO log,
`inventory.inbound_receipt_units{outcome=damaged_not_booked}`). Every other
inbound-receiving type is committed past; a message that is not a valid
CloudEvent, or carries an empty `sku`, a non-positive `quantity` or an unknown
`condition`, is logged and committed past. `POST /stock/receive` is unchanged.
See [ADR 0037](/docs/adr/0037).

| Env var | Default | Purpose |
| --- | --- | --- |
| `INBOUND_RECEIPT_CONSUMER_GROUP` | *(none, required)* | Stable consumer group id (chart value `config.inboundReceiptConsumerGroup`). The consumer always runs; `StockReceived` leaves through the outbox like every other event. |

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKERS` | *(none, required)* | Comma-separated broker list. No `localhost:9092` default: the process exits at boot without it. |
| `EVENT_PUBLISHER` | *(unset)* | Guard only: unset or `kafka`. Use cases write `outbox_events` rows inside their own transaction and a background relay in `cmd/inventory` drains them onto the topics (ADR 0017, `OUTBOX_RELAY_INTERVAL`, default `1s`). There is no log publisher and no direct publishing (ADR 0042). |

There is one Kafka broker platform-wide: the in-cluster broker deployed by
`warehouse-infra`, whose external listener is published on the host at
`localhost:9092`. This repository's `docker-compose.yml` defines its own
disposable Kafka on `localhost:19092` (`KAFKA_HOST_PORT`) so local runs never
collide with it.

```bash
# local Postgres and Kafka from the compose file; the other required variables
# (LOCATION_LOOKUP_MODE and the four *_CONSUMER_GROUP ids) are listed in the
# quickstart
docker compose up -d --wait postgres kafka
DATABASE_URL=postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable \
  KAFKA_BROKERS=localhost:19092 go run ./cmd/inventory
```

## Message shapes

Every message is a CloudEvents 1.0 structured-mode event with the Kafka
header `content-type: application/cloudevents+json; charset=UTF-8`
([ADR-0024](../adr/0024-cloudevents-mandatory-envelope.md)); the full
attribute table is on the [Events page](/docs/api-reference/events). What
lands on the topic:

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
  "data": { "sku": "SKU-1", "quantity": 5, "demand_ref": "order-42" }
}
```

Both carry the identical `data` shape, deliberately: the downstream projection
applies one to decrement and the other to increment the same counter.

## The downstream projection

`wes-work-planning` turns these two events into `UsableInventoryObserved`,
a read model **keyed by SKU** (package `internal/domain/inventoryview/`,
table `usable_inventory_view`, exposed at `GET /inventory-view/{sku}`).

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (WES or operator)
    participant I as inventory-storage
    participant K as Kafka<br/>warehouse.inventory.events
    participant W as wes-work-planning

    C->>I: POST /reservations {sku, quantity, demandRef}
    I->>I: sum usable across StockUnits<br/>reserve first-fit, record Allocations
    I->>K: StockReserved {sku, quantity, demand_ref}
    I-->>C: 201 Created + Location
    K->>W: consume
    W->>W: dedupe on CloudEvents id (processed_events)
    W->>W: UsableInventoryObserved[sku] -= quantity

    Note over C,I: the physical pick fails
    C->>I: DELETE /reservations/{id}
    I->>I: Revoke() — release quantity back to each StockUnit
    I->>K: ReservationRevoked {sku, quantity, demand_ref}
    I-->>C: 204 No Content
    K->>W: consume
    W->>W: UsableInventoryObserved[sku] += quantity
```

Two design notes that a consumer must respect:

- **Keyed by SKU, not by path.** Inventory reservations are SKU-scoped;
  `wes-work-planning`'s own `CLAUDE.md` calls out explicitly that a path
  mapping must not be forced onto them because it does not exist.
- **Idempotency is mandatory.** Kafka delivery is at-least-once.
  `wes-work-planning` inserts each CloudEvents `id` into a `processed_events` table
  before applying the effect, and skips on a primary-key violation — without
  that, one redelivery double-decrements observed usable.

## Consuming this topic yourself

If you are building another consumer:

1. **Read the spec, not this page.**
   [`apis/asyncapi.yaml`](https://github.com/IQVO/inventory-storage/blob/main/apis/asyncapi.yaml)
   is the contract and is Spectral-linted in CI.
2. **The integration topic carries `StockReserved`, `ReservationRevoked`,
   the transfer replies and facts (ADR 0030, ADR 0033), `StockRelocated`
   (ADR 0039) and `BinStockLevelChanged` (ADR 0040).** The legacy
   `ProductClassified` was removed at stage E (2026-10-17,
   [ADR 0034](/docs/adr/0034)): product classification is owned by
   product-master, so read
   `com.warehouse.wms.product-master.product.ProductClassified` from
   `warehouse.product-master.events`. `warehouse.inventory.analytics` is internal to this
   service's analytics projector; do not build against it.
3. **Dispatch on the full `type` string; ignore unknown types.** The catalog
   grows. Reject (DLQ/skip) anything that is not a valid CloudEvent.
4. **Deduplicate on the CloudEvents `id`.** At-least-once.
5. **Ordering is per reservation only** (Kafka key = reservation id,
   `Hash` balancer — ADR-0021).
6. **Treat the events as a projection, not as truth.** For an authoritative
   answer, call `GET /inventory/{sku}/usable`. The event stream exists so
   consumers can keep a cheap local view warm, not so they can reimplement the
   ledger.

## Verifying end to end

The integration was smoke-tested for real against the shared broker, not just
unit-tested. To repeat it:

```bash
# 1. Postgres and Kafka up (compose Kafka: host port 19092)
docker compose up -d --wait postgres kafka

# 2. service up (DATABASE_URL, KAFKA_BROKERS, LOCATION_LOOKUP_MODE and the four
#    *_CONSUMER_GROUP variables exactly as in the quickstart)
go run ./cmd/inventory &

# 3. seed a bin + stow, then reserve (the Idempotency-Key header is mandatory)
curl -s -X POST localhost:8080/reservations \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: smoke-1' \
  -d '{"sku":"SKU-1","quantity":5,"demandRef":"order-42"}'

# 4. confirm it landed
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic warehouse.inventory.events --from-beginning
```

The unit-level equivalent lives in
`internal/adapters/outbound/kafka/golden_test.go`, which asserts the exact
CloudEvent JSON and `content-type` header the adapter produces against an in-memory `Writer` fake — no broker
required, which is why it runs in the default `go test ./...` suite.

## Deployment

The service ships as a container (`Dockerfile` at the repo root, published to
GHCR by the `docker-publish` CI job on `main`) and as a Helm chart
(`charts/inventory-storage`, linted by the `helm-lint` job). In the local
Kubernetes stack its REST API is reached through Kong at
`http://localhost:8000/api/inventory-storage`, and it runs inside the Istio
mesh for east-west traffic; `KAFKA_BROKERS` and the consumer-group ids are
ordinary (mandatory) chart values (`kafka.brokers` comes from `warehouse-infra`'s
`_platform.yaml`); `LOCATION_LOOKUP_MODE=kafka` is the chart default
(`config.locationLookupMode`), which `warehouse-infra` also sets explicitly.
