---
title: Domain Message Flow
sidebar_label: Domain Message Flow
sidebar_position: 6
description: ddd-crew Domain Message Flow Modelling for inventory-storage — four business scenarios with every command, query and event between contexts, numbered, using only real routes, MCP tools and CloudEvents types.
---

# Domain Message Flow

Following [ddd-crew Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling).
Each scenario is a sequence of messages between bounded contexts (and the
people or tools acting on them). Every arrow is prefixed **`cmd:`**
(command), **`qry:`** (query) or **`evt:`** (event) and numbered; every
message is a real REST route, MCP tool or CloudEvents `type` — nothing is
invented. Solid arrows are synchronous calls; open arrows are Kafka events.
Responses are folded into notes so that every arrow is a domain message.

## 1. Order allocation reserves usable stock

`order-management` takes in an order, checks handling classification, then
reserves each line. Work Planning learns about the reservations from events,
never by asking.

```mermaid
sequenceDiagram
    autonumber
    participant OM as order-management
    participant INV as inventory-storage
    participant WP as wes-work-planning
    OM->>INV: qry: GET /products/{sku}/classification
    Note over OM,INV: 200 handling tags, temperature class, DOT class, or 404 unclassified
    OM->>INV: cmd: ReserveStock POST /reservations with Idempotency-Key per order line
    Note over INV: replay guard, then reserve first-fit against usable
    Note over OM,INV: 201 reservation with allocations and pick locations, or 409 insufficient-usable
    INV-)WP: evt: com.warehouse.wms.inventory-storage.reservation.StockReserved on warehouse.inventory.events
    Note over WP: UsableInventoryObserved for the SKU decremented
    OM-)WP: evt: com.warehouse.wes.order-management.order.OrderAllocated on warehouse.order-management.events
```

Source: `order-management/internal/application/usecases/receive_order.go`,
`allocation.go`, `internal/adapters/outbound/inventorystorage/client.go`;
this repo's `internal/adapters/inbound/http/server.go`,
`internal/application/usecases/reserve_stock.go`,
`internal/adapters/outbound/kafka/publisher.go`;
`wes-work-planning/internal/adapters/inbound/kafka/consumer.go`.
Omitted: the analytics copy of `StockReserved`, `OrderPartiallyAllocated`,
and what Work Planning does with the order afterwards.

## 2. Order cancellation gives the stock back

A cancelled order revokes every reservation it holds. The revoke is the
compensation: quantity returns to usable and can be re-reserved from any
holding.

```mermaid
sequenceDiagram
    autonumber
    participant OP as Operator
    participant OM as order-management
    participant INV as inventory-storage
    participant WP as wes-work-planning
    participant OA as warehouse-ops-agent
    OP->>OM: cmd: CancelOrder DELETE /orders/{id}
    OM->>INV: cmd: RevokeReservation DELETE /reservations/{id} for each reservation
    Note over INV: lazy expiry first, then Revoke and release every allocation
    Note over OM,INV: 204, or 409 reservation-already-resolved
    INV-)WP: evt: com.warehouse.wms.inventory-storage.reservation.ReservationRevoked on warehouse.inventory.events
    Note over WP: UsableInventoryObserved for the SKU incremented
    OA->>INV: qry: GET /reservations?demandRef= for the Order Lifecycle view
    Note over OA,INV: 200 every reservation for the demand, now REVOKED or EXPIRED
```

Source: `order-management/internal/application/usecases/cancel_order.go`
and its `DELETE /orders/{id}` route;
this repo's `revoke_reservation.go`, `reservation_expiry.go`,
`get_reservations_by_demand_ref.go`;
`warehouse-ops-agent/internal/adapters/outbound/restclient/clients.go`.
Omitted: `OrderCancelled` from order-management and the analytics copies of
the events.

## 3. Inbound: register, classify, receive and stow a hazmat SKU

facility-layout's zone data reaches this context only as events, cached
locally. A stow of a classified SKU is checked against that cache and
against the bin's current occupants.

```mermaid
sequenceDiagram
    autonumber
    participant FL as facility-layout
    participant IC as Inventory control
    participant INV as inventory-storage
    participant PJ as inventory-projector
    participant PM as product-master
    FL-)INV: evt: com.warehouse.wms.facility-layout.zone.ZoneRegistered on warehouse.facility.events
    FL-)INV: evt: com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered on warehouse.facility.events
    Note over INV: facility location cache maps slot to zone, hazmat and temperature class
    IC->>INV: cmd: RegisterBin PUT /bins/{binId}
    PM-)INV: evt: com.warehouse.wms.product-master.product.ProductClassified on warehouse.product-master.events
    Note over INV: local classification copy, version-guarded (ADR 0034), PUT /products/{sku}/classification answers 410
    IC->>INV: cmd: ReceiveStock POST /stock/receive
    INV-)PJ: evt: com.warehouse.wms.inventory-storage.stock.StockReceived on warehouse.inventory.analytics
    IC->>INV: cmd: StowStock POST /stock/stow
    Note over INV: placement check from cache, same-bin DOT segregation, capacity
    INV-)PJ: evt: com.warehouse.wms.inventory-storage.stock.ItemStowed on warehouse.inventory.analytics
```

Source: `internal/adapters/outbound/facilitycache/consumer.go`,
`internal/application/usecases/register_bin.go`, `apply_product_classification.go`,
`receive_stock.go`, `stow_stock.go`, `internal/adapters/outbound/kafka/analytics_publisher.go`.
"Inventory control" is whoever drives these routes — an operator or the
`e2e-tests` warehouse-day simulator; no sibling context does.
Omitted: `LocationRecorded` (in-process only by decision — no consumer) and
the 409 rejections.

## 4. Availability, cycle count and the accuracy report

Read-side consumers ask for usable stock; a cycle count corrects the ledger
and its effect becomes visible in the Flow & Accuracy report.

```mermaid
sequenceDiagram
    autonumber
    participant NF as network-fulfillment
    participant OA as warehouse-ops-agent
    participant IC as Inventory control
    participant INV as inventory-storage
    participant PJ as inventory-projector
    participant RP as inventory-reports
    NF->>INV: qry: GET /inventory/{sku}/usable
    OA->>INV: qry: MCP check_availability
    OA->>INV: qry: MCP get_bin_occupancy
    IC->>INV: cmd: RunCycleCount POST /bins/{binId}/cycle-count
    Note over INV: shortfall marks units UNLOCATED, usable drops at once
    INV-)PJ: evt: com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected on warehouse.inventory.analytics
    INV-)PJ: evt: com.warehouse.wms.inventory-storage.stock.ItemUnlocated on warehouse.inventory.analytics
    INV-)PJ: evt: com.warehouse.wms.inventory-storage.bin.CycleCountCompleted on warehouse.inventory.analytics
    Note over PJ: upsert flow_accuracy_rollup per SKU, bin and hour
    OA->>RP: qry: GET /reports/flow-accuracy
```

Source: `network-fulfillment/internal/adapters/outbound/inventoryclient/client.go`,
`warehouse-ops-agent/internal/adapters/outbound/mcpclient/inventory_storage.go`,
`reports_clients.go`; this repo's `run_cycle_count.go`,
`internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/inbound/http/reports_handler.go`.
`inventory-projector` and `inventory-reports` are this context's own
binaries, drawn separately because they talk to it only through the
analytics topic. Omitted: the overage branch (no `ItemUnlocated`) and the
MCP report tool.
