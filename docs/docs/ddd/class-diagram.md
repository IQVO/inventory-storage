---
title: UML Class Diagrams
sidebar_label: Class Diagrams
sidebar_position: 8
description: UML class diagrams of internal/domain — aggregates, entities, value objects, enumerations and domain events — plus the application ports and the hexagonal ports-and-adapters map, all with real type names from the code.
---

# UML Class Diagrams

Real type names, key fields and every state-changing method from
`internal/domain/**`, split into three diagrams to stay readable, followed
by the ports and the hexagonal adapter map. Go has no classes: a "class"
here is a Go type; private fields are shown with `-`, exported methods with
`+`. Stereotypes: `<<AggregateRoot>>`, `<<Entity>>`, `<<ValueObject>>`,
`<<Enumeration>>`, `<<DomainEvent>>`, `<<Repository>>` (an outbound port).

## 1. Aggregates, value objects and enumerations

```mermaid
classDiagram
    direction LR
    class StockUnit {
        <<AggregateRoot>>
        -id string
        -sku SKU
        -binID BinId
        -quantity Quantity
        -reserved Quantity
        -state State
        -version int
        +NewStockUnit(id, sku, binID, qty) StockUnit
        +Usable() Quantity
        +Reserve(qty Quantity) error
        +ReleaseReservation(qty Quantity) error
        +Pick(qty Quantity) error
        +MarkUnlocated()
    }
    class State {
        <<Enumeration>>
        AVAILABLE
        RESERVED
        PICKED
        REMOVED
        UNLOCATED
    }
    class Bin {
        <<AggregateRoot>>
        -id BinId
        -capacity Quantity
        -occupied Quantity
        -version int
        +NewBin(id, capacity) Bin
        +Available() Quantity
        +IsFull() bool
        +Occupy(qty Quantity) error
        +Release(qty Quantity) error
        +Resize(capacity Quantity) error
    }
    class Reservation {
        <<AggregateRoot>>
        -id string
        -sku SKU
        -quantity Quantity
        -demandRef string
        -allocations List~Allocation~
        -status Status
        -createdAt Time
        -expiresAt Time
        -version int
        +New(id, sku, qty, demandRef, allocations, createdAt, timeout) Reservation
        +IsExpired(now Time) bool
        +Revoke() error
        +Confirm(now Time) error
        +Expire() error
    }
    class Status {
        <<Enumeration>>
        ACTIVE
        CONFIRMED
        REVOKED
        EXPIRED
    }
    class Allocation {
        <<Entity>>
        +StockUnitID string
        +BinID BinId
        +Quantity Quantity
    }
    class ProductClassification {
        <<AggregateRoot>>
        -sku SKU
        -handlingTags Set~HandlingTag~
        -temperatureClass TemperatureClass
        -dotHazardClass DOTHazardClass
        +New(sku, tags, temperatureClass, dotHazardClass) ProductClassification
        +HandlingTags() List~HandlingTag~
        +HasTag(tag HandlingTag) bool
        +IsHazmat() bool
        +IsTemperatureSensitive() bool
    }
    class HandlingTag {
        <<Enumeration>>
        Hazmat
        Fragile
        TemperatureSensitive
        Oversized
        HighValue
    }
    class TemperatureClass {
        <<Enumeration>>
        Ambient
        Chilled
        Frozen
    }
    class DOTHazardClass {
        <<ValueObject>>
        int 1 to 9, 0 unspecified
        +ParseDOTHazardClass(value int) DOTHazardClass
        +Incompatible(a, b DOTHazardClass) bool
    }
    class SlotAttributes {
        <<ValueObject>>
        +Hazmat bool
        +TemperatureClass TemperatureClass
        +Known bool
    }
    class SKU {
        <<ValueObject>>
        string, non-empty
    }
    class BinId {
        <<ValueObject>>
        string, non-empty
    }
    class Quantity {
        <<ValueObject>>
        int, never negative
        +Add(other Quantity) Quantity
        +Sub(other Quantity) Quantity or error
        +GreaterThan(other Quantity) bool
    }

    StockUnit --> State : state
    StockUnit ..> BinId : located in, by id
    StockUnit --> SKU
    StockUnit --> Quantity
    Bin --> BinId : identity
    Reservation "1" *-- "1..*" Allocation : allocations
    Reservation --> Status : status
    Allocation ..> StockUnit : StockUnitID, by id
    Allocation ..> BinId : pick location
    ProductClassification "1" o-- "1..5" HandlingTag : handlingTags
    ProductClassification --> TemperatureClass
    ProductClassification --> DOTHazardClass
    SlotAttributes --> TemperatureClass
```

Source: `internal/domain/stock/stock_unit.go`, `stock/state.go`,
`internal/domain/location/bin.go`, `internal/domain/reservation/reservation.go`,
`internal/domain/product/classification.go`, `product/segregation.go`,
`internal/domain/shared/sku.go`, `bin_id.go`, `quantity.go`.
Omitted: getters, `Rehydrate*` constructors, `Version()`, the `Parse*`
helpers of the enumerations, and the `Err*` sentinels (listed on the
[Aggregate Design Canvas](./aggregate-design-canvas.md)). Dotted arrows are
references **by identity** across aggregate boundaries — no aggregate holds
a pointer to another. `Allocation` has no identity of its own outside its
`Reservation`; it is shown as `<<Entity>>` because it is persisted as a row
keyed by `(reservation_id, stock_unit_id)`.

## 2. Domain events

```mermaid
classDiagram
    direction TB
    class DomainEvent {
        <<interface>>
        +EventName() string
        +OccurredAt() Time
    }
    class base {
        +Name string
        +At Time
    }
    class StockReceived {
        <<DomainEvent>>
        +SKU SKU
        +Quantity Quantity
    }
    class ItemStowed {
        <<DomainEvent>>
        +SKU SKU
        +BinID BinId
        +Quantity Quantity
    }
    class LocationRecorded {
        <<DomainEvent>>
        +StockUnitID string
        +BinID BinId
    }
    class StockReserved {
        <<DomainEvent>>
        +ReservationID string
        +SKU SKU
        +Quantity Quantity
        +DemandRef string
    }
    class ReservationRevoked {
        <<DomainEvent>>
        +ReservationID string
    }
    class ReservationExpired {
        <<DomainEvent>>
        +ReservationID string
    }
    class StockPicked {
        <<DomainEvent>>
        +ReservationID string
        +SKU SKU
        +Quantity Quantity
    }
    class ItemUnlocated {
        <<DomainEvent>>
        +StockUnitID string
        +SKU SKU
        +BinID BinId
        +Quantity Quantity
    }
    class CycleCountCompleted {
        <<DomainEvent>>
        +BinID BinId
        +CountedQty Quantity
        +SystemQty Quantity
        +Discrepancy bool
    }
    class DiscrepancyDetected {
        <<DomainEvent>>
        +BinID BinId
        +CountedQty Quantity
        +SystemQty Quantity
    }
    class ProductClassified {
        <<DomainEvent>>
        +SKU SKU
        +HandlingTags List~HandlingTag~
        +TemperatureClass TemperatureClass
        +DOTHazardClass DOTHazardClass
        +At Time
    }

    DomainEvent <|.. base
    base <|-- StockReceived
    base <|-- ItemStowed
    base <|-- LocationRecorded
    base <|-- StockReserved
    base <|-- ReservationRevoked
    base <|-- ReservationExpired
    base <|-- StockPicked
    base <|-- ItemUnlocated
    base <|-- CycleCountCompleted
    base <|-- DiscrepancyDetected
    DomainEvent <|.. ProductClassified
```

Source: `internal/domain/shared/events.go`, `internal/domain/product/classification.go`.
Omitted: the `New*` constructors. The `base <|--` edges are Go struct
**embedding**, drawn as inheritance; `ProductClassified` implements the
interface directly in package `product`.

## 3. Application ports

```mermaid
classDiagram
    direction LR
    class StockRepo {
        <<Repository>>
        +Save(ctx, unit StockUnit) error
        +FindByID(ctx, id string) StockUnit
        +FindBySKU(ctx, sku SKU) List~StockUnit~
        +FindByBin(ctx, binID BinId) List~StockUnit~
        +NextID(ctx) string
    }
    class LocationRepo {
        <<Repository>>
        +Save(ctx, bin Bin) error
        +FindByID(ctx, id BinId) Bin
    }
    class ReservationRepo {
        <<Repository>>
        +Save(ctx, r Reservation) error
        +FindByID(ctx, id string) Reservation
        +FindByDemandRef(ctx, demandRef string) List~Reservation~
        +NextID(ctx) string
    }
    class ProductClassificationRepo {
        <<Repository>>
        +Save(ctx, c ProductClassification) error
        +FindBySKU(ctx, sku SKU) ProductClassification
    }
    class LocationClassificationLookup {
        <<interface>>
        +GetSlotAttributes(ctx, binID BinId) SlotAttributes
    }
    class EventPublisher {
        <<interface>>
        +Publish(ctx, event DomainEvent) error
    }
    class UnitOfWork {
        <<interface>>
        +Execute(ctx, fn) error
    }
    class ReservationMetrics {
        <<interface>>
        +ReservationCreated(ctx)
        +ReservationRevoked(ctx)
    }
    class Clock {
        <<interface>>
        +Now() Time
    }
    class ReserveStock {
        +Stock StockRepo
        +Reservations ReservationRepo
        +Events EventPublisher
        +Clock Clock
        +Metrics ReservationMetrics
        +Timeout Duration
        +UnitOfWork UnitOfWork
        +Execute(ctx, sku, qty, demandRef) Reservation
    }
    class StowStock {
        +Stock StockRepo
        +Locations LocationRepo
        +Events EventPublisher
        +Clock Clock
        +Classifications ProductClassificationRepo
        +LocationLookup LocationClassificationLookup
        +UnitOfWork UnitOfWork
        +Execute(ctx, sku, qty, binID) StockUnit
    }
    ReserveStock --> StockRepo
    ReserveStock --> ReservationRepo
    ReserveStock --> EventPublisher
    ReserveStock --> UnitOfWork
    ReserveStock --> ReservationMetrics
    ReserveStock --> Clock
    StowStock --> StockRepo
    StowStock --> LocationRepo
    StowStock --> ProductClassificationRepo
    StowStock --> LocationClassificationLookup
    StowStock --> EventPublisher
    StowStock --> UnitOfWork
```

Source: `internal/application/ports/ports.go`,
`internal/application/usecases/reserve_stock.go`, `stow_stock.go`.
Omitted: the other nine use cases, which follow the same struct-of-ports
shape (see [Use Cases](./use-cases.md)); `ctx` is `context.Context`.

## 4. Hexagonal ports and adapters

```mermaid
flowchart LR
    subgraph IN["Inbound adapters"]
        HTTP["inbound/http<br/>chi router, DTOs, RFC 7807,<br/>RequireIdempotencyKey, /readyz"]
        MCP["inbound/mcp<br/>check_availability, get_bin_occupancy,<br/>revoke_reservation, report tool"]
        AKC["inbound/kafka<br/>AnalyticsConsumer"]
        RPT["inbound/http<br/>ReportsHandlers"]
    end
    subgraph APP["Application"]
        UC["usecases<br/>11 use cases"]
        PORTS["ports<br/>StockRepo, LocationRepo, ReservationRepo,<br/>ProductClassificationRepo, LocationClassificationLookup,<br/>EventPublisher, UnitOfWork, ReservationMetrics, Clock"]
        REPORT["analytics/report<br/>ReportStore, ProjectionStore, ProcessedEvents"]
    end
    subgraph DOM["Domain"]
        D["stock, location, reservation,<br/>product, shared"]
    end
    subgraph OUT["Outbound adapters"]
        PG["outbound/postgres<br/>repos, UnitOfWork,<br/>OutboxPublisher, OutboxRelay, Sweeper"]
        MEM["outbound/memory<br/>repos, SystemClock"]
        KAF["outbound/kafka<br/>Publisher, AnalyticsPublisher, RelaySink"]
        EVT["outbound/events<br/>log, buffered, multi"]
        FC["outbound/facilitycache<br/>Kafka-fed cache"]
        FLC["outbound/facilitylayout<br/>HTTP client, breaker, permissive"]
        TEL["outbound/telemetry<br/>metrics, OTel"]
        AS["outbound/analyticsstore<br/>Postgres projection and reader"]
    end
    HTTP --> UC
    MCP --> UC
    UC --> PORTS
    UC --> D
    PORTS --> D
    AKC --> REPORT
    RPT --> REPORT
    PG -.implements.-> PORTS
    MEM -.implements.-> PORTS
    KAF -.implements.-> PORTS
    EVT -.implements.-> PORTS
    FC -.implements.-> PORTS
    FLC -.implements.-> PORTS
    TEL -.implements.-> PORTS
    AS -.implements.-> REPORT
```

Source: `internal/adapters/**`, `internal/application/ports/ports.go`,
`internal/analytics/report/ports.go`, the four `cmd/*/main.go` composition
roots. Omitted: `cmd/mcp`'s reports REST client (`inboundmcp.NewReportsRESTClient`)
and `internal/adapters/kafka/cloudevents`, the shared envelope helper used
by every Kafka adapter. The MCP adapter reads `StockRepo.FindByBin`
directly for `get_bin_occupancy`; the HTTP adapter reads
`ProductClassificationRepo` directly for `GET /products/{sku}/classification`.
