---
title: EventStorming
sidebar_label: EventStorming
sidebar_position: 7
description: Design-level EventStorming of inventory-storage in ddd-crew cheat-sheet notation — inbound and stow, the reservation lifecycle, and cycle counting — with every sticky traced to code and hotspots taken from real documented gaps.
---

# EventStorming

Design-level [EventStorming](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet),
reconstructed from the code rather than from a workshop: every sticky below
is a real use case, aggregate, domain event, policy or read model, and every
hotspot is a gap already documented in an ADR, a code comment or these docs.
Time flows left to right.

## Legend

```mermaid
flowchart LR
    A["Actor"]:::actor
    C["Command"]:::command
    AG["Aggregate"]:::aggregate
    E["Domain Event"]:::event
    P["Policy"]:::policy
    R["Read Model"]:::readmodel
    X["External System"]:::external
    H["Hotspot"]:::hotspot
    D["Decided hotspot"]:::decided
    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000
    classDef event fill:#f6a04d,stroke:#a85d14,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#78281f,color:#fff
    classDef decided fill:#a9dfbf,stroke:#1e8449,color:#000
```

## Process 1 — Inbound: register, classify, receive, stow

```mermaid
flowchart LR
    IC["Inventory control"]:::actor
    FL["facility-layout"]:::external
    ZE["ZoneRegistered / LocationSlotRegistered<br/>LocationSlotDecommissioned"]:::event
    CACHE["Facility location cache"]:::readmodel
    RB["RegisterBin"]:::command
    BIN["Bin"]:::aggregate
    PM["product-master<br/>ProductClassified, ADR 0034"]:::external
    CP["ApplyProductClassification"]:::command
    PC["ProductClassification"]:::aggregate
    RS["ReceiveStock"]:::command
    SRE["StockReceived"]:::event
    SS["StowStock"]:::command
    PLC["Placement check<br/>hazmat zone, temperature class"]:::policy
    SEG["Same-bin DOT segregation"]:::policy
    SU["StockUnit"]:::aggregate
    IS["ItemStowed"]:::event
    LR["LocationRecorded<br/>in-process only, no consumer"]:::event
    H1["Bin id never checked against<br/>facility-layout slot catalogue"]:::hotspot
    H2["Fragile, Oversized, HighValue<br/>have no placement rule"]:::hotspot
    H3["Decided 2026-10-06: default LOCATION_LOOKUP_MODE<br/>kept permissive (ADR 0013/0020)<br/>cold-cache safety, cluster injects kafka"]:::decided

    FL --> ZE --> CACHE
    IC --> RB --> BIN
    PM --> CP --> PC --> PLC
    IC --> RS --> SRE
    IC --> SS --> PLC
    CACHE --> PLC
    PLC --> SEG --> SU
    SS --> BIN
    SU --> IS
    SU --> LR
    RB -.- H1
    PLC -.- H2
    PLC -.- H3

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000
    classDef event fill:#f6a04d,stroke:#a85d14,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#78281f,color:#fff
    classDef decided fill:#a9dfbf,stroke:#1e8449,color:#000
```

Source: `internal/application/usecases/register_bin.go`, `apply_product_classification.go`,
`receive_stock.go`, `stow_stock.go`, `internal/adapters/outbound/facilitycache/consumer.go`,
`internal/domain/product/segregation.go`,
`internal/adapters/outbound/kafka/publisher.go` and `analytics_publisher.go`
(`ProductClassified`).
Omitted: the rejection paths (they raise no event) and the HTTP fallback
lookup.

## Process 2 — Reservation lifecycle

```mermaid
flowchart LR
    OM["order-management"]:::external
    PK["Picker or simulator"]:::actor
    AG["MCP host / agent"]:::actor
    RSV["ReserveStock"]:::command
    RG["Replay guard<br/>same demandRef, SKU, quantity"]:::policy
    LE["Lazy expiry on every read"]:::policy
    RES["Reservation"]:::aggregate
    SU["StockUnit"]:::aggregate
    BIN["Bin"]:::aggregate
    SR["StockReserved"]:::event
    REV["RevokeReservation"]:::command
    RR["ReservationRevoked"]:::event
    CPK["ConfirmPick"]:::command
    SP["StockPicked"]:::event
    RE["ReservationExpired"]:::event
    WP["wes-work-planning"]:::external
    UI["Usable inventory per SKU"]:::readmodel
    RBD["Reservations by demandRef"]:::readmodel
    FE["fulfillment-execution"]:::external
    H5["Decided 2026-10-06: lazy expiry kept (ADR 0003)<br/>no sweeper; an unread expired hold<br/>keeps stock until read"]:::decided
    H6["Replay guard is best-effort,<br/>concurrent first attempts can both pass"]:::hotspot
    H7["Decided 2026-10-06: confirm-pick is event-driven<br/>pick-completion event, no sync call (ADR 0032, Proposed)<br/>blocked on fields in fulfillment-execution / wes-work-planning"]:::decided

    OM --> RSV --> RG --> RES
    RES --> SU
    RES --> SR --> WP
    OM --> REV
    AG --> REV
    REV --> LE
    REV --> RES
    RES --> RR --> WP
    PK --> CPK --> LE
    FE -.->|"pick-completion event<br/>planned, ADR 0032"| CPK
    CPK --> RES
    CPK --> BIN
    RES --> SP
    LE --> RE
    SU --> UI
    RES --> RBD
    LE -.- H5
    RG -.- H6
    CPK -.- H7

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000
    classDef event fill:#f6a04d,stroke:#a85d14,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#78281f,color:#fff
    classDef decided fill:#a9dfbf,stroke:#1e8449,color:#000
```

Source: `internal/application/usecases/reserve_stock.go`,
`revoke_reservation.go`, `confirm_pick.go`, `reservation_expiry.go`,
`get_usable.go`, `get_reservations_by_demand_ref.go`,
`internal/domain/reservation/reservation.go`, `internal/adapters/inbound/mcp/tools.go`.
Omitted: the analytics copies of the events and the 409 paths.

## Process 3 — Cycle count and accuracy

```mermaid
flowchart LR
    IC["Inventory control"]:::actor
    RCC["RunCycleCount"]:::command
    SU["StockUnit"]:::aggregate
    DD["DiscrepancyDetected"]:::event
    SHORT["Shortfall marks whole units UNLOCATED"]:::policy
    IU["ItemUnlocated"]:::event
    CCC["CycleCountCompleted"]:::event
    PJ["Projector policy<br/>upsert per SKU, bin, hour"]:::policy
    FA["Flow and Accuracy report"]:::readmodel
    OA["warehouse-ops-agent"]:::external
    H8["Overage is reported but never<br/>reconciled, needs a receiving process"]:::hotspot

    IC --> RCC --> SU
    SU --> DD --> SHORT --> IU
    SU --> CCC
    DD --> PJ
    IU --> PJ
    CCC --> PJ
    PJ --> FA --> OA
    DD -.- H8

    classDef actor fill:#fff59d,stroke:#b59f00,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6f9f,color:#000
    classDef aggregate fill:#f7d84a,stroke:#a68b00,color:#000
    classDef event fill:#f6a04d,stroke:#a85d14,color:#000
    classDef policy fill:#c39bd3,stroke:#7d3c98,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#78281f,color:#fff
```

Source: `internal/application/usecases/run_cycle_count.go`,
`internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/postgres_projection.go`,
`internal/adapters/inbound/http/reports_handler.go`.
Omitted: the clean-count branch (only `CycleCountCompleted` with
`discrepancy=false`).

## Stickies and their evidence

| Sticky | Kind | Code evidence |
| --- | --- | --- |
| Inventory control, Picker or simulator, MCP host / agent | Actor | REST callers of `PUT /bins/{binId}`, `POST /stock/*`, `POST /bins/{binId}/cycle-count`, `POST /reservations/{id}/confirm-pick`; MCP `revoke_reservation` |
| RegisterBin, ApplyProductClassification (Kafka, ADR 0034), ReceiveStock, StowStock, ReserveStock, RevokeReservation, ConfirmPick, RunCycleCount | Command | `internal/application/usecases/*.go`, routed in `internal/adapters/inbound/http/server.go` |
| StockUnit, Bin, Reservation, ProductClassification | Aggregate | `internal/domain/stock`, `location`, `reservation`, `product` |
| StockReceived, ItemStowed, LocationRecorded, StockReserved, ReservationRevoked, ReservationExpired, StockPicked, ItemUnlocated, CycleCountCompleted, DiscrepancyDetected | Domain Event | `internal/domain/shared/events.go` |
| ProductClassified | Domain Event | `internal/domain/product/classification.go` |
| ZoneRegistered, LocationSlotRegistered, LocationSlotDecommissioned | Domain Event (external) | consumed in `internal/adapters/outbound/facilitycache/consumer.go` |
| Placement check | Policy | `StowStock.checkPlacement` |
| Same-bin DOT segregation | Policy | `StowStock.checkSegregation`, `product.Incompatible` |
| Replay guard | Policy | `ReserveStock.activeReservationFor`, `isReplayOf` |
| Lazy expiry on every read | Policy | `expireIfDue`, `expireAllIfDue` |
| Shortfall marks whole units UNLOCATED | Policy | `RunCycleCount.markShortfallUnlocated` |
| Projector upsert | Policy | `inbound/kafka.AnalyticsConsumer`, `analyticsstore.PostgresProjection` |
| Facility location cache | Read Model | `facilitycache.Consumer.GetSlotAttributes` |
| Usable inventory per SKU | Read Model | `usecases.GetUsable` |
| Reservations by demandRef | Read Model | `usecases.GetReservationsByDemandRef` |
| Flow and Accuracy report | Read Model | `internal/analytics/report`, table `flow_accuracy_rollup` |
| facility-layout, order-management, wes-work-planning, fulfillment-execution, warehouse-ops-agent | External System | see the [Context Map](/docs/ecosystem/context-map) |

## Hotspots and where they are documented

| # | Hotspot | Documented in |
| --- | --- | --- |
| H1 | A bin id is never validated against facility-layout's slot catalogue | [Context Map](/docs/ecosystem/context-map), "What is still not built" |
| H2 | `Fragile`, `Oversized`, `HighValue` carry no placement rule | [Context Map](/docs/ecosystem/context-map); ADR 0009 |
| H3 | **Decided 2026-10-06: kept permissive** (ADR 0013/0020) — the binary default `LOCATION_LOOKUP_MODE=permissive` stays: a cold facility cache would reject every receipt, and the cluster already injects `kafka` | `cmd/inventory/main.go` `buildLocationLookup`; ADR 0013, ADR 0020 |
| H4 | ~~`ProductClassified` is raised but never published~~ **Resolved 2026-10-06**: published through the outbox on both topics (ADR 0031); `LocationRecorded` stays in-process (no consumer) | [ADR 0031](/docs/adr/0031), [Domain Events](./domain-events.md) |
| H5 | **Decided 2026-10-06: kept** (ADR 0003) — no background sweeper for timed-out reservations; expiry stays lazy | [Domain Events](./domain-events.md#lazy-expiry-no-sweeper-resolved-at-the-next-read); ADR 0003 |
| H6 | The `ReserveStock` replay guard is best-effort under concurrency | code comment on `activeReservationFor` in `reserve_stock.go` |
| H7 | **Decided 2026-10-06: event-driven** (ADR 0032, *Proposed*) — picks are confirmed from a pick-completion event published by fulfillment-execution, never a sync REST/MCP call; blocked on fields that event does not carry yet | [ADR 0032](/docs/adr/0032), [Context Relationships](./context-relationships.md) |
| H8 | Cycle-count overage is reported, never reconciled | `run_cycle_count.go` comment; [Use Cases](./use-cases.md) |
