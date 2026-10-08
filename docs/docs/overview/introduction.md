---
slug: /overview
title: Introduction
sidebar_label: Introduction
description: What the Inventory & Storage bounded context is, what it owns, and what it deliberately does not own.
---

# Inventory & Storage

:::warning[Study project]
This documentation site is an educational Domain-Driven Design exercise. It
follows real industry-standard patterns and terminology, but it is **not a
production system** and is **not affiliated with, endorsed by, or
representative of any real-world company**.
:::

**Inventory & Storage** is the WMS-tier authoritative record of *what is held
where, and what portion of it is usable*.

It is one of the Go services that make up the `warehouse-systems` platform.
This one sits in the **WMS tier** (the "what and where" layer) and is a **Core
subdomain**: it owns inventory truth. Everything else on the platform — work
planning, labour, task execution — asks this service what stock reality is,
and none of them are allowed to write to its aggregates.

## The one sentence that explains the design

> Every physical item has exactly one known bin, **or** it is flagged
> `Unlocated`.

That is the invariant the whole bounded context exists to protect. Chaotic
stow, the item-scan + location-scan rule, cycle counting, and the `Unlocated`
state are all consequences of taking that sentence literally.

## What it owns

| Capability | What that means here |
| --- | --- |
| **Stock ledger** | `StockUnit` aggregates — a quantity of a SKU at a specific bin, with a lifecycle state. |
| **Bin-accurate location** | Chaotic (random) stow: any SKU may occupy any free bin; the system records the exact bin it landed in. |
| **Capacity enforcement** | A `Bin` has a capacity; the sum of stock stowed into it may never exceed that capacity. |
| **Revocable reservations** | Allocation is a `Reservation` with a timeout that can always be revoked and re-satisfied from a different physical holding. |
| **Usable inventory** | The read model that actually constrains release: on-hand minus active reservations minus held/unlocated stock. |
| **Cycle counting** | Verifying a bin's physical contents against system records, reconciling shortfalls by flagging stock `Unlocated`. |

## What it deliberately does not own

Naming the boundary is as important as naming the capability. This service:

- **does not pick, pack, ship, or route associates** — that is
  `fulfillment-execution` (task lifecycle) and `wes-work-planning` (release
  and flow balance);
- **does not plan labour or headcount** — that is `workforce-management`;
- **does not own product master data** — SKU handling classification (hazmat,
  temperature class, DOT hazard class) belongs to `product-master`; this
  service keeps a local copy of it for its stow placement and DOT segregation
  rules ([ADR 0034](/docs/adr/0034));
- **does not model the physical building** (site, area, zone, aisle, bay,
  level, position) — that is `facility-layout`, a separate Generic subdomain;
- **does not decide what to buy or forecast demand** — that is upstream of the
  whole platform;
- **does not decide what a bin physically is** — `PUT /bins/{binId}`
  (ADR 0025) only registers a bin id and its capacity so this service can
  enforce occupancy; the slot's place in the building stays
  `facility-layout`'s.

Per `warehouse-systems-ddd.md`, keeping worker identity, real-time floor
conditions and task sequencing *out* of the inventory system of record is what
lets that record stay stable and auditable while labour policy changes weekly.

## How it fits with its neighbours

```mermaid
flowchart LR
  subgraph WMS["WMS tier — what & where"]
    INV["inventory-storage<br/>(Core)<br/>stock truth"]
    OM["order-management<br/>order intake · allocation"]
    PM["product-master<br/>(Supporting) — SKU master data"]
  end
  subgraph WES["WES tier — when & in what order"]
    WP["wes-work-planning<br/>(Core) — the conductor"]
    FE["fulfillment-execution<br/>(Core) — Pick/Pack/SLAM"]
    WM["workforce-management<br/>(Supporting) — headcount"]
  end
  subgraph GEN["Generic subdomain"]
    FL["facility-layout<br/>physical warehouse map"]
  end

  INV -- "warehouse.inventory.events<br/>StockReserved / ReservationRevoked" --> WP
  WP -- "warehouse.work-planning.events" --> FE
  WM -- "warehouse.workforce.events" --> WP
  FE -- "warehouse.fulfillment.events" --> WP
  FL -- "warehouse.facility.events<br/>zone classification cache" --> INV
  PM -- "warehouse.product-master.events<br/>ProductClassified local copy" --> INV
  OM -. "sync REST<br/>reserve / revoke" .-> INV

  classDef core fill:#0f766e,stroke:#134e4a,color:#fff;
  classDef supp fill:#7c3aed,stroke:#4c1d95,color:#fff;
  classDef gen fill:#64748b,stroke:#334155,color:#fff;
  class INV,WP,FE,OM core;
  class WM,PM supp;
  class FL gen;
```

Solid edges are Kafka topics; the dotted edge is a synchronous REST call
into this service (`order-management` is the only command caller among the
sibling contexts; `network-fulfillment` and `warehouse-ops-agent` only
read). Two topics feed this service's stow rules: `facility-layout`'s, cached
for zone attributes (ADR 0013), and `product-master`'s, kept as the local
product classification copy (ADR 0034). See
[the context map](/docs/ecosystem/context-map) for every wire, including the
other consumed topics (`TaskCompleted`, ADR 0035; network-inventory-planning's
transfer commands, ADR 0030) and the synchronous callers.

## Where to go next

- **[Architecture](./architecture.md)** — the hexagonal layering and the strict
  dependency rule, enforced by executable fitness tests.
- **[Quickstart](./quickstart.md)** — run the service and exercise every
  endpoint with `curl`.
- **[Domain vision](/docs/business-context/domain-vision)** — why this service
  exists in this shape.
- **[API Reference](/docs/api-reference)** — generated from the real,
  Spectral-linted `apis/openapi.yaml`.
- **[DDD artifacts (ddd-crew)](/docs/ddd/ddd-artifacts)** — core domain
  chart, bounded context canvas, aggregate design canvas, EventStorming,
  message flows, UML/ER/sequence diagrams, all derived from the code.
- **[ADRs](/docs/adr)** — the consequential decisions, in Nygard format.
