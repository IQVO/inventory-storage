---
title: Core Domain Chart
sidebar_label: Core Domain Chart
sidebar_position: 2
description: ddd-crew Core Domain Chart for inventory-storage — business differentiation against model complexity, with the evidence behind the Core classification and the evolution note.
---

# Core Domain Chart

Following the [ddd-crew Core Domain Charts](https://github.com/ddd-crew/core-domain-charts),
this context is plotted on **business differentiation** (y) against **model
complexity** (x). The classification is the one already recorded on the
[Subdomain Classification](./subdomain-classification.md) page: **Core
subdomain, WMS tier** ("Inventory & Slotting" in the reference model). This
chart does not re-decide it; it shows where inside the Core quadrant the
context sits, and where its own capabilities fall.

```mermaid
quadrantChart
    title inventory-storage on the Core Domain Chart
    x-axis Low model complexity --> High model complexity
    y-axis Low differentiation --> High differentiation
    quadrant-1 Core
    quadrant-2 Decisive / Short-term Core
    quadrant-3 Generic
    quadrant-4 Supporting
    inventory-storage: [0.78, 0.84]
    Revocable reservations: [0.74, 0.9]
    Chaotic stow ledger: [0.62, 0.86]
    Usable inventory read: [0.3, 0.78]
    Hazmat placement and DOT segregation: [0.72, 0.44]
    Flow and Accuracy report: [0.4, 0.3]
    Bin registration: [0.18, 0.2]
```

Source: classification from `docs/docs/ddd/subdomain-classification.md`;
complexity evidence from `internal/domain/**`, `internal/application/usecases/**`,
`migrations/*.up.sql` and `docs/docs/adr/`. Coordinates are **estimates** —
ddd-crew charts are a conversation aid, not a measurement. Omitted: sibling
contexts (each repository charts itself).

## Why this position

### Business differentiation — high

| Evidence | Where |
| --- | --- |
| The reference model classifies "Inventory & Slotting (chaotic storage, bin-accurate location, stow strategy)" as **Core**: "random stow + bin-accurate tracking is a genuine operational innovation and the backbone of pick-path efficiency." | [Subdomain Classification](./subdomain-classification.md) |
| Every promise the platform makes to a customer is only as good as this context's **usable** answer — `order-management` reserves against it, `network-fulfillment` quotes it to an external network. | `GET /inventory/{sku}/usable`, `POST /reservations` |
| Revocable reservations are what keep a failed physical pick from stranding an order. | [ADR 0003](/docs/adr/0003-revocable-reservations) |
| Build-not-buy was chosen explicitly; the invariants are implemented as domain code, not a vendor model. | [ADR 0001](/docs/adr/0001-hexagonal-ports-and-adapters), [ADR 0002](/docs/adr/0002-chaotic-storage-over-fixed-slotting) |

### Model complexity — high

| Measure (counted in code on `develop`) | Value |
| --- | --- |
| Aggregate roots in `internal/domain/` | 4 — `StockUnit`, `Bin`, `Reservation`, `ProductClassification` |
| Enforced invariants with a failing-path test | 30 aggregate invariants + 4 stow-time cross-aggregate rules ([Aggregate Design Canvas](./aggregate-design-canvas.md)) |
| Lifecycle states | 5 `StockUnit` states, 4 `Reservation` statuses |
| Application use cases | 11 |
| Domain events | 11 (2 integration, 9 analytics) |
| Regulatory rule set | 9 × 9 DOT segregation matrix derived from 49 CFR §177.848 with four documented simplifications ([ADR 0010](/docs/adr/0010-dot-hazard-class-and-same-bin-segregation)) |
| Consistency mechanisms | transactional outbox, Idempotency-Key, optimistic concurrency, lazy expiry (ADRs 0017-0019, 0003) |
| ADRs | 29 |

### The capability points

- **Revocable reservations** and the **chaotic stow ledger** are the
  differentiators and carry most of the invariants — top right.
- **Usable inventory** is high-value but computationally simple (a sum over
  `StockUnit.Usable()`), so it sits in the Decisive quadrant: a short-term
  core that is valuable precisely because the ledger under it is right.
- **Hazmat placement and DOT segregation** is complex (regulation-grounded,
  fail-open/fail-closed asymmetry) but supports rather than defines the
  business — Supporting, edging towards Core.
- **Flow and Accuracy report** (ADR 0011) and **bin registration**
  (ADR 0025) are Generic: necessary plumbing any WMS needs.

## Evolution

| Stage (Wardley) | Assessment |
| --- | --- |
| Genesis | Passed — chaotic stow and revocable reservations are well understood. |
| **Custom-built** | **Where the model sits today**: a hand-built, heavily tested model (90% coverage gate on domain + application, blocking mutation testing on `internal/domain/stock`, arch-go fitness tests, Gherkin acceptance specs) that is still evolving — pick location (ADR 0025), lazy expiry, the transactional outbox (ADR 0017) and the housekeeping sweeper (ADR 0026) were all added well after the initial model (ADRs 0001-0003). |
| Product | The direction of travel: the REST and event contracts are frozen-by-version Published Language that several contexts build against unchanged. |
| Commodity | Not expected while bin-accurate truth and revocable allocation remain the platform's edge. |

The design obligation that follows from Core: protect the model hard —
no framework types in the domain, no adapter imports from the application
layer, no write access from any other bounded context.
