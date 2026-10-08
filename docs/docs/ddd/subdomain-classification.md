---
title: Subdomain Classification
sidebar_label: Subdomain Classification
description: Why Inventory & Storage is a Core subdomain, with the justification from the reference model.
---

# Subdomain Classification

## Verdict: **Core subdomain**, WMS tier

`amazon-fulfillment-ddd.md` classifies the subdomains of a fulfillment
operation by competitive differentiation. This context maps to
**Inventory & Slotting**:

| Subdomain | Type | Why |
| --- | --- | --- |
| Fulfillment Orchestration & Optimization | Core | Continuous re-planning to the fastest/cheapest path is the differentiator. |
| **Inventory & Slotting** *(chaotic storage, bin-accurate location, stow strategy)* | **Core** | **Random stow + bin-accurate tracking is a genuine operational innovation and the backbone of pick-path efficiency.** |
| Picking | Core | Directly drives throughput and accuracy at scale. |
| Receiving / Inbound | Supporting | Necessary and non-trivial, but not the differentiator. |
| Packing & Cartonization | Supporting | Optimization matters but is a well-understood problem. |
| Labor & Workforce Management | Supporting | Important, industry-common. |
| Equipment / Automation Control | Supporting | Device-agnostic control is largely a solved integration category. |
| Order Management / ERP interface | Generic | Commodity integration surface. |

The `warehouse-systems-ddd.md` context map agrees from the other direction:
WMS is the **Core Domain** because it "owns inventory truth and order
fulfillment — the actual business differentiator for most retailers/3PLs," and
inventory truth is precisely what this service owns.

## What "Core" buys and obliges

Classifying a subdomain Core is a resourcing decision. It means:

- **Build, do not buy.** The invariants here are the differentiator, so they
  are implemented as explicit domain code with unit tests per failing path,
  not delegated to a vendor's data model.
- **Invest in correctness disproportionately.** Which is why this repo carries
  ~99% domain coverage, a `gremlins` mutation-testing pass over
  `internal/domain/...`, executable Gherkin acceptance specs, and arch-go
  fitness tests — a level of rigour that would be over-engineering for a
  Generic subdomain.
- **Protect the model hard.** No framework types in the domain, no adapter
  imports from the application layer, and no write access from any other
  bounded context.

## Where the neighbours sit

| Service | Tier | Classification | Reasoning |
| --- | --- | --- | --- |
| **inventory-storage** | WMS | **Core** | Owns inventory truth: bin-accurate location + usable inventory. |
| wes-work-planning | WES | Core | The conductor — waveless release and flow balance. `warehouse-systems-ddd.md`: WES is Core *if* operational efficiency is your differentiator (you build/tune your own DC), which is the stance this platform takes. |
| fulfillment-execution | WES | Core | The Pick/Pack/SLAM task lifecycle; throughput and accuracy at scale. |
| workforce-management | — | Supporting | Labour & workforce allocation: "important, industry-common," not the differentiator. |
| facility-layout | — | Generic | Physical warehouse structure — the same bucket as Cartonization and WCS: extract it once rather than duplicating it in every consumer. |
| product-master | WMS | Supporting | SKU product master data (handling classification, physical profile); the source of truth this service's local classification copy is fed from ([ADR 0034](/docs/adr/0034)). Classification per product-master's own ADR 0001. |

## Why this is *not* one bounded context with its neighbours

Two boundaries are worth defending explicitly, because both are tempting to
collapse.

### Inventory is not Work Planning

The reference model warns that WMS acquiring knowledge of workers, shifts,
real-time location or travel distance means "a supporting/generic concern
(labor orchestration) has leaked into the core domain (order fulfillment
truth), and every labor policy change now forces a WMS regression."

So this service has **no** task, worker, assignment, station, congestion or
travel-path concept — not even a nullable field. It publishes stock reality
and stops.

### Inventory is not Facility Layout

A `Bin` here is an id, a capacity and an occupancy. It is *not* the physical
warehouse structure. `facility-layout`'s own `CLAUDE.md` makes the case
directly:

> `inventory-storage` (WMS tier) needs location validity to accept a stow;
> `wes-work-planning` / `fulfillment-execution` (WES tier) need zone/aisle
> adjacency for travel-path and congestion reasoning. Neither owns it; both
> consume it. This is why it is its own bounded context and its own service,
> not a package bolted onto `inventory-storage`.

That is `warehouse-systems-ddd.md`'s "extract generic logic instead of
duplicating it" discipline — the same argument it makes for Cartonization —
applied to physical location.

## Same word, different model: allowed and expected

`warehouse-systems-ddd.md` calls this out as a discipline, not a bug:

> **Same word, different model is allowed — and expected.** `Task` in WMS and
> `Task` in WES are deliberately different classes with different lifecycles.
> Don't force a shared DTO/entity across the boundary; translate at the ACL.

The applications of that rule to this context are listed in the
[Ubiquitous Language](/docs/business-context/ubiquitous-language) page. The
practical enforcement is that this repository shares **no Go types** with any
sibling repository — integration happens through JSON on a Kafka topic and
through HTTP, never through an imported package.
