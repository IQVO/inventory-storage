---
title: Domain Vision
sidebar_label: Domain Vision
description: Why the Inventory & Storage bounded context exists, and the business problem it solves.
---

# Domain Vision

## The platform-level vision

From `fulfillment-reference-ddd.md`, the vision statement for the whole
fulfillment domain:

> Fulfil customer orders from many disparate SKUs at massive scale by receiving
> goods, storing them under chaotic storage, and reliably picking, packing, and
> shipping them along the fastest/cheapest path — continuously re-optimizing
> physical work in real time.

## This context's slice of it

**Inventory & Storage owns the "storing them under chaotic storage" clause,
and the truth that everything downstream depends on.**

A fulfillment centre is 800,000–1,000,000 sq ft — "18 football fields under one
roof" — holding millions of units across hundreds of thousands of bins.
Nothing about that scale works unless one system can answer, authoritatively
and instantly:

1. **Where is this SKU?** Not "in the building" — *in which bin*, and how many.
2. **How much of it can I actually promise?** Not on-hand — *usable*.

Those two questions are the entire reason this bounded context exists.

## Why "where" is the hard part

Chaotic (random) stow means there is no fixed home for a product. An inbound
unit of `SKU-1` goes to whatever bin has room — possibly a different floor from
the last unit of `SKU-1` that arrived. That is a deliberate operational choice
(see [Chaotic storage](./chaotic-storage.md)), and it buys real throughput. But
it moves the entire burden of "where is it" from the physical layout into the
software.

The consequence is stated bluntly in the reference material:

> Placing an item without scanning is precisely how inventory becomes "lost" —
> the physical item exists but the system does not know where.

So the core invariant of this context is not a database constraint, it is an
operational one:

> **Every physical item has exactly one known bin, OR is explicitly flagged
> `Unlocated`.**

There is no third state, and specifically there is no *silent* third state. A
stow that arrives without both an item-scan and a location-scan is rejected —
`ErrStowRequiresItemAndLocation` — rather than being accepted "so the operator
isn't blocked." Accepting it would create exactly the invisible loss the
invariant exists to prevent. When reconciliation *does* find a shortfall, the
system says so out loud: it emits `DiscrepancyDetected`, marks the affected
stock `Unlocated`, and stops counting it as usable.

## Why "how much" is the other hard part

The naive answer — on-hand quantity — is wrong, and being wrong here strands
customer orders.

Between "the system says there are 10" and "10 units leave the building" sit a
pod that never arrives, a tote that gets lost, a chute jam, a short pick, and a
damaged unit. The reference model's WES tier re-plans continuously *because*
physical execution fails routinely. An inventory system that hands out hard,
irrevocable allocations against on-hand quantity produces one of two failure
modes, both bad:

- it over-promises (allocating stock that is already spoken for), or
- it strands (an order holds an allocation against a unit that can never be
  delivered, and no other order can use that stock either).

This context avoids both by making **usable inventory** the constrained
quantity and **reservations revocable**. See
[Usable inventory](./usable-inventory.md) and
[Revocable reservations](./revocable-reservations.md).

## Position in the WMS / WES / WCS layering

| Tier | Time horizon | Answers | This service |
| --- | --- | --- | --- |
| **WMS** | minutes → days | *What needs to happen, and why* | ← **here** |
| WES | seconds → minutes | *Who does it, right now, in what order* | `wes-work-planning`, `fulfillment-execution` |
| WCS | ms → seconds | *How the machine performs the next step* | not modelled on this platform |

Being WMS-tier is what justifies the deliberate omissions listed on the
[Introduction](/docs/overview). `warehouse-systems-ddd.md` is explicit about
the cost of blurring this line:

> if WMS assigns specific workers, the inventory/order system of record is now
> coupled to real-time floor conditions — every re-slot of the warehouse or
> shift-pattern change forces a change to the system that must otherwise stay
> stable and auditable.

The inventory ledger has to be the *stable* thing. Task sequencing, congestion,
interleaving and labour policy all change weekly; stock truth must not.

## Design consequences, traced back

| Vision clause | Design consequence in this repo |
| --- | --- |
| Any item, any free bin | `Bin` has capacity but no SKU affinity; `StockUnit` is `(SKU, bin, qty)` |
| An unscanned stow loses inventory | `NewStockUnit` rejects a missing SKU **or** missing bin |
| A bin cannot hold more than it holds | `Bin.Occupy` rejects when `occupied + qty > capacity` |
| Physical delivery fails | `Reservation.Revoke()` returns quantity to usable; reservations expire on a timeout |
| Only usable stock constrains release | `GetUsable` projects on-hand − reserved − held/unlocated |
| Loss must be visible, never silent | `ItemUnlocated`, `DiscrepancyDetected`, `CycleCountCompleted` |
| WES needs stock reality, not write access | Integration events on `warehouse.inventory.events`; no inbound writes |
