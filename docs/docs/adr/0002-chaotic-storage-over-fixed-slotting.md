---
id: 0002-chaotic-storage-over-fixed-slotting
slug: /adr/0002-chaotic-storage-over-fixed-slotting
title: 0002. Chaotic (random) stow over fixed slotting
sidebar_label: 0002. Chaotic storage
description: ADR 0002 — model storage as chaotic stow with mandatory dual scanning, rather than fixed product locations.
---

# 0002. Chaotic (random) stow over fixed slotting

## Status

Accepted. Present from the first implementation of the `Bin` and `StockUnit`
aggregates; it is the decision that shapes the rest of this bounded context.

## Context

A warehouse must decide where an inbound unit goes. There are two families of
answer.

**Fixed slotting** gives every SKU a home: `SKU-1` lives in aisle A, bay 3.
Finding stock needs no system — a human can walk to the shelf. Capacity
planning is per-SKU and legible.

**Chaotic (random) stow** gives a SKU no home at all. The unit goes wherever
there is free space; the associate scans the item, scans the location, and the
system records the exact bin.

The reference model (`fulfillment-reference-ddd.md`) documents chaotic stow as the
approach used at fulfillment-centre scale, and states the reasoning as a
first-class domain rule rather than an operational accident:

> maximizes space utilisation, shortens pick paths, prevents zone congestion,
> and gives redundancy (one seller's 500 units spread across ~400 locations on
> multiple floors).

It also states the cost, just as plainly:

> Placing an item without scanning is precisely how inventory becomes "lost" —
> the physical item exists but the system does not know where.

The forces:

- **Space is the scarcest resource.** Fixed slotting reserves space for a
  product whether or not it is in stock; that reserved-but-empty space is pure
  waste at a million square feet.
- **Pick-path length dominates throughput.** Concentrating a fast-moving SKU in
  one place concentrates travel and congestion there too.
- **Physical failure is routine.** A blocked pod or a jammed aisle should take
  out a fraction of a SKU's availability, not all of it.
- **Without a home shelf, the record *is* reality.** There is no physical
  fallback. If the record is wrong, the item is lost even though it is sitting
  twenty feet away.

## Decision

**We will model storage as chaotic stow, and make the two rules that keep it
survivable hard domain invariants.**

1. **No SKU affinity anywhere in the model.** The `Bin` aggregate has an id, a
   capacity and an occupancy — and deliberately *no* SKU field, no preferred
   product, no reserved-for. Any SKU may occupy any bin with room. The absence
   of that field is the decision.

2. **A stow requires both an item-scan and a location-scan.**
   `stock.NewStockUnit` returns `ErrStowRequiresItemAndLocation` if either the
   SKU or the `BinId` is empty. The aggregate refuses to exist rather than
   record a half-truth. The HTTP adapter maps this to `400 Bad Request`.

3. **Bin capacity is enforced in the domain.** `Bin.Occupy` returns
   `ErrBinFull` when `occupied + qty > capacity`, mapped to `409 Conflict`.
   Overflow would be physically re-homed into a neighbouring bin, unscanned —
   the exact loss mode the design exists to prevent. Under chaotic storage,
   rejecting a stow costs nothing: pick another bin.

4. **Loss is an explicit state, never silent.** `StateUnlocated` means the item
   exists but its location is unknown. Unlocated stock contributes **zero**
   usable quantity, and `RunCycleCount` is the operation that discovers and
   records it, emitting `DiscrepancyDetected` and `ItemUnlocated`.

5. **A cycle-count overage is reported, never auto-reconciled.** More stock
   present than recorded means goods entered the building without a receipt.
   Inventing `StockUnit`s to match would corrupt the ledger, so the discrepancy
   is raised for a separate receiving/audit process.

## Consequences

### Easier

- **Every free slot is usable by every SKU** — no reserved-but-empty space.
- **Redundancy comes for free**, which is what makes
  [revocable reservations](./0003-revocable-reservations.md) actually useful:
  when a specific pick fails there is almost always another holding of the same
  SKU to re-satisfy from.
- **The `Bin` aggregate stays trivially small** — three fields, five methods,
  fully unit-testable — because slotting policy is not its concern.
- **A SKU's stock is naturally partitioned across many `StockUnit`s**, so there
  is no single hot row per SKU to contend on.

### Harder

- **The software is now load-bearing for physical findability.** There is no
  human fallback. This is the cost that justifies the disproportionate quality
  investment in this repo.
- **Reads fan out.** "How much `SKU-1` do we have?" means summing every
  `StockUnit` for that SKU rather than reading one row —
  `StockRepo.FindBySKU` plus a loop, on every `GetUsable` and every
  `ReserveStock`.
- **A reservation may span several bins**, so revoke and confirm-pick have to
  walk a list of `Allocation`s and touch several aggregates.
- **Cycle counting becomes mandatory operational work**, not an occasional
  audit — it is the only mechanism that detects divergence.
- **Reconciliation is deliberately coarse.** A shortfall marks whole
  `StockUnit`s `UNLOCATED` rather than splitting located from lost portions.
  This under-reports usable rather than over-reporting it, which is the safe
  direction, but it does discard information a finer model would keep.

### Deferred

Placement *policy* — which free bin an item should go to given temperature
class, hazmat or size — is deliberately **not** modelled here. It belongs to
`facility-layout`'s `PlacementRule` aggregate. Pulling it in later would
re-introduce fixed slotting through the back door; see the
[Context Map](/docs/ecosystem/context-map).
