---
id: 0050-deterministic-forward-first-allocation
slug: /adr/0050
title: "ADR 0050: Reservations draw the forward pick face first, in one deterministic order (amends 0003 section 3)"
sidebar_label: "50. Forward-first allocation (amends 0003 s3)"
sidebar_position: 50
description: "ADR 0050 — ReserveStock draws a SKU's forward pick face (from the approved slot plan, held as a local read model fed by SlotPlanApproved) before its reserve bins, then smaller usable first, bin id, stock unit id. The order is the domain policy allocation.Order, not SQL. Amends ADR 0003 section 3 (first-fit); transfer allocation and sealed-case reservation are unchanged."
---

# 0050. Reservations draw the forward pick face first, in one deterministic order

## Status

Accepted (2026-11-05). Amends [ADR 0003](./0003-revocable-reservations.md)
section 3 ("draws first-fit") only; the rest of 0003 stands. ADR 0003 is history
and is not edited.

## Context

`ReserveStock` summed the usable quantity of every `StockUnit` of the SKU and
drew greedily in the order the repository returned the rows. `FindBySKU` has no
`ORDER BY`, so the "first fit" was Postgres heap order: not stable, and
unrelated to where the SKU is meant to be picked. Two consequences:

- Slotting (slotting-optimization) assigns each fast SKU a forward pick face
  (golden zone) and pick-face-replenishment keeps it filled, yet a pick could
  drain a reserve or overstock bin while the pick face sat full. Slotting then
  changed nothing, and a replenishment trigger (forward bin low) was never
  raised by picking.
- The same inputs could allocate differently on two runs.

## Decision

1. **A named domain policy, `allocation.Order`** (`internal/domain/allocation`),
   is the one place that orders a SKU's candidate units. It is a pure function
   of the units and of the SKU's forward faces. The rule, a strict total order
   compared in this sequence:

   1. role: units in a FORWARD bin of the SKU before units in any RESERVE bin;
   2. usable quantity ascending (the unit closest to fully consumed first, so a
      draw empties and frees small holdings before it breaks a big one);
   3. bin id ascending (byte order);
   4. stock unit id ascending (byte order).

   The draw takes each unit's usable quantity in that order until the request is
   covered. When the forward bin holds less than requested, the forward
   remainder is taken first and the rest spills to reserve in the same order
   (partial forward). When the forward bin is empty the reserve bins serve the
   pick, so the forward bin still falls under its minimum and the existing
   `BinStockLevelChanged` -> pick-face-replenishment trigger still fires.
   `StockUnit` carries no lot, expiry or receipt date, so there is no
   FEFO/FIFO rule to preserve; when one is added it slots in between 1 and 2.
2. **A bin's role is per SKU, and it is known locally.** A bin is FORWARD for a
   SKU when slotting-optimization's latest approved slot plan assigns that SKU
   to it. inventory-storage consumes `SlotPlanApproved` (already published by
   slotting-optimization on `warehouse.slotting-optimization.events`, with
   `site_id`, `plan_id`, `approved_at` and the FULL `assignments[{sku, slot}]`
   map) into two local tables, `forward_pick_face_plans` and
   `forward_pick_faces` (migration 0039). The consumer claims the CloudEvents
   id in `processed_events` in the same transaction as the replacement of the
   site's map; a plan applies only if its `approved_at` is later than the held
   plan's, or equal with a higher `plan_id`, so a late older plan is ignored.
   Reservation time reads only these tables: no REST or MCP call to a sibling.
   No producer change is needed (the consumer reads what slotting already
   publishes), so the consumer-first ordering is trivially met. Until the first
   plan is consumed, no SKU has a forward face and the order degrades to rules
   2 to 4.
3. **The Postgres adapter cannot disagree with the policy.** SQL only answers
   "which bins are this SKU's forward faces" and "all stock units of the SKU";
   the ordering is done by `allocation.Order` in Go. A real-Postgres test
   (testcontainers) proves forward-first on real rows.
4. **Scope.** The rule applies to `ReserveStock` (loose eaches, per owner).
   - `ReserveFullCases` (sealed cases) is unchanged: it takes whole cases oldest
     container first, a different physical operation, not an each pick from a
     pick face.
   - `AllocateTransferStock` is unchanged: a transfer is a bulk inter-site move
     that should be served from reserve, not strip the pick face that serves the
     origin site's own orders. (It still draws in repository order; making that
     order deterministic is a separate, follow-up change.)
5. **Contracts.** The `Allocation` shape is unchanged (it already carries the
   `binId`); the bin role is not persisted or published. `openapi.yaml`
   `reserve-stock` description and `asyncapi.yaml` (a consumed
   `warehouse.slotting-optimization.events` channel and `SlotPlanApproved`
   message) are updated. `SLOT_PLAN_CONSUMER_GROUP` is optional (default
   `inventory-storage-slot-plan`), so existing deployments boot unchanged.

## Alternatives considered

- **Keep pure first-fit.** Rejected: unstable and blind to slotting and
  replenishment.
- **ORDER BY in SQL.** Rejected: hides the rule in a query, cannot see the
  forward-face fact, and lets adapter and domain drift apart.
- **Replenishment-aware** (prefer the bin whose draw triggers no replenishment,
  or the pick-face-replenishment min/max policy): couples this context to a
  policy it does not own and needs a second read model; forward-first already
  makes replenishment demand arise naturally.
- **Slotting-class aware** (prefer by ABC class / golden-zone distance): the
  plan's assignments already encode the outcome of that analysis; re-deriving it
  here duplicates slotting-optimization.
- **Ask a sibling at reservation time:** violates the local-data rule.

## Consequences

- With a full forward bin and a full reserve bin a pick consumes the forward
  bin first; with the forward bin empty the reserve bin serves it.
- Forward bins drain faster and trigger replenishment sooner: expected and
  intended.
- The forward map is as fresh as the last consumed plan; a site that never
  approves a plan keeps the deterministic order without forward preference.
- Existing allocations are unchanged; only new reservations use the order.
