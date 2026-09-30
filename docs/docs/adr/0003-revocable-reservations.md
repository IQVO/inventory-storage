---
id: 0003-revocable-reservations
slug: /adr/0003-revocable-reservations
title: 0003. Revocable reservations over hard allocation
sidebar_label: 0003. Revocable reservations
description: ADR 0003 — allocation is a revocable, expiring, SKU-scoped claim rather than a hard binding to a physical holding.
---

# 0003. Revocable reservations over hard allocation

## Status

Accepted. The central storage/consistency decision of this bounded context,
present from the first implementation of the `Reservation` aggregate.

## Context

When demand arrives, inventory must be committed to it. The obvious model is a
**hard allocation**: bind 6 units of `SKU-1` from `StockUnit` X in bin `A-1-1`
to order 42, permanently, until the pick completes. It is simple, it is
auditable, and it is what most systems do first.

It fails on contact with a warehouse floor, because **physical delivery fails
routinely**:

- the pod carrying `A-1-1` is blocked or out of service,
- the tote is lost between pick and pack,
- a chute jams,
- the picker short-picks — 4 units present where the system said 6,
- the units are there but damaged.

Under hard allocation, each of those leaves order 42 holding a claim that can
never be satisfied — and, worse, **no other order can use that stock either**,
because it is allocated. The order is *stranded*, and unwinding it requires an
out-of-band operator correction while the SLA burns.

`CLAUDE.md` states the requirement directly:

> allocation [must be] a **revocable reservation** so physical delivery
> failures never strand an order.

Two alternatives were available and rejected:

- **Distributed transaction / two-phase commit** spanning reserve and physical
  completion. This means holding a lock for minutes across a process that
  fails routinely, with an inventory service that cannot make progress while a
  tote is missing. Wrong tool: the failure is not a coordination failure, it is
  a physical one.
- **Optimistic, no reservation at all** — check usable at release time and hope.
  This over-promises the moment two demands race, which is exactly what usable
  inventory exists to prevent.

Working in our favour: [chaotic storage](./0002-chaotic-storage-over-fixed-slotting.md)
spreads a SKU across many bins, so if one holding cannot be delivered, another
almost always can.

## Decision

**We will model allocation as a `Reservation`: a revocable, expiring,
SKU-scoped claim against *usable* inventory.**

1. **Revocable.** `Reservation.Revoke()` transitions `ACTIVE → REVOKED`;
   `RevokeReservation` then walks the reservation's recorded `Allocation`s and
   calls `StockUnit.ReleaseReservation(qty)` on each, returning exactly what was
   taken to exactly the units it came from. Usable goes back up immediately.

2. **Expiring.** Every reservation carries `expiresAt = createdAt + timeout`
   (`DefaultReservationTimeout` = 30 minutes), with time supplied by the
   `Clock` port. `Confirm` refuses an expired reservation (`ErrExpired`), so a
   stale claim can never turn into a pick.

3. **SKU-scoped, not bin-scoped.** `ReserveStock` sums usable across every
   `StockUnit` for the SKU and draws first-fit, recording an `Allocation` per
   unit touched. A reservation may therefore span several bins. Crucially,
   nothing binds a *future* reservation to those same units — revoke, reserve
   again, and the demand can be satisfied from an entirely different holding.
   This is what "re-allocatable against a different holding" means in code.

4. **Constrained by usable, not on-hand.** `qty > totalUsable` fails with
   `ErrInsufficientUsable` **before** anything is mutated, and
   `StockUnit.Reserve` re-checks per unit.

5. **No double-consume.** `ACTIVE` is the only status with outgoing
   transitions; `Revoke`, `Confirm` and `Expire` all return
   `ErrAlreadyResolved` otherwise. Revoking twice would return the quantity to
   usable twice — inventing stock.

6. **The reservation is the compensable step.** Revoke is the compensation.
   There is no distributed transaction and no lock held across the physical
   operation.

## Consequences

### Easier

- **A failed pick never strands an order.** Revoke, and the stock is
  immediately available to any demand, including the same one re-issued.
- **No long-lived locks.** The service stays available while a tote is missing;
  a stuck reservation costs at most its timeout.
- **Correct promises under concurrency.** Reserving against usable rather than
  on-hand means two demands cannot both be promised the same unit.
- **The downstream projection is trivially simple.** Because the two published
  events are exact inverses, `wes-work-planning` maintains
  `UsableInventoryObserved` with one decrement and one increment.
- **Recovery policy stays out of this context.** This service makes recovery
  *possible*; deciding what to do next (re-release, re-prioritise, split) is a
  WES-tier decision.

### Harder

- **Usable is not on-hand, and callers must understand that.** Hence
  `GET /inventory/{sku}/usable` as a first-class endpoint rather than a
  computed field.
- **Revoke and confirm-pick must be exact.** They walk `Allocation`s and touch
  several aggregates through several repositories, so ordering and error
  handling are the use case's responsibility. `ConfirmPick` touches three
  aggregates (`Reservation`, `StockUnit`, `Bin`).
- **Reservations are state that must be resolved.** An unresolved reservation
  holds quantity out of usable.
- **A no-double-consume bug would corrupt the ledger silently.** Which is why
  `ErrAlreadyResolved` has dedicated tests at the aggregate, use-case and
  Gherkin levels.

### Lazy expiry (decided 2026-09-26)

**There is no expiry sweeper, by choice.** `Reservation.Expire()` and the
`ReservationExpired` event are modelled, unit-tested, and now genuinely
raised — but the trigger is a *read*, not a timer. Every use case that looks
up a `Reservation` (`GetReservationsByDemandRef`, `RevokeReservation`,
`ConfirmPick`, and `ReserveStock`'s own idempotency lookup) first checks
whether it is `ACTIVE` and past `expiresAt`; if so, it transitions it to
`EXPIRED`, releases its allocated quantity back to usable, persists both
changes, and publishes `ReservationExpired` — all before returning. A
second, later read of the same reservation is a no-op (already `EXPIRED`,
`CONFIRMED`, or `REVOKED` reservations are never re-processed). `Confirm`
still independently refuses past `expiresAt` as a defence-in-depth check, but
in practice the lazy-expiry check on `ConfirmPick`'s own read usually gets
there first.

The remaining trade-off: a reservation nobody revokes and nobody reads again
still holds quantity out of usable indefinitely — lazy expiry only resolves a
reservation at the moment something looks it up. That gap is accepted for
now given this service's read volume (`GetReservationsByDemandRef` and the
reservation list surface are read regularly); if it becomes a problem, the
fix is a scheduled *read* (not a new domain concept) rather than reopening
this decision.
