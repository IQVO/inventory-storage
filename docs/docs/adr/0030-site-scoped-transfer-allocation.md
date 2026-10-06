---
slug: /adr/0030
title: "ADR 0030: Site-scoped transfer allocation as a command/reply consumer"
---

# 0030. Site-scoped transfer allocation as a command/reply consumer

## Status

Accepted

## Context

The network-inventory-planning context plans inter-warehouse transfers
to rebalance stock across the fulfillment network (its transfer saga:
Draft → … → Allocating → Allocated → …). Planning must never touch
physical stock itself — inventory-storage is the authoritative owner of
what is held where and how much of it is usable — so the saga's
allocation step is a command/reply exchange: planning publishes a
`TransferAllocationRequested` command per transfer line, and
inventory-storage answers allocated or rejected.

Three facts about this service made the naive implementation wrong:

1. **No site custody existed.** Every `StockUnit` row was site-less: a
   single-SKU usable count spanning the whole fleet. A global count
   cannot prevent the wrong facility from donating stock to a transfer —
   the exact failure the network-transfer design warns about. Site scope
   had to become a recorded, validated fact BEFORE any transfer could be
   allocated.

2. **The existing reservation idempotency was demandRef-shaped.**
   `ReserveStock`'s guard returns an existing ACTIVE reservation for the
   same `demandRef` + `(sku, quantity)` — a client-retry idempotency for
   order-management's synchronous calls. A transfer command is a
   different contract: planning may retry with a NEW event id for the
   same line, may replay the SAME id after a broker redelivery, and must
   get a stable answer for the line either way, forever (the ledger is
   the audit trail of what was decided, not just a dedupe cache). A
   demandRef lookup cannot carry that.

3. **An audit found `ReserveStock.Execute` saving its mutated
   StockUnits OUTSIDE the `UnitOfWork` closure** — a failed reservation
   save left stock decremented with nothing holding it. Any new
   allocation path built on the same shape would inherit the bug, and a
   transfer allocation touches MORE state (stock, reservation, ledger,
   outbox), so the blast radius would be larger.

## Decision

1. **Site custody is a validated, nullable fact.** `StockUnit` carries a
   `SiteID` (value object, non-empty); migration 0030 adds nullable
   `stock_units.site_id` (and `bins.site_id` for the registration
   path). Legacy site-less rows stay exactly as they are — **unallocatable
   for transfers** (`FindBySKUAtSite` excludes NULL-site rows) while
   continuing to serve ordinary demand unchanged. No default site is
   ever invented: guessing custody would recreate the wrong-facility
   failure the column exists to prevent.

2. **A dedicated `transfer_allocations` ledger** (migration 0031) holds
   one row per decided transfer line, **DB-unique on
   `transfer_line_id`**, with the requested quantity, the outcome
   (`ALLOCATED`/`REJECTED`), the reservation correlation (nullable) and
   the rejection reason (nullable). Replaying a line returns the
   original outcome without touching stock again; a same-line-id /
   different-payload command is answered with an
   `IDEMPOTENCY_CONFLICT` rejection while the original decision stands.

3. **The `AllocateTransferStock` use case** draws stock via
   `StockRepo.FindBySKUAtSite` and holds it in the existing
   `Reservation` aggregate (correlated to the transfer via the demandRef
   `transfer:<transfer_id>:<line_id>`). Stock saves + reservation +
   ledger row + reply-event outbox rows commit in **ONE UnitOfWork**;
   the ledger insert's unique constraint is the idempotency anchor. The
   closed rejection set is `ORIGIN_SITE_UNKNOWN`, `INSUFFICIENT_USABLE`,
   `IDEMPOTENCY_CONFLICT`.

4. **A new inbound Kafka consumer**
   (`internal/adapters/inbound/kafka`, env-gated by
   `TRANSFER_ALLOCATION_CONSUMER_MODE=kafka` + `DATABASE_URL`, group
   overridable via `TRANSFER_ALLOCATION_CONSUMER_GROUP`) consumes
   `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested`
   from `warehouse.network-inventory-planning.events`, following the
   at-least-once atomicity checklist: FetchMessage (never
   auto-committing ReadMessage), commit only after the handler settles,
   capped-backoff retry of the SAME message on transient errors,
   deterministic poison (malformed command, non-CloudEvents, unknown
   type) logged and committed past.

5. **Replies are published on `warehouse.inventory.events`** (through
   the transactional outbox) as
   `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated`
   (key/subject = reservation id; data carries the transfer correlation,
   per-unit allocations with pick bins, and `expires_at`) and
   `…reservation.TransferStockAllocationRejected` (key/subject =
   transfer_line_id; data carries the closed reason). Both are
   integration-topic only — the analytics projection has no transfer
   dimension. `ReserveStock`'s atomicity bug is fixed in the same change
   (stock saves moved inside its UnitOfWork closure) with a regression
   test.

## Consequences

- **Legacy stock is temporarily invisible to transfers.** Until rows
  gain a site (via site-scoped stow or a later backfill), every
  transfer command against them answers `ORIGIN_SITE_UNKNOWN`. That is
  the intended fail-closed behaviour, not an outage — the alternative
  (silently allocating unscoped stock) is precisely the bug this
  prevents. An operator backfill can flip sites on later; nothing in
  this design blocks it.

- **The ledger is append-only per line.** A transfer line can never be
  re-decided, even after its reservation is revoked or expires: the
  saga's later states (rejection-after-revoke, re-request) must use a
  new line id from planning. This keeps the audit trail honest and the
  reply stream replay-safe, at the cost of planning owning line-id
  freshness.

- **One more consumer group to operate.** The group id is
  env-configurable so a local process can never steal the live
  deployment's partitions; the feature is dark by default
  (`TRANSFER_ALLOCATION_CONSUMER_MODE=off`) until a deployment opts in.

- **The reply events lengthen the integration contract.** Both new
  types are additive and documented in `apis/asyncapi.yaml` and the
  CloudEvents ADR catalogue; existing consumers ignore unknown types by
  rule, so the fleet-wide rollout is decoupled.
