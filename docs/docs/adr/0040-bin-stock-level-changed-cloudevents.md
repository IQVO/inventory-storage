---
id: 0040-bin-stock-level-changed-cloudevents
slug: /adr/0040
title: "ADR 0040: BinStockLevelChanged — absolute per-(bin, sku) levels from the ledger's own transaction"
sidebar_label: "40. BinStockLevelChanged"
sidebar_position: 40
---

# 0040. BinStockLevelChanged — absolute per-(bin, sku) levels from the ledger's own transaction

## Status

**Accepted** (2026-10-17). Producer side of the pick-face replenishment design (consumer first:
pick-face-replenishment is already merged and decodes this contract). Extends the type
catalogue of [ADR 0024](./0024-cloudevents-mandatory-envelope.md) with one type. Builds on
[ADR 0017](./0017-transactional-outbox.md) (outbox), [ADR 0019](./0019-optimistic-concurrency-version-column.md)
(version columns), [ADR 0021](./0021-kafka-producer-partition-key.md) (partition key) and
[ADR 0039](./0039-intra-site-relocation-stockrelocated-cloudevents.md) (the sibling `StockRelocated`).

## Context

pick-face-replenishment must notice that a forward pick slot has fallen below its minimum. It
needs the stock level of one SKU in one bin, and this service is the only authority for it. Until
now nothing on `warehouse.inventory.events` carried a bin level:

- `StockPicked` is analytics-only and names no bin.
- `StockRelocated` reports a move, not what is left in either bin.
- `StockReserved` / `ReservationRevoked` carry a SKU and a quantity, not where.

A consumer cannot rebuild a level from those deltas without owning a second ledger, and a
delta feed breaks on the first lost or duplicated message. The contract therefore ships the
**level itself**, with enough ordering information to survive at-least-once delivery.

## Decision

1. **Event.** `com.warehouse.wms.inventory-storage.stock.BinStockLevelChanged` on
   `warehouse.inventory.events`, CloudEvents 1.0 through the outbox, `dataschema`
   `urn:warehouse:inventory-storage:events:BinStockLevelChanged:v1`. Kafka key = CloudEvents
   `subject` = `<bin>:<sku>`, so every level of one pair stays on one partition in version
   order. Integration topic only; the analytics data product does not carry it (same as
   `StockRelocated`).

   `data`: `site_code`, `bin`, `sku`, `on_hand`, `reserved`, `version`, `reason`, `occurred_at`
   (snake_case, all always present). The field names and types are the ones the consumer decodes.

2. **Absolute levels, never deltas.** After the change, for the (bin, sku):
   - `on_hand` = the sum of `quantity` over the SKU's **located, still-present** units in the
     bin: every state except `UNLOCATED` (a cycle count lost track of it) and `REMOVED`
     (quantity zero, fully picked out).
   - `reserved` = the sum of `reserved` over the same units, i.e. the part of `on_hand` bound by
     active reservations (a reservation's allocation names a unit and its bin, migration
     0008). `on_hand - reserved` is what a new reservation could still draw from the bin.

3. **Persisted per-(bin, sku) version.** Migration `0037_bin_sku_levels` (additive, with a
   tested down) adds `bin_sku_levels (bin_id, sku, version, updated_at)`, primary key
   `(bin_id, sku)`, plus the index `stock_units (bin_id, sku)` the level read uses. Every event
   bumps the row with one upsert (`INSERT ... ON CONFLICT DO UPDATE SET version = version + 1`)
   and carries the value it returned: 1 for the first event of a pair, strictly increasing after
   that, surviving restarts. A consumer keeps the highest version it has seen and ignores lower
   ones.

4. **Same transaction, row lock first.** The use case that moves the ledger tracks the
   (bin, sku) pairs it changed; just before the unit of work commits, the emitter
   (`usecases.BinLevelEmitter`) advances each pair and publishes the event through the existing
   `EventPublisher` (the outbox publisher / multi publisher; there is no second publish path).
   The order inside `ports.BinLevelLedger.Advance` is what makes it race-safe: the version upsert
   takes the `(bin, sku)` row lock **first**, the levels are read **after** it (a new READ
   COMMITTED statement, so it sees every change committed before the lock was granted plus the
   transaction's own writes), and the lock is held until commit. N concurrent changes of one pair
   therefore produce versions 1..N in commit order, each event carrying the level as of its own
   commit; a rolled-back change rolls its bump back, so versions have no gaps and the event
   exists if and only if the ledger change committed. Pairs are advanced in sorted order, so two
   transactions touching the same pairs lock the version rows alike (no deadlock).

5. **One event per distinct (bin, sku) per transaction, with the final level.** The emitter
   brackets the outermost unit of work (`BinLevelEmitter.Atomically`, a drop-in for the
   `atomically` helper); nested brackets defer to it. An order whose lines are picked in one
   `ConfirmPicksForOrder` transaction, or a reservation drawn from two units of one bin, raises
   one event per (bin, sku) carrying the level after the whole transaction. If several causes
   touch one pair in a transaction the last reason wins. Events are published in the order
   pairs were first tracked, so a relocation raises the **source bin first, then the
   destination**.

6. **Nothing is raised when the level did not change.** Rejected commands fail before any pair
   is tracked; idempotent replays (`ReserveStock` replay, a replayed transfer allocation or
   stow, a redelivered CloudEvent) return before the ledger is touched; a cycle count that
   matches or finds an overage moves no ledger level.

7. **A bin with no determinable site raises nothing.** `site_code` is mandatory and a consumer
   cannot place a level without it. The site is the bin's `EffectiveSite` (recorded custody,
   else the site segment of a facility-layout location code, ADR 0039). A bin that is not
   registered, or whose id is not a location code and has no recorded custody, consumes no
   version and emits no event. Production bins are location codes.

8. **Reasons** (`reason`): `STOW`, `PICK`, `RELOCATE`, `REPLENISHMENT`, `COUNT`, `RESERVE`,
   `REVOKE`, `UNLOCATED`. Consumers ignore values they do not know.
   - `REPLENISHMENT` is **reserved for the later pick-face replenishment change (P4)**. A
     relocation maps to it only when its own reason is already `REPLENISHMENT`
     (`relocationLevelReason`); today the relocation aggregate accepts only `SLOTTING`,
     `CONSOLIDATION` and `MANUAL`, so it is never emitted yet. The code path and its test exist so
     P4 is a one-line change in the relocation aggregate, with no producer rework.
   - `UNLOCATED` is in the closed set for an unlocated receive that names a bin. This service
     has no such path (`ReceiveStock` and the inbound-receiving booking name no bin), so it is
     never emitted; the cycle-count shortfall is `COUNT`.

### What emits, by use case

| use case | trigger | `reason` | bins touched |
| --- | --- | --- | --- |
| `StowStock` | `POST /stock/stow` | `STOW` | the target bin |
| `StowTransferStock` | `POST /transfers/{transferLineId}/stow` | `STOW` | every leg's bin |
| `ReserveStock` | `POST /reservations` (new reservation only) | `RESERVE` | every bin the allocation draws from |
| `AllocateTransferStock` | `TransferAllocationRequested` (ALLOCATED only) | `RESERVE` | every bin drawn from |
| `RevokeReservation` | `DELETE /reservations/{id}`, MCP revoke | `REVOKE` | every allocation's bin |
| lazy reservation expiry | any read that resolves a timed-out reservation (`ReserveStock`'s replay lookup, `GetReservationsByDemandRef`, `ConfirmPick`, `RevokeReservation`, `ConfirmPicksForOrder`) | `REVOKE` | every allocation's bin |
| `ConfirmPick` / `ConfirmPicksForOrder` | `POST /reservations/{id}/confirm-pick`, `TaskCompleted` PICK | `PICK` | every allocation's bin |
| `RelocateStock` / `ApplyRelocationTask` | `POST /relocations`, `TaskCompleted` RELOCATE | `RELOCATE` (`REPLENISHMENT` when the relocation's reason is) | source bin, then destination bin |
| `RunCycleCount` | `POST /bins/{binId}/cycle-count`, shortfall only | `COUNT` | the counted bin, one event per SKU whose units were marked unlocated |
| `ReceiveStock`, `BookInboundReceiptLine` | receive | none | staged stock has no bin |
| `StageTransferReceipt` | destination scan | none | no ledger change (the stow raises it) |
| `RegisterBin` | `PUT /bins/{id}` | none | no stock moves |

## Consequences

- pick-face-replenishment gets a gap-tolerant, replay-tolerant level feed: it applies an event
  only if `version` is higher than the stored one, so redelivery and reordering across
  partitions cannot corrupt its read model.
- Every stock-moving use case gains one `Levels` collaborator and a `Track` call after each
  write; a nil emitter keeps the previous behaviour, which is how tests and the in-memory
  configuration run. The emitter adds, per changed pair, one upsert, one SUM and one outbox row
  to a transaction that already writes the ledger.
- Cost on the hot path: the `(bin, sku)` version row is a serialisation point for concurrent
  changes of the **same** pair (not for different pairs). Changes of one pair already contend
  on the bin row for stow, pick and relocation (ADR 0019), so this adds no new contention for
  those; reserve and revoke, which touch only units, now also queue per pair.
- Every produced level is the ledger's level at commit time, but it is not a snapshot of the
  world: a consumer must not compare levels of different pairs as if simultaneous.
- No bootstrap/backfill: a pair that has not changed since this ships has no event yet, so a
  consumer starts empty and learns each pair at its next change. A sweep that re-emits every
  pair is a follow-up if the consumer needs one.
- `LocationRecorded` and `ItemStowed` are untouched; `StockRelocated` keeps its payload and
  description. No REST endpoint is added or changed.
