---
id: 0039-intra-site-relocation-stockrelocated-cloudevents
slug: /adr/0039
title: "ADR 0039: Intra-site stock relocation (RelocateStock, StockRelocated, RELOCATE tasks)"
sidebar_label: "39. Intra-site stock relocation"
sidebar_position: 39
---

# 0039. Intra-site stock relocation (RelocateStock, StockRelocated, RELOCATE tasks)

## Status

**Accepted** (2026-10-13, final-gaps decision E1). Implements this context's part of
slotting-optimization **ADR 0010 "Approved slot plans are executed as WES relocation
work"** (which supersedes slotting-optimization ADR 0007). Extends the type catalogue of
[ADR 0024](./0024-cloudevents-mandatory-envelope.md) with one type (table below). Builds on
[ADR 0017](./0017-transactional-outbox.md) (outbox), [ADR 0018](./0018-idempotency-key-middleware.md)
(Idempotency-Key), [ADR 0019](./0019-optimistic-concurrency-version-column.md) (version
columns), [ADR 0035](./0035-confirm-pick-from-task-completed.md) /
[ADR 0036](./0036-per-line-confirm-pick.md) (the TaskCompleted consumer) and
[ADR 0002](./0002-chaotic-storage-over-fixed-slotting.md) (no min/max replenishment here).

## Context

slotting-optimization proposes forward-pick slot plans (ABC velocity). A plan move is
physical floor work: someone takes the stock of one SKU out of one bin and puts it in
another bin of the same building. The fleet already has one conductor for floor work
(wes-work-planning plans, process-path-management routes, fulfillment-execution executes)
and one stock ledger (this service), joined by an existing pattern: fulfillment-execution's
`TaskCompleted` drives this ledger (confirm-pick, ADR 0035/0036). Decision E1 (Option A)
keeps that shape: WES plans and staffs `RELOCATE` work, this service records the moves.

Until now this service had **no way to move stock between bins**: a StockUnit is created by a
stow and only ever loses quantity (pick, cycle count). Every option for executing slot plans
needs a relocation command here.

Two facts of the existing model shape the design:

- A Reservation's allocation names a StockUnit **and its bin** as the pick location
  (ADR 0025). Moving reserved stock would send the picker to the wrong slot.
- Bins registered with `PUT /bins/{binId}` carry **no recorded site custody** (only the
  transfer paths record one, ADR 0031), while `BinId` values are facility-layout
  `LocationCode`s (ADR 0009) whose first of seven segments is the site.

## Decision

1. **Relocation aggregate** (`internal/domain/relocation`). A relocation moves a
   quantity of one SKU from `from_bin` to `to_bin` **of one site**. It validates: non-empty
   site, SKU and bins; `from_bin != to_bin` (same-bin rejected); quantity > 0; reason in the
   closed set `SLOTTING` | `CONSOLIDATION` | `MANUAL`; `source_ref` at most 200 characters.
   `Apply` draws the quantity from the **usable** (unreserved) quantity of the SKU's units in
   the source bin, ascending unit id; reserved quantity never moves
   (`StockUnit.RelocateOut`). It releases the source bin's occupancy, occupies the
   destination bin (capacity enforced), and creates **one new StockUnit** at the destination
   in the relocation's site custody. A source unit that reaches zero becomes `REMOVED`.
   Every rule is checked before anything changes; the ledger never goes negative.

2. **Site membership.** A bin belongs to the site when its recorded custody equals the site,
   or, when it has none, when its id is a well-formed `LocationCode` (exactly seven non-empty
   hyphen-joined segments) whose site segment equals the site (`Bin.EffectiveSite`). Any
   other bin has an unknown site and is rejected (fail closed). Transfers keep the stricter
   recorded-custody rule (`Bin.IsAtSite`, ADR 0031).

3. **Atomic and concurrent-safe.** Use case `RelocateStock` loads both bins and the source
   bin's units and, in **one** `UnitOfWork`, saves both bins, every touched source unit, the
   new destination unit, the `relocations` row and the `StockRelocated` outbox row. All
   aggregate saves are version-guarded (ADR 0019): a concurrent writer on either bin or a
   touched unit makes the save fail with `ErrConcurrentModification`, the transaction rolls
   back, and nothing moved.

4. **REST.** `POST /relocations` (body `siteCode`, `sku`, `fromBin`, `toBin`, `quantity`,
   optional `reason` defaulting to `MANUAL`, optional `sourceRef`) answers `201` with the
   relocation and `Location: /relocations/{id}`. It is behind `RequireIdempotencyKey`
   exactly like the other creating POSTs (ADR 0018): header required, same key + same body
   replays the stored response, same key + different body is `409 idempotency-key-reused` (422 until ADR 0049).
   Errors: `400` malformed/empty fields, `404 bin-not-found`, `409 bin-not-at-site`,
   `409 insufficient-usable`, `409 bin-full`, `409 concurrent-modification`,
   `422 same-bin` / `invalid-relocation-reason` / `zero-quantity` / `source-ref-too-long`.
   The use case loads both bins **before** it validates the rest of the request, so an
   unknown bin is always `404 bin-not-found`, whatever else is wrong with the body.
   `GET /relocations/{id}`
   returns the record (with its source legs and destination stock unit) or
   `404 relocation-not-found`. Migration `0036_relocations` (additive) adds the table.

5. **Event.** `com.warehouse.wms.inventory-storage.stock.StockRelocated` on
   `warehouse.inventory.events`, CloudEvents 1.0 through the outbox, `subject` = Kafka key =
   `relocation_id`, `dataschema` `urn:warehouse:inventory-storage:events:StockRelocated:v1`,
   snake_case `data`: `relocation_id`, `site_code`, `sku`, `from_bin`, `to_bin`, `quantity`,
   `reason`, `source_ref` (empty string when none), `occurred_at`. slotting-optimization
   consumes it to record plan execution progress (ADR 0010).

   | topic | `type` | `subject` |
   | --- | --- | --- |
   | `warehouse.inventory.events` | `com.warehouse.wms.inventory-storage.stock.StockRelocated` | relocation id |

6. **RELOCATE tasks.** The existing TaskCompleted consumer (same group, same DLQ, same
   shutdown semantics) gains a branch next to confirm-pick: when `task_type` is
   `RELOCATE`, use case `ApplyRelocationTask` applies the move from the **additive**
   TaskCompleted fields fulfillment-execution emits for RELOCATE tasks only — `sku`,
   `from_bin`, `to_bin`, `quantity` (the operator-confirmed count) and `relocation_ref` —
   with reason `SLOTTING` and `source_ref` = `relocation_ref` (the `task_id` when absent).
   TaskCompleted carries no site, so the site is the source bin's `EffectiveSite` and the
   destination must belong to it. Built contract-first: until the producer sends the fields
   a RELOCATE completion is dead-lettered as malformed, and no other task type changes.
   - **Dedupe** on the CloudEvent `id` (`processed_events`, consumer
     `task-completed-relocate`), claimed in the same transaction as the move.
   - **Deterministic failure -> DLQ immediately**: missing/invalid fields, unknown bin,
     bin not in the site, same bin, insufficient usable, destination full.
   - **Transient failure** (database, `ErrConcurrentModification`) retries the same message
     up to 5 times, then DLQ, as for confirm-pick.
   - **Cancellation never dead-letters**: a shutdown mid-handle leaves the offset
     uncommitted and the redelivery is absorbed by the claim.
   - PICK completions behave exactly as before (ADR 0035/0036).

7. **MCP.** A read-only `get_relocation` tool returns one relocation by id, next to the
   existing stock read tools.

## Consequences

- Slot plans can be executed end to end: wes-work-planning plans RELOCATE work,
  fulfillment-execution completes it, this ledger moves the stock and `StockRelocated` tells
  slotting-optimization the move happened.
- Each relocation creates one new StockUnit at the destination instead of merging into an
  existing unit of the SKU there. Usable-per-SKU is unchanged by a relocation; unit count
  grows with moves (units that reach zero become `REMOVED`, as after a pick).
- Only usable stock moves. A RELOCATE task whose confirmed quantity exceeds the usable
  quantity at the source (stock was reserved meanwhile) is dead-lettered for an operator,
  not partially applied.
- The derived-site rule makes `PUT /bins`-registered LocationCode bins relocatable without
  a new bin-site API; a bin id that is not a LocationCode cannot be relocated until it gets
  recorded custody.
- Dedupe is per CloudEvent id, as for confirm-pick. Two different TaskCompleted ids for
  the same `relocation_ref` would apply twice; fulfillment-execution publishes one
  completion per task, and the source ref is stored on the relocation for audit.
- One more table (`relocations`) and one more published type; no existing contract
  changes, and the TaskCompleted fields are fulfillment-execution's additive contract.
