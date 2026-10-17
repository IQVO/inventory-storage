---
id: 0041-apply-replenish-task-as-replenishment-relocation
slug: /adr/0041
title: "ADR 0041: A completed REPLENISH task is applied as a REPLENISHMENT relocation"
sidebar_label: "41. REPLENISH task as relocation"
sidebar_position: 41
---

# 0041. A completed REPLENISH task is applied as a REPLENISHMENT relocation

## Status

**Accepted** (2026-10-17). Ledger side of the pick-face replenishment design, **consumer first**:
the producers (fulfillment-execution emitting `TaskCompleted` with `task_type = REPLENISH`,
wes-work-planning planning that work) land after this change, and pick-face-replenishment
(already merged) closes its request from the `StockRelocated` this change produces. Reuses the
mechanism of [ADR 0039](./0039-intra-site-relocation-stockrelocated-cloudevents.md) (the RELOCATE
path) and switches on the `REPLENISHMENT` level reason that
[ADR 0040](./0040-bin-stock-level-changed-cloudevents.md) wired but could not yet reach.

## Context

pick-face-replenishment raises `ReplenishmentRequested` when a forward pick slot runs low. The
refill is physical work: fulfillment-execution runs a REPLENISH task and publishes `TaskCompleted`
with the additive fields `sku`, `from_bin`, `to_bin`, `quantity` (the RELOCATE shape) and
`request_ref`, the replenishment **request id**. This service is the stock ledger, so it must
move the stock. pick-face-replenishment then completes the request when it sees a
`StockRelocated` whose `reason` is `REPLENISHMENT` and whose `source_ref` equals its request id.

A replenishment move is, for the ledger, exactly an intra-site relocation: units of one SKU leave
a reserve bin and arrive in a forward bin of the same site, on-hand unchanged. Nothing about it
needs a second stock-moving code path.

## Decision

1. **Reuse the relocation mechanism.** `usecases.ApplyReplenishTask` drives the same
   `RelocateStock` as `POST /relocations` and `ApplyRelocationTask`, with reason `REPLENISHMENT`.
   It is a **separate use case** next to `ApplyRelocationTask`, not a generalisation of it:
   the RELOCATE path, its tests and its wire behaviour stay untouched, and the two differ in
   exactly three places (below). The shared field validation (`relocationCommand`) and the
   deterministic-rejection classification (`classifyRelocationError`) are reused, not copied.

2. **Consumed event.** `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` with
   `task_type == "REPLENISH"`, data `{task_id, task_type, sku, from_bin, to_bin, quantity, request_ref}`. The inbound consumer dispatches on the full type and then on `task_type`; PICK
   and RELOCATE are unchanged and any other `task_type` still goes to the confirm-pick path, which
   ignores it. `request_ref` is decoded additively. Same consumer, group, topic, DLQ and mode flag
   (`TASK_COMPLETED_CONSUMER_MODE`, `TASK_COMPLETED_CONSUMER_GROUP`) as the other branches: no new
   environment variable.

3. **What differs from RELOCATE.**
   - **Consumer name** `task-completed-replenish` in `processed_events` (RELOCATE keeps
     `task-completed-relocate`, confirm-pick its own), so the same CloudEvents id can never block
     another branch.
   - **Reason `REPLENISHMENT`**, a new additive value of the relocation reason set.
   - **`source_ref = request_ref`, required.** A REPLENISH completion without `request_ref`
     (missing or blank) is malformed and dead-lettered at once. There is **no task-id fallback**
     (RELOCATE falls back to the task id): the consumer of `StockRelocated` closes a request by
     `source_ref == request_id`, so a fallback would emit a relocation that can never close
     anything and would hide the producer's bug. The 200-character `source_ref` limit applies.
   - Same as RELOCATE: the site is the source bin's site (`TaskCompleted` carries none);
     `quantity` is the operator-confirmed count, **required** (missing, zero or negative is
     malformed, exactly as RELOCATE).

4. **Reason value.** `relocation.ReasonReplenishment = "REPLENISHMENT"`. `relocations.reason` is
   plain `TEXT NOT NULL` with no `CHECK` or enum (migration 0036), so **no migration** is needed.
   The aggregate (`relocation.New`) accepts it; `relocation.ParseReason`, the operator-facing
   parser behind `POST /relocations`, **keeps rejecting it** (`422 invalid-relocation-reason`).
   The reason is system-set, so the REST request contract is unchanged and no operator can forge a
   replenishment completion. `GET /relocations/{id}` can now return it, so only the **response**
   enum of `openapi.yaml` gains the value.

5. **One unit of work.** Exactly as ADR 0039: the CloudEvents-id claim, both bins, the source
   and destination stock units, the `relocations` row, the `StockRelocated` outbox row and the two
   `BinStockLevelChanged` outbox rows commit or roll back together. A redelivery claims nothing
   and changes nothing (`DUPLICATE`); a failure un-claims the id so the retry applies it.

6. **Outputs.** `StockRelocated` with `reason: "REPLENISHMENT"`, `source_ref: <request_ref>`,
   `quantity` = the moved quantity; then, from the same transaction, `BinStockLevelChanged` for the
   source bin and then the destination bin, both with `reason: "REPLENISHMENT"` (the mapping
   `relocationLevelReason` of ADR 0040, now reachable; its constant became
   `relocation.ReasonReplenishment`).

7. **Error classification** is RELOCATE's. A missing or invalid field, an unknown bin, a bin
   outside the site, the same bin, not enough **usable** stock, or a full destination is
   `ErrMalformedRelocationTask` (the one error type of both branches) and is dead-lettered
   immediately. A transient problem (database, `ErrConcurrentModification`) is retried up to the
   bounded attempts; a cancelled context commits and dead-letters nothing.

## What is NOT done

- **No partial-quantity policy** beyond RELOCATE's: the move is all-or-nothing for the confirmed
  quantity. A short completion is a different (smaller) `quantity` in the event, not a policy here;
  closing a request partially is pick-face-replenishment's concern.
- **No reservation interplay** beyond what `RelocateStock` already enforces: only the **usable**
  quantity moves, reserved units stay put, and a move larger than the usable stock is rejected.
  Replenishment neither creates nor releases reservations.
- No request-id existence check: this service does not know the replenishment context; `source_ref`
  is an opaque correlation, as for RELOCATE.
- No REST endpoint is added or changed (except the response enum noted above), no backfill, and
  no change to `StockRelocated`'s payload shape; the new reason value is additive.

## Consequences

- **Consumers of `StockRelocated` must tolerate `reason = REPLENISHMENT`.** `reason` was a
  three-value set; slotting-optimization's consumer carries `reason` in its fact but does not
  filter on it. It matches a relocation to a slot-plan move by `source_ref` and, when that is not a
  `<plan>:<seq>` reference (a replenishment request id is not), **falls back to matching the site's
  approved plan by SKU and bins**. A replenishment into a forward bin the plan assigns could
  therefore be recorded as the execution of an unrelated plan move. That is a defect to fix in
  slotting-optimization (act only on `SLOTTING`), tracked outside this repository; this change
  documents that consumers must act only on `SLOTTING`.
- Until the producers ship, nothing emits REPLENISH completions: the branch is dormant. Deploying
  this first is safe because an unknown `task_type` was already ignored and the new branch only
  acts on `REPLENISH`.
- ADR 0040's "reserved for the later pick-face replenishment change" is now realised; its table
  row for `RelocateStock` / `ApplyRelocationTask` gains `ApplyReplenishTask`.
- A REPLENISH completion whose producer forgets `request_ref` lands in the DLQ rather than
  producing an unclosable relocation; the DLQ message keeps the raw bytes for replay once the
  producer is fixed.
