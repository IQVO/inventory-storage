---
id: 0035-confirm-pick-from-task-completed
slug: /adr/0035
title: "ADR 0035: Confirm an order's picks from fulfillment-execution's TaskCompleted, on the last pick"
sidebar_label: "35. Confirm picks from TaskCompleted"
sidebar_position: 35
---

# 0035. Confirm an order's picks from fulfillment-execution's TaskCompleted, on the last pick

## Status

**Accepted** (2026-10-07); per-line path added by [ADR 0036](./0036-per-line-confirm-pick.md). Supersedes [ADR 0032](./0032-confirm-pick-event-driven.md)
(Proposed, 2026-10-06): its direction (event-driven, no synchronous cross-context
call) is kept and implemented here; its "missing fields" finding is resolved by
the correlation below. Audit decisions 17 and 17a (17a corrects 17: the first
version of this record confirmed an order's reservations on the *first* pick, which
review showed to be wrong; this text is the corrected, unmerged version).

Does not change [ADR 0003](./0003-revocable-reservations.md): lazy reservation
expiry stays (no background expirer). Builds on [ADR 0017](./0017-transactional-outbox.md)
(one `UnitOfWork` for state and outbox), [ADR 0024](./0024-cloudevents-mandatory-envelope.md)
(CloudEvents), [ADR 0026](./0026-housekeeping-sweeper-idempotency-keys-and-outbox.md)
(the housekeeping sweeper, extended here for retention), [ADR 0028](./0028-bootretry-boot-time-dial-retry.md)
(boot dial retry) and [ADR 0034](./0034-product-master-owns-classification.md) (the
`processed_events` dedupe table).

## Context

`POST /reservations/{id}/confirm-pick` (`ConfirmPick`) is what turns a reservation
into a physical decrement. The 2026-10-05 audit found that no sibling calls it, so
reserved stock is never confirmed as picked: reservations expire lazily (ADR 0003)
and the decrement never reaches this ledger. ADR 0032 decided the direction and
stopped, because it believed no existing event carried a correlation.

Verified in the sibling code on 2026-10-06 (and corrected on review the same day):

- **order-management** reserves with `demandRef = OrderId`, one reservation per
  order line. The `res-<order>-line-<n>-att-<k>` string is only the *client's
  idempotency key*; it is **not stored**. A `Reservation` stores only `sku`,
  `quantity` and `demandRef`, so it has **no line identity**.
- **wes-work-planning**'s `WorkReleased` carries the work unit `reference`, which is
  the OrderId.
- **fulfillment-execution** stores it as the Task's `orderRef` and publishes it as
  an optional `order_ref` on `TaskCompleted`. A **PICK task is per order LINE**: its
  work unit is `<order>-line-<n>`, so one order has one PICK task per line and
  **every one of them publishes the same `order_ref`**.
- A **Task carries no SKU and no quantity.**

So a completed task cannot be mapped to one reservation: the only thing the event
and the reservations share is the order.

## Decision

1. **Event-driven, no synchronous call.** inventory-storage consumes
   `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` (v1) from
   `warehouse.fulfillment.events`. fulfillment-execution adds an **optional**
   `order_ref` string to the v1 `data` (additive, omitted when empty; sibling PR in
   IQVO/fulfillment-execution). This service builds against that contract and treats
   a missing `order_ref` as "nothing to do", so it can ship before the producer.

2. **Confirm on the LAST pick, counted per order.** For a `task_type` of `PICK` with
   a non-empty `order_ref`, use case `ConfirmPicksForOrder` loads the reservations
   whose `demand_ref` equals `order_ref` and counts the pick in the new
   `order_pick_progress` table (`demand_ref` primary key, `picked_tasks`,
   `updated_at`; migration 0034, additive). Let
   `needed = count(ACTIVE) + count(CONFIRMED)` reservations of that order (REVOKED
   and EXPIRED are never picked, so they do not count). When
   `picked_tasks >= needed` and at least one reservation is ACTIVE, **every ACTIVE
   reservation is confirmed** by calling the existing `ConfirmPick` use case (its
   rules are reused, not duplicated). Before that, the pick is only recorded and the
   event is handled as a successful no-op (the offset commits, nothing is retried).
   An order with nothing confirmable (no reservation, or only REVOKED/EXPIRED ones)
   is a no-op that leaves **no progress row**.

3. **Why last, not first.** Confirming is irreversible (on-hand drops, bin capacity
   is released, `StockPicked` leaves the service). The first version confirmed the
   whole order on the first completion, which marked the *other lines'* stock as
   picked before anyone had picked it, with no undo. Confirming **late is safe**: a
   reservation keeps its stock unavailable to everyone else until it is confirmed,
   and one that is never confirmed expires lazily (ADR 0003) and returns its stock.
   Confirming **early is wrong**: it removes stock that is still on the shelf from
   the ledger. When correlation is ambiguous, the safe failure is to confirm late or
   not at all.

4. **Idempotent and atomic.** In **one** `UnitOfWork` transaction: the CloudEvents
   `id` claim in `processed_events` (consumer `task-completed-confirm-pick`), the
   `order_pick_progress` increment (a single upsert,
   `picked_tasks = picked_tasks + 1`, returning the new value), every `ConfirmPick`
   write (stock units, bins, reservations) and the `StockPicked` outbox rows. A
   failure rolls all of it back, including the counter, so the redelivery is applied
   in full. A redelivered id is skipped by the claim *before* the counter is touched,
   so the counter can never double-count. Two events of the same order handled
   concurrently serialize on the progress row, so exactly one of them observes the
   final count. A *new* id for an already confirmed order (an extra event) finds no
   ACTIVE reservation and changes nothing.

5. **Reservation outcomes on the last pick.** ACTIVE: confirmed. CONFIRMED and
   REVOKED: skipped. **EXPIRED: skipped, never an error.** A reservation past its
   timeout that is still ACTIVE in storage counts towards `needed` (storage has not
   resolved it yet) and, on the last pick, is resolved by lazy expiry exactly as any
   read would (stock returned to usable, `ReservationExpired` raised) and then
   skipped. Each skip is logged (WARN) and counted in
   `inventory.pick_confirmations{outcome=expired}`, because the physical pick
   happened but its stock was already back in the usable pool: that is an
   operational signal, not a reason to block the partition. No reservation for the
   order (a transfer, whose `demand_ref` is namespaced by `TransferDemandRef`, or a
   non-inventory order), a non-PICK `task_type` and every other event type are
   successful no-ops. A pick that is merely counted is logged at INFO with
   `picks_seen` / `picks_needed`.

6. **Retention of the counter.** `order_pick_progress` rows are swept by the existing
   housekeeping sweeper (ADR 0026): rows whose `updated_at` is older than
   `ORDER_PICK_PROGRESS_RETENTION` (default `720h`, 30 days; chart value
   `config.orderPickProgressRetention`; `0` keeps them forever) are deleted in
   batches, like the other two tables. The window is generous on purpose: if an
   order is still being picked when its row is swept, the count restarts and the
   order is confirmed late or not at all (its reservations then expire, ADR 0003),
   never early. So retention can only fail on the safe side.

7. **Consumer shape** (fleet at-least-once atomicity checklist):
   - fixed, env-overridable group (`TASK_COMPLETED_CONSUMER_GROUP`, default
     `inventory-storage-confirm-pick`), `FetchMessage`, offset committed only after the
     message settles; dispatch on the **full** type string, everything else ignored;
   - a transient failure retries the **same** message with capped backoff, up to 5
     attempts, then the message is **dead-lettered** to `warehouse.fulfillment.events.dlq`
     (raw key/value plus `x-dlq-*` headers) and committed, so one stuck message cannot
     wedge the partition; the offset is never committed before the DLQ write succeeds;
   - a malformed payload (valid CloudEvent whose `data` does not decode, or a completion
     the use case rejects as malformed) is dead-lettered **immediately**;
   - a message that is not a CloudEvent at all (the retired flat envelope) is skipped,
     not dead-lettered, with a **sampled** WARN (first five, then every 1000th): a new
     group replaying pre-CloudEvents history would otherwise write one DLQ message and
     one log line per legacy message;
   - **default off** (`TASK_COMPLETED_CONSUMER_MODE=off|kafka`, like
     `TRANSFER_ALLOCATION_CONSUMER_MODE`); `kafka` requires `DATABASE_URL` and
     `KAFKA_BROKERS` and fails boot with the real cause otherwise; the first broker
     dial runs under `bootretry`. `StockPicked` only leaves the service with
     `EVENT_PUBLISHER=kafka`.

8. **Short picks are NOT modelled.** A Task carries no per-line SKU or quantity, so
   the consumer cannot tell a full pick from a short one: on the last pick it
   confirms the reserved quantity of every ACTIVE line. Modelling short picks needs
   per-line quantities in wes-work-planning's `WorkUnit` and fulfillment-execution's
   `Task` (and a business rule for what a partial pick does to the remainder, which
   no ADR specifies). That is an explicit limitation of this decision, not a silent
   assumption.

## Consequences

- Reserved stock is finally confirmed as picked in a deployment that enables the
  consumer and runs a fulfillment-execution that publishes `order_ref`: **when the
  last of the order's pick tasks completes**, not before. Until both are rolled out
  nothing changes (consumer off by default; a missing `order_ref` is a no-op).
- **The count is a correlation by number, not by identity.** It assumes one PICK task
  per live reservation. It is exact for the fleet today (one task per order line,
  one reservation per line). It is only an approximation if that ever changes: a
  line's reservation that is revoked or has expired (and is already stored as such)
  stops being awaited although its task may still be completed, so the order can
  confirm one pick early in that case; an order that gets *more* PICK tasks than live
  reservations still confirms once and ignores the rest. Both deviations are bounded
  by the order's own reservations; neither can touch another order.
- Stock of an order's lines stays reserved (unavailable) until its last line is
  picked, which is exactly what is true on the floor.
- A pick completing after its reservation timed out is counted and logged, not
  recovered: the stock was already returned to usable and may have been re-reserved.
  Operators should alert on `inventory.pick_confirmations{outcome=expired}`.
- One more small table (`order_pick_progress`, one row per order with reservations
  that is mid-pick) bounded by the sweeper retention above. An order that never
  receives its last pick leaves a row for 30 days, then it is deleted.
- The REST route stays for operators and the simulator, and remains safe alongside the
  consumer: whichever confirms first wins, the other sees a resolved reservation (an
  operator confirming a line by hand makes that line a CONFIRMED one, which still
  counts towards `needed`).
- One more consumer group and one DLQ topic to operate. The `.dlq` topic is
  auto-created on first write like the other DLQs of the fleet.
- No published contract of this service changes; the only contract change (the
  optional `order_ref`) is fulfillment-execution's, additive, and consumed here. The
  migration (`0034_order_pick_progress`) is additive.

## Future per-line path

The clean long-term answer is per-line correlation, which needs changes outside this
service and is **not** done here: order-management sends the order line number
(`line_no`) when it reserves, `Reservation` stores it, and the line number travels on
`TaskCompleted`. Then each task confirms exactly its own reservation, short picks
become expressible, and `order_pick_progress` (with its retention) can be deleted. The
counter is deliberately an internal detail (no event, no REST surface) so replacing it
changes no contract.
