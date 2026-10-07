---
id: 0035-confirm-pick-from-task-completed
slug: /adr/0035
title: "ADR 0035: Confirm picks from fulfillment-execution's TaskCompleted, at order granularity"
sidebar_label: "35. Confirm picks from TaskCompleted"
sidebar_position: 35
---

# 0035. Confirm picks from fulfillment-execution's TaskCompleted, at order granularity

## Status

**Accepted** (2026-10-07). Supersedes [ADR 0032](./0032-confirm-pick-event-driven.md)
(Proposed, 2026-10-06): its direction (event-driven, no synchronous cross-context
call) is kept and implemented here; its "missing fields" finding is resolved by
the correlation below. Audit decision 17.

Does not change [ADR 0003](./0003-revocable-reservations.md): lazy reservation
expiry stays (no background sweeper). Builds on [ADR 0017](./0017-transactional-outbox.md)
(one `UnitOfWork` for state and outbox), [ADR 0024](./0024-cloudevents-mandatory-envelope.md)
(CloudEvents), [ADR 0028](./0028-bootretry-boot-time-dial-retry.md) (boot dial retry)
and [ADR 0034](./0034-product-master-owns-classification.md) (the `processed_events`
dedupe table).

## Context

`POST /reservations/{id}/confirm-pick` (`ConfirmPick`) is what turns a reservation
into a physical decrement. The 2026-10-05 audit found that no sibling calls it, so
reserved stock is never confirmed as picked: reservations expire lazily (ADR 0003)
and the decrement never reaches this ledger. ADR 0032 decided the direction and
stopped, because it believed no existing event carried a correlation.

Verified in the sibling code on 2026-10-06:

- **order-management** reserves with `demandRef = OrderId`, one reservation per
  order line (ids like `res-<order>-line-<n>-att-<k>`).
- **wes-work-planning**'s `WorkReleased` carries the work unit `reference`, which is
  the OrderId.
- **fulfillment-execution** stores it as the Task's `orderRef` and already enriches
  `TaskCompleted` with it internally (`work_unit_id` on the wire), but publishes no
  field named for it.
- A **Task carries no SKU and no quantity.**

So the correlation needs only one additive field on `TaskCompleted`; wes-work-planning
and order-management need no change.

## Decision

1. **Event-driven, no synchronous call.** inventory-storage consumes
   `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` (v1) from
   `warehouse.fulfillment.events`. fulfillment-execution adds an **optional**
   `order_ref` string to the v1 `data` (additive, omitted when empty; sibling PR in
   IQVO/fulfillment-execution, branch `feat/task-completed-order-ref`). This service
   builds against that contract and treats a missing `order_ref` as "nothing to do",
   so it can ship before the producer.

2. **Order granularity.** For a `task_type` of `PICK` with a non-empty `order_ref`,
   use case `ConfirmPicksForOrder` loads the reservations whose `demand_ref` equals
   `order_ref` and confirms **every ACTIVE one** by calling the existing `ConfirmPick`
   use case (its rules are reused, not duplicated). The order is the finest unit the
   event can name, which is why this granularity is correct: a Task has no SKU or
   quantity, so a per-line or per-quantity confirmation would have to be invented.

3. **Idempotent and atomic.** The CloudEvents `id` claim in `processed_events`
   (consumer `task-completed-confirm-pick`), every `ConfirmPick` write (stock units,
   bins, reservations) and the `StockPicked` outbox rows run in **one**
   `UnitOfWork` transaction. A failure rolls the claim back with everything else, so
   the redelivery is applied in full; a redelivered id is skipped by the claim, and a
   *new* id for the same order (a second PICK task) only finds reservations that are
   no longer ACTIVE.

4. **Reservation outcomes.** ACTIVE: confirmed. CONFIRMED and REVOKED: skipped.
   **EXPIRED: skipped, never an error.** A reservation past its timeout that is still
   ACTIVE in storage is resolved by lazy expiry exactly as any read would (stock
   returned to usable, `ReservationExpired` raised) and then skipped. Each skip is
   logged (WARN) and counted in `inventory.pick_confirmations{outcome=expired}`,
   because the physical pick happened but its stock was already back in the usable
   pool: that is an operational signal, not a reason to block the partition. No
   reservation for the order (a transfer, whose `demand_ref` is namespaced by
   `TransferDemandRef`, or a non-inventory order), a non-PICK `task_type` and every
   other event type are successful no-ops.

5. **Consumer shape** (fleet at-least-once atomicity checklist):
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

6. **Short picks are NOT modelled.** A Task carries no per-line SKU or quantity, so
   the consumer cannot tell a full pick from a short one: it confirms the reserved
   quantity of every ACTIVE line. Modelling short picks needs per-line quantities in
   wes-work-planning's `WorkUnit` and fulfillment-execution's `Task` (and a business
   rule for what a partial pick does to the remainder, which no ADR specifies). That
   is an explicit limitation of this decision, not a silent assumption.

## Consequences

- Reserved stock is finally confirmed as picked in a deployment that enables the
  consumer and runs a fulfillment-execution that publishes `order_ref`. Until both
  are rolled out nothing changes (consumer off by default; a missing `order_ref` is a
  no-op).
- **Order granularity cuts both ways.** If several PICK tasks ever carry the same
  order reference (work-planning releasing more than one work unit for one order),
  the *first* completion confirms all of the order's lines. One `WorkReleased`
  creates exactly one Task today and the fleet does not split an order that way, but
  this will need per-line correlation (the same prerequisite as short picks).
- A pick completing after its reservation timed out is counted and logged, not
  recovered: the stock was already returned to usable and may have been re-reserved.
  Operators should alert on `inventory.pick_confirmations{outcome=expired}`.
- The REST route stays for operators and the simulator, and remains safe alongside the
  consumer: whichever confirms first wins, the other sees a resolved reservation.
- One more consumer group and one DLQ topic to operate. The `.dlq` topic is
  auto-created on first write like the other DLQs of the fleet.
- No published contract of this service changes; the only contract change (the
  optional `order_ref`) is fulfillment-execution's, additive, and consumed here.
