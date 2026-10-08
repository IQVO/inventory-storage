---
id: 0032-confirm-pick-event-driven
slug: /adr/0032
title: "ADR 0032: Confirm picks from a pick-completion event, not from a REST call"
sidebar_label: "32. Confirm-pick, event-driven"
sidebar_position: 32
---

# 0032. Confirm picks from a pick-completion event, not from a REST call

## Status

**Superseded by [ADR 0035](./0035-confirm-pick-from-task-completed.md)**
(2026-10-07). Was Proposed on 2026-10-06; the direction decided here is kept and
implemented by 0035.

Does not change [ADR 0003](./0003-revocable-reservations.md): lazy reservation
expiry is **kept** ("Decided 2026-10-06: kept").

## Context

`POST /reservations/{id}/confirm-pick` (`ConfirmPick`) is what turns a
reservation into a physical decrement: it consumes the reservation, removes the
quantity from its stock unit(s), releases bin capacity and raises `StockPicked`.
The 2026-10-05 audit found that **no sibling context calls it** — only the
`e2e-tests` warehouse-day simulator does — so in a real deployment reserved
stock is never confirmed as picked: reservations simply expire (lazily, ADR
0003) and the physical decrement never reaches this ledger.

The architect decided on 2026-10-06 (decision 9c) how the gap closes:

- **Event-driven.** inventory-storage consumes a pick-completion integration
  event published by fulfillment-execution and confirms the matching reservation
  itself, by calling the existing `ConfirmPick` use case.
- **No synchronous REST or MCP call** from fulfillment-execution or
  wes-work-planning into this context (matches the fleet's event-driven
  direction and keeps those contexts unaware of `Reservation`).
- **Implement only if an existing event already carries what is needed;**
  otherwise record the missing fields and do not guess a payload.

## Finding: no existing event carries what is needed

fulfillment-execution's integration topic `warehouse.fulfillment.events`
(`apis/asyncapi.yaml` on `origin/develop`) publishes `TaskCompleted`,
`TaskCPTMissed` and `PackageManifested`. The only pick-shaped one is
`com.warehouse.wes.fulfillment-execution.task.TaskCompleted`, whose `data` is:

`task_id`, `station_id`, `work_unit_id`, `associate_id`, `duration_seconds`,
`task_type` (`PICK|PACK|SLAM|REBIN`).

`ItemPicked` exists, but only on the internal analytics topic
(`warehouse.fulfillment.analytics`) and carries only `task_id` and `task_type`.
Against what `ConfirmPick` needs, **none of the following is present**:

1. **A reservation correlation.** `ConfirmPick` takes a `reservationId`. This
   service's reservations are created by order-management with
   `demandRef = OrderId`, one reservation per order line (SKU + quantity,
   idempotency key `res-<order>-line-<n>-att-<k>`). `TaskCompleted` carries
   neither a reservation id nor the `demandRef`/OrderId — `work_unit_id` is
   wes-work-planning's own identifier, and nothing in the published contracts
   states that it equals the order id or maps to its lines.
2. **A SKU and a picked quantity per line.** A pick task can span several order
   lines (several reservations). Without `sku` + `quantity` per line the
   consumer cannot choose which reservation(s) the completion settles, nor
   detect a short pick.
3. **A short-pick rule.** `ConfirmPick` is all-or-nothing on a reservation (it
   consumes the full reserved quantity). What a *partial* pick should do (confirm
   what was picked and revoke/re-reserve the rest) is a business rule no ADR
   specifies; inventing it here is out of bounds.

Mapping `work_unit_id` → reservation would therefore be a guess, and a wrong
guess **silently removes stock from the wrong order's reservation**. Not
implemented.

## Decision

1. **Direction (final):** pick confirmation reaches this context as a
   CloudEvents integration event consumed by an idempotent Kafka consumer that
   calls `ConfirmPick`. No sync REST/MCP from siblings.

2. **Consumer shape when implemented** (so the follow-up is mechanical): a
   fixed-group, state-mutating consumer per the fleet at-least-once atomicity
   checklist — `FetchMessage` and commit only after the handler returns nil;
   capped-backoff retry of the same message on transient errors; deterministic
   poison (not CloudEvents, unknown `type`, malformed payload, unknown or
   already-resolved reservation) logged and committed past; the
   `processed_events` claim, `ConfirmPick`'s writes and the outbox rows in
   **one** `UnitOfWork`; a unique, env-overridable consumer group; DLQ for
   poison; testcontainers Kafka + Postgres integration test.

3. **Missing fields, by repository** (a new additive event or an additive
   version of an existing one; nothing here is a breaking change):

   | repo | must add |
   | --- | --- |
   | **fulfillment-execution** | publish a pick-completion event on `warehouse.fulfillment.events` (e.g. an integration-topic `PickCompleted`/`ItemPicked`, or additive fields on `TaskCompleted` for `task_type=PICK`) carrying, **per picked line**: `reservation_id` **or** (`demand_ref` = the OrderId used when reserving **and** `sku`), plus `quantity` picked. |
   | **wes-work-planning** | `WorkReleased` must hand fulfillment-execution the correlation it will echo back (`demand_ref` / order id, and the per-line `sku` + `quantity`, or the `reservation_id`s) — fulfillment-execution only knows `work_unit_id` today. |
   | **order-management** | only if wes-work-planning cannot obtain `reservation_id`s elsewhere: expose them to wes-work-planning (e.g. on `OrderAllocated` lines). `demandRef` + `sku` is already enough to find a reservation here via `FindByDemandRef`, so this is optional. |

   Product decisions still to take before the consumer is written (not made
   here): short-pick behaviour, and whether a duplicate/late completion for an
   already `EXPIRED`/`REVOKED` reservation is dropped or alarms.

4. **Lazy expiry stays (ADR 0003).** Decided 2026-10-06: kept — no background
   sweeper. The accepted trade-off (an unread, unrevoked reservation holds stock
   until something reads it) is unchanged; once picks are event-driven the
   reservation is resolved by the confirmation itself, which narrows the gap
   further.

## Consequences

- Until the upstream fields exist, **reserved stock is still not confirmed as
  picked in a real deployment**; the REST route remains for the simulator and
  operators. The docs mark the gap as "Decided 2026-10-06: event-driven
  (ADR 0032, Proposed, blocked on fulfillment-execution / wes-work-planning
  fields)" rather than as an open question.
- No change to the `ConfirmPick` use case, the REST contract or any published
  event in this record.
- When the consumer lands it adds one consumer group and one DLQ topic to
  operate, and supersedes the "no sibling confirms picks" notes in the context
  map.
