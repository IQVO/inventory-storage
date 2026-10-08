---
id: 0036-per-line-confirm-pick
slug: /adr/0036
title: "ADR 0036: Reservations store the order line; picks are confirmed per line"
sidebar_label: "36. Per-line confirm-pick"
sidebar_position: 36
---

# 0036. Reservations store the order line; picks are confirmed per line

## Status

**Accepted** (2026-10-07). Audit decision 18 (hops 1, 2 and 7 of the fleet's per-line
confirm-pick contract). Builds on [ADR 0035](./0035-confirm-pick-from-task-completed.md),
which recorded this path as its "Future per-line path" and keeps its counting as the
backward-compatible fallback; ADR 0035's status line points here and its body is
unchanged. Does not change [ADR 0003](./0003-revocable-reservations.md) (lazy
expiry), [ADR 0017](./0017-transactional-outbox.md) (one `UnitOfWork`), or
[ADR 0018](./0018-idempotency-key-middleware.md) (the middleware itself).

## Context

ADR 0035 confirms an order's reservations by *counting* the order's completed PICK
tasks, because a `Reservation` had no line identity: a PICK task is per order line, all
of an order's tasks carry the same `order_ref`, and nothing tied a task to one
reservation. The counting is safe (it confirms late, never early) but it is a
correlation by number, not by identity, and it has an edge ADR 0035 recorded: if a
line's reservation is already REVOKED or EXPIRED while its task is still being worked,
the order stops awaiting that pick and can confirm **one pick early**, marking an
unpicked line as picked with no undo.

The line number exists at the source: order-management knows it when it reserves,
`OrderAllocated` carries `line_no` per line, and wes-work-planning builds the work unit
id `<order>-line-<n>` from it. It is only lost in transit. Audit decision 18 carries it
explicitly, as an optional additive field, through every hop (order-management
`lineNo` on `POST /reservations` → this service's `Reservation.line_no` → wes-work-planning
`WorkUnit.line_no` / `WorkReleased.line_no` → fulfillment-execution `Task.source_line_no`
/ `TaskCompleted.line_no` → this service's consumer). Every field is optional, so the
four repos ship in any order. This record is this service's part.

## Decision

1. **`POST /reservations` accepts an optional `lineNo`** (integer ≥ 1; `null`/absent =
   unknown). 0 and negative values are rejected with `400 invalid-line-no`; a
   non-integer is the existing `400 malformed-request-body`. The `Reservation` aggregate
   stores it (`NewForLine` / `RehydrateForLine`; `New` / `Rehydrate` keep the unknown
   line), and every reservation response (the `201` of `POST /reservations` and
   `GET /reservations?demandRef=`) returns `lineNo`, **omitted** when unknown.
   `line_no` is immutable after creation.

2. **Additive migration `0035_reservation_line_no`:** `reservations.line_no INTEGER`,
   nullable, `CHECK (line_no IS NULL OR line_no >= 1)`. No backfill: the line was never
   stored, so any value would be invented. Every existing reservation reads back with an
   unknown line and is served by the ADR 0035 fallback. The in-memory repository stores
   the aggregate and needs no change.

3. **Replay guard.** `ReserveStock` already treats an ACTIVE reservation of the same
   `demandRef`, SKU and quantity as the retry of a dropped response. When the request
   names a line, that line must also match, so two lines of one order that share SKU and
   quantity are two reservations (before this, the second was indistinguishable from a
   retry of the first). An ACTIVE reservation with **no** line still answers a
   line-aware retry: handing it back is the conservative choice over holding the same
   stock twice for a retry that straddles the deploy. A request without a line matches
   exactly as before.

4. **Idempotency-Key semantics.** The middleware (ADR 0018) fingerprints the request
   body. A replay with the **same key and an identical body** (including the same
   `lineNo`) still returns the cached response byte for byte. **Adding `lineNo` to a
   request whose key was first used without it is a different body, so it answers
   `422 idempotency-key-reused`.** That is correct and expected, not a regression: the
   body really is different, and order-management derives a fresh key per attempt
   (`res-<order>-line-<n>-att-<k>`), so a client that starts sending `lineNo` is sending
   new attempts. A client that wants to attach the line to an in-flight attempt uses a
   new key; the application-level replay guard in (3) then hands back the same
   reservation instead of reserving twice.

5. **The consumer decodes the optional `line_no`** of `TaskCompleted` v1 (absent or
   `null` = unknown; a non-integer is a poison payload and is dead-lettered at once; a
   value below 1 is rejected by the use case as malformed and dead-lettered the same
   way). `ConfirmPicksForOrder` handles a PICK with a non-empty `order_ref` as follows
   (`PickCompletion.LineNo`):

   - **With a line, and the order has reservations for it:** every ACTIVE reservation
     with `demand_ref = order_ref` and `line_no` = the event's is confirmed through the
     existing `ConfirmPick` (normally exactly one; earlier attempts of the line are
     REVOKED). CONFIRMED and REVOKED are skipped; EXPIRED (including an ACTIVE one past
     its timeout, resolved by lazy expiry exactly as any read would) is skipped, logged
     (WARN) and counted in `inventory.pick_confirmations{outcome=expired}`, never an
     error. Other lines are never touched. **No `order_pick_progress` row is written.**
     Outcome `LINE_SETTLED`.
   - **With a line that no reservation carries, and the order has line-less (legacy)
     reservations:** fall back to the ADR 0035 last-pick counting **over those
     line-less reservations only** (`needed` counts their ACTIVE + CONFIRMED). Line-aware
     reservations of the same order are not part of that count and are never confirmed
     by it.
   - **With a line that no reservation carries and no line-less reservation to fall back
     to:** `LINE_NOT_FOUND`, a successful no-op (the event is still claimed).
   - **Without a line:** the ADR 0035 path, unchanged, over all the order's
     reservations.

   The event-id claim and every write stay in **one** `UnitOfWork`; a redelivered id is
   skipped by the claim, and a new id for an already confirmed line finds nothing ACTIVE
   (`AlreadyPicked`) and changes nothing. A failure rolls the claim, the confirmation
   writes and the outbox rows back together, so the redelivery is applied in full.

6. **ADR 0035's "one pick early" edge is closed for line-aware events.** With a line,
   a pick can only confirm its own line's reservation: a revoked or expired line no
   longer shifts the count, and a line whose pick has not arrived stays ACTIVE and
   reserved. ADR 0035's counting path, its `order_pick_progress` table and its
   retention sweep are **kept** as the backward-compatible fallback for events without
   `line_no` and for reservations without `line_no`; they can be retired when no
   line-less reservation or event can occur any more (a later record).

7. **Contracts.** REST is additive (a new optional request field and a new optional
   response field, plus one new `400` problem type). The consumed `TaskCompleted` v1
   gains the optional `line_no` (fulfillment-execution's change; documented in this
   service's `apis/asyncapi.yaml`, which now validates with 0 errors). No event this
   service publishes changes. The MCP server exposes no reservation-creating tool, so
   its tool schemas and evals are unchanged.

## Consequences

- With order-management sending `lineNo` and the three upstream hops carrying it,
  a mixed order whose lines 1 and 3 are picked confirms exactly lines 1 and 3; line 2
  stays ACTIVE with its stock still reserved (proved end to end against real Postgres
  and Kafka). Until all hops are rolled out, behaviour is ADR 0035's, per reservation
  and per event.
- Rollout order is free: a new consumer with old events/reservations counts; an old
  consumer ignores `line_no` (it decodes only the fields it knows) and counts.
- A client that starts sending `lineNo` mid-flight can see one `422
  idempotency-key-reused` for a key it had already used without it; the fix is a new
  key (decision 4).
- Short picks are still not modelled (a Task has no quantity). Per-line identity makes
  them expressible later; that needs per-line quantities upstream and a business rule
  no ADR specifies.
- `GET /reservations/{id}` does not exist in this service; the reservation read is
  `GET /reservations?demandRef=`, which carries `lineNo`.
- The first reservation for a line that races a retry of the same line is still
  protected only by the best-effort replay guard (see `REST_AUDIT.md`), not by a
  database uniqueness constraint; this record does not add one.
