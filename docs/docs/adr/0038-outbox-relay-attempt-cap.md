---
id: 0038-outbox-relay-attempt-cap
slug: /adr/0038
title: "ADR 0038: Outbox relay attempt cap"
sidebar_label: "38. Outbox relay attempt cap"
sidebar_position: 38
---

# 0038. Outbox relay attempt cap

## Status

**Accepted** (2026-10-12, ecosystem gap fix D12; this repository is the fleet's
reference implementation). Amends the per-key ordering consequence of
[ADR 0017](./0017-transactional-outbox.md); everything else in ADR 0017 (one
row per event and topic, written in the use case's transaction, drained by
one relay in `id` order) is unchanged.

## Context

The outbox relay ([ADR 0017](./0017-transactional-outbox.md)) sends rows one
at a time in `id` order and stops a pass at the first failed row, so a later
row never overtakes an earlier one. `postgres.WithMaxAttempts` has existed
since then, but `cmd/inventory` never passed it, so the relay ran with the
default of 0: unlimited retries.

That is right for a broker outage, which ends. It is wrong for a **poison
row**, one the broker will never accept (a message over the broker's size
limit, a topic that does not exist and cannot be created, a value the sink
rejects every time). Such a row fails every pass, every pass stops at it, and
every row behind it, for every key and both topics, waits forever. Nothing
pages: the relay logs a WARN per pass and the process stays healthy.

## Decision

1. **Every composition root that builds a relay passes an attempt cap.**
   `cmd/inventory` (the only binary here that runs a relay; `cmd/mcp` writes
   outbox rows but relays nothing) reads `OUTBOX_MAX_ATTEMPTS` and passes it
   as `postgres.WithMaxAttempts`. Default `20`
   (`postgres.DefaultOutboxMaxAttempts`); `0` means unlimited, the old
   behaviour. An invalid or negative value logs a WARN and uses the default,
   so a typo cannot silently remove the cap. The chart exposes it as
   `config.outboxMaxAttempts`.
2. **The convention is the one the option already had.** A row whose
   `attempts` has reached the cap is excluded from `claimBatch`
   (`AND attempts < $max`). It is **not** deleted and **not** moved: it stays
   in `outbox_events` with `published_at IS NULL`, its `attempts` and its
   `last_error`, for inspection. The housekeeping sweeper
   ([ADR 0026](./0026-housekeeping-sweeper-idempotency-keys-and-outbox.md))
   deletes only published rows, so it never removes an excluded row. There is
   no DLQ topic: a row the broker rejects would most likely be rejected on a
   DLQ topic of the same broker too, and the table already is the
   inspectable store.
3. **The failure that reaches the cap is logged at ERROR** with `outbox_id`,
   `topic`, `event_type`, `key`, `attempts`, `max_attempts` and the send
   error, once, after the attempt is committed. Failures below the cap stay
   at WARN.
4. **Re-arming is an operator action**: after fixing the cause,
   `UPDATE outbox_events SET attempts = 0 WHERE id = <id>;` makes the row
   claimable again on the next pass (runbook, "Outbox relay").
5. **Sensor.** The architecture fitness test `TestOutboxRelaysPassAttemptCap`
   fails when a non-test file under `cmd/` calls `NewOutboxRelay` without a
   `WithMaxAttempts(...)` argument.

## Consequences

- **Per-key ordering is no longer absolute.** Once a row is excluded, later
  rows overtake it, **including rows with the same key**: a consumer can see
  `StockReserved` for a reservation whose earlier event never arrived, or see
  it before a re-armed earlier row. This is the deliberate trade: the cap
  exists so one poison row cannot stop the whole relay, for every key, forever.
  Consumers are already idempotent on the CloudEvents `id` and must already
  tolerate redelivery ([ADR 0017](./0017-transactional-outbox.md)); a re-armed
  row is a late delivery of the same `id`.
- Ordering among rows that are **not** excluded is unchanged: within a pass
  the relay still stops at the first failure, so a transient failure (below
  the cap) never lets a later row overtake.
- With the defaults (1 s interval, cap 20) a row that fails every send is
  excluded after roughly 20 seconds of continuous failure. A broker outage
  longer than that also excludes the rows at the head of the queue, one per
  pass, as each reaches the cap: each one is logged at ERROR and must be
  re-armed. Raise `OUTBOX_MAX_ATTEMPTS` (or set `0`) where long broker outages
  are expected and a stalled relay is preferable to manual re-arming.
- An excluded row is visible with
  `SELECT id, topic, event_type, key, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= <cap>;`
- The rest of the fleet copies this: the same env var, default, semantics,
  ERROR log fields, integration test (a poison row, then a good row, which
  publishes) and fitness test.

## Verification

- `go test -tags=integration ./internal/adapters/outbound/postgres/ -run TestOutboxRelay -count=1`:
  `TestOutboxRelay_MaxAttempts_PoisonRowExcludedThenGoodRowPublishes`,
  `TestOutboxRelay_MaxAttempts_ReArmedRowIsClaimedAgain`,
  `TestOutboxRelay_NoCap_PoisonRowBlocksLaterRows`.
- `go test ./cmd/inventory/ -run 'TestOutboxMaxAttempts|TestDefaultOutboxMaxAttemptsIsTwenty'`.
- `go test ./internal/architecture/ -run OutboxRelay`.
