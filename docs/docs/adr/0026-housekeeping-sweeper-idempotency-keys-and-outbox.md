---
id: 0026-housekeeping-sweeper-idempotency-keys-and-outbox
slug: /adr/0026-housekeeping-sweeper-idempotency-keys-and-outbox
title: "0026. Housekeeping sweeper for idempotency keys and published outbox rows"
sidebar_label: "26. Housekeeping sweeper"
sidebar_position: 26
description: "ADR 0026 — a small background sweeper in cmd/inventory that deletes idempotency_keys older than a TTL (default 24h) and PUBLISHED outbox_events older than a retention (default 7d), closing the unbounded-growth gap left open by ADR 0017 and ADR 0018."
---

# 0026. Housekeeping sweeper for idempotency keys and published outbox rows

## Status

Accepted. Closes the "grows unboundedly" follow-up recorded in
[ADR 0017](./0017-transactional-outbox.md) (`outbox_events`) and
[ADR 0018](./0018-idempotency-key-middleware.md) (`idempotency_keys`).

## Context

Two tables only ever grow:

- `idempotency_keys` (ADR 0018) gets one row per `Idempotency-Key` seen on
  `POST /stock/receive` and `POST /reservations`, including the stored
  response body. Nothing deleted them; the `created_at` index was created
  specifically for a future cleanup job.
- `outbox_events` (ADR 0017) keeps every row after the relay marks it
  `published_at`. Published rows are only useful for short-term forensics.

Both were accepted as "known gaps" when introduced. At production traffic they
are an unbounded storage and vacuum cost.

## Decision

`internal/adapters/outbound/postgres.Sweeper` — **one** small type with one
loop (both jobs are "delete old rows in batches" and share an interval) —
started by the `cmd/inventory` composition root for every Postgres-backed
process (idempotency keys are written regardless of `EVENT_PUBLISHER`):

| Env var | Default | Meaning |
|---|---|---|
| `HOUSEKEEPING_INTERVAL` | `1h` | Sweep period. `0` disables the sweeper. |
| `IDEMPOTENCY_KEY_TTL` | `24h` | Delete `idempotency_keys` rows with `created_at` older than this. `0` keeps them forever. |
| `OUTBOX_RETENTION` | `168h` (7d) | Delete **published** `outbox_events` rows with `published_at` older than this. `0` keeps them forever. |

(Chart values: `config.housekeepingInterval`, `config.idempotencyKeyTtl`,
`config.outboxRetention`.) An unparsable or negative value logs a warning and
falls back to the default.

Rules the implementation guarantees:

1. **Unpublished outbox rows are never deleted**, however old — an event still
   waiting for the relay (broker outage) is not garbage.
2. Deletes are **batched** (1000 rows per statement, repeated until a batch
   comes back short) so a large backlog is removed in short transactions, not
   one long lock. Each statement targets an explicit key/id set chosen by a
   subquery, so it is safe to run in several replicas at once.
3. It runs one pass **immediately on start** and then every interval, never
   returns an error (a failed pass is logged and retried), and is stopped
   **before** the pool closes during graceful shutdown (bounded by
   `shutdownDrainTimeout`, ADR 0020).
4. The relay stays in `cmd/inventory`; `cmd/mcp` does not sweep.

## Consequences

- The 24h TTL is now the **replay window** of the idempotency contract: a
  retry of a `POST` whose key is older than the TTL is treated as a brand-new
  request, not replayed. Clients must retry within that window (the intended
  use is retrying a dropped response, seconds to minutes).
- Published outbox rows older than 7 days are gone; forensics beyond that must
  come from Kafka itself (topic retention) rather than the outbox table.
- Proven against a real Postgres (testcontainers):
  `TestSweeper_DeletesOnlyExpiredIdempotencyKeysAndPublishedOutboxRows`,
  `TestSweeper_ZeroTTLAndRetentionKeepEverything`,
  `TestSweeper_RunSweepsOnStartAndStopsOnCancel`
  (`internal/adapters/outbound/postgres/sweeper_integration_test.go`) and the
  env wiring end to end by `TestBuildAdapters_StartsHousekeepingSweeperFromEnv`
  (`cmd/inventory/housekeeping_integration_test.go`).

## Alternatives considered

- **A Kubernetes CronJob / pg_cron.** Rejected: another deployable (or
  extension) to operate, and the retention knobs would live outside the
  service's own config.
- **Partitioned tables with drop-partition.** Rejected as over-engineered for
  the current row volume.
- **Two independent jobs.** Rejected: one loop with two statements is simpler
  and shares the interval, shutdown and logging.
