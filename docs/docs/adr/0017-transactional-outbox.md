---
id: 0017-transactional-outbox
slug: /adr/0017-transactional-outbox
title: 0017. Transactional outbox + relay for Kafka publishing
sidebar_label: 0017. Transactional outbox
sidebar_position: 17
description: Aggregate saves and their domain event(s) now commit in one Postgres transaction via a UnitOfWork; a background relay drains an outbox_events table onto Kafka, so a mid-flight crash can never persist state without also queuing its event, or vice versa.
---

# 0017. Transactional outbox + relay for Kafka publishing

## Status

Accepted.

## Context

Before this change, every publishing use case (`ReceiveStock`, `StowStock`,
`ReserveStock`, `RevokeReservation`, `ConfirmPick`,
`GetReservationsByDemandRef` via lazy reservation expiry, `RunCycleCount`,
`ClassifyProduct`) called `Repo.Save` and then, as a separate step,
`EventPublisher.Publish`. With `EVENT_PUBLISHER=kafka` those were two
independent network calls (a Postgres write, then a Kafka write) with no
shared transaction: a crash, a dropped connection, or a Kafka broker outage
between the two left the aggregate durably saved with its event never
published — a StockReserved that never reaches order-management, an
inventory.analytics stream permanently missing an event. There was also a
half-built `internal/adapters/outbound/postgres/event_publisher.go` (an
`events` table insert with a comment admitting "a separate relay (out of
scope here) would forward rows to a broker") — a write-only sink nothing
ever drained, effectively a silent black hole for events that flowed
through it.

This fleet has already solved this for `process-path-management` (ADR
0003, single-topic variant) and, per the shared rollout brief, for
`wes-work-planning`, `fulfillment-execution`, `workforce-management`, and
`labor-performance` (the fan-out variant, two topics). inventory-storage
has two outbound Kafka topics of its own — the integration topic
(`warehouse.inventory.events`, `kafka.Publisher`) and the analytics topic
(`warehouse.inventory.analytics`, `kafka.AnalyticsPublisher`, ADR 0011) —
so it follows the fan-out variant.

## Decision

Adopt the transactional outbox pattern: a use case's aggregate save(s) and
its domain event(s) are written in the SAME Postgres transaction, as an
`outbox_events` row per (event, topic) rather than a direct Kafka call. A
separate background `OutboxRelay` process polls `outbox_events` for
unpublished rows and sends them to Kafka, marking each published on
success. Kafka delivery is decoupled from the request path entirely: the
HTTP handler's use case never talks to Kafka directly when the outbox is
active.

### `ports.UnitOfWork`

```go
type UnitOfWork interface {
    Execute(ctx context.Context, fn func(ctx context.Context) error) error
}
```

Every publishing use case gained an optional `UnitOfWork` field and wraps
its writes with a shared `atomically(ctx, uow, fn)` helper
(`internal/application/usecases/unit_of_work.go`): a nil `UnitOfWork` runs
`fn` directly (the in-memory / dev / `EVENT_PUBLISHER=log` configuration),
so no existing test or call site broke. `reservation_expiry.go`'s
`expireIfDue`/`expireAllIfDue` — shared by `ReserveStock`,
`RevokeReservation`, `ConfirmPick`, and `GetReservationsByDemandRef` — took
a `uow` parameter for the same reason.

### `postgres.UnitOfWork` and `querierFrom`

`internal/adapters/outbound/postgres/unit_of_work.go` implements the port
over one `pgxpool.Pool` transaction. Every repo (`StockRepo`,
`LocationRepo`, `ProductClassificationRepo`, `ReservationRepo`) resolves
its statement execution through `querierFrom(ctx, pool)` instead of
calling `pool.Exec/Query/QueryRow` directly, so a repo call made with the
`ctx` `UnitOfWork.Execute` hands to `fn` joins that same transaction,
while a call made with a plain `ctx` (outside any `UnitOfWork` scope, or
when none is wired) falls back to the pool exactly as before.
`ReservationRepo.Save` already opened its own transaction (for the
reservation row + its allocations); it now uses `beginOrJoin`, which joins
an outer `UnitOfWork` transaction when one is active instead of always
starting a fresh one — critical, since `ReserveStock` calls both
`Stock.Save` and `Reservations.Save` inside one `atomically` scope.

### `outbox_events` schema

Replaces the retired `events` table (migration `0005`):

```sql
CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    key          BYTEA,
    value        BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
```

One row per (event, topic): a single domain event that projects onto both
the integration topic and the analytics topic produces two rows, each
already fully encoded (envelope built, trace headers injected) at insert
time — the relay never re-derives anything from the domain event, it only
ever moves opaque bytes.

### `kafka.Encoder` / `kafka.Encoded`, split from `Publish`

Both `kafka.Publisher` (integration topic) and `kafka.AnalyticsPublisher`
(analytics topic) gained an `Encode(ctx, event) ([]Encoded, error)` method
containing everything `Publish` used to do except the actual
`WriteMessages` call: envelope construction, the integration publisher's
`ReservationRepo` lookup for `ReservationRevoked`'s enrichment, trace
header injection. `Publish` is now `Encode` + write, unchanged in
observable behavior (same unit tests pass). `Encode` returns an empty
slice — never an error — for an event outside its contract (e.g.
`LocationRecorded` against the integration publisher), so
`postgres.OutboxPublisher` can hand every encoder every event
indiscriminately.

`kafka.RelaySink` wraps a `*kafkago.Writer` with NO fixed `Topic` (the
relay's rows may be addressed at either topic); it sets
`kafkago.Message.Topic` per message from `Encoded.Topic`. This is the
reason `Publisher`/`AnalyticsPublisher`'s own writers keep a fixed `Topic`
and never set `Message.Topic` themselves — kafka-go rejects a message with
`Topic` set when the writer also has one fixed.

### `postgres.OutboxPublisher` and `postgres.OutboxRelay`

`OutboxPublisher` implements `ports.EventPublisher` (`Publish(ctx, event)
error`, singular — inventory-storage's existing shape, unchanged): for
every configured `kafka.Encoder` it calls `Encode`, then `INSERT`s one row
per resulting message via `querierFrom(ctx, pool)`, so the insert joins
whatever `UnitOfWork` transaction is active. It never touches the broker.

`OutboxRelay` claims up to `batchSize` unpublished rows with `SELECT ...
FOR UPDATE SKIP LOCKED` (safe for more than one relay instance, though
only one runs today), then sends them to its `Sink` **one row at a time**,
in `id` order, committing each success immediately. A send failure stops
that pass exactly at the failed row: everything before it in the batch is
already committed as published, the failed row's `attempts`/`last_error`
is recorded, and everything after it is left untouched — so a later event
can never overtake an earlier one for the same key. `Run` polls every
`OUTBOX_RELAY_INTERVAL` (default 1s), or immediately again with no wait
when a pass claims a full batch.

### Composition roots (`cmd/inventory/main.go` and `cmd/mcp/main.go`)

| `DATABASE_URL` | `EVENT_PUBLISHER` | publisher wired | relay |
|----------------|-------------------|------------------|-------|
| unset | `log` | log | no |
| unset | `kafka` | `MultiPublisher(integration, analytics)` direct | no |
| set | `log` | log | no |
| set | `kafka` | `OutboxPublisher(pool, integration, analytics)` | yes |

`cmd/mcp/main.go` applies the **same table** (same `EVENT_PUBLISHER` /
`DATABASE_URL` / `KAFKA_BROKERS` selection, a `UnitOfWork`, and the
`ReservationMetrics`) so an MCP-initiated `revoke_reservation` is
exactly as durable as `DELETE /reservations/{id}`: with a database and
`EVENT_PUBLISHER=kafka` it writes one outbox row per topic in the
revocation's transaction. The **relay runs only in `cmd/inventory`**; it
drains the shared `outbox_events` table, so the MCP pod needs no broker
connection in that configuration. This is proven by
`TestMCPRevokeReservation_WritesOutboxRowsOnBothTopics`
(`cmd/mcp/outbox_integration_test.go`, testcontainers Postgres). The
relay runs as a goroutine alongside the inventory HTTP server; on
shutdown its context is cancelled and the composition root waits for its
current pass to finish before closing the pool/writers.

## Delivery semantics

- **Atomic**: an aggregate save and its outbox row(s) commit together or
  not at all — proven by
  `TestOutbox_PublishFailure_RollsBackEverything` and
  `TestOutbox_ClassifyProduct_PublishFailure_RollsBackTheClassification`
  in `internal/adapters/outbound/postgres/outbox_integration_test.go`
  (forcing the `Encoder` to fail leaves neither the aggregate row nor any
  outbox row behind).
- **At-least-once** to Kafka: a crash between commit and the relay
  marking a row published could redeliver it on restart (the relay always
  re-claims anything with `published_at IS NULL`). Consumers of both
  topics must already be idempotent on `event_id`/aggregate id, per the
  existing integration contract.
- **Per-key ordering preserved within one relay instance**: rows are
  claimed and sent in `id` (insertion) order, and a failure never lets a
  later row jump ahead of an earlier, unpublished one.
- **Latency**: at most one `OUTBOX_RELAY_INTERVAL` (default 1s) after
  commit, or effectively immediate under sustained load (a full batch
  triggers an immediate next pass with no wait).

## Alternatives considered

- **Two-phase commit (Postgres + Kafka)**: rejected — kafka-go has no XA
  support, and distributed transactions across a database and a broker
  are exactly the operational complexity the outbox pattern exists to
  avoid.
- **Debezium / CDC off the WAL**: rejected for now — correct in
  principle, but a new infrastructure dependency (Kafka Connect) this
  fleet does not otherwise run; the relay's own polling is a few lines of
  Go with no new moving parts.
- **Leave the dead-end `events` table and add a relay on top of it**:
  rejected — that table stored raw domain events, not pre-encoded
  messages, which would force the relay to duplicate every encoder's
  `Encode` logic (including the integration publisher's
  `ReservationRepo` enrichment lookup) outside the original transaction,
  reading the aggregate a second time with no guarantee it still matches
  what was true at write time.

## Consequences

- Every publishing use case now depends on an extra, optional
  `UnitOfWork` field — a small, mechanical widening of each struct's
  surface, worth it for the atomicity guarantee.
- The relay is a new, always-on background goroutine per `cmd/inventory`
  replica; it is intentionally dumb (poll, claim, send, mark) and adds no
  new external dependency beyond the Postgres pool and Kafka writer this
  binary already has.
- `outbox_events` grows unboundedly today (rows are marked published but
  never deleted); a follow-up housekeeping job (delete rows older than N
  days with `published_at IS NOT NULL`) is left as a known gap, consistent
  with how `process-path-management`'s ADR 0003 left the same gap open.

## Verification performed

- `go build ./...`, `go vet ./...`, `golangci-lint run ./...` (0 issues).
- `go test ./... -race` — every existing unit/httptest/bdd test green,
  including `internal/architecture` (hexagonal fitness + the fleet-wide
  "Kafka integration tests use testcontainers" rule).
- `go build -tags=integration ./...`, `go vet -tags=integration ./...`.
- `go test -tags=integration ./internal/adapters/outbound/postgres/... -run Outbox -race -count=1`
  against a throwaway `testcontainers-go/modules/postgres` container:
  commit-together (aggregate + outbox row), rollback-together (encoder
  failure leaves neither), relay publishes in order and marks rows, relay
  stops at a failed row and recovers in order on the next pass — all
  green.
- `go test -tags=integration ./internal/adapters/outbound/postgres/... -race -count=1`
  against a real (non-testcontainers) `DATABASE_URL` Postgres, to confirm
  the `querierFrom`/`beginOrJoin` swap did not regress the pre-existing
  standalone (no-`UnitOfWork`) repo path — all green.
- `make coverage`: 97.9% on `./internal/domain/...,./internal/application/...`
  (gate 90%), up from 97.5% on `origin/develop` before this change.
- `make arch-test`: hexagonal fitness green, including
  `TestKafkaIntegrationTestsUseTestcontainers` (the new outbox integration
  test itself was checked against that rule).
