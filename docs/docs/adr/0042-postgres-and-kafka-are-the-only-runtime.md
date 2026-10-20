---
id: 0042-postgres-and-kafka-are-the-only-runtime
slug: /adr/0042
title: "ADR 0042: Postgres and Kafka are the only runtime"
sidebar_label: "42. Postgres and Kafka only"
sidebar_position: 42
description: "ADR 0042 — inventory-storage no longer has an in-memory mode, a log-only publisher, a permissive location lookup or opt-in Kafka consumers: DATABASE_URL and KAFKA_BROKERS are mandatory, the outbox relay and all four consumers are always on, a dead consumer ends the process, and /readyz checks Postgres and Kafka."
---

# 0042. Postgres and Kafka are the only runtime

## Status

**Accepted** (2026-10-20). Supersedes the optional-runtime parts of
[ADR 0017](./0017-transactional-outbox.md) (direct, outbox-less Kafka publishing
and the independent `EVENT_PUBLISHER` / `DATABASE_URL` toggles) and of the
consumer ADRs ([0030](./0030-site-scoped-transfer-allocation.md),
[0034](./0034-product-master-owns-classification.md),
[0035](./0035-confirm-pick-from-task-completed.md),
[0037](./0037-stock-receipts-from-inbound-receiving.md)), whose consumers were
"off" unless a mode or a group id was set. No schema or event contract changes.

## Context

Until now the binaries could boot in several shapes: no `DATABASE_URL` meant
in-memory repositories, no `EVENT_PUBLISHER` meant a log-only publisher,
`EVENT_PUBLISHER=kafka` without a database meant direct publishing with no
outbox, `LOCATION_LOOKUP_MODE` unset meant a lookup that answered "unknown" for
every bin, and each of the four Kafka consumers started only when its mode or
group id was set. Each shape was a way to run green without the infrastructure
the service exists to use:

- **Deployments that look healthy and do nothing.** A pod with the log
  publisher or a consumer left off passes every probe while no event leaves and
  no command arrives.
- **Tests that cannot see real-database failures.** The in-memory adapters
  accept text Postgres rejects (a NUL byte aborts the surrounding transaction,
  SQLSTATE 22021 then 25P02), so a hostile request was a 200 in tests and a 500
  in production. The Schemathesis `contract` job ran against that in-memory
  boot, so it could not see it either, and the OpenAPI document declared
  `Idempotency-Key` optional only because that boot did not enforce it.

On 2026-10-19 the fleet decided that a service whose design needs Postgres and
Kafka runs on Postgres and Kafka, and says so at boot.

## Decision

Postgres and Kafka are the only runtime for `inventory-storage`.

- **Mandatory configuration.** `DATABASE_URL`, `KAFKA_BROKERS` (no
  `localhost:9092` default), `LOCATION_LOOKUP_MODE` and the four consumer-group
  ids (`TRANSFER_ALLOCATION_CONSUMER_GROUP`, `PRODUCT_MASTER_CONSUMER_GROUP`,
  `INBOUND_RECEIPT_CONSUMER_GROUP`, `TASK_COMPLETED_CONSUMER_GROUP`) must be
  set. `loadConfig` validates everything before telemetry, the listener or any
  dial, and every problem is an error that names the variable. A restarted pod
  resumes from its committed offsets because the group ids are stable.
- **No in-memory mode, no log publisher, no direct publishing.** Events always
  leave through the transactional outbox. The `memory` adapters and the log
  publisher stay in the tree as test doubles for unit and BDD tests; no `cmd/`
  binary imports them (sensor: `TestRuntimeBinaryHasNoInMemoryAdapters`).
- **`EVENT_PUBLISHER`, `TRANSFER_ALLOCATION_CONSUMER_MODE` and
  `TASK_COMPLETED_CONSUMER_MODE` are kept only as guards**: unset or `kafka` is
  the single runtime, any other value (`log`, `off`) is a boot error naming the
  variable. `warehouse-infra` still sets them, so they cannot be removed yet.
- **`LOCATION_LOOKUP_MODE` is `kafka` or `http`.** Unset and `permissive` are
  refused: a lookup that answers "unknown" for every bin validates no location
  at all. `http` additionally needs `FACILITY_LAYOUT_BASE_URL`.
- **Always on.** The outbox relay and the four consumers (transfer allocation,
  product-master classification, inbound-receiving receipts, `TaskCompleted`
  confirm-pick) always run.
- **A dead worker ends the process.** A consumer that returns for any reason
  other than shutdown is reported to `run`, which drains (readiness flips
  first, then HTTP, then the consumers) and returns the error, so the process
  exits non-zero and the orchestrator restarts it. `run(ctx)` takes the signal
  context from `main`; the consumers run on a context detached from that
  cancellation so the shutdown order is unchanged.
- **`/readyz`** is the drain gate, a Postgres ping and a Kafka broker-metadata
  request. While a dependency is down it answers 503 `application/problem+json`
  of type `dependency-unavailable`, naming the dependency. Topics are never
  checked, because the relay's writers auto-create their own. The verdict is
  cached for three seconds and is detached from the caller's cancellation.
  `/healthz` stays liveness only. The projector's admin port gained the same
  `/readyz` (its own Postgres and Kafka).
- **A NUL byte never reaches Postgres**: 404 in the request path, 400 in the
  query string or the body (`rejectNUL`, the first middleware).
- **`Idempotency-Key` is `required: true` in `apis/openapi.yaml`.** The
  middleware already answered 400 `idempotency-key-required` in every
  deployment; the document now says what the service does.

## Consequences

- Local runs need both services. The quickstart and the configuration
  reference describe only this runtime.
- A deployment that forgets Postgres or Kafka fails loudly: the process exits
  at boot, and the Helm chart refuses to render without a database, a broker
  list, a real publisher, a lookup mode and the four group ids.
- Tests that need real behaviour run on testcontainers
  (`TestRunOnRealPostgresAndKafka`); fast unit and BDD tests keep the doubles.
- The `contract` job runs Schemathesis against a `postgres:16` and a
  `confluentinc/confluent-local` service container, with a hooks file that
  gives every request a fresh `Idempotency-Key`.
- A dependency outage takes the pod out of the Service (503 on `/readyz`)
  instead of being invisible. The pod is not restarted: `/healthz` is
  independent.

### What changed per binary

| Binary | Before | Now |
| --- | --- | --- |
| `cmd/inventory` | memory without `DATABASE_URL`; log publisher without `EVENT_PUBLISHER`; consumers and relay opt-in; permissive location lookup by default | `DATABASE_URL`, `KAFKA_BROKERS`, `LOCATION_LOOKUP_MODE` and four group ids mandatory; outbox, relay and all consumers always on; dead consumer exits non-zero; `/readyz` checks Postgres and Kafka |
| `cmd/mcp` | memory without `DATABASE_URL`; log publisher by default | `DATABASE_URL` mandatory; always publishes through the outbox (the relay stays in `cmd/inventory`, so it still needs no broker) |
| `cmd/inventory-projector` | `KAFKA_BROKERS` defaulted to `localhost:9092`; consumer death only logged | `KAFKA_BROKERS` and `ANALYTICS_DATABASE_URL` mandatory; exits non-zero when its consumer dies; `/readyz` |
| `cmd/inventory-reports` | already required `ANALYTICS_DATABASE_URL` and uses a read-only pool | unchanged |

## Deliberately out of scope

Recorded here so they are not mistaken for done:

- **`LOCATION_LOOKUP_MODE=http`** (the synchronous REST call to facility-layout
  behind a circuit breaker) is unchanged. It is about how this service treats a
  sibling context over REST, not about Postgres or Kafka, and it stays as the
  documented rollback for `kafka`. Follow-up: decide the same question for the
  REST sync edges fleet-wide.
- **`cmd/mcp` has no `/readyz`.** It never dials Kafka and holds only a pool, so
  its probes stay on `/healthz`.
- **Readiness probe overrides in `warehouse-infra`.** The chart default points
  the `readinessProbe` at `/readyz`, but
  `helm-values/inventory-storage.yaml` on `develop` overrides it to `/healthz`
  (checked 2026-10-20), so a broker outage will not remove the pod from the
  Service until that override is dropped. Tracked as one fleet-wide
  infrastructure change.
- **Telemetry shutdown delay.** With no reachable OTLP collector, shutdown waits
  about 5 s for the exporter to give up. It never blocks boot.
