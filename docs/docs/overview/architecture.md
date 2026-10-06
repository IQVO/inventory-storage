---
title: Architecture
sidebar_label: Architecture
description: Hexagonal / ports-and-adapters layering, the strict dependency rule, and how it is enforced.
---

# Architecture

The service is **hexagonal (ports & adapters)** with one non-negotiable rule:

> **domain depends on nothing; application depends on domain; adapters depend
> on application/domain.**

No framework type, no `chi` router, no `pgx` connection, and no SQL string ever
appears inside `internal/domain/`.

## Layout

```text
cmd/inventory/               main.go — composition root (OLTP REST API)
cmd/inventory-projector/     analytics writer (ADR 0011)
cmd/inventory-reports/       analytics read-only report REST (ADR 0011)
cmd/mcp/                     MCP server, Streamable HTTP (ADR 0008)
internal/
  domain/                    pure Go — no framework, no SQL types
    location/                Bin aggregate (capacity, occupancy)
    stock/                   StockUnit aggregate (SKU@bin, qty, state)
    reservation/             Reservation aggregate (revocable, timeout)
    product/                 ProductClassification aggregate + DOT segregation
    shared/                  SKU, BinId, Quantity value objects; domain events
  analytics/report/          analytics read-model region (imports no OLTP code)
  application/
    ports/                   OUT interfaces (see Ports below)
    usecases/                one struct per use case
  adapters/
    inbound/http/            chi handlers, DTOs, RFC 7807 error mapping; reports REST
    inbound/kafka/           analytics-topic consumer (projector)
    inbound/mcp/             MCP tools, resources and prompts
    outbound/postgres/       pgxpool repos + golang-migrate migrations
    outbound/memory/         thread-safe in-memory repos (tests, local dev)
    outbound/events/         log, buffered and multi (fan-out) publishers
    outbound/kafka/          integration + analytics publishers (kafka-go)
    outbound/analyticsstore/ analytical Postgres projection + reader
    outbound/facilitycache/  Kafka-fed facility-layout location cache (ADR 0013)
    outbound/facilitylayout/ sync HTTP + permissive location lookups
    outbound/telemetry/      OTel setup, trace-aware slog, metrics
  architecture/              arch-go fitness tests encoding the rule above
migrations/                  golang-migrate SQL files (OLTP)
migrations/analytics/        golang-migrate SQL files (analytical DB)
apis/                        openapi.yaml + asyncapi.yaml (Spectral-linted)
features/                    Gherkin acceptance specs (godog)
charts/inventory-storage/    Helm chart
web/                         inventory_mfe Module Federation remote
```

## The dependency flow

```mermaid
flowchart TB
  HTTP["inbound/http<br/>chi handlers, DTOs"]
  UC["application/usecases<br/>ReceiveStock, StowStock, ReserveStock, …"]
  P["application/ports<br/>StockRepo · LocationRepo · ReservationRepo<br/>ProductClassificationRepo · LocationClassificationLookup<br/>EventPublisher · UnitOfWork · ReservationMetrics · Clock"]
  D["domain<br/>stock · location · reservation · product · shared"]
  PG["outbound/postgres"]
  MEM["outbound/memory"]
  EV["outbound/events"]
  KAF["outbound/kafka"]

  HTTP --> UC
  UC --> P
  UC --> D
  P --> D
  PG -.implements.-> P
  MEM -.implements.-> P
  EV -.implements.-> P
  KAF -.implements.-> P

  classDef dom fill:#0f766e,stroke:#134e4a,color:#fff;
  classDef app fill:#2563eb,stroke:#1e3a8a,color:#fff;
  classDef adp fill:#64748b,stroke:#334155,color:#fff;
  class D dom;
  class UC,P app;
  class HTTP,PG,MEM,EV,KAF adp;
```

Solid arrows are compile-time imports; dashed arrows are interface
satisfaction. Note that **no arrow points from the application layer into an
adapter** — the application only ever names an interface in
`application/ports`, and `cmd/inventory/main.go` is the only place that decides
which implementation gets plugged in.

## The rule is executable, not aspirational

`internal/architecture/architecture_test.go` uses
[arch-go](https://github.com/arch-go/arch-go) to assert the layering as a
normal Go test, and CI runs it as a blocking `arch-test` job. The rules it
encodes:

1. `internal/domain/...` must not import any other `internal/...` package.
2. `internal/application/...` may import only `internal/domain/...` and
   itself — never `internal/adapters/...`.
3. `internal/adapters/inbound/...` must not import
   `internal/adapters/outbound/...` (and vice versa).
4. Nothing under `internal/...` may import `cmd/...` — `cmd` stays the leaf
   composition root.
5. `internal/application/ports` may contain only interfaces.
6. `internal/analytics/...` imports nothing internal except itself, and the
   OLTP domain must not import the analytics store (ADR 0011).

Further fitness tests in the same package (ADR 0029) guard the MCP adapter's
dependency rule, forbid reintroducing auth middleware, keep Kafka consumer
groups out of inline literals, require testcontainers for Kafka integration
tests, pin the event catalogue to `apis/asyncapi.yaml`, enforce
CloudEvents-only encoding, and require `CommitInterval` on replay consumers.

If someone reaches from a use case into `pgx` for a "quick fix," the build goes
red. See [ADR 0006](/docs/adr/0006-arch-go-fitness-tests).

## Ports

| Port | Responsibility | Implementations |
| --- | --- | --- |
| `StockRepo` | Persist/retrieve `StockUnit`; find by ID, SKU, bin; mint IDs | `postgres`, `memory` |
| `LocationRepo` | Persist/retrieve `Bin` | `postgres`, `memory` |
| `ReservationRepo` | Persist/retrieve `Reservation`; mint IDs | `postgres`, `memory` |
| `ProductClassificationRepo` | Persist/retrieve `ProductClassification` by SKU | `postgres`, `memory` |
| `LocationClassificationLookup` | Zone attributes (hazmat, temperature class) for a bin, for stow placement rules | `facilitycache` (Kafka-fed cache), `facilitylayout` (sync HTTP client, permissive stub) |
| `EventPublisher` | Publish a `shared.DomainEvent` | `events` (log/buffered/multi fan-out), `postgres.OutboxPublisher` (one `outbox_events` row per event × topic inside the caller's transaction, ADR 0017), `kafka` (integration + analytics, direct publish when there is no database) |
| `UnitOfWork` | Bracket a use case's `Save`(s) and `Publish`(es) in one atomic scope | `postgres.UnitOfWork` (joins a transaction already on the context, e.g. the idempotency middleware's); `nil` in in-memory mode |
| `ReservationMetrics` | Count reservations created/revoked (`inventory.reservations`) | `telemetry` |
| `Clock` | `Now()` — makes reservation timeouts deterministic in tests | `memory.SystemClock`, fixed clocks in tests |

The `Clock` port matters more than it looks: reservation expiry is a *domain*
concept, so time must be injected rather than read from `time.Now()` inside an
aggregate. Every domain event's `occurredAt` comes from that injected clock.

## Composition root

`cmd/inventory/main.go` is the only file that knows both an interface and its
implementation, and reads almost all configuration (the exceptions are
`CORS_ALLOWED_ORIGINS`, read by the HTTP adapter, and `ENVIRONMENT`, read by
the telemetry adapter):

| Env var | Default | Effect |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Listen address |
| `DATABASE_URL` | *(unset)* | If unset, the in-memory adapters are used and no database is required. If set, migrations run and the Postgres adapters are wired. |
| `MIGRATIONS_PATH` | `migrations` | Where golang-migrate looks for SQL files |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | Direct (non-PgBouncer) DSN used only for the migration step (ADR 0023) |
| `EVENT_PUBLISHER` | `log` | `kafka` swaps in the integration + analytics publishers; with `DATABASE_URL` set they are reached through the transactional outbox and a background relay (ADR 0017) |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Poll interval of the outbox relay (`EVENT_PUBLISHER=kafka` with Postgres only) |
| `HOUSEKEEPING_INTERVAL` / `IDEMPOTENCY_KEY_TTL` / `OUTBOX_RETENTION` | `1h` / `24h` / `168h` | Housekeeping sweeper (ADR 0026); `0` disables the sweeper or keeps that table's rows forever |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `LOCATION_LOOKUP_MODE` | `permissive` | `kafka` (Kafka-fed facility-layout cache; requires `KAFKA_BROKERS`), `http` (sync call), or `permissive` (no lookup) |
| `FACILITY_LAYOUT_BASE_URL` | *(unset)* | facility-layout base URL for `LOCATION_LOOKUP_MODE=http` |
| `OTEL_SERVICE_NAME` / `OTEL_EXPORTER_OTLP_ENDPOINT` / `SERVICE_VERSION` / `LOG_LEVEL` | `inventory-storage` / `localhost:4317` / `dev` / `info` | Telemetry and logging |

The in-memory fallback is deliberate: `go run ./cmd/inventory` with no
environment at all starts a fully functional service, which is what makes the
`httptest` suite and the godog acceptance suite cheap to run.

## Quality gates

Everything below runs in `.github/workflows/ci.yml` on every push and pull
request:

| Job | What it enforces |
| --- | --- |
| `lint` | `golangci-lint` against the committed `.golangci.yml` |
| `guide-lint` | The harness guide/repo linters and hook self-tests (`scripts/harness/*.py`) |
| `complexity` | `golangci-lint` with `gocyclo,gocognit,cyclop,funlen,nestif` only; informational `gocyclo` report in the job summary |
| `test` | Unit tests with `-race`; coverage gate on domain + application |
| `bdd` | godog acceptance suite over the Gherkin specs in `features/` |
| `contract` | Schemathesis property-based contract tests driven from `apis/openapi.yaml` |
| `evals-tests` | The MCP eval gate (E1 schema, E2 wire conformance, E3 behavioral) as its own named check — the same suites also run inside `test` |
| `integration` | Real Postgres 16 service container for the Postgres adapters; testcontainers-started Kafka for the facility-layout cache consumer |
| `mutation-fast` | `gremlins` on `internal/domain/stock` — blocking |
| `mutation` | `gremlins` on all of `internal/domain` (scheduled/dispatch only) |
| `drift` | Dead code, `go mod tidy -diff`, knip, coverage quality (scheduled/dispatch only, advisory) |
| `api-lint` | Spectral against `apis/openapi.yaml` **and** `apis/asyncapi.yaml` |
| `vuln` | `govulncheck ./...` |
| `docs-api-drift` | Regenerates the REST reference from `apis/openapi.yaml` and fails on any diff |
| `helm-lint` | `ct lint` against `charts/inventory-storage` (pull requests) |
| `arch-test` | The hexagonal fitness tests above |
| `web` | Lint, typecheck, test and build of the `web/` remote |
| `trivy-scan` | Container image scan, blocking on fixable CRITICAL/HIGH (pull requests) |
| `docker-publish` | Gated on `lint`, `complexity`, `test`, `bdd`, `contract`, `integration`, `mutation-fast`, `vuln`, `api-lint`, `arch-test`, `evals-tests`; pushes to GHCR on `main` only |
| `release` | `main`-only, gated on `docker-publish`: computes the next `vMAJOR.MINOR.PATCH` tag and cuts the GitHub release |

This documentation site is built and deployed by a separate workflow,
`.github/workflows/docs.yml`, which never touches `ci.yml`.
