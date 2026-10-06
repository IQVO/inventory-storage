---
id: 0001-hexagonal-ports-and-adapters
slug: /adr/0001-hexagonal-ports-and-adapters
title: 0001. Hexagonal (ports & adapters) architecture
sidebar_label: 0001. Hexagonal architecture
description: ADR 0001 — adopt ports & adapters with a strict inward-only dependency rule.
---

# 0001. Hexagonal (ports & adapters) architecture

## Status

Accepted. Established with the initial implementation of the bounded context
and never relaxed since; reinforced by [ADR 0006](./0006-arch-go-fitness-tests.md),
which made the rule executable.

## Context

This bounded context is a **Core subdomain**: it owns inventory truth, and its
invariants — every item has exactly one known bin or is `Unlocated`; a stow
needs both scans; reserved never exceeds usable; a bin never overflows — *are*
the business value. If those rules are correct and cheap to reason about, the
service is doing its job.

Several forces push against keeping them clean:

- **Infrastructure churn is guaranteed and domain churn is not.** The storage
  engine, the router, the event transport and the deployment target will all be
  swapped or upgraded over the service's life. "A stow requires an item-scan
  and a location-scan" will not change, because the physical warehouse does not
  change.
- **The idiomatic Go temptation is to reach for the database from the handler.**
  It works, it is short, and it makes every invariant a property of a SQL
  statement rather than of a type. Under chaotic storage that is dangerous:
  invariants that live in queries are invariants that get bypassed by the next
  query someone writes.
- **The invariants must be testable without infrastructure.** Correctness here
  is worth a large test investment (mutation testing, exhaustive failing-path
  tests). That investment only pays if tests are milliseconds, not seconds.
- **The service must run without a database.** A developer poking at the API,
  the `httptest` suite and the godog acceptance suite all need a fully
  functional service with no Postgres and no Kafka.

## Decision

**We will structure the service as a hexagon (ports & adapters), with a strict
inward-only dependency rule:**

> **domain depends on nothing; application depends on domain; adapters depend
> on application/domain.**

Concretely:

- `internal/domain/` is **pure Go**. No `chi`, no `pgx`, no `database/sql`, no
  `context` in aggregate methods, no struct tags for serialisation. Aggregates
  (`StockUnit`, `Bin`, `Reservation`) and value objects (`SKU`, `BinId`,
  `Quantity`) enforce their own invariants at construction and on every
  mutation.
- `internal/application/ports/` declares the **outbound interfaces** the
  application needs: `StockRepo`, `LocationRepo`, `ReservationRepo`,
  `EventPublisher`, `Clock`. They are owned by the application, expressed in
  domain types, and named for what the application wants — not for what any
  particular technology offers.
- `internal/application/usecases/` holds **one struct per use case**, with
  collaborators as plain fields. No use case imports an adapter package.
- `internal/adapters/` implements the ports: `inbound/http` (chi, DTOs, error
  mapping), `outbound/postgres`, `outbound/memory`, `outbound/events`,
  `outbound/kafka`.
- There are **four composition roots**, one per binary — `cmd/inventory`
  (REST + outbox relay), `cmd/mcp` (MCP server, ADR-0008), `cmd/inventory-projector`
  and `cmd/inventory-reports` (the analytical data product, ADR-0011). They
  are the only places that know both an interface and its implementation, and
  they read the application's configuration from environment variables
  (`getenv` in each `main.go`). Two narrow, documented exceptions read an
  environment variable inside an adapter because the value is
  adapter-private, not wiring: `CORS_ALLOWED_ORIGINS` in
  `internal/adapters/inbound/http/server.go` (`corsAllowedOrigins`) and
  `ENVIRONMENT` / the OTLP endpoint normalisation in
  `internal/adapters/outbound/telemetry/telemetry.go`. Neither the domain nor
  the application layer reads the environment (enforced by the arch-go
  fitness tests, ADR-0006).

`Clock` is a port for the same reason the repositories are: reservation expiry
is a *domain* concept, so time is injected rather than read from `time.Now()`
inside an aggregate.

## Consequences

### Easier

- **Invariants are unit-testable in microseconds.** The domain has no I/O, so
  every failing path (`ErrBinFull`, `ErrInsufficientUsable`,
  `ErrStowRequiresItemAndLocation`, `ErrAlreadyResolved`) is a table-driven
  test. This is what made ~99% domain coverage and a full `gremlins` mutation
  pass affordable.
- **Two storage backends, no domain change.** `memory` and `postgres`
  implement the same ports. `go run ./cmd/inventory` with no `DATABASE_URL`
  starts a working service.
- **Kafka was purely additive.** Cross-service integration shipped as a new
  `outbound/kafka` package implementing the existing `EventPublisher`,
  selected by an env var, with zero changes to any aggregate, invariant or use
  case.
- **The wire contract and the model can evolve independently.** DTOs live in
  the HTTP adapter; a `stockUnitResponse` is not a `stock.StockUnit`.
- **Determinism.** Injecting `Clock` makes expiry tests exact instead of
  sleep-based.

### Harder

- **More files and more indirection.** A single field added end-to-end touches
  the aggregate, the port implementations, the DTO and the mapping. For a CRUD
  service this would be over-engineering; for a Core subdomain it is the point.
- **Mapping code is real work.** Every adapter converts between domain types
  and its own representation, and that mapping has to be integration-tested
  (which is why there is a Postgres integration test per outbound adapter).
- **Cross-aggregate operations need explicit orchestration.** `ConfirmPick`
  touches `Reservation`, `StockUnit` and `Bin` through three separate
  repositories rather than one transaction-scoped query. Ordering becomes a
  use-case concern the author must get right.
- **The rule is easy to violate under deadline pressure.** Nothing in the Go
  compiler stops a use case importing `pgx`. That gap is what
  [ADR 0006](./0006-arch-go-fitness-tests.md) closes.
