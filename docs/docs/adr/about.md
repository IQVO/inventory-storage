---
slug: /adr
title: Architecture Decision Records
sidebar_label: About ADRs
description: Why this service keeps ADRs, the template it uses, and how to propose a new one.
---

# Architecture Decision Records

## What these are

An **Architecture Decision Record (ADR)** captures a single architecturally
significant decision, its context, and its consequences — at the moment it was
made, in the words that were true then.

The value is not the decision; it is the **context**. Six months later, the
code shows *what* was chosen and can never show *why*, or what the alternatives
were, or what was known at the time. A reader who does not know the why has two
bad options: assume the decision was arbitrary and change it, or assume it was
sacred and work around it. An ADR gives them a third.

## The format

These records use **Michael Nygard's template** — the de facto standard:

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play: technical, business, operational. What is true that makes
this a decision rather than an obvious default? Written in full sentences and
in the present tense, describing the situation as it is.

## Decision
The response to those forces, stated actively: "We will…"

## Consequences
What becomes easier and what becomes harder — both. A record with only
positive consequences has not been thought about.
```

One decision per file, numbered `0001-`, `0002-`, … Numbers are never reused
and files are never deleted.

## Immutability

**An accepted ADR is never edited to change its decision.** If a decision is
reversed, write a new ADR that supersedes it, and update the old one's
*Status* line to point at the successor. The historical record of what was
believed at the time is the entire asset.

Typos, broken links and formatting are of course fair game.

## The records

| # | Title | Status |
| --- | --- | --- |
| [0001](./0001-hexagonal-ports-and-adapters.md) | Hexagonal (ports & adapters) architecture | Accepted |
| [0002](./0002-chaotic-storage-over-fixed-slotting.md) | Chaotic (random) stow over fixed slotting | Accepted |
| [0003](./0003-revocable-reservations.md) | Revocable reservations over hard allocation | Accepted |
| [0004](./0004-kafka-integration-events.md) | Kafka and a shared envelope for integration events | Superseded by [0024](./0024-cloudevents-mandatory-envelope.md) (envelope) |
| [0005](./0005-rfc-7807-problem-details.md) | RFC 7807 Problem Details for all error responses | Accepted |
| [0006](./0006-arch-go-fitness-tests.md) | arch-go fitness tests to enforce the dependency rule | Accepted |
| [0007](./0007-godog-bdd-acceptance-tests.md) | godog/Gherkin acceptance tests as executable specification | Accepted |
| [0008](./0008-mcp-inbound-adapter.md) | Model Context Protocol as an inbound adapter, not a new service | Accepted |
| [0009](./0009-product-classification-as-sku-master-data.md) | Product classification as SKU-level master data, enforced at stow time | Accepted |
| [0010](./0010-dot-hazard-class-and-same-bin-segregation.md) | Optional DOT hazard class and same-bin DOT segregation at stow time | Accepted |
| [0011](./0011-analytical-data-product.md) | Per-service analytical data product (report) via a separate analytics topic | Accepted (envelope superseded by [0024](./0024-cloudevents-mandatory-envelope.md)) |
| [0012](./0012-adopt-mfe-console-architecture.md) | Adopt the fleet's micro-frontend console architecture (ADR-0002 in `warehouse-ops-agent`) | Accepted |
| [0013](./0013-location-classification-via-facility-events.md) | Location classification from facility-layout's events, not a synchronous call | Accepted |
| [0014](./0014-rest-identity-adoption.md) | Adopt the fleet REST identity standard (static bearer keys, read/read-write scopes; `warehouse-ops-agent` ADR 0005) | Superseded by [0015](./0015-remove-rest-identity-layer.md) |
| [0015](./0015-remove-rest-identity-layer.md) | Remove the fleet REST identity layer (static bearer keys, read/read-write scopes) | Accepted |
| [0016](./0016-standard-metrics-convention.md) | Standard metrics convention across the fleet | Accepted |
| [0017](./0017-transactional-outbox.md) | Transactional outbox + relay for Kafka publishing | Accepted |
| [0018](./0018-idempotency-key-middleware.md) | Transactional Idempotency-Key middleware for POST /stock/receive and POST /reservations | Accepted |
| [0019](./0019-optimistic-concurrency-version-column.md) | Optimistic concurrency (version column) for StockUnit, Bin, Reservation | Accepted |
| [0020](./0020-resilience-circuit-breaker-retry-dlq-shutdown.md) | Circuit breaker, read-only retry, Kafka DLQ, and graceful shutdown hardening | Accepted |
| [0021](./0021-kafka-producer-partition-key.md) | Key every integration/analytics Kafka message by its aggregate id, and route by Hash not LeastBytes | Accepted |
| [0022](./0022-horizontal-autoscaling-and-pgxpool-tuning.md) | Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning | Accepted |
| [0023](./0023-migrations-direct-postgres-connection.md) | Run golang-migrate against a direct Postgres connection, not PgBouncer | Accepted |
| [0024](./0024-cloudevents-mandatory-envelope.md) | CloudEvents 1.0 as the mandatory event envelope | Accepted |
| [0025](./0025-bin-registration-endpoint-and-pick-location.md) | Declarative bin registration over REST (`PUT /bins/{binId}`), and the pick location (`binId`) on every reservation allocation | Accepted |

Each of these reconstructs a decision that is actually visible in this
repository's history, `CLAUDE.md`, `TASKS.md` or code — none is a generic
placeholder.

## Proposing a new one

1. Copy the template above into `docs/docs/adr/NNNN-short-kebab-title.md`,
   taking the next free number.
2. Open it with **Status: Proposed**.
3. Write the *Context* before the *Decision*. If the context does not make the
   decision feel inevitable, the context is incomplete — or the decision is
   wrong.
4. Fill in *Consequences* honestly, including the ones you dislike.
5. Add it to the table above and to `sidebars.ts`.
6. Raise it as a pull request; the discussion belongs on the PR, not inside the
   record.
7. On merge, flip the status to **Accepted**.

## What deserves an ADR

Something is architecturally significant if reversing it later would be
expensive — it constrains the structure of the code, pins a contract other
teams depend on, or is costly to undo.

**Yes:** the layering rule; how allocation works; the integration transport and
envelope; the error format on a public API; a quality gate that blocks merges.

**No:** which linter rules are enabled; a library upgrade; a refactor with no
external consequence. Padding the log with trivia makes the significant records
harder to find.
