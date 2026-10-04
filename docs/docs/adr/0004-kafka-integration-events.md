---
id: 0004-kafka-integration-events
slug: /adr/0004-kafka-integration-events
title: 0004. Kafka and a shared envelope for integration events
sidebar_label: 0004. Kafka integration events
description: ADR 0004 — publish integration events over Kafka with a platform-wide envelope, as an additive adapter behind the existing port.
---

# 0004. Kafka and a shared envelope for integration events

## Status

**Superseded by [ADR-0024](./0024-cloudevents-mandatory-envelope.md)** for
the envelope: the flat `event_id` / `event_type` / `occurred_at` / `source` /
`data` wrapper described below is retired, and every message is now a
CloudEvents 1.0 structured-mode event. Kafka as the transport and the
additive-adapter decision still stand. Originally accepted. Introduced as a strictly additive change (`Task 7`, commit *"Task 7:
cross-service Kafka integration (additive)"*). The CloudEvents `type`
convention was formalised later, alongside `apis/asyncapi.yaml`.

## Context

`wes-work-planning` — the WES-tier conductor — needs stock reality to plan and
release work. It needs to know when usable inventory for a SKU goes down
(something was reserved) and when it comes back up (a reservation was revoked).

Three options were on the table:

- **Give it database access.** Fastest to build, and immediately fatal to the
  bounded-context boundary: `warehouse-systems-ddd.md` requires that "WES does
  **not** get write access to WMS's Order/Inventory aggregates — only to the
  published events/API." Read access is barely better — it couples Work
  Planning to this service's schema and turns every migration into a
  cross-repository breaking change.
- **Have it poll `GET /inventory/{sku}/usable`.** Correct but wasteful and
  laggy: the conductor would have to poll every SKU it cares about, and would
  learn about a revoke only on the next tick.
- **Publish integration events.** This service states facts; anyone interested
  subscribes. The publisher does not need to know who is listening, which is
  what makes it an **Open Host Service** rather than a point-to-point
  integration.

The service already had a `ports.EventPublisher` port with log, buffered and
Postgres implementations, and every use case already published past-tense
domain events through it. The gap was purely transport.

Constraints that shaped the shape of the answer:

- **The existing test suite must be unaffected.** Requiring a broker for
  `go test ./...` would be a serious regression.
- **The envelope must be identical across all five services.** Five slightly
  different envelopes would push translation into every consumer.
- **One shared broker.** It runs from `~/warehouse-systems/docker-compose.kafka.yml`;
  this repo must connect to it, not stand up a competing one.
- **Not everything is integration.** Ten domain events exist; only two are of
  cross-context interest. Publishing all ten would expose internal model
  detail as a public contract.

## Decision

**We will publish integration events to Kafka from a new outbound adapter
behind the existing `EventPublisher` port, selected by environment variable,
using a platform-wide envelope.**

1. **Client library:** `github.com/segmentio/kafka-go` — pure Go, no cgo.
2. **Topic:** `warehouse.inventory.events`, one per bounded context, matching
   the platform's `warehouse.<context>.events` naming.
3. **New package `internal/adapters/outbound/kafka/`** implementing the
   existing `ports.EventPublisher`. No aggregate, invariant or use case
   changed.
4. **Selected by env:** `EVENT_PUBLISHER=kafka|log`, **defaulting to `log`**,
   so the existing suite and local runs need no broker. Brokers come from
   `KAFKA_BROKERS` (default `localhost:9092`).
5. **Publish exactly two events** — `StockReserved` and `ReservationRevoked` —
   both with `data: {sku, quantity, demand_ref}`. Everything else hits the
   adapter's `default: return nil` branch and stays in-process.
6. **`ReservationRevoked` is enriched in the adapter.** The domain event
   carries only a reservation id; the adapter looks the reservation up through
   `ports.ReservationRepo` to fill in `sku` / `quantity` / `demand_ref`.
   Enrichment for a consumer's convenience is an adapter concern — putting
   those fields on the domain event to save a lookup would let an integration
   requirement leak into the domain model.
7. **The `type` convention is platform-wide:**
   `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>` — all
   lowercase except the final PascalCase name. Here the subdomain segment is
   `wms` and the entity segment is the aggregate: `stock`, `reservation`,
   `bin`.
8. **The contract is a checked-in, linted artefact:** `apis/asyncapi.yaml`
   (AsyncAPI 2.6.0), Spectral-linted by the `api-lint` CI job, documenting the
   complete catalog and marking each catalog-only message explicitly.

## Consequences

### Easier

- **The boundary holds.** Work Planning gets stock reality without schema
  access and without write access.
- **Additive, reversible.** `EVENT_PUBLISHER=log` restores the previous
  behaviour exactly. The Kafka work touched no domain code.
- **Testable without a broker.** `Writer` is a one-method interface, so
  `publisher_test.go` asserts the exact envelope against an in-memory fake and
  runs in the default suite.
- **New consumers cost this repo nothing.** A sixth service subscribes from the
  published spec; no change here.
- **A small public surface.** Two events, not ten — the internal model can
  evolve freely.

### Harder

- **At-least-once delivery is now every consumer's problem.** Consumers must
  deduplicate; `wes-work-planning` does so with a `processed_events` table
  keyed by event id.
- **No ordering guarantee** *(as originally accepted; superseded by
  [ADR-0021](./0021-kafka-producer-partition-key.md): messages are now keyed
  by reservation id and routed by `Hash`, giving per-reservation ordering)*.
  The `LeastBytes` balancer with no partition key meant no per-SKU ordering. Acceptable for an increment/decrement projection,
  but it is why the REST read remains authoritative.
- **Publish failures fail the request.** `Publish` errors propagate out of the
  use case, so a broker outage surfaces as a `500`. That is the honest
  behaviour for now, but it couples request success to broker availability —
  a transactional outbox would decouple them and is not built.
- **Two envelopes coexist.** The adapter emits the legacy flat envelope
  (`event_id` / `event_type` / `occurred_at` / `source` / `data`), while the
  AsyncAPI document describes the CloudEvents target the platform is moving to.
  The `data` payloads match field-for-field; the context attributes do not.
  Documented explicitly in `apis/asyncapi.yaml` and on the
  [Events page](/docs/api-reference/events) rather than hidden — migrating the
  adapter is outstanding work.
- **The `ReservationRevoked` enrichment needs a repo read at publish time**,
  which is an extra query and a failure mode (`ErrReservationNotFound`) that
  would not exist if the payload came straight off the event.

## Verification

Unit-tested against an in-memory `Writer` fake, and smoke-tested for real: with
the shared broker running and `EVENT_PUBLISHER=kafka`, a live
`POST /reservations` against the running binary was confirmed to land on
`warehouse.inventory.events` via a console consumer before the task was
considered done.
