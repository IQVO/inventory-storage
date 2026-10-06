---
name: how-to-add-an-integration-event
description: Publish or consume a cross-service Kafka event: CloudEvents 1.0 type naming, AsyncAPI, transactional outbox, consumer-group rules. Use when touching internal/adapters kafka or outbox code, a publisher/consumer, or apis/asyncapi*.yaml.
---

# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event, or
consume one from a sibling bounded context. This fleet's Kafka is ONE
broker platform-wide — every design decision below exists because that
shared-broker reality has already caused a real incident once.

## Publishing a new integration event

### 1. Is it actually cross-service?

Not every domain event this service raises belongs on the wire. Check
`internal/adapters/outbound/kafka/publisher.go`'s doc comment — this repo
forwards only `StockReserved`/`ReservationRevoked`, the two transfer replies and
`ProductClassified` (ADR-0031); everything else is a
local concern (with `EVENT_PUBLISHER=kafka` most of them also go to the
separate `warehouse.inventory.analytics` topic, ADR-0011 — that is not the
integration contract). Note that publishing goes through the transactional
outbox (ADR-0017): `internal/adapters/outbound/postgres/outbox_publisher.go`
writes the already-encoded Kafka messages to `outbox_events` in the same
transaction as the aggregate save, and `outbox_relay.go` drains them onto
Kafka. Before adding a
new event to the Kafka publisher, confirm a sibling context genuinely needs
to react to it — check `docs/docs/ecosystem/context-map.md` for who's
actually downstream.

### 2. Envelope: CloudEvents 1.0, structured mode (mandatory, ADR-0024)

Every message is a CloudEvents 1.0 JSON document
(`application/cloudevents+json`) — there is no other envelope:

```json
{
  "specversion": "1.0",
  "id": "<uuid, minted once in Encode>",
  "source": "/warehouse/inventory-storage",
  "type": "com.warehouse.wms.inventory-storage.<entity>.<EventName>",
  "subject": "<aggregate id>",
  "time": "<RFC3339 UTC occurred-at>",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:inventory-storage:<events|analytics>:<EventName>:v1",
  "data": { /* the actual payload, business types only */ }
}
```

`type` follows `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`,
all lowercase except the final PascalCase event name. For this repo:
subdomain `wms`, context `inventory-storage`, entity = the raising
aggregate as catalogued in `apis/asyncapi.yaml` (`stock`, `reservation`,
`bin`; `product` for ProductClassified). Never hand-build the envelope —
call `cloudevents.New(cloudevents.Spec{...})` from
`internal/adapters/kafka/cloudevents` and add
`cloudevents.ContentTypeHeader()` to the message headers.

### 3. Implementation

Add the event struct to `internal/domain/<aggregate>/` (it should already
exist as a domain event the aggregate raises — publishing wires an
EXISTING domain event onto Kafka, it doesn't invent a new payload shape at
the adapter layer). In the Kafka publisher adapter:

- Add the event's case (entity, subject, payload) and encode it with
  `cloudevents.New`
- Give the message a partition key that keeps ordering where it matters
  (usually the aggregate id)
- Use `Topic` — this service's own topic constant
  (`warehouse.<context>.events`), never a sibling's

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under this service's channel,
  matching the entity-grouping convention already there (group by
  aggregate, not chronologically)
- Update the hand-written `docs/docs/api-reference/events.md` catalog
  (this repo has no generated AsyncAPI HTML — no `gen-async-docs` script
  and no `static/asyncapi/`). The `api-lint` CI job Spectral-lints
  `apis/asyncapi.yaml`; `docs-api-drift` only covers the REST reference
  generated from `apis/openapi.yaml`.

### 5. Test

Add a golden exact-JSON case to `internal/adapters/outbound/kafka/golden_test.go`
(all CloudEvents attributes + `content-type` header) — never a real broker
in a unit test. If this event
now needs a `_integration_test.go` asserting real delivery, it MUST use
testcontainers (see the fitness test `TestKafkaIntegrationTestsUseTestcontainers`
in `internal/architecture/` — a skip-gated `KAFKA_BROKERS` test or a
hardcoded `localhost:9092` fails CI).

## Consuming an integration event from a sibling context

### 1. Never import the sibling's Go packages

This service knows a sibling's topic name and payload shape ONLY — never
its Go types. See `internal/adapters/outbound/facilitycache/consumer.go`'s
own doc comment: "This service has no business knowing anything else
about that context beyond this topic name and the CloudEvents types/payload
shapes below." Decode with `cloudevents.Decode`, dispatch on the FULL
`type` string (exact strings are in ADR-0024's cross-service table),
read the payload with `DataAs`, dedupe on `id`, and DLQ/skip anything
Decode rejects — never parse a legacy flat shape. Hand-mirror the payload struct locally; do not add a Go module
dependency on the sibling repo (an architecture fitness test in most
repos in this fleet would catch that anyway for the stricter contexts —
check this repo's own `internal/architecture/` for a
`TestNoSiblingContextOutboundCalls`-style guard before assuming it's
allowed).

### 2. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist. Picking the wrong one for your use
case is THE most common integration-event mistake in this fleet, and it
was learned from a real incident (wes-work-planning#67).

**Pattern A — long-lived, single-instance consumer group (a named
constant).** Use when exactly ONE instance of this consumer ever runs at
a time (e.g. this service's own analytics projector). The group id is a
plain named constant (`AnalyticsConsumerGroup`), reused across restarts —
that's correct because Kafka's committed-offset resume semantics are
EXACTLY what you want: pick up where the single instance left off.

**Pattern B — per-process-unique consumer group (a generated id).** Use
when this consumer rebuilds a complete read model from a topic's FULL
history on every start (an event-sourced local cache, not a work queue) —
see `facilitycache/consumer.go`'s `consumerGroupPrefix` +
`uniqueConsumerGroup()`. The group id MUST be unique per process instance
(hostname+PID+timestamp), NEVER a fixed shared string. Consumer group
offsets are shared infrastructure state: a brand-new process joining a
group an EARLIER instance already consumed resumes from that instance's
committed offset, so the new process gets marked "ready" with an empty
local cache having replayed nothing — a silent correctness bug, not a
crash.

**Never do this** (the actual incident): a fixed shared consumer group id
on a consumer meant to run as exactly one instance per environment. When
a local dev/test harness process joins the SAME broker's SAME group as a
live in-cluster Deployment, Kafka's rebalance protocol hands the
partition to only ONE of the two group members — the other silently
starves. Fix: make the group id env-configurable
(`KAFKA_CONSUMER_GROUP`/`<SERVICE>_CONSUMER_GROUP`), never hardcode it as
a literal string. This fleet's `internal/architecture/`
`TestKafkaConsumerGroupNeverHardcodedInline` fitness test (where present)
enforces this statically — an inline `GroupID: "literal"` fails CI.

### 3. Readiness gate, if this consumer backs a local cache

If the consumer replays a topic's full history to build a cache other
code depends on, expose a `Ready()` gate the health check consults, and
block readiness (not process startup — a transient Kafka outage
shouldn't be fatal) until the initial replay finishes. A readiness check
that only re-evaluates on a NEW message arriving deadlocks forever on an
ordinary restart where a shared/already-caught-up group never gets a new
message to trigger it — use `OffsetFetch` against the group's committed
offset, or (simpler and less bug-prone) just use the per-process-unique
group pattern above, which sidesteps the whole class of bug.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a sibling-package import
```

Prove any new fitness-test-adjacent behavior actually matters by running
the specific scenario against a real broker if this repo has
testcontainers-based integration tests for the consumer/publisher touched.
