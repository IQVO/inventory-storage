---
id: 0031-publish-product-classified
slug: /adr/0031
title: "ADR 0031: Publish ProductClassified through the outbox on both topics"
sidebar_label: "31. Publish ProductClassified"
sidebar_position: 31
---

# 0031. Publish ProductClassified through the outbox on both topics

## Status

Accepted. Supersedes **only** the "in-process only / never published" clause
of [ADR 0024](./0024-cloudevents-mandatory-envelope.md) **for
`ProductClassified`**. Every other decision in ADR 0024 (CloudEvents 1.0
structured mode, `type`/`dataschema` conventions, consumer rules) stands, and
`LocationRecorded` remains in-process only.

## Context

`ProductClassification` is SKU-level master data this service owns as source
of truth ([ADR 0009](./0009-product-classification-as-sku-master-data.md)).
`ClassifyProduct` has always raised a `ProductClassified` domain event, but
neither Kafka encoder mapped it, so ADR 0024 recorded it as delivered
in-process only ("if it is ever published, its entity segment is `product`").
The consequence is that every sibling that needs a SKU's handling attributes
**polls `GET /products/{sku}/classification`** per call: order-management,
wes-work-planning and fulfillment-execution each carry a REST client plus a
`PRODUCT_CLASSIFICATION_MODE` switch and a circuit breaker for it.

The 2026-10-05 audit flagged this as a hotspot, and the architect decided on
2026-10-06 (decision 9b): master data owned here is published as an event so
siblings can keep a local copy instead of polling. `LocationRecorded`, by
contrast, has no consumer and no stated need, and stays in-process.

## Decision

1. **`ProductClassified` is published** on both existing topics, through the
   transactional outbox ([ADR 0017](./0017-transactional-outbox.md)): the
   `OutboxPublisher` fans it out to the integration encoder
   (`warehouse.inventory.events`) and the analytics encoder
   (`warehouse.inventory.analytics`) inside the same `UnitOfWork` as the
   classification save, so the event commits with the aggregate or not at all.
   The direct `EVENT_PUBLISHER=kafka` path forwards it too.

2. **Wire contract** (additive; no existing type or payload changes):

   | attribute | value |
   | --- | --- |
   | `type` | `com.warehouse.wms.inventory-storage.product.ProductClassified` (entity `product`) |
   | `subject` / Kafka key | the SKU, so reclassifications of one SKU stay ordered on one partition ([ADR 0021](./0021-kafka-producer-partition-key.md)) |
   | `dataschema` | `urn:warehouse:inventory-storage:events:ProductClassified:v1` and `...:analytics:ProductClassified:v1` |
   | `data` | `{sku, handling_tags[], temperature_class?, dot_hazard_class?}` — the same shape on both topics |

   `handling_tags` is the aggregate's stable enum order. `temperature_class`
   and `dot_hazard_class` are omitted when unset. The payload is a
   **full-state replacement** of the SKU's classification: a consumer
   overwrites its local row, and an absent optional field means "none". It is
   SKU handling data only — no PII.

3. **The analytics projector ignores it.** `cmd/inventory-projector`
   dispatches on the full `type` and acknowledges unknown types without
   touching the read model (regression test:
   `TestAnalyticsConsumer_IgnoresProductClassified`). The analytics-topic copy
   exists so the type is available on the stream that already carries this
   context's other occurrences; the Flow & Accuracy report does not use it.

4. **`LocationRecorded` stays in-process** (no consumer; a test pins that
   neither encoder maps it). Publishing it later would be a new ADR.

5. **No consumer changes here.** REST `GET /products/{sku}/classification`
   stays (additive migration path). Moving order-management,
   wes-work-planning and fulfillment-execution off polling is each of those
   repositories' own decision; they can adopt the event whenever they choose.

## Consequences

- Siblings can build a local classification copy from
  `warehouse.inventory.events` (replay from the earliest offset gives them the
  current state per SKU, since each message is a full replacement keyed by
  SKU). Until a SKU is next re-classified, a consumer that starts mid-stream
  only learns its classification from the REST read or from a topic replay —
  this change does not backfill historical classifications onto the topic.
- The integration topic's published contract grows by one additive message;
  consumers already must ignore unknown `type` values (ADR 0024). The known
  consumer of that topic (wes-work-planning's stock projection) dispatches
  on the full `type` and is unaffected.
- One more outbox row per classify call per topic (two rows).
- Verification: exact-JSON goldens for both topics
  (`internal/adapters/outbound/kafka/product_classified_test.go`), and a
  testcontainers Postgres test proving the rows commit with — and roll back
  with — the classification
  (`internal/adapters/outbound/postgres/product_classified_outbox_integration_test.go`).
