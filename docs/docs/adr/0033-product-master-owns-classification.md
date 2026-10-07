---
id: 0033-product-master-owns-classification
slug: /adr/0033
title: "ADR 0033: product-master owns product classification; this service keeps a local copy"
sidebar_label: "33. product-master owns classification"
sidebar_position: 33
---

# 0033. product-master owns product classification; this service keeps a local copy

## Status

**Accepted** (2026-10-06). Companion of product-master ADR 0001 (bounded
context) and ADR 0003 (migration); this record implements **stages B and C**
of product-master ADR 0003 on the inventory-storage side.

- Supersedes the **ownership** part of
  [ADR 0009](./0009-product-classification-as-sku-master-data.md) ("this
  service owns `ProductClassification` as source of truth"). ADR 0009's
  stow-time placement rules (hazmat zone, matching temperature class,
  fail-open/fail-closed asymmetry) stay in force unchanged.
- Supersedes the **publication** part of
  [ADR 0031](./0031-publish-product-classified.md) ("`ClassifyProduct`
  publishes `ProductClassified` on both topics"). The wire contract of the
  legacy type is unchanged; only the one-shot backfill command below still
  emits it.
- Does **not** change [ADR 0010](./0010-dot-hazard-class-and-same-bin-segregation.md):
  the DOT segregation matrix stays here, because co-locating goods in a bin is
  still this context's job.

## Context

product-master ADR 0001 creates a `product-master` bounded context as the
single source of truth for SKU-level product master data, including the
handling classification (handling tags, temperature class, DOT hazard
class). ADR 0009 had put that classification here only because no other home
existed; ADR 0009 itself calls it "a property of the product, not of any
physical holding".

`StowStock` still needs the classification at stow time, for two rules that
remain this context's responsibility:

1. placement: a Hazmat SKU needs a hazmat-rated zone, a TemperatureSensitive
   SKU needs a zone with a matching temperature class (ADR 0009);
2. same-bin DOT segregation against the bin's current occupants (ADR 0010).

product-master ADR 0003 sets out a strangler migration with five stages that
uses event-carried state transfer only (no shared database, no cross-context
REST). Stage A (product-master's legacy importer of this service's
`ProductClassified`) is product-master's own work. Stages B and C need
changes here.

## Decision

### Stage B: one-shot backfill command

The main binary gains a subcommand:

```text
kubectl exec deploy/inventory-storage -- ./inventory republish-product-classifications
```

(the image's `WORKDIR` is `/app` and its `ENTRYPOINT` is `./inventory`;
`/app/inventory republish-product-classifications` works from any directory).

- It needs only `DATABASE_URL` (the pod already has it). It does not run
  migrations, start the HTTP server, consume Kafka or talk to the broker.
- It reads every row of `product_classifications` in SKU order, in batches
  of 500 (keyset pagination on `sku`), and for each row enqueues the
  **existing** legacy CloudEvent
  `com.warehouse.wms.inventory-storage.product.ProductClassified`
  (`dataschema urn:warehouse:inventory-storage:events:ProductClassified:v1`,
  payload `{sku, handling_tags[], temperature_class?, dot_hazard_class?}`,
  key and subject = SKU) into `outbox_events` for the integration topic
  `warehouse.inventory.events`. Each batch commits in its own transaction.
  The running pod's outbox relay publishes the rows; product-master's legacy
  importer (its ADR 0003 stage A) consumes them.
- Only the integration topic is written. The analytics copy of the type
  (ADR 0031) has no consumer (the projector ignores it), so the backfill does
  not add rows to `warehouse.inventory.analytics`.
- The command prints `republished <n> product classifications` and exits 0;
  any database error exits non-zero. It is idempotent to re-run: every
  message is a full-state replacement keyed by SKU, and product-master skips
  a `native` classification and ignores an identical one, so a second run
  changes nothing downstream. A crash midway leaves the committed batches
  enqueued; re-running simply enqueues everything again.
- The legacy encoder mapping (and its exact-JSON goldens,
  `internal/adapters/outbound/kafka/product_classified_test.go`) stays,
  for this command only. The normal write path no longer raises the event.

### Stage C: cutover of write authority

1. **`PUT /products/{sku}/classification` returns `410 Gone`** with problem
   type `https://errors.inventory-storage.warehouse-systems.dev/classification-moved`
   and a detail naming product-master's `PUT /products/{sku}/classification`.
   The request body is not read. The `ClassifyProduct` use case is removed
   (it had no MCP tool).
2. **Local copy consumer.** A new inbound Kafka consumer reads
   product-master's topic `warehouse.product-master.events` with a stable
   consumer group taken from env `PRODUCT_MASTER_CONSUMER_GROUP`. When the
   variable is unset the consumer is not started (the local copy then only
   holds the legacy rows). It requires `DATABASE_URL` and `KAFKA_BROKERS`;
   boot fails if the group is set without them.
   - It acts only on the full type
     `com.warehouse.wms.product-master.product.ProductClassified`, payload
     `{sku, handling_tags[], temperature_class?, dot_hazard_class?, classification_source, version}`;
     every other type on the topic is committed past untouched.
   - Use case `ApplyProductClassification` claims the CloudEvents `id` in
     `processed_events` (consumer `product-master-classification`) and
     upserts `product_classifications` in **one** `UnitOfWork`. The upsert
     applies only when the incoming `version` is greater than the stored
     one (a missing row is inserted). A duplicate `id` or a stale `version`
     is a no-op that still commits the offset.
   - It raises **no domain event**: nothing reaches the outbox, so there is
     no publish loop between the two contexts.
   - At-least-once rules: `FetchMessage` + `CommitMessages` only after the
     handler settles; a transient (database) error retries the same message
     with capped backoff; a message that is not a valid CloudEvent, has an
     undecodable payload or breaks the classification invariants is logged
     and committed past.
3. **Migration `0032_product_master_local_copy`** adds to
   `product_classifications`: `version BIGINT NOT NULL DEFAULT 0` and
   `classification_source TEXT NOT NULL DEFAULT 'inventory-storage'`. Legacy
   rows keep version 0, so the first product-master version (1 or more)
   always wins; applied rows store product-master's `classification_source`
   (`native` or `legacy-import`). It also creates `processed_events
   (consumer, event_id, processed_at)` with primary key
   `(consumer, event_id)`; this repository had no inbound dedupe table in the
   OLTP database (the transfer consumer dedupes on its own ledger's
   `transfer_line_id`).
4. **`StowStock` is unchanged.** It reads the same
   `ports.ProductClassificationRepo.FindBySKU` as before; the table is now
   fed by the consumer instead of a REST write.
5. **`GET /products/{sku}/classification` stays**, served from the local
   copy, and is marked `deprecated: true` in `apis/openapi.yaml`. It is
   removed at product-master ADR 0003 stage E.

### Stage E (not in this change)

The deprecated `GET` endpoint, the backfill command and the legacy encoder
mapping are removed after the cluster is verified (product-master ADR 0003).

## Consequences

- Classifications become eventually consistent here: a SKU classified in
  product-master moments before its first stow may still stow as
  unclassified (fail-open, ADR 0009). That is the same window any cached
  reader already had.
- Between deployment and the first run of the backfill, product-master knows
  no legacy classification; the runbook in product-master ADR 0003 runs the
  backfill right after product-master is ready.
- Operators classify SKUs in product-master only. A client still calling
  this service's `PUT` gets a 410 with the new address instead of a silent
  write that product-master would never see.
- One more consumer group to operate (`PRODUCT_MASTER_CONSUMER_GROUP`).
- `ProductClassified` is no longer published on the normal write path, so
  any sibling that had started to read it from `warehouse.inventory.events`
  must read product-master's topic instead (none had, per ADR 0031).
