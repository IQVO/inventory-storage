---
id: 0043-promote-exception-facts-to-integration-topic
slug: /adr/0043
title: "ADR 0043: Promote DiscrepancyDetected, ItemUnlocated and ItemStowed to the integration topic"
sidebar_label: "43. Promote exception facts to the integration topic"
sidebar_position: 43
description: "ADR 0043 — the three exception facts that until now lived only on warehouse.inventory.analytics (DiscrepancyDetected, ItemUnlocated, ItemStowed) are promoted to warehouse.inventory.events with byte-identical data, subject and Kafka key, because exception-management and returns-processing consume them cross-context; only the dataschema stream differs."
---

# 0043. Promote DiscrepancyDetected, ItemUnlocated and ItemStowed to the integration topic

## Status

**Accepted** (2026-10-21). Amends
[ADR 0011](./0011-analytical-data-product.md) (these three types were
analytics-only) and the type catalogue of
[ADR 0024](./0024-cloudevents-mandatory-envelope.md) (rows appended there).
No payload, subject or Kafka-key change; no consumer of this repo changes.

## Context

Until now only the Inventory Flow & Accuracy data product (ADR 0011) saw the
three facts that record inventory *exceptions*:

- `bin.DiscrepancyDetected` — a cycle count found the bin's system quantity
  did not match physical reality (`{bin_id, counted, system}`),
- `stock.ItemUnlocated` — a short count flagged stock units Unlocated
  (`{sku, bin_id, stock_unit_id, quantity}`),
- `stock.ItemStowed` — the chaotic-stow fact that makes quantity usable
  (`{sku, bin_id, quantity}`).

They were published to `warehouse.inventory.analytics` only. That was
deliberate while no other bounded context needed them: the fleet rule is
that a cross-context contract lives on the producer's **integration** topic
(`warehouse.inventory.events`), and an analytics-only fact is not a contract.

Two new contexts change that:

- **exception-management** (WES) opens and triages exception cases from
  `DiscrepancyDetected` and `ItemUnlocated` — a cycle-count shortfall is the
  canonical source of a warehouse exception case;
- **returns-processing** (WMS) closes its restock loop on `ItemStowed` — a
  dispositioned RESTOCK line is staged, and the loop ends when the returned
  quantity is actually stowed.

Keeping them analytics-only would force a sibling context to subscribe to
another context's internal data product, coupling public contracts to a
private stream.

## Decision

Promote all three types to `warehouse.inventory.events` — **also**, not
instead: each continues to reach `warehouse.inventory.analytics`
unchanged. On the integration topic they carry **exactly** what the
analytics publisher emits:

- the same full `type` string
  (`com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected`,
  `com.warehouse.wms.inventory-storage.stock.ItemUnlocated`,
  `com.warehouse.wms.inventory-storage.stock.ItemStowed`),
- the same `subject` and the same Kafka key, including the one deliberate
  split: `ItemUnlocated` is keyed by SKU (all facts about one SKU's units
  stay on one partition) while its `subject` is the lost stock unit id,
- **byte-identical `data`** — the payload mapping exists twice (once per
  encoder) and `TestPromoted_DataIsByteIdenticalAcrossTopics` pins the raw
  `data` bytes, `type`, key and subject equal across the two encoders.

The only difference is the `dataschema` stream:
`urn:warehouse:inventory-storage:events:<Event>:v1` on the integration
topic vs `...:analytics:<Event>:v1` (ADR 0024's per-topic stream rule).
The events leave through the transactional outbox like every other
integration fact (ADR 0017); no new publisher path exists.

## Consequences

- **These three types are now a breaking-change surface.** Before the
  promotion a payload change was an internal analytics matter; now it
  breaks exception-management and returns-processing. Any change to a
  field, the subject or the Kafka key is a new `.v2` type + `dataschema`
  version, never a mutation (ADR 0024).
- The payload mapping is deliberately duplicated in
  `promoted.go` rather than shared with `analytics_publisher.go`; the
  byte-identity test makes drift a build failure rather than a silent
  contract fork.
- Consumers must keep ignoring unknown `type` values (fleet rule): a
  consumer that only wants the exception facts simply does not act on the
  other eleven types on the topic.
- `apis/asyncapi.yaml` references the same message components from both
  channels; the `dataschema` pattern widens to
  `(events|analytics)` per promoted type. The CloudEvents catalogue in
  ADR 0024 gains one appended row per type.
- Volume: the integration topic now carries every stow. Consumers that
  were already on the topic (wes-work-planning, pick-face-replenishment)
  see new `type` values they must skip — they already do, by dispatching
  only on their known full type strings.
