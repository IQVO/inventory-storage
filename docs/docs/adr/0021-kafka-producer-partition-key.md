---
id: 0021-kafka-producer-partition-key
slug: /adr/0021-kafka-producer-partition-key
title: "21. Key every integration/analytics Kafka message by its aggregate id, and route by Hash not LeastBytes"
sidebar_label: "21. Kafka producer partition key"
sidebar_position: 21
description: "ADR 0021 — fix a per-aggregate ordering gap on warehouse.inventory.events exposed by the Phase 3 partition-scaleup (1 to 8 partitions): key every StockReserved/ReservationRevoked message by the reservation id, and switch every writer's Balancer from LeastBytes to Hash, since LeastBytes ignores Message.Key entirely when choosing a partition."
---

# 21. Key every integration/analytics Kafka message by its aggregate id, and route by Hash not LeastBytes

## Status

Accepted.

## Context

`warehouse-infra` PR #42 (merged) bumped every business Kafka topic on the
fleet's shared broker from 1 partition to 8, as part of the Phase 3
production-readiness plan. With exactly one partition, every producer was
accidentally safe: every message for every topic landed on that single
partition in publish order, so any consumer reading it observed a strict
total order regardless of whether the producer ever set a partition key.
That PR's own producer-key audit (documented in
`docs/kafka-partition-scaleup.md` in `warehouse-infra`) went through every
outbound Kafka publisher in the fleet and found this repo's integration
publisher (`internal/adapters/outbound/kafka/publisher.go`) publishing
`StockReserved` and `ReservationRevoked` onto `warehouse.inventory.events`
with **no `Key` set on the `kafka-go` `Message{}` at all**.

Going from 1 to 8 partitions turns that into a real bug: `StockReserved`
and a later `ReservationRevoked` for the exact same reservation can now be
routed to different partitions and consumed out of order by a downstream
consumer reading per-partition (e.g. `wes-work-planning`'s observed-usable
projection), which was never true when there was only one partition to
land on. This repo's own **analytics** publisher
(`analytics_publisher.go`) was already keying every message by the correct
aggregate id (SKU for stock-location events, reservation id for
reservation-lifecycle events) — the gap was specific to the integration
publisher, not a design question, matching what the audit found in
`order-management` and `workforce-management`'s equivalent integration
publishers (each fixed in its own repo's companion PR).

**A second, easy-to-miss layer of the same bug**: setting `Message.Key` is
necessary but not sufficient. Every writer in this package (the direct
`Publisher`/`AnalyticsPublisher` writers via `NewWriter`/
`NewAnalyticsPublisher`, and the transactional-outbox `RelaySink`, ADR
0017) used `kafka-go`'s `LeastBytes` balancer. `LeastBytes` selects a
partition purely by tracking cumulative bytes written per partition — it
never reads `Message.Key` at all. A real-broker integration test written
against a real 8-partition topic
(`TestPublisher_RealBroker_SameReservationLandsOnSamePartition`) proved
this directly: even after adding the reservation-id `Key` to every
message, `StockReserved` and `ReservationRevoked` for the same reservation
still landed on different partitions, because `LeastBytes` ignored the key
entirely. The analytics publisher had carried a correct `Key` since it was
first written, but inherited the exact same silent gap through its own
`LeastBytes` writer — its per-aggregate ordering guarantee was equally
illusory until this fix, just never audited because the flagged file/line
in the cross-repo PR only named the integration publisher's *missing Key*,
not the balancer choice underneath either publisher.

## Decision

1. Every message this repo publishes to `warehouse.inventory.events`
   (`StockReserved`, `ReservationRevoked`) now carries a Kafka message
   `Key` equal to the event's `ReservationID` — the natural ordering key
   shared by both event types for the same reservation aggregate (mirrors
   how the analytics publisher keys `ReservationRevoked`/`ReservationExpired`
   by reservation id — and, since the amendment below, `StockReserved` and
   `StockPicked` too; reservation
   id is the one field guaranteed present on every reservation-lifecycle
   event this publisher forwards, including the case where
   `ReservationRevoked` must look up the reservation's SKU via
   `ports.ReservationRepo` to build the payload).
2. Every `kafka-go` `Writer` this repo constructs
   (`kafka.NewWriter`, `kafka.NewAnalyticsPublisher`, `kafka.NewRelaySink`)
   now sets `Balancer: &kafkago.Hash{}` instead of `&kafkago.LeastBytes{}`.
   `Hash` computes an FNV-1a hash of `Message.Key` and routes deterministically
   by `hash(key) % partitionCount`, so two messages with an identical `Key`
   always land on the same partition regardless of how many partitions the
   topic has — this is the actual mechanism that makes "key by aggregate
   id" mean anything at the partition-routing level. `Hash` falls back to
   round-robin for a message with a nil key (kafka-go's own documented
   behavior), so this change is a strict improvement with no new failure
   mode for a caller that has no natural key.
3. This is a same-repo mirror of the analytics publisher's existing
   keying convention for the `Key` value itself, but a new, previously
   unaudited fix for the balancer, applied uniformly to every writer in
   the package rather than only the one the cross-repo audit named.

## Consequences

- Per-reservation ordering (`StockReserved` before its own later
  `ReservationRevoked`) is now guaranteed on `warehouse.inventory.events`
  regardless of the topic's partition count, verified against a real
  8-partition broker in
  `TestPublisher_RealBroker_SameReservationLandsOnSamePartition`
  (testcontainers, `-tags=integration`). Cross-reservation and cross-SKU
  ordering remains unguaranteed by design — `GET /inventory/{sku}/usable`
  stays the authoritative source for correctness-sensitive reads, exactly
  as `.claude/rules/integration-events.md` already documented before this
  change.
- The analytics topic (`warehouse.inventory.analytics`) gets the same
  ordering guarantee from the balancer fix **plus a key correction
  (amendment)**. The original text of this ADR claimed the analytics
  publisher's `Key` "was already correct", but it keyed `StockReserved` and
  `StockPicked` by SKU while keying `ReservationRevoked`/`ReservationExpired`
  by reservation id, so ONE reservation's lifecycle on the analytics topic
  spanned partitions (a consumer could see `ReservationRevoked` before
  the `StockReserved` it revokes). Now every reservation-lifecycle analytics
  event (`StockReserved`, `StockPicked`, `ReservationRevoked`,
  `ReservationExpired`) is keyed by the **reservation id**; only events that
  have no reservation stay keyed by their own aggregate (SKU for
  `StockReceived`/`ItemStowed`/`ItemUnlocated`, bin id for the cycle-count
  events). Proven against a real 8-partition broker by
  `TestAnalyticsPublisher_RealBroker_ReservationLifecycleLandsOnSamePartition`
  (`analytics_partition_integration_test.go`, testcontainers).
- `Hash`'s FNV-1a routing means a topic's key-to-partition mapping is
  stable only for a fixed partition count; growing the topic again in the
  future (partitions can only increase in Kafka) will re-shuffle which
  partition each key lands on, same as any keyed producer using a
  hash-based balancer — this is expected and matches every other
  already-safe publisher in the fleet (per the original audit), not a new
  risk this change introduces.
- `LeastBytes`'s actual purpose — balancing message *volume* across
  partitions rather than message *count* — is given up in exchange for
  ordering correctness. For this topic's traffic shape (small,
  fixed-shape JSON envelopes) the volume-balancing benefit was marginal;
  correctness of per-aggregate ordering is not.
- A future publisher added to this package must use `Hash` (or another
  key-aware balancer), never `LeastBytes`, or it silently reintroduces
  this exact gap — call this out explicitly in code review for any new
  `kafkago.Writer{}` construction in this repo.
