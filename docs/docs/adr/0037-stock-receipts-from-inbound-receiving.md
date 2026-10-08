---
id: 0037-stock-receipts-from-inbound-receiving
slug: /adr/0037
title: "ADR 0037: Stock receipts from inbound-receiving"
sidebar_label: "37. Stock receipts from inbound-receiving"
sidebar_position: 37
---

# 0037. Stock receipts from inbound-receiving

## Status

**Accepted** (2026-10-08). Inventory-storage's side of the handover recorded in
inbound-receiving's ADR 0003 (`IQVO/inbound-receiving`, `docs/adr/0003-local-copies-and-handover.md`),
which fixes the producer contract this record consumes. Does not change
[ADR 0017](./0017-transactional-outbox.md) (one `UnitOfWork`),
[ADR 0018](./0018-idempotency-key-middleware.md) (the REST middleware) or
[ADR 0024](./0024-cloudevents-mandatory-envelope.md) (only its consumed-types
list and the cross-service contract block gain one row each).

## Context

`POST /stock/receive` has been a bare quantity acknowledgement: someone calls it
with a SKU and a count, `ReceiveStock` raises `StockReceived` (analytics topic),
and the quantity is staged, not yet usable, until an RF operator stows it with an
item scan and a location scan. Nothing upstream says *what was supposed to
arrive* or *what condition it arrived in*.

The new `inbound-receiving` bounded context (wms subdomain) owns that: ASNs, dock
appointments and receipts. When a receipt line is counted it publishes
`ReceiptLineReceived` on `warehouse.inbound-receiving.events` (through its own
transactional outbox) with `condition` of `Good` or `Damaged`. Its ADR 0003 decided
the handover is event-driven: no REST or MCP call goes in either direction, and
there is no acknowledgement event back.

## Decision

1. **Ownership.** inbound-receiving owns ASNs, dock appointments and receipts.
   inventory-storage owns stock and does not copy any of that. It reads exactly
   one of the receipt facts: the line that was received.

2. **Good lines are booked as staged receipts.** A `ReceiptLineReceived` with
   `condition=Good` runs the **existing `ReceiveStock` use case** with
   `(sku, quantity)` and nothing else. The effect is identical to
   `POST /stock/receive`: `StockReceived` is raised through the outbox to the
   analytics topic (`warehouse.inventory.analytics`), the quantity is **staged and
   not yet usable**, and stow stays the RF action (item scan plus location scan).
   Inbound-receiving prescribes no location and this service does not choose one.
   The domain is unchanged: a new application use case, `BookInboundReceiptLine`,
   sits in front of `ReceiveStock`.

3. **Damaged lines are not booked in v1.** A `Damaged` line is acknowledged,
   logged at INFO and counted in `inventory.inbound_receipt_units{outcome=damaged_not_booked}`
   (units; the booked ones go to `outcome=booked`). It raises no event and changes
   no stock. Quarantine is a later ADR; the damaged quantity stays recorded on the
   receipt in inbound-receiving, which is the source of truth for the dock.

4. **`POST /stock/receive` is unchanged.** It keeps working, with its
   `Idempotency-Key` middleware, for ad-hoc receipts (no ASN). Nothing in REST,
   MCP or the console changes.

5. **A new inbound Kafka consumer** reads `warehouse.inbound-receiving.events`:
   - Stable consumer group from env `INBOUND_RECEIPT_CONSUMER_GROUP`; **unset
     means the consumer does not exist** (no literal group id at any call site).
     Set, it requires `DATABASE_URL` and `KAFKA_BROKERS` or boot fails with the
     real cause. First start under a new group reads from the first offset.
   - Dispatches on the **full** type
     `com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived`; every other
     type on the topic (ASN, appointment, `ReceiptOpened`, `ReceiptClosed`) is
     ignored and its offset committed.
   - Anything that is not a valid CloudEvent, has an undecodable payload, an empty
     `sku`, a non-positive `quantity` or a `condition` other than `Good` / `Damaged`
     is **skipped with a WARN and committed past**. It is never retried and never
     claimed.
   - **Dedupe on the CloudEvents `id`**: the id is claimed in `processed_events`
     (consumer name `inbound-receipt-line`) **in the same database transaction** as
     `ReceiveStock`'s outbox write. A failure rolls both back, so the redelivery is
     applied in full; a redelivered id is a no-op. A `Damaged` line is claimed too,
     so a redelivery does not inflate the metric.
   - `FetchMessage`, then `CommitMessages` only after the message settles. A
     transient error retries the **same message** with capped backoff (the
     product-master consumer's policy, [ADR 0034](./0034-product-master-owns-classification.md)).

6. **Wiring.** The consumer starts only in the API binary (`cmd/inventory`), on the
   shared inbound-consumer context, so shutdown drains it with the others. The
   chart exposes a dedicated value `config.inboundReceiptConsumerGroup`, rendered
   as the env var only when non-empty. For `StockReceived` to leave the service
   `EVENT_PUBLISHER=kafka` must be set, as for every other event; a boot WARN says
   so when it is not.

7. **Failure on this side is this side's skip-and-log.** inbound-receiving has no
   compensation flow in v1, and neither side acknowledges the other. A line this
   service skips stays counted on the receipt; the WARN carries the CloudEvents id,
   receipt id, SKU and quantity for the operator.

## Consequences

- A receipt closed in inbound-receiving now produces staged stock here with no
  human re-keying; stow is the first human step.
- At-least-once delivery has an exactly-once effect per CloudEvents `id`. Two lines
  for the same SKU under different ids are two receipts (they are two counts).
- Receipt context (receipt id, ASN, line number, `received_at`) is **not stored**
  here: `StockReceived` carries only `{sku, quantity}` and is stamped by this
  service's clock, exactly like the REST path. Reconciling stock to a receipt is
  done in inbound-receiving. Carrying the receipt on `StockReceived` would be an
  additive analytics-contract change for a later ADR.
- Damaged units never reach stock and are only visible as a metric and a log line
  here, plus the receipt in inbound-receiving, until the quarantine ADR.
- The producer contract is pinned in `apis/asyncapi.yaml`'s consumed channel. A
  breaking change there is a new versioned type and a change here, never a silent
  drift. No contract contradicted the design on 2026-10-08.
