---
id: 0044-consume-return-line-staged
slug: /adr/0044
title: "ADR 0044: Consume returns-processing's ReturnLineStaged"
sidebar_label: "44. Consume returns-processing's ReturnLineStaged"
sidebar_position: 44
---

# 0044. Consume returns-processing's ReturnLineStaged

## Status

**Accepted** (2026-10-21). inventory-storage's side of the handover decided in
returns-processing's ADR 0002 (`IQVO/returns-processing`), which pins the
producer contract this record consumes. Extends
[ADR 0037](./0037-stock-receipts-from-inbound-receiving.md) with a second
producer driving the SAME staged-receipt seam; does not change
[ADR 0017](./0017-transactional-outbox.md) (one `UnitOfWork`),
[ADR 0024](./0024-cloudevents-mandatory-envelope.md) (only its consumed-types
list and the cross-service contract block gain one row each) or
[ADR 0042](./0042-postgres-and-kafka-are-the-only-runtime.md) (the consumer
always runs).

## Context

The new `returns-processing` bounded context (wms subdomain) owns the reverse
flow of goods: RMAs, return receipts, inspection grades and the disposition
decision per inspected line. Of the five disposition outcomes only RESTOCK
means the units re-enter sellable stock; for that outcome returns-processing
publishes `disposition.ReturnLineStaged` on
`warehouse.returns-processing.events` (through its own transactional outbox)
with `data` exactly inbound-receiving's `ReceiptLineReceived` shape plus
`rma_id`, `disposition_id` and `site_id`, and with `condition` the constant
`Good` — a staged return line is by definition restockable. Its ADR 0002
fixed the handover as event-driven: no REST or MCP call in either direction,
and there is no acknowledgement event back.

inventory-storage already has the seam for "goods acknowledged into the
building, staged awaiting stow": ADR 0037's `BookInboundReceiptLine` in front
of the existing `ReceiveStock` use case.

## Decision

1. **Ownership.** returns-processing owns RMAs, return receipts, grades and
   dispositions. inventory-storage owns stock and copies none of that. It
   reads exactly one of the disposition facts: the RESTOCK handover line.

2. **A staged return line runs the SAME staged-receipt use case.** A new
   application use case, `BookReturnLine`, sits in front of `ReceiveStock`
   with `(sku, quantity)` and nothing else — the same use case an inbound
   receipt line and `POST /stock/receive` drive. The effect is identical:
   `StockReceived` is raised through the outbox to the analytics topic, the
   quantity is **staged and not yet usable**, and stow stays the RF action
   (item scan plus location scan). returns-processing prescribes no location
   and this service does not choose one. The domain is unchanged.

3. **Provenance is the claim, not a field.** `StockReceived` carries only
   `{sku, quantity}` and is stamped by this service's clock — ADR 0037 froze
   that analytics contract and explicitly deferred carrying receipt context
   on it. So the flow's provenance record mirrors the inbound convention
   exactly: the CloudEvents `id` is claimed in `processed_events` under the
   consumer name **`return-line-staged`** (the mirror of
   `inbound-receipt-line`), and `receipt_id`, `rma_id`, `disposition_id`,
   `site_id` and the producer's `received_at` are logged on the INFO line.
   A `receipt_ref = ret-<disposition_id>` column was considered and rejected:
   it would need a new analytics-contract field (a later ADR's additive
   change, per ADR 0037) to answer a question returns-processing can already
   answer from its own disposition record. The two claim scopes are disjoint,
   so the same `id` under both flows is two effects, never a silent swallow.

4. **A new inbound Kafka consumer** reads `warehouse.returns-processing.events`:
   - Stable consumer group from env `RETURN_LINE_CONSUMER_GROUP`; **unset
     means boot fails** with the variable named (no literal group id at any
     call site, no default), exactly as `INBOUND_RECEIPT_CONSUMER_GROUP` is
     enforced. The consumer always runs (ADR 0042). First start under a new
     group reads from the first offset.
   - Dispatches on the **full** type
     `com.warehouse.wms.returns-processing.disposition.ReturnLineStaged`;
     every other type on the topic (`ReturnAuthorized`,
     `ReturnAuthorizationRejected`, the `ReturnReceipt*` types,
     `UnexpectedReturnReceived`, `DispositionDecided`) is ignored and its
     offset committed.
   - Anything that is not a valid CloudEvent, has an undecodable payload, an
     empty `sku`, a non-positive `quantity`, an empty `disposition_id` or a
     `condition` other than the constant `Good` is **skipped with a WARN and
     committed past**. It is never retried and never claimed.
   - **Dedupe on the CloudEvents `id` in the same transaction as the
     effect**: the claim and `ReceiveStock`'s outbox write run in ONE
     `UnitOfWork`, so a failure rolls both back and the redelivery is applied
     in full; a redelivered `id` is a no-op.
   - `FetchMessage`, then `CommitMessages` only after the message settles. A
     transient error retries the **same message** with capped backoff (the
     fleet consumer policy, ADR 0034/0037).

5. **Wiring.** The consumer starts only in the API binary (`cmd/inventory`), on
   the shared inbound-consumer context, so shutdown drains it with the
   others. The chart exposes `config.returnLineConsumerGroup` (rendered as
   the env var; the render fails when it is empty) with default
   `inventory-storage-return-lines`.

6. **Failure on this side is this side's skip-and-log.** No compensation flow
   in v1, and neither side acknowledges the other. A line this service skips
   stays recorded on the disposition in returns-processing; the WARN carries
   the CloudEvents id, disposition id, receipt id, SKU and quantity for the
   operator.

## Consequences

- A RESTOCK disposition now produces staged stock here with no human
  re-keying; stow is the first human step, exactly as for a dock receipt.
- At-least-once delivery has an exactly-once effect per CloudEvents `id`. Two
  dispositions for the same SKU under different ids are two staged receipts.
- Return context is **not stored** here, same as the inbound-receipt context
  (ADR 0037): reconciling stock to a return is done in returns-processing,
  which is the source of truth for the disposition.
- The producer contract is pinned in `apis/asyncapi.yaml`'s consumed channel.
  A breaking change there is a new versioned type and a change here, never a
  silent drift.
- The other four outcomes (REFURBISH, LIQUIDATE, DISPOSE, RTV) never touch
  this service; `UnexpectedReturnReceived` is exception-management's input,
  not ours.
