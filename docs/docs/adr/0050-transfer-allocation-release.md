---
id: 0050-transfer-allocation-release
slug: /adr/0050
title: "ADR 0050: Transfer allocation release — the cancel-after-allocation compensation leg"
sidebar_label: "50. Transfer allocation release"
sidebar_position: 50
description: "ADR 0050: inventory-storage consumes TransferAllocationReleaseRequested and answers TransferStockAllocationReleased, resolving the origin reservation of a transfer cancelled after allocation. Idempotent by transfer_line_id through a second ledger (transfer_allocation_releases), one UnitOfWork per decision, outcomes RELEASED | ALREADY_RESOLVED — both settle the saga as success."
---

# 0050. Transfer allocation release — the cancel-after-allocation compensation leg

## Status

Accepted

## Context

ADR 0030 built the allocation leg of the network transfer saga as a
command/reply exchange: network-inventory-planning publishes
`TransferAllocationRequested`, inventory-storage holds the stock and
answers `TransferStockAllocated` / `TransferStockAllocationRejected`.
But the saga had no compensation path: a transfer cancelled (or stuck,
or abandoned) AFTER its allocation left the origin reservation holding
stock in this service until lazy expiry noticed the timeout — hours of
phantom unusable stock, and no way for planning to close the saga
cleanly. network-inventory-planning's ADR-0003 documented exactly this
limitation: "a CANCELLED-after-allocation path still needs an explicit
reservation revocation command."

## Decision

**A second command/reply leg, the mirror image of the allocation leg.**

1. **New consumed command.** The transfer consumer
   (`internal/adapters/inbound/kafka/transfer_consumer.go`, same topic,
   same consumer group, same at-least-once checklist) now also dispatches
   the full type
   `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationReleaseRequested`
   with `data` `{transfer_id, transfer_line_id, reservation_id,
   origin_site_id, sku, quantity, reason}`.

2. **New use case `ReleaseTransferAllocation`.** It loads the
   `transfer_allocations` ledger row by `transfer_line_id` — the
   allocation ledger, not the command, is the record of WHAT was decided
   and which reservation holds the stock — validates the command against
   it, then resolves the reservation through RevokeReservation's existing
   logic: lazy expiry first (`expireIfDue`), then `Revoke()` +
   `releaseAllocations`. The reservation save, the ledger row and the
   outbox row commit in ONE UnitOfWork.

3. **A second ledger, `transfer_allocation_releases`** (migration 0039),
   DB-unique on `transfer_line_id`, gives the release leg the allocation
   leg's exact idempotency discipline: a byte-identical replay returns the
   original outcome and republishes nothing; the same line id with a
   DIFFERENT payload is answered with the original outcome and reason
   `IDEMPOTENCY_CONFLICT` while the original decision stands.

4. **New published fact.**
   `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationReleased`
   on `warehouse.inventory.events` through the transactional outbox, key
   and `subject` = `transfer_line_id` (mirroring
   `TransferStockAllocationRejected`). `data` is `{transfer_id,
   transfer_line_id, reservation_id, origin_site_id, sku, quantity,
   outcome, reason}` with `outcome` the CLOSED set `RELEASED` |
   `ALREADY_RESOLVED` — a reservation that had already expired (lazily
   resolved here) or been confirmed means the hold is already gone, which
   settles the saga as success just the same. `reason` carries the
   command's cancellation reason through.

5. **Deterministic refusals publish nothing.** A release command naming
   a `transfer_line_id` this service never decided
   (`ErrUnknownTransferLine`), or one disagreeing with the allocation
   ledger row (`ErrTransferReleaseMismatch`), is logged by the consumer
   and committed past — never retried, never wedging the partition, and
   no event: there is no decision to correlate an answer to.

6. **No `ReservationRevoked` on this leg.** Transfer reservations never
   emitted `StockReserved` on the integration topic (the allocation leg
   publishes `TransferStockAllocated` instead), so emitting
   `ReservationRevoked` here would make wes-work-planning's
   observed-usable projection increment a count it never decremented.
   `TransferStockAllocationReleased` is the only event this leg
   publishes; the reservation's own lifecycle events stay off the wire
   for transfer demand refs.

## Consequences

- A transfer cancelled after allocation settles within seconds, and its
  origin stock is usable again at once (or confirmed gone) — no more
  stranded reservations waiting on lazy expiry.
- The `RELEASING` saga state in network-inventory-planning has a
  deterministic settling fact for both outcomes.
- Replays and conflicts are answered without touching stock twice; the
  ledger pair (`transfer_allocations`, `transfer_allocation_releases`)
  is the full audit trail of the line's allocation and release.
