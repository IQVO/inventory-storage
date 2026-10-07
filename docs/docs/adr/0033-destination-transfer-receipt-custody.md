---
id: 0033-destination-transfer-receipt-custody
slug: /adr/0033
title: "ADR-0033: Destination transfer receipt custody — stage, quarantine, stow"
sidebar_label: "31. Destination transfer receipt custody"
sidebar_position: 31
---

# 0033. Destination transfer receipt custody — stage, quarantine, stow

## Status

Accepted

## Context

ADR 0030 closed the ORIGIN half of the network transfer saga: a
planning command reserves origin-site stock, ledgered in
`transfer_allocations` (DB-unique on `transfer_line_id`), and the reply
rides the transactional outbox. The DESTINATION half did not exist.
When the truck arrives, this service had no way to answer the only
question that matters: *did the goods we expect arrive, where did they
go, and whose usable stock just rose?*

The network-transfer design names the failure modes that a naive
"receive it like any inbound PO" implementation hits:

- **Silent absorption.** Receiving whatever count arrives against
  whatever the ledger promised, without recording the difference, makes
  over/short invisible — the shrinkage or windfall becomes ordinary
  usable stock and no one is ever accountable for it.
- **Unrecognized arrivals.** A scan whose `transfer_line_id` this
  service never allocated (a mis-keyed scan, a transfer from another
  network leg, an unsanctioned shipment) must not raise availability.
  The design rule is explicit: a destination scan without a recognized
  transfer is **quarantined as an inventory exception** rather than
  silently raising availability.
- **Double-apply.** Receiving is scan/REST-driven from the dock; a lost
  HTTP response and a retry, or a double scan, must never create stock
  twice. The end-to-end invariant the fleet verifies is "destination
  usable rises ONCE after stow".

One design input deserves its own note: fulfillment-execution publishes
`com.warehouse.wes.fulfillment-execution.transfer.TransferArrived` on
`warehouse.fulfillment.events` (subject/key = task id). **This service
deliberately does NOT consume it.** TransferArrived is a *work-execution
fact* about a fulfillment task completing; it is not a *custody fact*.
Custody at the destination is taken by a human scan of what physically
arrived — the count that establishes over/short — recorded through this
service's own REST surface. Consuming TransferArrived here would let a
task-completion event raise usable stock with no physical count behind
it, the exact silent-absorption failure above. (NIP may later consume
TransferArrived to drive its in-transit → received saga transitions;
that is a planning-state concern, not a stock concern.)

## Decision

1. **`StageTransferReceipt`** (`POST /transfers/{transferLineId}/receipt`,
   `Idempotency-Key` required) records ONE destination scan:
   `{transfer_id, transfer_line_id, destination_site_id, sku,
   received_quantity}`. The lookup key is the ORIGIN ledger — the same
   `transfer_allocations` row ADR 0030 wrote:

   - no row, a REJECTED row, a `transfer_id` mismatch, or a destination
     equal to the transfer's own origin site → **quarantine**: an
     `inventory_exceptions` row (kind `UNKNOWN_TRANSFER` or
     `UNRECOGNIZED_TRANSFER`, migration 0032) is written in the same
     transaction, NOTHING that could raise usable stock is published,
     and the caller gets an explicit 422 problem whose `exception`
     extension carries the quarantine row's coordinates;
   - a recognized ALLOCATED row → a `transfer_receipts` row is created
     (state `STAGED`, `expected_quantity` = the ledger's
     `requested_quantity`, `received_quantity` as counted, `variance` =
     received − expected **signed**), and
     `com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged`
     is published through the same transactional outbox. **No usable
     stock moves at stage.**

2. **Variance is explicit, never absorbed.** Over-receipt (positive) and
   short-receipt (negative) are recorded on the receipt row (CHECK
   constraint `variance = received_quantity - expected_quantity`), ride
   the `TransferReceiptStaged` event, and appear in the REST response.
   The stow places exactly the RECEIVED quantity — never the expected
   one — so stock always matches the physical count and the difference
   is always accountable somewhere.

3. **`StowTransferStock`** (`POST /transfers/{transferLineId}/stow`,
   `Idempotency-Key` required) places a STAGED receipt's goods:
   `{bins: [{bin_id, quantity}]}`. Every bin must be registered and
   belong to the receipt's `destination_site_id` — the site-custody fact
   from ADR 0030 fails closed (another site's bin, or a legacy site-less
   bin, is a 409) — and the bin quantities must sum EXACTLY to the
   receipt's received quantity. One `StockUnit` per leg is created AT
   the destination site (`NewStockUnitAtSite`), the receipt moves
   `STAGED → STOWED` with its stow legs recorded, and
   `com.warehouse.wms.inventory-storage.stock.TransferStockStowed`
   (subject/key = `transfer_line_id`) is published. **This is the only
   path that raises the destination site's usable stock for a
   transfer.**

4. **Idempotency is database-level, like the origin side.** The
   `transfer_receipts` row is DB-UNIQUE on `transfer_line_id`
   (migration 0032); the STOWED transition is a guarded UPDATE
   (`WHERE state = 'STAGED'`, zero rows = already stowed). A replayed
   identical stage returns the ORIGINAL receipt (`replay: true`,
   nothing republished); a different payload on the same line is a 409
   (`transfer-receipt-conflict`) with the original immutable. A
   replayed stow for a STOWED receipt returns the original allocations,
   creates NO second StockUnit, and publishes nothing. A repeated
   identical QUARANTINED scan is also a benign replay: the
   `inventory_exceptions` unique key on the exact scan tuple
   `(transfer_line_id, destination_site_id, sku, received_quantity)`
   keeps one row. The REST `Idempotency-Key` middleware (ADR 0018) in
   front of both endpoints is defense in depth that also replays the
   original HTTP response byte-for-byte.

5. **One UnitOfWork per decision.** Exception row OR receipt row OR
   stock units + bins + receipt transition, plus the outbox row(s) for
   the published event — all commit or none does. A forced outbox
   failure mid-stow rolls back the stock rows and leaves the receipt
   STAGED (proven against real Postgres by the integration test).

6. **A quarantine is a committed outcome, not an error path.** The
   exception row is durable state (auditable, resolvable by a human);
   the caller receives BOTH the exception (in the response's
   `exception` member) and the explicit problem (as the 422).

## Consequences

- The custody invariant closes: origin usable fell at allocation (ADR
  0030), destination usable rises at stow, once, only through
  `TransferStockStowed`; `wes-work-planning`'s observed-usable
  projection can consume the new events by their full type strings.
- Over/short is a first-class fact. Downstream, NIP can reconcile
  `variance` against the saga state and decide corrective transfers;
  this service does not auto-correct — a physical custody break is
  never silently repaired.
- `TransferArrived` stays unconsumed here (documented above); if the
  fleet later wants a dock-pre-alert, it belongs in planning's saga,
  not in stock custody.
- The `bins.site_id` column from migration 0030 becomes load-bearing on
  the stow path: bins reach it via site-scoped registration; the REST
  bin-registration surface does not yet expose site (a v2 concern, and
  a backfill story for legacy bins).
- Two new published messages are catalogued in `apis/asyncapi.yaml`
  and ADR-0024's type table; REST docs regenerate from
  `apis/openapi.yaml` (`npm run gen-api-docs inventory`).
