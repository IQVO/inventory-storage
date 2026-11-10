---
id: 0052-held-stock-and-disposition
slug: /adr/0052
title: "ADR 0052: Held stock and disposition — non-Good receipts stay visible, owned and never usable"
sidebar_label: "52. Held stock and disposition"
sidebar_position: 52
description: "ADR 0052: inventory-storage books the Damaged lines of inbound-receiving's ReceiptLineReceived as HELD stock (reason DAMAGED) instead of dropping them. A held unit has no usable quantity and cannot be reserved. A disposition (RELEASE, RETURN_TO_VENDOR, SCRAP, REWORK) closes or re-labels the hold. Events StockHeld, StockReleased and StockDispositioned; REST list + disposition; MCP list_held_stock (L1)."
---

# 0052. Held stock and disposition

## Status

Accepted

## Context

ADR 0037 books only `condition=Good` lines of inbound-receiving's
`ReceiptLineReceived`. A `Damaged` line is claimed, logged and counted
(`inventory.inbound_receipt_units{outcome=damaged_not_booked}`) and then
**vanishes** from the inventory view: nobody owns the units and nothing says
whether they go back to the vendor, are scrapped, reworked or released. ADR
0037 and ADR 0045 both name "quarantine is a later ADR". Competing WMS treat
quarantine (hold) plus a disposition as table stakes. This is feature IF-6 of
the inbound-receiving evolution plan (Q8) and the foundation for the later
QC-pending hold (IF-7).

## Decision

1. **A new stock state `HELD`** (`stock.StateHeld`, next to `StateUnlocated`
   and `StateRemoved`).
   - A held unit has `Usable() == 0` and `Reserve*` returns `ErrUnitHeld`,
     exactly like an unlocated unit. Every guard that rejects
     `StateUnlocated`/`StateRemoved` (`AddQuantity`, `MoveTo`, `RelocateOut`,
     `ReserveWhole`, `CanReserveWhole`) rejects `StateHeld` as well.
   - **Held stock lives in its own aggregate and table.** A hold has no bin
     (the goods are in the receiving or quarantine area, not stowed), so it is
     not a `stock_units` row. The `HeldUnit` aggregate (`internal/domain/stock`)
     is persisted in `held_stock` (migration 0041) and is invisible to every
     `StockRepo` query by construction. The `StateHeld` guards on `StockUnit`
     and the `HELD` exclusion in the bin-level SQL are defence in depth: a
     `HELD` row that ever reached `stock_units` still would not be promised or
     counted.
2. **A held unit carries a reason and its source.** Reason is the closed set
   `DAMAGED`, `QC_PENDING`, `SUSPECT`, `OTHER`. Source is the receipt id, the
   ASN number, the line number, the SKU and the quantity, plus the optional
   `owner_ref` and `sscc` of the line (pack provenance, kept as for Good) and
   the carton LPNs recorded for the line (see 3).
3. **Booking.** `BookInboundReceiptLine` books a `Damaged` line as a held unit
   with reason `DAMAGED` instead of dropping it. A `Good` line is unchanged.
   The CloudEvents id claim, the held-unit save and the outbox write run in ONE
   `UnitOfWork`; a redelivered id is a no-op.
   **Damaged carton contents.** `ReceiptCartonReceived` never books a quantity
   (ADR 0045: the line event is the sole quantity authority), so a `Damaged`
   carton content does NOT open a second hold, which would count the same
   units twice. It records its provenance instead: `LinkReceiptCarton`
   writes `(receipt_id, line_no, lpn, sscc)` into `held_stock_carton_marks`,
   idempotent on the CloudEvents id and independent of arrival order, and the
   list read joins the marks onto the held units of that line.
4. **Disposition**, closed set, naming borrowed from returns-processing's
   `DispositionDecided` (this service neither consumes nor calls
   returns-processing):

   | Disposition | Effect on the held unit | Stock effect |
   |---|---|---|
   | `RELEASE` | leaves the hold (state `REMOVED`, disposition recorded) | the quantity is received as a fresh staged receipt (`StockReceived`): not usable until stowed, exactly like a Good line |
   | `RETURN_TO_VENDOR` | `REMOVED`, disposition recorded | none: the units leave the building |
   | `SCRAP` | `REMOVED`, disposition recorded | none |
   | `REWORK` | stays `HELD`, reason updated (`reason` in the request, default `OTHER`), rework count +1 | none |

   Quantity is conserved: a disposition applies to the whole held quantity of
   the unit (no partial disposition in v1), so held plus disposed always equals
   the booked quantity. A `REMOVED` unit accepts no further disposition
   (`ErrHeldUnitClosed`, 409). The decision, the unit save and the events
   commit in one `UnitOfWork`.
5. **Events** on `warehouse.inventory.events`, CloudEvents type
   `com.warehouse.wms.inventory-storage.stock.<Event>`, key = subject = held
   unit id, additive (no existing payload changes):
   - `StockHeld` `{held_unit_id, sku, quantity, reason, receipt_id,
     asn_number, line_no, owner_ref?, sscc?, held_at}`
   - `StockReleased` `{held_unit_id, sku, quantity, receipt_id, asn_number,
     line_no, released_at}`
   - `StockDispositioned` `{held_unit_id, sku, quantity, disposition, reason,
     receipt_id, asn_number, line_no, decided_at}` (raised for all four
     dispositions; `StockReleased` is raised in addition on `RELEASE`).

   All three also go to `warehouse.inventory.analytics` with the same type, like
   the sibling stock events (`StockReceived`, `ItemStowed`, `ItemUnlocated`);
   the projector ignores types it does not fold (a release shows in the report
   through the `StockReceived` it raises), so no report changes. In
   `apis/asyncapi.yaml` each message carries `x-no-consumer-reason` until a
   consumer exists.
6. **REST** (fleet guideline, ADR 0049; RFC 9457 problems; `Idempotency-Key`
   on the POST): `GET /held-stock` (filters `reason`, `sku`, `olderThanHours`,
   keyset paging) and `GET /held-stock/{id}`; `POST /held-stock/{id}/dispositions`.
7. **MCP**: one read tool `list_held_stock` at authority L1. There is no write
   tool: the disposition stays REST only. The authority golden is updated.
8. **Metric** `inventory.held_units{reason}`: an up-down counter of units
   currently held, +quantity at hold, -quantity when the unit leaves the hold,
   and a move between reasons on `REWORK`. The `damaged_not_booked` outcome of
   `inventory.inbound_receipt_units` is replaced by `held` (a Damaged line is
   no longer dropped).
9. **Migration 0041**, additive: new tables `held_stock` and
   `held_stock_carton_marks`. No existing row or table is altered.

## Audit of every reader of usable quantity

A held unit must never be promised or counted as usable. Held units are not
`stock_units` rows, so each reader below that goes through `StockRepo` cannot
see them; the second column says what was checked and the verdict.

| Reader | Source of its numbers | Verdict |
|---|---|---|
| `StockUnit.Usable`, `Reserve`, `ReserveFor`, `ReserveWhole`, `CanReserveWhole`, `AddQuantity`, `MoveTo`, `RelocateOut` | the aggregate | guarded: `StateHeld` returns 0 usable / `ErrUnitHeld`; unit tests |
| `ReserveStock` (and `ReserveFullCases`) | `StockRepo.FindBySKU` / container units, sums `Usable()` | cannot see held units; a `HELD` unit would add 0 |
| `AllocateTransferStock` (transfer allocation) | `StockRepo.FindBySKUAtSite`, sums `Usable()` | same |
| `allocation` ordering (`order.go`) | `Usable()` of the loaded units | same |
| `GetUsable` (`GET /inventory/{sku}/usable`, MCP `get_usable`, A2A `check_availability`) | `StockRepo.FindBySKU` | same: usable never includes held; held stock is read only through the new held-stock read |
| `RunCycleCount` | `StockRepo.FindByBin`; skips `UNLOCATED`/`REMOVED` | extended to skip `HELD`; a held unit is not in a bin so a count never marks it unlocated |
| `RelocateStock` / relocation domain | `StockRepo.FindByBin`, `Usable()` | cannot see held units; guarded by `RelocateOut` |
| `BreakCase`, `StowContainer`, container unit exit | `StockRepo.FindByID/FindByBin` | `present` check extended to exclude `HELD` |
| `StowStock`, `StowTransferStock` (bin capacity occupants) | `StockRepo.FindByBin` | held units occupy no bin capacity: they are not in `stock_units` |
| `ConfirmPick`, `reservation_expiry`, `RevokeReservation` | `StockRepo.FindByID` by an allocation's unit id | held units are never allocated, so no allocation names one |
| `BinLevelLedger` (memory + Postgres) → `BinStockLevelChanged` `on_hand`/`reserved` | `stock_units` where state not UNLOCATED/REMOVED | extended: `HELD` excluded in SQL and in memory, so a bin level never counts a held unit |
| MCP `get_bin_stock` (`mcp/tools.go`, `mapping.go`), HTTP `stock_handler` | `StockRepo.FindByBin`/`FindBySKU`, `Usable()` | cannot see held units |
| Analytics projector and report (`internal/analytics`) | the analytics topic, not stock units | folds `StockReceived`/`ItemStowed`/...; the new types are ignored; a release raises `StockReceived`, so flow stays right |
| Downstream: order-management ATP, network-inventory-planning NIP, replenishment | `StockReserved`, `BinStockLevelChanged`, `TransferStockAllocated` events | those events never include a held unit (rows above), so held stock is never promised |

## Consequences

- Damaged units stay visible and owned: a list read, an age filter and a metric
  replace the silent log line. ADR 0037's "Damaged is not booked" is superseded
  for the `Damaged` condition; `Good` is untouched.
- Usable stock, reservations, transfer allocation, bin levels and cycle counts
  behave exactly as before; the only new path into usable stock is `RELEASE`,
  which goes through the same staged-receipt seam as a fresh receipt.
- One extra table read when a `Damaged` line arrives; none on the Good path.
- Not in scope (v1): partial disposition, a configurable hold reason on the
  wire (the producer's condition maps only to `DAMAGED`; `QC_PENDING` and
  `SUSPECT` are accepted for later producers such as IF-7), a held-stock
  analytics report, and the exception-management case per hold.
