---
id: 0046-containers-no-site-on-carton-local-packaging-view
slug: /adr/0046
title: "ADR 0046: Containers as built — site from the bin, local PackagingView, wire shapes (amends 0045)"
sidebar_label: "46. Containers as built (amends 0045)"
sidebar_position: 46
description: "ADR 0046 — amends ADR 0045: ReceiptCartonReceived carries no site, so a container's site is set at StowContainer from the bin; unitsPerPack and the case GTIN come from a local PackagingView fed by product-master pack-level events; wire payloads are snake_case; ReserveFullCases is reachable on POST /reservations; BinStockLevelChanged versions are per owner."
---

# 0046. Containers as built: site from the bin, local PackagingView, wire shapes

## Status

**Accepted** (2026-10-24). **Amends [ADR 0045](./0045-container-lpn-sealed-cases-breakcase-owner-segregated-stock.md)**
(which stays the record of the model; this ADR replaces only the points listed
below). It records what Phase 2 of the mastercase / GTIN programme built and the
resolutions R1, R4, R6, R7 and R8 of `.hermes/plans/mastercase-briefs/contracts.md`
(sections 7 and 8). Where the two ADRs disagree, this one wins.

## Context

ADR 0045 was written before inbound-receiving and product-master shipped their
halves of the contract. Building against the merged contracts settled the
questions it left open:

- inbound-receiving's staged stock is unlocated and its `ReceiptCartonReceived`
  carries **no `siteId`** (R6). ADR 0045 gave `Container` a `siteId` from the
  start and put `siteId` in `ContainerRegistered`.
- product-master shipped `ProductPackLevelDeclared` / `ProductPackLevelRetired`
  (CloudEvents on its integration topic). ADR 0045 named a "PackagingView" but
  left it to a later phase and said an unknown GTIN stays pending (R8).
- Existing inventory-storage payloads are snake_case (R1), so the camelCase
  names of the contract are logical names only.

## Decision

### 1. No site on the carton; the container's site comes from the bin (R6)

`ReceiptCartonReceived` is consumed **without** `site_id`. A `Container` is
registered with an empty site and no bin. `StowContainer{lpn, bin}` derives the
site from the bin (`Bin.SiteID`) and sets it on the container on the first stow;
later stows and `BreakCase` require the target bin to be at that site
(`ErrContainerNotAtSite`). Consequences for ADR 0045:

- the `siteId` field of `ContainerRegistered` is **dropped** (there is none to
  report); `ContainerStowed`, `ContainerEmptied` keep `site_id`;
- the "LPN is unique per site" rule is enforced as unique **LPN** (a primary key),
  which is stricter and correct because the site is unknown at registration; a
  duplicate SSCC on another receipt line is rejected and kept as a `REJECTED`
  pending link for audit (reason `LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE`).

### 2. A local PackagingView, with its own inbox (R8)

`pack_levels` is an event-fed projection of product-master's
`com.warehouse.wms.product-master.product.ProductPackLevelDeclared` and
`...ProductPackLevelRetired`, keyed by GTIN (and indexed by `(sku, level)`).
It stores `sku, level, units_per_pack, child_gtin, effective_from/to, version`.

- Applied in one `UnitOfWork` with a `processed_events` claim under consumer name
  `product-master-pack-level`, so a redelivery is a no-op; a stale `version` never
  overwrites a newer row; a retired GTIN stays resolvable (in-flight cartons).
- No REST call to product-master on any path. `unitsPerPack` and "is this a CASE
  GTIN of this SKU" are answered from this table.
- A carton whose GTIN is not in the view yet stays in `container_pending_links`
  and is re-evaluated when `ProductPackLevelDeclared` arrives, exactly like a
  carton whose line is not yet booked. A GTIN that belongs to another SKU, is not
  a CASE, or whose quantity differs from `units_per_pack` becomes `REJECTED`.

### 3. Carton link, idempotency and the double-count rule (as ADR 0045 section 6, made precise)

- Consumer name `inbound-carton`. The payload is snake_case:
  `receipt_id, asn_number, sscc, parent_sscc, sealed, lpn, owner_ref,
  contents[{line_no, sku, gtin, qty, lot, expiry, condition}], received_at`.
  Unknown fields are ignored. The carton books **no** quantity.
- `ReceiptLineReceived` accepts the new optional `owner_ref`, `pack_gtin`,
  `cases`, `sscc`. `owner_ref` segregates the booked staged quantity and the
  `receipt_line_bookings` row; `pack_gtin`, `cases`, `sscc` are provenance only
  and are never added to the quantity (the booking stays `quantity` in eaches).
- The pending-link table is idempotent in either arrival order; the invariant
  `sum(container qty) per (receipt_id, line_no) <= booked eaches` is checked
  inside the same transaction that registers.

### 4. Reservation entry point and read model

- `ReserveFullCases` is reachable through the existing `POST /reservations`
  with `cases` instead of `quantity` (mutually exclusive) and an optional
  `ownerRef`. ADR 0045 said "not a new public write endpoint"; no new path was
  added, but the existing one gained a field (documented in `apis/openapi.yaml`).
- `GET /inventory/{sku}/usable` takes an optional `ownerRef` and returns
  `reservableCases`. `GET /containers/{lpn}` and `GET /bins/{bin}/containers`
  are read endpoints; the MCP server gains `get_container`.

### 5. `BinStockLevelChanged` is versioned per owner

`bin_sku_levels` now has primary key `(bin_id, sku, owner_ref)` (`''` =
fleet-owned). Each owner's level in a `(bin, sku)` has its own monotonic
`version`; an event for fleet stock is byte-identical to before, and an event for
an owner carries the additive `owner_ref`. A consumer keyed by `(bin, sku)` that
ignores `owner_ref` would interleave two version sequences, so consumers must key
by `(bin, sku, owner_ref)` once an owner exists. (Not caused by this change alone:
ADR 0045 section 5 already said the level is the owner's level.)

### 6. Event payloads on the wire (settles ADR 0045 section 7)

All four are CloudEvents 1.0 structured JSON, type
`com.warehouse.wms.inventory-storage.container.<Event>`, topic
`warehouse.inventory.events`, subject and key = `lpn`, `data` snake_case:

| Event | `data` |
| --- | --- |
| `ContainerRegistered` | `lpn, sku, gtin, units_per_pack, qty_eaches, owner_ref?, lot?, expiry?, receipt_id, line_no, at` |
| `ContainerStowed` | `lpn, sku, qty_eaches, owner_ref?, site_id, bin, at` |
| `CaseBroken` | `lpn, sku, qty_eaches, owner_ref?, from_bin, loose_bin, at` |
| `ContainerEmptied` | `lpn, sku, owner_ref?, site_id, reason (BROKEN or PICKED), at` |

`BinStockLevelChanged` gains optional `owner_ref`. Each type has a golden JSON
test and is declared in `apis/asyncapi.yaml` and in the catalogue table of
[ADR 0024](./0024-cloudevents-mandatory-envelope.md).

### 7. Errors and the break

`ErrContainerReserved` and `ErrBinHandlingUnitMismatch` (R4) map to HTTP 409
Problem Details, as do `ErrContainerNotSealed`, `ErrContainerNotStowed`,
`ErrContainerNotAtSite`, `ErrContainerQuantityMismatch`, `ErrBreakNotConserving`
and `ErrOwnerMismatch`; an unknown LPN is 404 and a malformed `ownerRef` 400. `BreakCase` is a full-quantity break: `SEALED -> OPEN ->
EMPTY` in one transaction, `OPEN` never persisted (R4); the open question of
ADR 0045 is closed as "v1 never persists `OPEN`".

## Consequences

- A container has no site until it is stowed; reports that need "containers at a
  site" before stow must use the receipt, not the container.
- inventory-storage now owns one more event-fed projection (`pack_levels`) and
  one more consumer name; a missed product-master event leaves cartons pending
  (visible), never wrong.
- Consumers of `BinStockLevelChanged` must be owner-aware before any owner's stock
  is booked; for fleet stock nothing changes.
- ADR 0045's remaining open questions are closed: wire casing is snake_case;
  `ErrContainerReserved` and `ErrBinHandlingUnitMismatch` are in the contract (R4).

## Alternatives considered

1. **Add `site_id` to `ReceiptCartonReceived`.** Rejected by R6: receiving's staged
   stock is unlocated, so any site it sent would be a guess.
2. **Call product-master REST to resolve a GTIN.** Rejected by R8: a runtime
   dependency on the stow/receipt path; a local projection is already the fleet
   pattern (ADR 0013).
3. **A separate endpoint for `ReserveFullCases`.** Rejected: the reservation
   resource is the same; a mutually exclusive `cases` field keeps one seam.
