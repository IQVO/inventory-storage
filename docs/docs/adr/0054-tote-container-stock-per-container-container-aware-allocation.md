---
id: 0054-tote-container-stock-per-container-container-aware-allocation
slug: /adr/0054
title: "ADR 0054: Tote containers, stock per container and container-aware allocation for the WCS"
sidebar_label: "54. Tote containers for the WCS"
sidebar_position: 54
description: "ADR 0054 — the WCS container is an inventory-storage license plate: a new Tote aggregate (reusable, any SKUs) beside the sealed-case Container of ADR 0045, one LPN namespace, stock units tagged with their container, the tote's bin as its logical location, allocations that name their container, a container-aware draw that amends ADR 0050 for container units only, and no loose stock in an AutomatedStorage bin. Additive."
---

# 0054. Tote containers, stock per container and container-aware allocation

## Status

**Accepted** (2026-11-20). Additive: no existing route, event, field or column
changes meaning. Builds on [ADR 0045](./0045-container-lpn-sealed-cases-breakcase-owner-segregated-stock.md)
and [ADR 0046](./0046-containers-no-site-on-carton-local-packaging-view.md)
(Container and LPN), [ADR 0013](./0013-location-classification-via-facility-events.md)
(facility events as a local read model), [ADR 0025](./0025-bin-registration-endpoint-and-pick-location.md)
(pick location on allocations) and [ADR 0053](./0053-idempotency-replay-classes.md).
**Amends [ADR 0050](./0050-deterministic-forward-first-allocation.md)** for units held
in a container only. Records the `inventory-storage` row of the WCS design
(warehouse-docs `docs/wcs/ecosystem-impact.md`, seam decisions 1 and 2) and answers
its open question "Container id format and who prints labels".

## Context

The WCS (material-flow-control, asrs-control, amr-fleet-control) moves containers:
it retrieves container C from the AS/RS to a station and stores it back. Seam
decision 1 makes this context the owner of the container: "its id, its contents and
its logical location. The WCS references the id only." Seam decision 2 makes each
AS/RS one logical location: "`inventory-storage` records that container C, with its
contents, is in that location. Rack positions stay inside `asrs-control`."

This context already has a `Container` (ADR 0045): a sealed case, one SKU, one
fixed quantity, linked 1:1 to a `SEALED_CASE` stock unit, never reused, keyed by
its LPN (the SSCC, or inbound-receiving's internal `LPN-<site>-<id>`). The AS/RS
stores **totes**: reusable bins, filled at a decant station with any SKUs, picked
from at a goods-to-person station and returned, many times.

## Decision

1. **The container id is the LPN.** One license-plate namespace: the existing rule
   `^[A-Za-z0-9._-]{1,64}$`, unique across sealed cases and totes through a
   `license_plates` registry (primary key `lpn`, `kind` CASE or TOTE) claimed in the
   unit of work that registers either. A `containerId` in any WCS message is such an
   LPN.
2. **Labels.** This service mints no LPN and prints nothing. A tote is labelled once
   when commissioned (pre-printed by the supplier, or printed by the floor client
   from the LPN as CODE128) and registered with `PUT /totes/{lpn}`. A carton keeps
   its SSCC label or inbound-receiving's internal LPN label (ADR 0045).
3. **`Tote` is its own aggregate** (`internal/domain/container/tote.go`): `lpn`,
   `siteId` (set at the first placement, then fixed), `binId` (its logical location),
   `version` (ADR 0019). It is not a kind of `Container`: the REST `Container`
   schema requires `sku`, `gtin`, `unitsPerPack`, `qtyEaches`, `receiptId`, `lineNo`,
   and making them optional is a breaking response change.
4. **Stock per container.** A `StockUnit` carries an optional `containerLpn`. A
   tote's contents are LOOSE units in the tote's bin tagged with the tote. Every
   `SEALED_CASE` unit is tagged with its case's LPN (backfilled). A unit never
   changes container; eaches stay the ledger unit.
5. **Commands.**
   - `RegisterTote` — `PUT /totes/{lpn}`: 201 when created, 200 when the tote exists;
     `409 license-plate-taken` when the LPN is a sealed case. Event `ToteRegistered`.
   - `FillTote` — `POST /totes/{lpn}/fills {sku, quantity, ownerRef?}`: a stow into
     the tote (item scan + tote scan): the same placement checks, capacity check and
     events as `POST /stowages` (`ItemStowed`, `LocationRecorded`,
     `BinStockLevelChanged`) plus `ToteFilled`, at the tote's current bin. A tote in
     no bin: `409 tote-not-placed`.
   - `StowTote` — `POST /totes/{lpn}/stowages {bin}`: moves the tote and every unit
     in it to `bin` in one unit of work (occupancy in eaches, `BinStockLevelChanged`
     for both bins, `LocationRecorded` per unit, `ToteStowed`). Same bin: no-op.
     Another site: `409 container-not-at-site`. A tote holding reserved stock:
     `409 tote-reserved` (the allocation names the pick location, ADR 0025).
   - Reads: `GET /totes/{lpn}` (tote with its contents), `GET /bins/{binId}/totes`.
6. **Allocations name their container.** `Allocation` gains `ContainerLPN`
   (`reservation_allocations.container_lpn`, REST `allocations[].containerId`,
   omitted when empty) for every draw from a unit in a tote or a sealed case.
7. **Container-aware draw (amends ADR 0050, units in a container only).** After the
   forward pick faces, when they do not cover the request, the first unit in
   ADR 0050 order that is in a container and whose usable quantity covers the
   whole remainder is drawn next: one retrieval instead of several. A SKU with no
   container stock is ordered exactly as ADR 0050.
8. **No loose stock in an `AutomatedStorage` bin.** The facility cache keeps each
   slot's `locationType` from `LocationSlotRegistered` (already published). A loose
   stow, a relocation into, a case break into and a transfer stow into a bin whose
   type is `AutomatedStorage` (facility-layout ADR 0048) is refused `409
   automated-storage-requires-container`. An unknown type keeps today's behaviour
   (fail open, as ADR 0013). A sealed case (`POST /containers/{lpn}/stowages`) and a
   tote are containers and are accepted.
9. **Events** (CloudEvents 1.0, outbox, `warehouse.inventory.events`, subject = key
   = `lpn`, `data` snake_case, `dataschema urn:warehouse:inventory-storage:events:<Event>:v1`):

| Type `com.warehouse.wms.inventory-storage.tote.<Event>` | `data` |
| --- | --- |
| `ToteRegistered` | `lpn, at` |
| `ToteFilled` | `lpn, sku, qty_eaches, owner_ref?, site_id, bin, stock_unit_id, at` |
| `ToteStowed` | `lpn, site_id, from_bin?, bin, contents[{sku, qty_eaches, owner_ref?}], at` |

10. **Carton linking.** A received carton whose LPN is already a tote is REJECTED
    as `LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE` (never merged), exactly like a
    duplicate SSCC (ADR 0046).

## Amendment (2026-11-21): what the build settled

Recorded after the WCS plan 05a reviews; each point states behaviour the code on
`develop` has and names the test that pins it.

1. **`PUT /totes/{lpn}` stays idempotent under concurrency.** Two registrations of
   the same new LPN can both read "no tote" and race on the `license_plates` primary
   key. The loser's claim waits for the winner's transaction, then finds the plate
   held by a TOTE and the tote row present: it re-reads and answers the winner's tote
   with `created=false` (200). Exactly one `ToteRegistered` is raised.
   `409 license-plate-taken` (FINAL, ADR 0053) is answered only when the plate is
   held by a sealed case. Tests: `TestConcurrentRegisterToteOfOneNewLPNAnswersTheExistingTote`,
   `TestRegisterToteLosingThePlateToASealedCaseIsLicensePlateTaken` (use case) and
   `TestConcurrentRegisterToteOverPostgresAnswersTheExistingTote` (Postgres, one unit
   of work per racer).
2. **Rejection reason reuse (decision 10).** A carton whose LPN is a tote is rejected
   with the existing reason `LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE`. No new reason
   value is added, so no consumer of the rejection sees an unknown value. Read the
   reason as "this LPN already names another container"; the name dates from ADR 0046,
   when the only other holder was a receipt line.
3. **One plate, one owner, in one transaction.** `ToteRepo.Add` and `ContainerRepo.Add`
   claim the plate in `license_plates` and insert their row in the same transaction:
   the use case's unit of work when one is active, otherwise a transaction of their
   own (`claimAndInsert`). A failed insert leaves no plate claimed. Tests:
   `TestToteAddClaimRollsBackWithAFailedInsert`, `TestCaseAddClaimRollsBackWithAFailedInsert`,
   `TestLicensePlateClaimRollsBackWithTheUnitOfWork`, `TestOneLicensePlateNamespaceAcrossCasesAndTotes`.
   The reads that come before the claim (`RegisterTote` looking for a sealed case, the
   carton linker looking for a tote) are only checks: the claim decides. When a carton
   loses the claim to a tote registered between the linker's read and its insert, the
   pending link is dropped without the rejection of decision 10, as when a redelivered
   case loses (ADR 0046). No plate gets two owners; only the rejection record is missing.
4. **Rollout window of migration 0042.** The migration backfills a CASE plate for every
   existing `containers` row. During a rolling deploy a pod still running pre-0042 code
   can register a sealed case after the backfill, and that case gets no plate.
   A later `PUT /totes/{lpn}` of that LPN is still refused, because `RegisterTote`
   reads `containers` in the same unit of work. Only a tote registered at the same
   moment as such a case can take its LPN. There is no code defence: one more
   `containers` read inside `ToteRepo.Add` would be the same check-then-act read,
   because pre-0042 code takes no lock on `license_plates`. Once the last pre-0042 pod
   is gone, close the window by re-running the backfill
   (`INSERT INTO license_plates (lpn, kind) SELECT lpn, 'CASE' FROM containers ON CONFLICT (lpn) DO NOTHING`)
   and check that `SELECT lpn FROM containers JOIN totes USING (lpn)` returns no row.
5. **`StowTote` refusal precedence.** Every refusal is checked before the same-bin
   no-op and before any write, unit by unit: `tote-reserved` when the unit is
   reserved, `unit-held` or `unit-unlocated`, then the placement rules of its SKU at
   the destination. So stowing a tote into the bin it is already in answers 409 when
   one of its units is reserved, held or unlocated, instead of the no-op of
   decision 5. A held unit going into a full destination answers `unit-held`, not
   `bin-full`. The route is new in this ADR, so no existing caller changes behaviour.

## Not in this change

- Moving a container from `TaskCompleted`. The WCS putaway ends with
  fulfillment-execution's `TaskCompleted` (context map relationship 9); its payload
  carries no container today. Proposed for the fulfillment-execution ADR-0015
  amendment (WCS plan 05b): an optional snake_case `container_id` on
  `TaskCompleted`; with it and `to_bin` this context calls `StowTote` (putaway) or
  draws the relocation from that container's unit (replenishment). Until then the
  move is the REST command above.
- MCP and A2A tools for totes (warehouse-ops-agent pins the MCP registry; they come
  with the read-only container-position views of the ecosystem-impact row).
- Lot and expiry on tote contents (a fill carries none; FEFO is out of scope as in
  ADR 0045).

## Consequences

- The WCS can address any license plate this context knows; a sealed case stored in
  an AS/RS that handles cartons works with the existing `POST /containers/{lpn}/stowages`.
- Two aggregates share one id space; the registry makes the uniqueness a database
  fact. A tote and its units are written in one unit of work.
- Allocation from automated storage prefers one container per request; ADR 0050's
  forward-first and tie-breaking rules are unchanged.
- An `AutomatedStorage` bin can only be filled through a tote or a sealed case;
  inventory control must register the AS/RS bin (`PUT /bins/{binId}`) with a
  capacity in eaches large enough for the AS/RS.
- Consumers of `warehouse.inventory.events` dispatch on the full type; the three new
  types are ignored by each of them (proved in the PR description).

## Alternatives considered

1. **A `kind` on `Container`.** Rejected: invariants differ (one SKU and a fixed
   quantity vs any SKUs and a varying quantity) and the REST schema cannot carry a
   tote without a breaking change.
2. **The WCS owns container ids.** Rejected by seam decision 1 (the WCS must never
   know contents).
3. **Mint `TOTE-<ulid>` here.** Rejected: totes are labelled once for life, before
   they ever hold stock; an id issued here would still need a printed label, and
   ADR 0045 keeps this service out of identity minting.
4. **Hold rack positions as bins.** Rejected by seam decision 2: every reshuffle
   would be a stock transaction.
