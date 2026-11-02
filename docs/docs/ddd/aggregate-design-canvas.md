---
title: Aggregate Design Canvas
sidebar_label: Aggregate Design Canvas
sidebar_position: 4
description: The ddd-crew Aggregate Design Canvas v1.1 for each of the six aggregate roots — state transitions, enforced invariants with their Err values and failing-path tests, handled commands, created events, throughput and size.
---

# Aggregate Design Canvas

Following the [ddd-crew Aggregate Design Canvas](https://github.com/ddd-crew/aggregate-design-canvas)
(v1.1), one canvas per aggregate root found in `internal/domain/`. There
are **six**: `StockUnit`, `Bin`, `Reservation`, `ProductClassification`,
`Container`, `Relocation`.
Every invariant below is enforced **in the domain layer** (or, where noted,
in the use case that owns the cross-aggregate rule) and has a failing-path
test.

Aggregates reference each other **by identity, never by pointer**: a
`Reservation` holds `Allocation{StockUnitID, BinID}` values, a `StockUnit`
holds a `BinId`. Each aggregate is loaded, changed and saved through its own
repository port, and every `Save` is version-guarded (ADR 0019). A use case
that touches several aggregates (stow, revoke, confirm-pick, expiry,
relocate, break-case) keeps
them consistent inside one `UnitOfWork` transaction (ADR 0017) — the one
place this context deliberately trades "one aggregate per transaction" for
a single database transaction.

Throughput and size figures are **estimates** for a single mid-size
fulfilment centre, not measurements — the service has no production
traffic.

---

## StockUnit

### 1. Name

`StockUnit` — `internal/domain/stock.StockUnit`

### 2. Description

A quantity of one SKU at one bin, with a reserved portion and a lifecycle
state. It is the core aggregate of the context: every physical item has
exactly one known bin, or is flagged `UNLOCATED`. Total stock for a SKU is a
sum across units; there is deliberately no "SKU balance" aggregate to
contend on.

| Field | Meaning |
| --- | --- |
| `id` | Identity, minted by `StockRepo.NextID` |
| `sku` | Item scan |
| `binID` | Location scan — never changes after creation |
| `quantity` | On-hand at this bin |
| `reserved` | Portion bound to demand |
| `state` | `AVAILABLE` / `RESERVED` / `PICKED` / `REMOVED` / `UNLOCATED` |
| `version` | Optimistic-concurrency metadata, never read by business logic |

### 3. State Transitions

```mermaid
stateDiagram-v2
    [*] --> AVAILABLE: NewStockUnit, stow with item and location scan
    AVAILABLE --> RESERVED: Reserve(qty)
    RESERVED --> RESERVED: Reserve(qty) again
    RESERVED --> AVAILABLE: ReleaseReservation, reserved reaches 0
    RESERVED --> PICKED: Pick(qty), quantity remains
    RESERVED --> REMOVED: Pick(qty), quantity reaches 0
    PICKED --> RESERVED: Reserve(qty)
    PICKED --> REMOVED: Pick(qty), quantity reaches 0
    AVAILABLE --> UNLOCATED: MarkUnlocated, cycle-count shortfall
    RESERVED --> UNLOCATED: MarkUnlocated
    PICKED --> UNLOCATED: MarkUnlocated
    REMOVED --> [*]
```

Source: `internal/domain/stock/state.go`, `internal/domain/stock/stock_unit.go`.
Omitted: `ReleaseReservation` on a `PICKED` unit (state stays `PICKED`);
`Pick` performs no state check of its own, so a still-reserved `UNLOCATED`
unit can be picked into `PICKED`/`REMOVED` — the guard is that
`RunCycleCount` only marks units it can count.

`MarkUnlocated` is deliberately unconditional — it takes no error return. A
cycle count that finds stock missing must always be able to say so.

### 4. Enforced Invariants

| # | Invariant | Enforced by | Failing-path test |
| --- | --- | --- | --- |
| S1 | A stow requires both an item scan and a location scan | `NewStockUnit` → `stock.ErrStowRequiresItemAndLocation` | `TestNewStockUnit_RequiresSKU`, `TestNewStockUnit_RequiresBin` |
| S2 | Quantity is never negative | `shared.NewQuantity` / `Quantity.Sub` → `shared.ErrNegativeQuantity` | `TestNewQuantity_RejectsNegative`, `TestQuantity_Sub_RejectsNegativeResult` |
| S3 | A stow of zero or fewer units is invalid | `NewStockUnit` → `shared.ErrZeroQuantity` | `TestNewStockUnit_RejectsZeroQuantity` |
| S4 | Reserved never exceeds usable — no negative usable | `Reserve` → `stock.ErrInsufficientUsable` | `TestStockUnit_Reserve_ExceedsUsable_Rejected` |
| S5 | Unlocated or removed stock is never reservable and contributes 0 usable | `Reserve` → `stock.ErrUnitUnlocated`; `Usable()` returns 0 | `TestStockUnit_Reserve_UnlocatedUnit_Rejected`, `TestStockUnit_Usable_RemovedState_IsZero` |
| S6 | A pick cannot exceed what was reserved, nor what is on hand | `Pick` → `stock.ErrInsufficientReserved` | `TestStockUnit_Pick_ExceedsReserved_Rejected`, `TestStockUnit_Pick_ExceedsOnHandQuantity_Rejected` |
| S7 | Release cannot return more than was reserved | `ReleaseReservation` → `stock.ErrInsufficientReserved` | `TestStockUnit_ReleaseReservation_ExceedsReserved_Rejected` |
| S8 | No lost update | `postgres.StockRepo.Save` version guard → `usecases.ErrConcurrentModification` | `optimistic_concurrency_integration_test.go` |

### 5. Corrective Policies

- **Revoke / expiry release.** When a reservation is revoked or lazily
  expired, `releaseAllocations` calls `ReleaseReservation` on every unit it
  drew from, in the same transaction.
- **Cycle-count shortfall.** `RunCycleCount` marks whole units `UNLOCATED`
  until the shortfall is covered, so lost stock stops counting toward usable
  at once. Overage is not corrected here — it is reported for a separate
  receiving/audit process.
- **Concurrent modification.** A lost version race surfaces as `409`; the
  caller re-fetches and retries.

### 6. Handled Commands

| Command | Method | Use case / entry point |
| --- | --- | --- |
| Stow | `NewStockUnit` | `StowStock` — `POST /stowages` |
| Reserve | `Reserve` | `ReserveStock` — `POST /reservations` |
| Release reservation | `ReleaseReservation` | `RevokeReservation` (`DELETE /reservations/{id}`, MCP `revoke_reservation`), lazy expiry |
| Pick | `Pick` | `ConfirmPick` — `POST /reservations/{id}/pick-confirmations`, and per ACTIVE reservation from `ConfirmPicksForOrder` on `TaskCompleted`: the picked line's reservation when the event carries `line_no` ([ADR 0036](/docs/adr/0036)), the order's on its last pick otherwise ([ADR 0035](/docs/adr/0035)) |
| Mark unlocated | `MarkUnlocated` | `RunCycleCount` — `POST /bins/{binId}/cycle-counts` |

### 7. Created Events

The aggregate itself raises nothing; the use cases build these from it
(`internal/domain/shared/events.go`).

| Event | Full CloudEvents `type` | Raised by |
| --- | --- | --- |
| StockReceived | `com.warehouse.wms.inventory-storage.stock.StockReceived` | `ReceiveStock` (no unit exists yet) |
| ItemStowed | `com.warehouse.wms.inventory-storage.stock.ItemStowed` | `StowStock` |
| LocationRecorded | not published (would be `...stock.LocationRecorded`) — **decided 2026-10-06: stays in-process**, no consumer | `StowStock` |
| ItemUnlocated | `com.warehouse.wms.inventory-storage.stock.ItemUnlocated` | `RunCycleCount` |

### 8. Throughput (estimate)

High. Every stow creates one unit; every reservation, revoke, pick and
expiry touches one or more units of a SKU. Contention concentrates on fast
movers whose few units are drawn by many concurrent reservations — the
reason `Save` is version-guarded.

### 9. Size (estimate)

Small: seven fields. A unit lives from stow until `REMOVED` or
`UNLOCATED` — typically a few to a few dozen state changes over hours to
weeks, depending on how many reservations draw from it.

---

## Bin

### 1. Name

`Bin` — `internal/domain/location.Bin`

### 2. Description

A coded slot in chaotic storage: an id, a capacity and an occupancy. Any SKU
may occupy any free bin; the only hard constraint is
`sum(stock in bin) <= capacity`. The `Bin` has **no SKU field and no SKU
affinity** — that absence is what makes storage chaotic rather than
fixed-slot. It knows nothing about zones or aisles (that is
`facility-layout`).

| Field | Meaning |
| --- | --- |
| `id` | Bin code, e.g. `A-1-1` |
| `capacity` | Maximum units the slot holds |
| `occupied` | Units currently stowed |
| `version` | Optimistic-concurrency metadata |

### 3. State Transitions

`Bin` has no status enum; the states below are derived from `occupied`
and `IsFull()`.

```mermaid
stateDiagram-v2
    [*] --> Empty: NewBin(id, capacity), RegisterBin creates
    Empty --> PartlyOccupied: Occupy(qty) below capacity
    Empty --> Full: Occupy(qty) equal to capacity
    PartlyOccupied --> PartlyOccupied: Occupy or Release
    PartlyOccupied --> Full: Occupy(qty) reaches capacity
    Full --> PartlyOccupied: Release(qty) or Resize up
    PartlyOccupied --> Empty: Release(qty) to 0
    Full --> Empty: Release(qty) of everything
    PartlyOccupied --> Full: Resize down to occupancy
```

Source: `internal/domain/location/bin.go`, `internal/application/usecases/register_bin.go`.
Omitted: rejected transitions (they return errors and leave state unchanged);
a bin is never deleted.

### 4. Enforced Invariants

| # | Invariant | Enforced by | Failing-path test |
| --- | --- | --- | --- |
| B1 | `occupied <= capacity`; a full bin rejects a stow | `Occupy` → `location.ErrBinFull` | `TestBin_Occupy_ExceedsCapacity_Rejected`, `TestStowStock_ExceedsBinCapacity_Rejected` |
| B2 | Capacity must be positive | `NewBin` / `Resize` → `location.ErrInvalidCapacity` | `TestNewBin_RejectsInvalidCapacity` |
| B3 | A bin needs an id | `NewBin` → `shared.ErrEmptyBinID` | `TestNewBin_RejectsEmptyID` |
| B4 | A bin can never be resized below what it holds (exactly to occupancy is allowed) | `Resize` → `location.ErrCapacityBelowOccupancy` | `TestBin_Resize`, `TestRegisterBin`, `TestRegisterBin_Endpoint` |
| B5 | You cannot release more than is occupied | `Release` → `location.ErrReleaseExceedsOccupancy` | `TestBin_Release_ExceedsOccupancy_Rejected` |
| B6 | Occupying or releasing zero units is meaningless | `Occupy` / `Release` → `shared.ErrZeroQuantity` | `TestBin_Occupy_RejectsZeroQuantity`, `TestBin_Release_RejectsZeroQuantity` |
| B7 | A resize racing a stow never clobbers occupancy | `postgres.LocationRepo.Save` version guard → `usecases.ErrConcurrentModification` | `optimistic_concurrency_integration_test.go` |

### 5. Corrective Policies

- **Pick frees capacity.** `ConfirmPick` calls `Bin.Release` for each
  allocation in the same transaction as the `StockUnit.Pick`.
- **Declarative convergence.** `RegisterBin` is idempotent: repeating it
  with the same capacity is a no-op, so an inventory-control client can
  safely re-send its whole bin list.

### 6. Handled Commands

| Command | Method | Use case / entry point |
| --- | --- | --- |
| Register / resize | `NewBin`, `Resize` | `RegisterBin` — `PUT /bins/{binId}` |
| Occupy | `Occupy` | `StowStock` — `POST /stowages` |
| Release | `Release` | `ConfirmPick` — `POST /reservations/{id}/pick-confirmations`, and per ACTIVE reservation from `ConfirmPicksForOrder` on `TaskCompleted`: the picked line's reservation when the event carries `line_no` ([ADR 0036](/docs/adr/0036)), the order's on its last pick otherwise ([ADR 0035](/docs/adr/0035)) |

### 7. Created Events

`RegisterBin` raises **no** event (ADR 0025). The cycle-count facts are
grouped under the `bin` entity because they are about a bin, though
`RunCycleCount` reads `StockUnit`s and never loads the `Bin` aggregate:

| Event | Full CloudEvents `type` |
| --- | --- |
| CycleCountCompleted | `com.warehouse.wms.inventory-storage.bin.CycleCountCompleted` |
| DiscrepancyDetected | `com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected` |

### 8. Throughput (estimate)

High on stow and pick (every stow and every confirmed allocation writes its
bin), low on registration (layout changes). Hot spots are bins near the
receive dock under chaotic stow.

### 9. Size (estimate)

Tiny: four fields. Long-lived — a bin exists as long as the physical slot,
accumulating one change per stow and per picked allocation (thousands over
its life), but each change only moves two integers.

---

## Reservation

### 1. Name

`Reservation` — `internal/domain/reservation.Reservation`

### 2. Description

A **revocable**, expiring binding of a quantity of a SKU to a demand. It
records `Allocation`s — which `StockUnit` it drew from, the pick location
(`BinID`, ADR 0025) and how much — so a revoke returns exactly that
quantity and a confirm-pick consumes exactly it. Nothing binds a future
reservation to the same holding: that is what makes a failed pick
recoverable (ADR 0003).

| Field | Meaning |
| --- | --- |
| `id` | Identity, minted by `ReservationRepo.NextID` |
| `sku`, `quantity` | What is claimed |
| `demandRef` | Opaque upstream reference (order + line); replay-guard and lookup key |
| `lineNo` | Optional order line the reservation is for (`line_no`, nullable; ADR 0036). Set at creation from `POST /reservations`'s `lineNo`, immutable, `nil` when unknown (a reservation made before migration 0035, or by a client that does not send it); with `demandRef` it is what the `TaskCompleted` consumer confirms by |
| `allocations` | `[]Allocation{StockUnitID, BinID, Quantity}` |
| `status` | `ACTIVE` / `CONFIRMED` / `REVOKED` / `EXPIRED` |
| `createdAt`, `expiresAt` | `expiresAt = createdAt + timeout` (default 30 min) |
| `version` | Optimistic-concurrency metadata |

### 3. State Transitions

```mermaid
stateDiagram-v2
    [*] --> ACTIVE: New, ReserveStock against usable
    ACTIVE --> CONFIRMED: Confirm(now) before expiresAt
    ACTIVE --> REVOKED: Revoke
    ACTIVE --> EXPIRED: Expire, lazy expiry on next read
    CONFIRMED --> [*]
    REVOKED --> [*]
    EXPIRED --> [*]
```

Source: `internal/domain/reservation/reservation.go`,
`internal/application/usecases/reservation_expiry.go`.
Omitted: the rejected transitions — any call on a non-`ACTIVE` reservation
returns `ErrAlreadyResolved`.

### 4. Enforced Invariants

| # | Invariant | Enforced by | Failing-path test |
| --- | --- | --- | --- |
| R1 | Reserved quantity ≤ usable quantity at reserve time | `ReserveStock.allocate` → `usecases.ErrInsufficientUsable`; `StockUnit.Reserve` re-checks per unit | `TestReserveStock_ExceedsUsable_Rejected` |
| R2 | Revoke returns quantity to usable | `RevokeReservation` → `releaseAllocations` → `StockUnit.ReleaseReservation` | `TestRevokeReservation_ReturnsQuantityToUsable`, `TestStockUnit_ReleaseReservation_ReturnsToUsable` |
| R3 | No double-consume: only `ACTIVE` may transition | `Revoke` / `Confirm` / `Expire` → `reservation.ErrAlreadyResolved` | `TestReservation_Revoke_Twice_Rejected`, `TestReservation_Confirm_Twice_Rejected`, `TestReservation_Expire_Twice_Rejected`, `TestConfirmPick_AfterRevoke_Rejected` |
| R4 | Expires after a timeout; never confirmed late | `IsExpired(now)`; `Confirm` → `reservation.ErrExpired` | `TestReservation_IsExpired`, `TestReservation_Confirm_AfterExpiry_Rejected` |
| R5 | Must allocate against something | `New` → `reservation.ErrNoAllocations` | `TestNew_RequiresAtLeastOneAllocation` |
| R6 | One active reservation per (demandRef, SKU, quantity, and line when the request names one) — best effort; an ACTIVE reservation with no line still answers a line-aware retry | `ReserveStock.activeReservationFor` / `isReplayOf` (use case, not DB-enforced) | `reserve_stock_multi_line_test.go`, `reserve_stock_line_test.go` |
| R7 | Empty `demandRef` is rejected | HTTP handler → `400 missing-demand-ref` | `server_test.go` |
| R8 | `lineNo`, when present, is between 1 and 2147483647 (the 32-bit `line_no` column) | `reservation.ValidLineNo` in `NewForLine` → `reservation.ErrInvalidLineNo`; HTTP handler → `400 invalid-line-no`; TaskCompleted `line_no` outside the range → `ErrMalformedPickCompletion` (dead-lettered); column `INTEGER CHECK (line_no IS NULL OR line_no >= 1)` | `TestNewForLine_RejectsANonPositiveLineNo`, `TestNewForLine_LineNoBoundaryTable`, `TestReserveStock_Endpoint_NonPositiveLineNo_Rejected`, `TestReserveStock_Endpoint_LineNoBoundaryTable` |

### 5. Corrective Policies

- **Lazy expiry.** `expireIfDue` runs on every read path
  (`GetReservationsByDemandRef`, `ReserveStock`'s replay guard,
  `RevokeReservation`, `ConfirmPick`): a timed-out `ACTIVE` reservation is
  released, set `EXPIRED` and `ReservationExpired` is raised in one
  transaction. There is no background sweeper.
- **Revoke as compensation.** A failed physical pick is compensated by
  `DELETE /reservations/{id}`; deciding what to do next is
  `wes-work-planning`'s or `order-management`'s call, not this context's.

### 6. Handled Commands

| Command | Method | Use case / entry point |
| --- | --- | --- |
| Reserve | `New`, `NewForLine` | `ReserveStock` — `POST /reservations` (Idempotency-Key, optional `lineNo`, ADR 0036) |
| Revoke | `Revoke` | `RevokeReservation` — `DELETE /reservations/{id}`, MCP `revoke_reservation` |
| Confirm pick | `Confirm` | `ConfirmPick` — `POST /reservations/{id}/pick-confirmations`, and per ACTIVE reservation from `ConfirmPicksForOrder` on `TaskCompleted`: the picked line's reservation when the event carries `line_no` ([ADR 0036](/docs/adr/0036)), the order's on its last pick otherwise ([ADR 0035](/docs/adr/0035)) |
| Expire | `Expire` | lazy, inside the four read paths above |

### 7. Created Events

| Event | Full CloudEvents `type` | Topics |
| --- | --- | --- |
| StockReserved | `com.warehouse.wms.inventory-storage.reservation.StockReserved` | integration + analytics |
| ReservationRevoked | `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` | integration + analytics |
| ReservationExpired | `com.warehouse.wms.inventory-storage.reservation.ReservationExpired` | analytics only |
| StockPicked | `com.warehouse.wms.inventory-storage.reservation.StockPicked` | analytics only |

### 8. Throughput (estimate)

Highest of the six: one reservation per order line, plus one revoke or
confirm each, plus reads by `demandRef` from the Order Lifecycle console.
Each reservation is written by one caller at a time, so per-instance
contention is low; contention moves to the `StockUnit`s it draws from.

### 9. Size (estimate)

Small: a handful of fields plus usually one to three allocations. Short
lifetime — two to three events (created, then confirmed / revoked /
expired) within the 30-minute default timeout. Rows are never deleted.

---

## ProductClassification

### 1. Name

`ProductClassification` — `internal/domain/product.ProductClassification`

### 2. Description

SKU-level master data, independent of any `StockUnit` or bin, describing how
an item must be handled. product-master is the **source of truth**
([ADR 0034](/docs/adr/0034), superseding the ownership part of ADR 0009); this
aggregate is a version-guarded local copy of it: a closed set of
`HandlingTag`s, a `TemperatureClass` required only for
`TemperatureSensitive` SKUs, and an optional US DOT hazard class meaningful
only for `Hazmat` SKUs (ADR 0010). `StowStock` enforces placement and
same-bin segregation from it; no sibling context reads it from here any more (the REST
`GET` was removed at stage E).

| Field | Meaning |
| --- | --- |
| `sku` | The classified SKU (identity) |
| `handlingTags` | Set of `Hazmat` / `Fragile` / `TemperatureSensitive` / `Oversized` / `HighValue` |
| `temperatureClass` | `Ambient` / `Chilled` / `Frozen`, or empty |
| `dotHazardClass` | `1`-`9`, or `0` (`DOTHazardClassUnspecified`) |

### 3. State Transitions

No status enum: a classification either does not exist or is current, and
re-classifying replaces it wholesale.

```mermaid
stateDiagram-v2
    [*] --> Unclassified
    Unclassified --> Classified: ApplyProductClassification, product.New (version >= 1)
    Classified --> Classified: ApplyProductClassification, newer version replaces
```

Source: `internal/domain/product/classification.go`,
`internal/application/usecases/apply_product_classification.go`. Since
[ADR 0034](/docs/adr/0034) the rows are a version-guarded local copy of
product-master's classification; this service no longer authors them.
Omitted: there is no delete or unclassify operation.

### 4. Enforced Invariants

| # | Invariant | Enforced by | Failing-path test |
| --- | --- | --- | --- |
| P1 | At least one handling tag | `New` → `product.ErrNoHandlingTags` | `TestNew_TableDriven/no_tags_rejected` |
| P2 | `HandlingTag` is a closed enum | `ParseHandlingTag` / `New` → `product.ErrUnknownHandlingTag` | `TestParseHandlingTag/unknown`, `TestNew_TableDriven/unknown_tag_rejected` |
| P3 | Tags form a set | `New` → `product.ErrDuplicateHandlingTag` | `TestNew_TableDriven/duplicate_tag_rejected` |
| P4 | `TemperatureSensitive` requires a valid `TemperatureClass` | `New` → `product.ErrTemperatureClassRequired` / `product.ErrUnknownTemperatureClass` | `TestNew_TableDriven/temperature_sensitive_without_class_rejected` |
| P5 | No `TemperatureClass` without `TemperatureSensitive` | `New` → `product.ErrTemperatureClassNotApplicable` | `TestNew_TableDriven/temperature_class_without_temperature_sensitive_tag_rejected` |
| P6 | `DOTHazardClass` only with `Hazmat` | `New` → `product.ErrDOTHazardClassNotApplicable` | `TestNew_TableDriven/dot_hazard_class_without_hazmat_tag_rejected` |
| P7 | `DOTHazardClass` in 1-9 | `ParseDOTHazardClass` / `New` → `product.ErrInvalidDOTHazardClass` | `TestNew_TableDriven/dot_hazard_class_out_of_range_rejected` |
| P8 | `Hazmat` never requires a DOT class | `New` accepts `DOTHazardClassUnspecified` | `TestNew_TableDriven/hazmat_alone_succeeds` |

Rules this aggregate's data drives, enforced in `StowStock` (they span a
classification, a bin's occupants and facility-layout's zone data):

| Rule | Error |
| --- | --- |
| Hazmat SKU needs a hazmat-rated zone | `usecases.ErrHazmatZoneRequired` |
| Temperature-sensitive SKU needs a matching zone temperature class | `usecases.ErrTemperatureClassMismatch` |
| Zone lookup failed for a classified SKU (fail-closed) | `usecases.ErrLocationClassificationUnavailable` |
| Incompatible DOT classes may not share a bin (49 CFR §177.848, four simplifications) | `usecases.ErrHazmatClassIncompatible` via `product.Incompatible` |

Unclassified SKUs, unknown bins and unclassified occupants are **fail-open**.

### 5. Corrective Policies

- **Re-classification replaces.** Corrections are made in product-master
  (ADR 0034); a newer `version` replaces the local copy wholesale.
- None for already-stowed stock: re-classifying a SKU does not re-check
  bins it already occupies.

### 6. Handled Commands

| Command | Method | Use case / entry point |
| --- | --- | --- |
| Apply product-master's classification | `product.New` | `ApplyProductClassification` — Kafka `warehouse.product-master.events` (`PUT /products/{sku}/classification` was removed at stage E) |

### 7. Created Events

None. The aggregate raises no event since [ADR 0034](/docs/adr/0034). The
legacy `com.warehouse.wms.inventory-storage.product.ProductClassified`
(published on every write from 2026-10-06 under [ADR 0031](/docs/adr/0031))
and the `RepublishProductClassifications` backfill were removed at stage E
(2026-10-17).

### 8. Throughput (estimate)

Low writes (product-master's `ProductClassified` messages), high reads: every
classified-SKU stow. Siblings keep their own copies from product-master's
events; the REST read `GET /products/{sku}/classification` was removed at
stage E.

### 9. Size (estimate)

Tiny: four fields. Lives as long as the SKU; a handful of re-classifications
over its life.

---

## Container

### 1. Name

`Container` — `internal/domain/container.Container`

### 2. Description

The identity of one physical sealed case in storage, keyed by its LPN —
the carton's SSCC, or an internal LPN minted by inbound-receiving for an
unlabelled carton ([ADR 0045](/docs/adr/0045), as built per
[ADR 0046](/docs/adr/0046)). A Container does **not** hold stock: the
eaches stay on a `StockUnit` in the ledger; the Container adds identity
and a pack state on top of it, linked 1:1 through `stockUnitID` while the
case is stowed. The receipt carries no site: the first stow sets the
container's site from the bin's site.

| Field | Meaning |
| --- | --- |
| `lpn` | Identity — 1 to 64 characters of `[A-Za-z0-9._-]` |
| `sku`, `gtin`, `unitsPerPack` | What the case holds and how it packs |
| `quantity` | Eaches in the case; in v1 always equals `unitsPerPack` (one case) |
| `ownerRef` | Owner of the stock (owner-segregated stock, ADR 0045) |
| `siteID` | Set at the first stow from the bin; never changes afterwards |
| `binID` | Where the case sits; empty until stowed and after emptying |
| `state` | `SEALED` / `OPEN` / `EMPTY` |
| `lot`, `expiry` | Carried from the receipt (AI 10 / AI 17), never interpreted |
| `stockUnitID` | The 1:1 link to the backing `SEALED_CASE` `StockUnit` |
| `receiptID`, `lineNo` | Provenance: the receipt line the carton arrived on |
| `version` | Optimistic-concurrency metadata (ADR 0019) |

### 3. State Transitions

```mermaid
stateDiagram-v2
    [*] --> SEALED: Register, a settled receipt carton link
    SEALED --> SEALED: Stow into another bin of the same site
    SEALED --> OPEN: Break, eaches move to loose stock
    OPEN --> EMPTY: Decant
    SEALED --> EMPTY: EmptyWhole, whole-case pick or count shortfall
    EMPTY --> [*]
```

Source: `internal/domain/container/container.go`,
`internal/application/usecases/stow_container.go`, `break_case.go`,
`container_unit_exit.go`. In v1 `OPEN` exists only inside one break
transaction: `BreakCase` runs `Break` and `Decant` in one `UnitOfWork`, so
a stored container is always `SEALED` or `EMPTY`. Omitted: the rejected
transitions (they return errors and leave state unchanged).

### 4. Enforced Invariants

| # | Invariant | Enforced by | Failing-path test |
| --- | --- | --- | --- |
| C1 | A sealed container holds exactly one case: `quantity == unitsPerPack` (cases is 1 in v1) | `Register` → `container.ErrContainerQuantityMismatch` | `TestRegisterEnforcesQuantityEqualsUnitsPerPackTimesOneCase` |
| C2 | The LPN is 1–64 characters of `[A-Za-z0-9._-]` | `Register` → `container.ErrInvalidLPN` | `TestRegisterRejectsInvalidIdentity` |
| C3 | Provenance is complete: receipt id present, line no ≥ 1, lot ≤ 20 and expiry ≤ 32 characters | `Register` → `container.ErrInvalidContainer` | `TestRegisterRejectsInvalidIdentity` |
| C4 | Only a `SEALED` container can be stowed, broken or emptied whole | `Stow` / `Break` / `EmptyWhole` → `container.ErrContainerNotSealed` | `TestStowRequiresASealedContainer`, `TestBreakNeedsSealedAndStowed` |
| C5 | A break needs a stowed container and conserves eaches: the produced quantity equals the container quantity, no more and no fewer | `Break` → `container.ErrContainerNotStowed`, `container.ErrBreakNotConserving` | `TestBreakNeedsSealedAndStowed`, `TestBreakConservesQuantity` |
| C6 | Only an `OPEN` container decants | `Decant` → `container.ErrContainerNotOpen` | `TestDecantNeedsAnOpenContainer` |
| C7 | A container never changes site: the first stow sets it from the bin, a later stow must stay in that site | `Stow` → `container.ErrContainerNotAtSite` | `TestRestowToAnotherSiteIsRejected`, `TestRestowMovesTheUnitWithinTheSiteAndRefusesAnotherSite` |
| C8 | A sealed case stows only in a `CASE` or `PALLET` bin — a bin with no recorded handling unit fails closed; loose eaches go to `EACH` or unrecorded bins | `StowContainer` via `ports.BinHandlingUnitLookup` → `container.ErrBinHandlingUnitMismatch` | `TestHandlingUnitAcceptance`, `TestStowContainerRejections` |
| C9 | A reserved case is never re-stowed or broken: the reservation names the case, not its eaches | `StowContainer` / `BreakCase` → `container.ErrContainerReserved` | `TestReservedContainerCannotBeRestowed` |
| C10 | No lost update | `postgres.ContainerRepo.Save` version guard → `usecases.ErrConcurrentModification` | `container_integration_test.go` |

### 5. Corrective Policies

- **Double-count guard.** `ContainerLinker` settles a pending carton link
  only while `sum(container qty)` per (receipt id, line no) stays within
  the booked quantity, so a container can never exist for units the ledger
  has not counted — whichever of `ReceiptLineReceived` and
  `ReceiptCartonReceived` arrives first. A rejected link keeps its reason
  on the row for audit (`QUANTITY_MISMATCH`, `GTIN_BELONGS_TO_OTHER_SKU`,
  `GTIN_IS_NOT_A_CASE`, `LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE`,
  `INVALID_CONTAINER`).
- **Idempotent stow.** Stowing a container into the bin it already
  occupies changes nothing; re-stowing within the same site moves its
  `SEALED_CASE` unit.
- **Rejected break changes nothing.** Conservation is checked before any
  stock moves; a rejected break leaves the sealed case and the ledger
  untouched.

### 6. Handled Commands

| Command | Method | Use case / entry point |
| --- | --- | --- |
| Register | `Register` | `ContainerLinker.Settle` — pending links from inbound-receiving's `ReceiptCartonReceived` (Kafka), settled once the receipt line is booked |
| Stow | `Stow` | `StowContainer` — `POST /containers/{lpn}/stowages` |
| Break case | `Break`, `Decant` | `BreakCase` — `POST /containers/{lpn}/case-breaks` |
| Empty whole | `EmptyWhole` | `container_unit_exit` — a confirmed whole-case pick or a cycle-count shortfall, inside the caller's `UnitOfWork` |
| Read | — | `GetContainer` — `GET /containers/{lpn}`, MCP `get_container`; `ListBinContainers` — `GET /bins/{binId}/containers` |

### 7. Created Events

All four go to the integration topic `warehouse.inventory.events`, keyed
by LPN (ADR 0045/0046).

| Event | Full CloudEvents `type` | Raised by |
| --- | --- | --- |
| ContainerRegistered | `com.warehouse.wms.inventory-storage.container.ContainerRegistered` | `ContainerLinker` on settle (no quantity booked, no site yet) |
| ContainerStowed | `com.warehouse.wms.inventory-storage.container.ContainerStowed` | `StowContainer` |
| CaseBroken | `com.warehouse.wms.inventory-storage.container.CaseBroken` | `BreakCase` — eaches conserved from `fromBin` to `looseBin` |
| ContainerEmptied | `com.warehouse.wms.inventory-storage.container.ContainerEmptied` | `BreakCase` (reason `BROKEN`), whole-case exit (`PICKED`, `UNLOCATED`) |

### 8. Throughput (estimate)

Moderate: one container per received sealed carton, one stow each, and a
break or whole-case exit per case consumed. Registration rate follows
inbound receiving; breaks follow each-picking demand for case-packed SKUs.

### 9. Size (estimate)

Small: fifteen fields, most set once at registration. A container lives
from receipt until the case is empty — days to weeks — with one stow, at
most a few re-stows inside its site, and one terminal break or whole-case
exit.

---

## Relocation

### 1. Name

`Relocation` — `internal/domain/relocation.Relocation`

### 2. Description

One intra-site move of a quantity of one SKU from a source bin to a
destination bin of the same site ([ADR 0039](/docs/adr/0039)). It is the
stock ledger's record of slot-plan execution (slotting-optimization's
approved plans, normally via `RELOCATE` tasks) and of manual re-slotting —
and, since [ADR 0041](/docs/adr/0041), of pick-face replenishment: a
completed `REPLENISH` task is applied as a `REPLENISHMENT` relocation.
Both bins and every touched `StockUnit` change in one transaction, the
ledger never goes negative, and the record answers
`GET /relocations/{id}`.

| Field | Meaning |
| --- | --- |
| `id` | Identity, minted by `RelocationRepo.NextID` |
| `siteID` | The site both bins must belong to; a relocation never crosses sites |
| `sku`, `quantity` | What moves |
| `fromBin`, `toBin` | Source and destination; always different |
| `reason` | `SLOTTING` / `CONSOLIDATION` / `MANUAL` (operator-requested), `REPLENISHMENT` (system-set only, ADR 0041) |
| `sourceRef` | Caller's correlation reference, ≤ 200 characters (`request_ref` of a REPLENISH task) |
| `legs` | Once applied: which source `StockUnit`s contributed how much |
| `destinationStockUnitID` | Once applied: the new destination `StockUnit` |
| `occurredAt` | Once applied: when the move happened |

### 3. State Transitions

No status enum: a relocation is requested, then applied exactly once.

```mermaid
stateDiagram-v2
    [*] --> Requested: New, spec validated
    Requested --> Applied: Apply moves units and bin occupancy
    Applied --> [*]
```

Source: `internal/domain/relocation/relocation.go`. Every rule is checked
before anything changes, so a rejected `Apply` leaves the units, the bins
and the relocation untouched.

### 4. Enforced Invariants

| # | Invariant | Enforced by | Failing-path test |
| --- | --- | --- | --- |
| L1 | Source and destination bins differ | `New` → `relocation.ErrSameBin` | `TestNew_RejectsInvalidSpecs` |
| L2 | The reason is in the closed set; `REPLENISHMENT` is system-set only — `New` accepts it, the operator-facing `ParseReason` behind `POST /relocations` rejects it | `New` / `ParseReason` → `relocation.ErrUnknownReason` | `TestParseReason`, `TestReplenishmentReason_AcceptedByNewRejectedByParseReason` |
| L3 | The moved quantity is positive | `New` → `shared.ErrZeroQuantity` | `TestNew_RejectsInvalidSpecs` |
| L4 | `sourceRef` is at most 200 characters | `New` → `relocation.ErrSourceRefTooLong` | `TestNew_RejectsInvalidSpecs`, `TestNew_SourceRefAtTheLimitIsAccepted` |
| L5 | Both bins are the ones the relocation names and belong to its site | `Apply` → `relocation.ErrBinMismatch`, `relocation.ErrBinNotAtSite` | `TestApply_BinsMustBeTheOnesNamed`, `TestApply_BinsMustBelongToTheSite` |
| L6 | A relocation moves stock exactly once | `Apply` → `relocation.ErrAlreadyApplied` | `TestApply_TwiceIsRejected` |
| L7 | The usable quantity covers the move, the destination has room and the source occupancy covers the move — all checked before any change | `Apply` → `stock.ErrInsufficientUsable`, `location.ErrBinFull`, `location.ErrReleaseExceedsOccupancy` | `TestApply_InsufficientUsableChangesNothing`, `TestApply_DestinationFullChangesNothing`, `TestApply_SourceOccupancyBelowTheQuantityIsRejected`, `TestApply_NoUnitsIsInsufficient` |
| L8 | Only usable units of the SKU in the source bin are drawn, in ascending unit id, so concurrent movers lock rows alike | `Apply` draw plan | `TestApply_SkipsReservedQuantityOtherSKUsOtherBinsAndDeadUnits`, `TestApply_StopsDrawingOnceTheQuantityIsCovered`, `TestRelocation_ConcurrentMoversOfTheSameStock` |

### 5. Corrective Policies

- **Check before change.** A rejected `Apply` mutates nothing: when the
  source release fails after the destination was occupied, the occupancy
  is restored, so neither bin changes.
- **Dead-letter the hopeless.** A `RELOCATE` or `REPLENISH` task
  completion that can never apply (missing field, unknown bin, other site,
  same bin, insufficient usable, destination full) is dead-lettered at
  once rather than retried.
- **Replenishment closes the loop.** `ApplyReplenishTask` applies the move
  with `sourceRef` = the replenishment request id, so
  pick-face-replenishment can close its request from `StockRelocated`.

### 6. Handled Commands

| Command | Method | Use case / entry point |
| --- | --- | --- |
| Request a move | `New`, `Apply` | `RelocateStock` — `POST /relocations` |
| Apply a completed RELOCATE task | `New`, `Apply` | `ApplyRelocationTask` — Kafka `TaskCompleted` (`task_type=RELOCATE`, additive `relocation_ref`), consumer `task-completed-relocate` ([ADR 0039](/docs/adr/0039)) |
| Apply a completed REPLENISH task | `New`, `Apply` | `ApplyReplenishTask` — Kafka `TaskCompleted` (`task_type=REPLENISH`, required `request_ref`), consumer `task-completed-replenish` ([ADR 0041](/docs/adr/0041)) |
| Read | — | `GetRelocation` — `GET /relocations/{id}`, MCP `get_relocation` |

### 7. Created Events

| Event | Full CloudEvents `type` | Topic | Raised by |
| --- | --- | --- | --- |
| StockRelocated | `com.warehouse.wms.inventory-storage.stock.StockRelocated` | `warehouse.inventory.events`, keyed by relocation id | `Relocation.Event()` from the applied relocation, published by `RelocateStock` |

`RelocateStock` also raises two `BinStockLevelChanged` (source then
destination, ADR 0040) from the same transaction. Known consumers of
`StockRelocated`: slotting-optimization (slot-plan execution progress;
acts only on `SLOTTING`) and pick-face-replenishment (closes a request
when `reason=REPLENISHMENT` and `source_ref` is its request id).

### 8. Throughput (estimate)

Low to moderate: one relocation per executed slot-plan move, replenishment
or manual re-slot — far below reservation traffic, but each move writes
both bins, every source unit it draws from and the new destination unit in
one transaction.

### 9. Size (estimate)

Small: the spec plus, once applied, one leg per contributing source unit
(usually one) and the destination unit id. Rows are never deleted; the
record is the audit trail of every intra-site move.

---

## Read models and projections (not aggregates)

| Read model | Where | Built from |
| --- | --- | --- |
| Usable inventory per SKU | `usecases.GetUsable` → `UsableInventory` | Σ `StockUnit.Usable()` at read time — never stored |
| Bin occupancy (MCP `get_bin_occupancy`) | `inbound/mcp` → `toBinOccupancy` | `StockRepo.FindByBin` at read time |
| Bin capacity view | `usecases.GetBin` | the `Bin` aggregate, read-only |
| Reservations by demand ref | `usecases.GetReservationsByDemandRef` | `ReservationRepo.FindByDemandRef` (with lazy expiry) |
| Facility location cache | `outbound/facilitycache.Consumer` | `warehouse.facility.events`, in memory, rebuilt on every start |
| Inventory Flow & Accuracy report | `internal/analytics/report`, table `flow_accuracy_rollup` | `warehouse.inventory.analytics`, written only by `cmd/inventory-projector` |

## Value objects (`internal/domain/shared`)

| Type | Rule | Errors |
| --- | --- | --- |
| `SKU` | Non-empty | `ErrEmptySKU` |
| `BinId` | Non-empty | `ErrEmptyBinID` |
| `Quantity` | Non-negative; every arithmetic operation that would go negative errors rather than clamping. `NewPositiveQuantity` also rejects zero. | `ErrNegativeQuantity`, `ErrZeroQuantity` |

`Quantity.Sub` is the workhorse of the "no negative usable" rule and is
where boundary tests were added during mutation testing.

## The four named invariants

`CLAUDE.md` singles out four as the Definition of Done for the context:

| Invariant | Domain test | Use-case test |
| --- | --- | --- |
| Bin-capacity rejection | `TestBin_Occupy_ExceedsCapacity_Rejected` | `TestStowStock_ExceedsBinCapacity_Rejected` |
| Stow requires item + location | `TestNewStockUnit_RequiresSKU` / `_RequiresBin` | — |
| Reservation ≤ usable | `TestStockUnit_Reserve_ExceedsUsable_Rejected` | `TestReserveStock_ExceedsUsable_Rejected` |
| Revoke returns to usable | `TestStockUnit_ReleaseReservation_ReturnsToUsable` | `TestRevokeReservation_ReturnsQuantityToUsable` |

All four are also covered as black-box Gherkin scenarios under `features/`,
driven through the real HTTP router. See the
[class diagram](./class-diagram.md) for the types and the
[sequence diagrams](./sequence-diagrams.md) for how the use cases drive these
aggregates.
