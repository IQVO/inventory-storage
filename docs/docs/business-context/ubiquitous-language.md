---
title: Ubiquitous Language
sidebar_label: Ubiquitous Language
sidebar_position: 1
description: The exact vocabulary of the Inventory & Storage bounded context, with definitions and where each term lives in code.
---

# Ubiquitous Language

These are the terms this bounded context uses, with the definitions it uses
them with. They are not synonyms for each other and they are not
interchangeable with the same English words used in the WES-tier contexts.

## Core terms

### StockUnit

A quantity of a SKU at a **specific bin**. Every physical item has exactly one
known bin OR is flagged `Unlocated` (lost). This is the core rule of the
context.

A `StockUnit` is an aggregate root, not a row of a "stock levels" table. It
carries its own quantity, its own reserved portion, and its own lifecycle
state. Total stock for a SKU is a *sum across* `StockUnit`s, and there is
deliberately no single "SKU balance" aggregate to contend on.

*Code:* `internal/domain/stock.StockUnit`

### ProductClassification

SKU-level master data — independent of any `StockUnit` or `Bin` — describing
how an item must be handled. Carries a **closed** set of `HandlingTag`s
(`Hazmat`, `Fragile`, `TemperatureSensitive`, `Oversized`, `HighValue`) and,
only when `TemperatureSensitive` is present, a required `TemperatureClass`
(`Ambient`/`Chilled`/`Frozen`). It may also carry an **optional**
`DOTHazardClass` (1-9, top-level US DOT hazard class only), meaningful only
when `Hazmat` is present — but unlike `TemperatureClass`, never *required*
by `Hazmat`, so SKUs classified as `Hazmat` before this field existed
continue to validate unchanged. This service is the **source of truth** for
this classification — WMS-tier master data, not derived from or shared with
any other bounded context.

Unlike `location.LocationType` (an open tag list on a Zone), `HandlingTag` is
deliberately a **closed enum**: the taxonomy is fixed business vocabulary.

`TemperatureClass` here is a small, deliberate duplication of
facility-layout's own `Zone.TemperatureClass` concept — bounded contexts do
not share Go types across repository boundaries; each side names and
validates the concept in its own ubiquitous language. See
[ADR 0009](/docs/adr/0009-product-classification-as-sku-master-data).

`DOTHazardClass` grounds a real regulation — 49 CFR §177.848 — with four
documented study-project simplifications (division/zone collapse to
top-level class, `X`/`O` both block same-bin storage, Class 1 is maximally
restrictive, Class 9 is broadly compatible). See
[ADR 0010](/docs/adr/0010-dot-hazard-class-and-same-bin-segregation).

*Code:* `internal/domain/product.ProductClassification`

### Bin / Location

A **coded slot**. Under chaotic storage, any SKU may occupy any free bin; the
only hard constraint is that the sum of stock held in a bin must not exceed its
capacity. A full bin rejects a stow.

Note the deliberate narrowness: a `Bin` here is an id, a capacity and an
occupancy. It knows nothing about aisles, zones, travel distance or
temperature class — that structure belongs to the `facility-layout` bounded
context. Bins are brought under management by **bin registration**
(`PUT /bins/{binId}`, ADR 0025).

*Code:* `internal/domain/location.Bin` (package `location`, type `Bin` — the
glossary's "Location" has no type of its own)

### Stow

Placing inbound stock into a bin. A stow is **invalid without BOTH an
item-scan and a location-scan** — this is precisely how inventory gets lost if
skipped. `StowStock` is the only operation that brings a `StockUnit` into
existence.

*Code:* `internal/application/usecases.StowStock`, `stock.NewStockUnit`

### Usable inventory

Stock **immediately available to fulfil**: on-hand minus active reservations
minus held/damaged/unlocated stock.

Usable — not total — is what constrains release, and this context exposes it
explicitly rather than making callers derive it. A `StockUnit` in state
`UNLOCATED` or `REMOVED` contributes zero usable quantity regardless of its
recorded quantity.

*Code:* `stock.StockUnit.Usable()`, `usecases.GetUsable`

### Reservation

A **revocable** binding of a quantity to demand, with a timeout.

Physical delivery can fail — pod blocked, tote lost, chute jam, short pick — so
a reservation must be releasable and re-allocatable against a different
holding. A `Reservation` records `Allocation`s (which `StockUnit`s it drew
from, and how much from each) so a revoke returns exactly that quantity to
exactly those units; but nothing binds a *future* reservation to the same
physical holding.

*Code:* `internal/domain/reservation.Reservation`

### Allocation

A line on a `Reservation` recording that `n` units were drawn from a specific
`StockUnit`, and the bin that unit sits in — the **pick location**
(`BinID`, ADR 0025; a `StockUnit` never changes bin, so it is stable). It
exists so revoke and confirm-pick are exact and so a picker knows where to
go. It is an internal part of the `Reservation` aggregate, not an aggregate
of its own.

*Code:* `reservation.Allocation`

### Cycle count

Verifying a bin's contents against system records and reconciling
discrepancies. A shortfall (counted below system) flags stock `Unlocated`; an
overage is reported as a discrepancy for a separate receiving/audit process,
because reconciling *upward* means goods entered the building without being
received, which is not this operation's job to invent.

*Code:* `usecases.RunCycleCount`

### Unlocated

The explicit "lost" state: the physical item exists somewhere, but the system
no longer knows where. It is a first-class state precisely so that loss is
never silent, and so that lost stock stops counting toward usable immediately.

*Code:* `stock.StateUnlocated`

### Demand reference (`demandRef`)

An opaque string identifying *what* a reservation is for — typically an order
or work-unit reference from an upstream context. This service stores and
echoes it; it never parses it or looks it up. That opacity is the
Anti-Corruption boundary: order semantics do not leak into the inventory model.

## Value objects

| Term | Rule |
| --- | --- |
| `SKU` | Non-empty string identifying a stock keeping unit. Empty is rejected at construction (`ErrEmptySKU`). |
| `BinId` | Non-empty string identifying a coded slot (`ErrEmptyBinID`). |
| `Quantity` | A **non-negative** count. Every operation that would drive it negative returns an error rather than clamping — "no negative usable" is enforced at construction *and* on every arithmetic operation. `NewPositiveQuantity` additionally rejects zero, for requests like "reserve nothing." |
| `HandlingTag` | A closed enum: `Hazmat`, `Fragile`, `TemperatureSensitive`, `Oversized`, `HighValue`. `ParseHandlingTag` rejects anything else (`ErrUnknownHandlingTag`). |
| `TemperatureClass` | A closed enum: `Ambient`, `Chilled`, `Frozen`. Required and non-empty iff the classification carries `TemperatureSensitive`; empty otherwise. |
| `DOTHazardClass` | An `int` 1-9 (top-level US DOT hazard class, not a closed enum of named constants). `ParseDOTHazardClass` rejects anything outside 1-9. Meaningful/settable only when `HandlingTags` contains `Hazmat`, but never required by it — zero (`DOTHazardClassUnspecified`) is valid even on a Hazmat classification. See [ADR 0010](/docs/adr/0010-dot-hazard-class-and-same-bin-segregation). |

## States

**`StockUnit.State`**

| State | Meaning |
| --- | --- |
| `AVAILABLE` | On-hand, stowed, no active reservation. |
| `RESERVED` | At least part of the quantity is bound to demand. |
| `PICKED` | Part of the quantity was physically removed; some remains at the bin. |
| `REMOVED` | Quantity reached zero — fully picked out. |
| `UNLOCATED` | A cycle count could not account for this quantity. |

**`Reservation.Status`**

| Status | Meaning |
| --- | --- |
| `ACTIVE` | Live and holding quantity out of usable. |
| `CONFIRMED` | Consumed by a successful pick. |
| `REVOKED` | Cancelled; quantity returned to usable. |
| `EXPIRED` | Timed out before confirmation; quantity returned to usable. |

`ACTIVE` is the only status from which a transition is legal. Attempting to
revoke, confirm or expire an already-resolved reservation returns
`ErrAlreadyResolved` — that is the no-double-consume rule.

## Further terms (process and integration)

| Term | Definition | Code identifier |
| --- | --- | --- |
| **Receive / staged receipt** | Acknowledging inbound goods for a SKU before they are located. Not persisted as an aggregate; the ledger starts at stow. | `usecases.ReceiveStock`, `usecases.StagedReceipt` |
| **Pick location** | The bin an allocation draws from — where the picker goes. | `reservation.Allocation.BinID` (JSON `allocations[].binId`) |
| **Confirm pick** | The accounting consequence of a successful physical pick: consumes the reservation, removes on-hand quantity, frees bin capacity. | `usecases.ConfirmPick`, `stock.StockUnit.Pick`, `location.Bin.Release` |
| **Revoke** | Cancelling an `ACTIVE` reservation; its quantity returns to usable. | `usecases.RevokeReservation`, `reservation.Reservation.Revoke` |
| **Lazy expiry** | A timed-out `ACTIVE` reservation is expired when it is next read, not by a timer. | `usecases.expireIfDue` / `expireAllIfDue`, `reservation.Reservation.Expire` |
| **Replay guard** | A `ReserveStock` retry with the same `demandRef`, SKU and quantity returns the existing `ACTIVE` reservation. | `ReserveStock.activeReservationFor`, `isReplayOf` |
| **Bin registration** | Declaratively creating or resizing a bin to a stated capacity. | `usecases.RegisterBin`, `location.Bin.Resize`, `RegisterBinOutcome` |
| **Placement rule** | Hazmat SKUs need a hazmat-rated zone; temperature-sensitive SKUs need a matching zone temperature class. | `StowStock.checkPlacement`, `product.SlotAttributes` |
| **Slot attributes** | This context's view of a facility-layout slot's zone: hazmat flag, temperature class, and whether it is known at all. | `product.SlotAttributes`, `ports.LocationClassificationLookup` |
| **Same-bin segregation** | Two SKUs whose DOT hazard classes are incompatible may not share a bin. | `StowStock.checkSegregation`, `product.Incompatible` |
| **Discrepancy** | A cycle count whose counted quantity differs from the system quantity. | `shared.DiscrepancyDetected`, `CycleCountResult.Discrepancy` |
| **Idempotency key** | Caller-supplied header that makes `POST /stock/receive` and `POST /reservations` safe to retry. | `RequireIdempotencyKey` in `internal/adapters/inbound/http`, table `idempotency_keys` |
| **Flow & Accuracy report** | Hourly per-SKU/bin rollup of flow and accuracy counters — the analytical data product. | `internal/analytics/report`, table `flow_accuracy_rollup` |

### Where the code name differs from the spoken term

| Spoken term | Code name | Note |
| --- | --- | --- |
| Location | `location.Bin`, `shared.BinId` | "Location" survives only as the package name and in `LocationRepo` / `LocationRecorded`. |
| Usable inventory | `usecases.UsableInventory`, `StockUnit.Usable()` | The read model is computed on read, not stored. |
| Cycle count | `usecases.RunCycleCount`, `CycleCountCompleted` | The command is "run"; the fact is "completed". |
| Demand reference | `DemandRef` (Go), `demandRef` (REST), `demand_ref` (event `data`) | Three casings of one term — the wire contracts are frozen. |
| Lost / unlocated | `stock.StateUnlocated`, `ItemUnlocated` | "Lost" appears only in comments; every identifier says Unlocated. |
| Hazard class | `product.DOTHazardClass` | Always the US DOT top-level class 1-9. |
| Classification | `product.ProductClassification` | Never the zone classification, which is `SlotAttributes`. |

## Words that mean something different elsewhere

`warehouse-systems-ddd.md` calls this trap out explicitly, and it applies
directly here:

| Word | Here (WMS tier) | Elsewhere |
| --- | --- | --- |
| **Task** | *not used* — this context has no task model at all | `fulfillment-execution`: an execution-level unit bound to a worker at a moment, largely disposable once complete |
| **Reservation / Allocation** | a revocable claim on usable quantity, owned here | WES tier only *observes* the effect via events; it never holds one |
| **Location** | a coded bin with a capacity | `facility-layout`: a `LocationSlot` in a Site→Area→Zone→Aisle→Bay→Level→Position hierarchy |
| **Pick** | `ConfirmPick` — the *accounting* consequence of a pick | `fulfillment-execution`: the physical task lifecycle, claim/lease/complete |
| **TemperatureClass** | a value on this service's own `ProductClassification`, required only for `TemperatureSensitive` SKUs | `facility-layout`: a value on `Zone.TemperatureClass` describing what a physical zone can hold — same name and meaning, deliberately duplicated rather than shared (ADR 0009) |

Do not share a DTO or type across those boundaries. Translate at the
Anti-Corruption Layer instead.
