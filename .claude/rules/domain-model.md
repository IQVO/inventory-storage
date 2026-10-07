---
paths:
  - "internal/domain/**"
  - "internal/application/**"
  - "features/**"
---

# Domain model: ubiquitous language, aggregates, events, use cases

## Ubiquitous Language (use these exact names)

- **StockUnit** — a quantity of a SKU at a specific Bin. Every physical item
  has exactly one known bin OR is flagged Unlocated (lost). This is the core
  rule.
- **Bin / Location** — a coded slot. Chaotic storage: any SKU may occupy any
  free bin; capacity must not be exceeded.
- **Stow** — placing inbound stock into a bin. INVALID without BOTH an
  item-scan and a location-scan (this is precisely how inventory gets lost
  if skipped).
- **Usable inventory** — stock immediately available to fulfil (on-hand minus
  active reservations minus held/damaged). Usable, not total, is what
  constrains release. Expose this explicitly.
- **Reservation** — a REVOCABLE binding of a quantity to demand, with a
  timeout. Physical delivery can fail (pod blocked, tote lost, chute jam,
  short pick), so a reservation must be releasable and re-allocatable
  against a different holding.
- **Cycle count** — verify a bin's contents; reconcile discrepancies; may
  flag Unlocated.
- **ProductClassification** — SKU-level master data (independent of any
  bin): a closed set of `HandlingTag`s (`Hazmat`, `Fragile`,
  `TemperatureSensitive`, `Oversized`, `HighValue`) plus a `TemperatureClass`
  (`Ambient`/`Chilled`/`Frozen`), required only when `TemperatureSensitive`
  is set. **product-master owns it (ADR-0034)**: this service keeps a
  version-guarded LOCAL COPY in `product_classifications`, fed by
  product-master's `ProductClassified` events, and never authors a
  classification. Unclassified SKUs carry no constraints (fail-open).
- **HandlingTag** — one of the five closed classification values above. Not
  an open tag set like `facility-layout`'s `LocationType` — these carry real
  regulatory/physical meaning, so the enum is deliberately closed.
- **DOTHazardClass** — an optional US DOT hazard class (1-9, grounded in
  49 CFR §177.848), meaningful only when `HandlingTag.Hazmat` is set but
  still OPTIONAL even then (nullable, for backward compat with
  already-classified hazmat SKUs that predate this field). Drives the
  same-bin segregation check below. See ADR-0010 (`docs/docs/adr/`) for the
  derived 9×9 class-level incompatibility matrix and its 4 documented
  simplification rules (division→class collapse, X-and-O both treated as
  incompatible for a single bin, Class 1 maximally restrictive, Class 9
  broadly compatible).

## Aggregates & invariants (enforce in domain, unit-tested)

- **StockUnit**: quantity >= 0; a stow requires item + location; state
  transitions Available -> Reserved -> Picked/Removed, or -> Unlocated. No
  negative usable.
- **Bin/Location**: sum(stock qty in bin) <= capacity; a full bin rejects
  stow. `Bin.Resize(capacity)` rejects capacity <= 0 (`ErrInvalidCapacity`)
  and capacity below current occupancy (`ErrCapacityBelowOccupancy`);
  resizing exactly to occupancy is allowed (ADR-0025).
- **Reservation**: reserved qty <= usable qty at reserve time; expires after
  timeout; revoke() returns quantity to usable; cannot double-consume.
- **ProductClassification**: `TemperatureSensitive` requires a non-empty,
  valid `TemperatureClass`; absence of `TemperatureSensitive` means
  `TemperatureClass` must be empty. `DOTHazardClass` (1-9) is optional even
  when `Hazmat` is set. `StowStock` enforces placement rules for classified
  SKUs by reading the target bin's zone attributes from `facility-layout`
  (Hazmat SKU requires a hazmat-rated zone; `TemperatureSensitive` SKU
  requires a matching zone `TemperatureClass`) — see ADR-0009. It ALSO
  enforces same-bin DOT segregation (ADR-0010): a hazmat SKU with a
  `DOTHazardClass` is rejected if the target bin already holds a SKU whose
  class is `Incompatible` per the 9×9 matrix — this check is purely LOCAL
  (StockRepo + ProductClassificationRepo, no cross-context call).
  **Fail-open** for unclassified SKUs, unknown/unmodeled bins, and
  unclassified occupants; **fail-closed** only when a classified SKU's zone
  lookup genuinely fails (`ErrLocationClassificationUnavailable`).
- Read models (usable-by-SKU, bin occupancy) are PROJECTIONS from events.

## Domain events (past tense)

StockReceived, ItemStowed, LocationRecorded, StockReserved,
ReservationExpired, ReservationRevoked, StockPicked, ItemUnlocated,
CycleCountCompleted, DiscrepancyDetected, ProductClassified — eleven total.
**StockReserved**, **ReservationRevoked** and the two transfer replies cross
the service boundary via Kafka — see `integration-events.md`.
**ProductClassified** is legacy since ADR-0034: no use case raises it; only
the one-shot `republish-product-classifications` backfill re-emits it
(integration topic), and it is retired at product-master ADR 0003 stage E.

## Use cases (application layer)

1. `ReceiveStock(sku, qty)` -> staged stock awaiting stow
2. `StowStock(sku, qty, binId)` -> validates item+location scan, respects
   capacity, enforces hazmat/temperature placement rules AND same-bin DOT
   segregation for classified SKUs
3. `ReserveStock(sku, qty, demandRef)` -> revocable Reservation against
   usable. Replay guard matches (demandRef, sku, qty): one demand holds one
   ACTIVE reservation per line/SKU, so a different SKU or quantity under the
   same demandRef is a new reservation, never a retry
4. `RevokeReservation(reservationId)` -> returns qty to usable
5. `ConfirmPick(reservationId)` -> consumes reservation, StockPicked. Called
   by `POST /reservations/{id}/confirm-pick` and, for a completed order, by
   `ConfirmPicksForOrder` (use case 9)
6. `GetUsable(sku)` -> usable-inventory read model
7. `RunCycleCount(binId, countedQty)` -> reconcile, may raise
   Discrepancy/Unlocated
8. `ApplyProductClassification(eventId, sku, handlingTags, temperatureClass?,
   dotHazardClass?, classificationSource, version)` -> the product-master
   consumer's use case (ADR-0034): claims the CloudEvents id in
   `processed_events` and upserts the local copy in ONE UnitOfWork, applying
   only when `version` > stored version (legacy rows are version 0). Raises
   NO domain event. Invariant violations are
   `ErrMalformedProductClassification` (deterministic, committed past).
   `ClassifyProduct` is removed; `PUT /products/{sku}/classification` is 410.
   `RepublishProductClassifications` is the one-shot stage-B backfill
   (re-emits every row as the legacy `ProductClassified` through the outbox).
9. `ConfirmPicksForOrder(eventId, taskType, orderRef)` -> the fulfillment
   `TaskCompleted` consumer's use case (ADR-0035): for a PICK with an order
   ref, claims the event id, counts the pick (`order_pick_progress`) and, on the
   order's LAST pick, confirms its ACTIVE reservations via `ConfirmPick`, all in
   ONE UnitOfWork. Earlier picks only record progress. Short picks are not
   modelled.
9. `RegisterBin(binId, capacity)` -> idempotent, declarative bin
   registration: creates an absent bin, no-ops on same capacity, resizes
   otherwise via `Bin.Resize` (rejects below occupancy). No domain event —
   local topology master data (ADR-0025)
10. `GetBin(binId)` -> bin capacity/occupancy read model

## Design notes (from README)

- **Reservation is SKU-scoped, not bin-scoped.** `ReserveStock` draws from
  whichever `StockUnit`s have usable quantity (first-fit across bins) and
  records exactly which units/quantities it drew from as `Allocation`s on
  the `Reservation`. `RevokeReservation` returns quantity to those same
  units, but because a fresh `ReserveStock` call is free to draw from any
  unit with usable quantity, a subsequent reservation can be satisfied from
  a **different physical holding** — this is what makes a reservation
  revocable without stranding an order when a specific pick fails.
  Each `Allocation` also records `BinID` — the pick location, i.e. the bin
  of the StockUnit it drew from (a StockUnit never changes bin) — so a
  picker knows where to go (ADR-0025).
- **StockUnit lifecycle**: `AVAILABLE` -> `RESERVED` (any reserved quantity
  present) -> `PICKED` (physically removed, quantity remains) or `REMOVED`
  (quantity reached zero), or -> `UNLOCATED` (cycle count could not account
  for it). `Usable = on-hand - reserved`, and is zero for `UNLOCATED` /
  `REMOVED` units.
- **ReceiveStock does not create a `StockUnit`.** A `StockUnit` requires
  both a SKU and a Bin (item-scan + location-scan) by construction — that is
  the domain's stow-requires-both invariant. Receiving stages goods
  (publishes `StockReceived`) without persisting an aggregate; the durable
  record starts at `StowStock`.
- **Cycle count shortfall** marks whichever `StockUnit`s cover the shortfall
  fully `UNLOCATED` (not split into located/lost sub-quantities), publishing
  `ItemUnlocated` per unit touched, kept simple by design. An overage is
  reported as a `DiscrepancyDetected`/`CycleCountCompleted(discrepancy=true)`
  pair for a separate receiving/audit process to reconcile.

## Reservation expiry is lazy: no background sweeper

`Reservation.Expire()` IS driven today, but lazily, not on a schedule:
`expireIfDue` / `expireAllIfDue` (`usecases/reservation_expiry.go`) run on
every read of a reservation — `GetReservationsByDemandRef`, `ReserveStock`'s
replay-guard lookup, `RevokeReservation` and `ConfirmPick`. A timed-out
`ACTIVE` reservation found there is, in one UnitOfWork scope, released back
to usable (`releaseAllocations`), transitioned to `EXPIRED`, saved, and
`ReservationExpired` is published (analytics topic only).

- `Reservation.Confirm(now)` still returns `ErrExpired` past `expiresAt` as a
  second line of defence;
- a timed-out reservation that nobody ever reads again stays `ACTIVE` in
  storage and keeps holding quantity out of usable until it is read or
  revoked — the remaining, documented gap. The housekeeping sweeper
  (ADR-0026) only prunes `idempotency_keys` and published `outbox_events`;
  it does not touch reservations.
