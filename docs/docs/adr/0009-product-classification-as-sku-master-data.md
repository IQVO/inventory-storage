---
id: 0009-product-classification-as-sku-master-data
slug: /adr/0009-product-classification-as-sku-master-data
title: 9. Product classification as SKU-level master data, enforced at stow time
sidebar_label: 9. Product classification
sidebar_position: 9
description: ADR 0009 — a closed-enum ProductClassification aggregate owned by this service as source of truth, with hazmat/temperature placement rules enforced at stow time via a synchronous, fail-open/fail-closed-asymmetric read from facility-layout.
---

# 9. Product classification as SKU-level master data, enforced at stow time

## Status

Accepted. Ownership part ("this service owns `ProductClassification` as
source of truth") superseded by [ADR 0033](./0033-product-master-owns-classification.md):
product-master owns it and this service keeps a local copy; the stow-time
placement rules stay in force.

## Context

Nothing in this bounded context knew whether a SKU was hazardous, fragile,
temperature-sensitive, oversized, or high-value. `StowStock` placed any SKU
into any free bin subject only to capacity (ADR 0002's chaotic storage) — a
correct model of *space*, but silent on *handling*. In a real warehouse that
gap is a safety and compliance problem: a hazmat item stowed next to ordinary
stock, or a frozen item stowed in an ambient zone, is not a capacity failure
the existing invariants would ever catch.

Two questions had to be answered before any enforcement could be built:

1. **Who owns this classification?** It is a property of the *product*, not
   of any physical holding — a SKU is Hazmat regardless of which bin, or how
   many bins, currently hold it. That makes it master data, and
   `warehouse-systems-ddd.md`'s WMS/WES split places master data ownership in
   the WMS tier. `inventory-storage` already owns `StockUnit` (what is held
   where) and now also owns `ProductClassification` (what a SKU *is*) — two
   separate aggregates, both keyed by SKU-adjacent identity, neither derived
   from the other.

2. **Where does the placement rule actually get enforced?** The rule needs
   two facts at the moment of stow: what the SKU requires, and what the
   target bin's zone provides. This service has the first (once it owns
   classification) but not the second — `facility-layout` is the bounded
   context that models `Zone.Hazmat` and `Zone.TemperatureClass`. Enforcing
   the rule here therefore requires a cross-context read, in parallel with
   `facility-layout` adding a new
   `GET /locations/{locationCode}/classification` endpoint for exactly this
   purpose.

Constraints that shaped the answer:

- **The taxonomy is closed.** Hazmat/Fragile/TemperatureSensitive/Oversized/
  HighValue is fixed business vocabulary — unlike `location.LocationType`
  (an open tag list on a Zone), there is no operational reason to let this
  set grow ad hoc. A closed Go enum with a validating parser is the right
  shape, not an open string tag.
- **This service does not own `facility-layout`'s domain model, and never
  will.** `TemperatureClass` already exists there as `Zone.TemperatureClass`.
  Importing that package across a repository boundary would violate the
  bounded-context rule (`warehouse-systems-ddd.md`: no shared Go code across
  contexts) as badly as importing its database would.
- **A cross-service HTTP dependency changes the failure surface of
  `StowStock`.** Before this change, `StowStock`'s availability depended only
  on this service's own repos. Adding a synchronous read to another service
  means facility-layout being down can now block a stow — but only if
  handled carelessly. The existing precedent in this platform
  (`fulfillment-execution`'s documented `path_id`-prefix simplification in
  its `INTEGRATION.md`) shows the house style: make integration
  simplifications explicit rather than silently correct, and keep the
  blast radius as small as the actual risk.
- **The existing test suite, CI, and deployments must be unaffected.** The
  same requirement that shaped ADR 0004's `EVENT_PUBLISHER=kafka|log`
  pattern applies here.

## Decision

**We will add `ProductClassification` as a new aggregate this service owns
as source of truth, and enforce hazmat/temperature placement rules in
`StowStock` via a synchronous read from facility-layout, with a documented
fail-open/fail-closed asymmetry.**

1. **New domain package `internal/domain/product/`.** `HandlingTag` is a
   closed `string` enum (`Hazmat`, `Fragile`, `TemperatureSensitive`,
   `Oversized`, `HighValue`) with a validating `ParseHandlingTag`.
   `ProductClassification` is the aggregate root, keyed by `SKU`, holding a
   `HandlingTags` set and a `TemperatureClass`. Its invariant:
   `TemperatureSensitive` requires a valid, non-empty `TemperatureClass`;
   absence of `TemperatureSensitive` means `TemperatureClass` must be empty.
   Construction/replacement raises `ProductClassified(sku, tags,
   temperatureClass, occurredAt)`.

2. **A deliberate, small duplication of `TemperatureClass`.** This package
   defines its own `TemperatureClass` (`Ambient`/`Chilled`/`Frozen`) —
   identical by name and meaning to facility-layout's `Zone.TemperatureClass`,
   but a distinct Go type owned by this repository. This is the same pattern
   already in force for the shared words "Zone" and "Location" across these
   two contexts (see the ubiquitous-language "words that mean something
   different elsewhere" table): bounded contexts do not share Go types
   across repository boundaries, even when the underlying concept and its
   valid values genuinely coincide. Translating at the integration edge
   (the outbound adapter, below) is the Anti-Corruption Layer, not an
   annoyance to engineer away.

3. **New use case `ClassifyProduct`** (`ProductClassificationRepo.Save`,
   `EventPublisher`, `Clock`) registers or replaces a SKU's classification.
   It is idempotent by SKU — re-classifying an already-classified SKU
   replaces rather than errors, the same "replace, don't error" pattern
   `fulfillment-execution`'s `RegisterStation` already uses for a
   structurally identical situation (recertifying a station).

4. **New outbound port `LocationClassificationLookup`** returns a
   `product.SlotAttributes{Hazmat, TemperatureClass, Known}` for a `BinId`.
   `Known=false` means "no constraint info available for this bin, permit
   the stow" — the fail-open default for any bin facility-layout has not
   (yet) modeled.

5. **`BinId` values are treated as facility-layout `LocationCode` values
   directly.** No translation table, no lookup-of-a-lookup. This is a
   documented cross-context simplification, in the same spirit as
   `fulfillment-execution`'s `path_id`-prefix convention: both bounded
   contexts already code physical slots as short human-readable strings
   (`A-1-1`), and assuming the same string identifies the same physical
   location across the platform is a reasonable simplification for this
   round, not a permanent guarantee. The same assumption is restated where it
   is relied upon, in the `facilitylayout.Client` doc comment
   (`internal/adapters/outbound/facilitylayout/client.go`). A future divergence between the two
   contexts' coding schemes would require a real translation layer here.

6. **Two adapters behind the port, selected by `LOCATION_LOOKUP_MODE`
   (`http`|`permissive`), default `permissive`.** `facilitylayout.Client` is
   a plain `net/http` GET against `FACILITY_LAYOUT_BASE_URL +
   /locations/{locationCode}/classification`; `facilitylayout.PermissiveLookup`
   is a no-op that always returns `Known=false`. This is exactly the
   `EVENT_PUBLISHER=kafka|log` pattern from ADR 0004, applied to an inbound
   dependency instead of an outbound one, for an identical reason: existing
   tests, CI, and deployments that do not opt in see zero behavioural
   change.

7. **A 404 from facility-layout is `Known=false` (fail-open).** That
   location simply is not modeled there yet — treating it as a hard error
   would make classification enforcement a precondition for every stow in
   the system, which is a far larger blast radius than the safety problem
   being solved.

8. **Any transport or 5xx error is `ErrLocationClassificationUnavailable` —
   but StowStock only surfaces it as a stow-blocking failure when the SKU
   being stowed carries `Hazmat` or `TemperatureSensitive`.** This is the
   deliberate fail-open/fail-closed asymmetry at the heart of this decision:
   *classified, rule-relevant* SKUs fail closed (a hazmat item cannot be
   stowed blind when the safety check itself is unavailable), while every
   other SKU — unclassified, or classified with only `Fragile`/`Oversized`/
   `HighValue` — is never blocked by a facility-layout outage. The whole
   stow path's availability is not coupled to facility-layout's; only the
   availability of stowing specifically hazmat/temperature-sensitive
   inventory is.

9. **`StowStock` gains two optional, nil-safe struct fields:**
   `Classifications ports.ProductClassificationRepo` and `LocationLookup
   ports.LocationClassificationLookup`. When either is nil, `StowStock`
   behaves exactly as it did before this ADR — no placement check runs at
   all. Every pre-existing `StowStock` test (and every construction of
   `StowStock` in `features_test.go`, `server_test.go`, `cmd/mcp`) continues
   to compile and pass unmodified.

10. **New typed errors** `ErrHazmatZoneRequired` and
    `ErrTemperatureClassMismatch`, mapped to `409 Conflict` via RFC 7807
    (ADR 0005), consistent with every existing conflict-class error in
    `adapters/inbound/http/errors.go`.

11. **New REST surface:** `PUT /products/{sku}/classification` (create or
    replace; `201`/`200`) and `GET /products/{sku}/classification` (`200`/
    `404`), following the existing DTO/`writeJSON`/`writeError` conventions
    exactly.

## Consequences

### Easier

- **Real hazmat/temperature safety at stow time**, not just a capacity
  check. The two riskiest handling categories in the taxonomy are now
  actively enforced, not merely recorded.
- **Classification is a first-class, queryable resource** independent of
  physical stock — a SKU can be classified before it is ever received, and
  the classification survives every stow/pick/cycle-count cycle its
  `StockUnit`s go through.
- **Fully additive to the existing StowStock contract.** No existing test,
  deployment, or caller changes behaviour unless it opts into
  `LOCATION_LOOKUP_MODE=http` and actually classifies a SKU.
- **The permissive default keeps the blast radius contained.** A team can
  ship this ADR's code without also standing up the cross-service dependency
  — enforcement is opt-in at deploy time, exactly like Kafka publishing was.

### Harder

- **A cross-service HTTP dependency now exists on the stow path**, where
  none did before. Even scoped to classified SKUs only, this is new
  operational surface: a new client, a new timeout to tune
  (`facilitylayout.DefaultTimeout`), a new external failure mode to
  monitor.
- **The fail-open/fail-closed asymmetry is a judgment call, not a law of
  physics.** Treating an *unknown* bin as safe-to-stow (fail-open) is a
  reasonable default while facility-layout's location catalog is still
  being populated, but it means a genuinely hazmat-restricted zone that
  facility-layout simply hasn't modeled yet will not be protected. This is
  the same class of tradeoff ADR 0004 made explicit for at-least-once
  Kafka delivery: recorded here rather than silently assumed.
- **The `BinId`-as-`LocationCode` simplification is load-bearing and
  undocumented anywhere in code — only here and in this ADR.** If the two
  contexts' coding schemes ever diverge, every classified SKU's placement
  check silently starts consulting the wrong location, most likely
  surfacing as unexplained fail-opens (mismatched codes look like unknown
  bins) rather than a loud error.
- **`TemperatureClass` now exists as two distinct Go types with identical
  values across two repositories.** Anyone reading `product.TemperatureClass`
  without this ADR could reasonably assume it is the same type as
  facility-layout's `Zone.TemperatureClass` and try to share code between
  them; there is no compiler check preventing that mistake, only this
  record and the ubiquitous-language table.
- **`ClassifyProduct`'s idempotent-replace semantics have no audit trail.**
  Re-classifying a SKU (e.g. narrowing it from Hazmat to non-Hazmat) simply
  overwrites the prior classification; there is no history of what a SKU
  used to be classified as, unlike `Reservation`'s append-only
  `Allocation`s. If classification history ever becomes a compliance
  requirement, this will need revisiting.
