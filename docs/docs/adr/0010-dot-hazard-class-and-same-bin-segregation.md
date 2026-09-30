---
id: 0010-dot-hazard-class-and-same-bin-segregation
slug: /adr/0010-dot-hazard-class-and-same-bin-segregation
title: 10. Optional DOT hazard class and same-bin DOT segregation at stow time
sidebar_label: 10. DOT hazard class & segregation
sidebar_position: 10
description: ADR 0010 — an optional, class-level (1-9) US DOT hazard class on ProductClassification, and a same-bin segregation check at stow time derived from 49 CFR §177.848 with four documented study-project simplifications.
---

# 10. Optional DOT hazard class and same-bin DOT segregation at stow time

## Status

Accepted.

## Context

ADR 0009 gave this service a `Hazmat` `HandlingTag` — a boolean. That is
enough to route a hazmat SKU into a hazmat-rated zone (facility-layout's
`Zone.Hazmat`), but it cannot express the next, real-world safety fact:
**not all hazmat is compatible with other hazmat.** A warehouse that stows
an oxidizer (DOT class 5) in the same bin as a flammable solid (class 4)
has created a real fire-safety violation, and the prior round's boolean
tag has no way to notice.

The US Department of Transportation has already solved exactly this
problem for transport and storage of hazardous materials: **49 CFR
§177.848 ("Segregation of hazardous materials")** defines a
division/zone-level table describing which hazard classes may not share a
vehicle or storage facility (`X`), which must be kept a safe distance
apart (`O`), and which have no restriction (blank). This is real,
citable, load-bearing federal regulation — not an invented business rule —
and grounding this feature in it, rather than in an ad hoc compatibility
list, is what makes the resulting check defensible.

Two things stood between "the regulation exists" and "this service can
enforce it":

1. **Granularity mismatch.** §177.848(d)'s table operates at the
   *division/zone* level (1.1, 1.2, ..., 2.3 gas zone A, 2.3 gas zone B,
   4.1, 4.2, 4.3, ...) and Class 1 (explosives) additionally requires a
   *compatibility-group* table (§177.848(f), groups A-L/S) layered on
   top. Modeling that full granularity would mean carrying
   division/zone/compatibility-group as SKU-level master data — a much
   larger surface than this study project's scope, and well past what
   ADR 0009's `HandlingTag` taxonomy was designed to carry.
2. **Bin, not vehicle.** The regulation's `O` rating ("must be kept
   separated by distance") describes a spatial relationship a transport
   vehicle or a large storage facility can express (put them in different
   compartments, different aisles). A single discrete warehouse `Bin` in
   this service's model (ADR 0002's chaotic storage) has no notion of
   partial separation — an item either occupies the bin or it does not.

Given those two mismatches, doing nothing (deferring "real" DOT
enforcement to a hypothetical future division-level model) would leave
this service without ANY class-level hazmat safety check, which is worse
than a conservative approximation. The house style for this exact
tension — a citable real-world rule, simplified for a bounded, honestly
documented reason — already exists in this platform:
`fulfillment-execution`'s `path_id`-prefix convention (referenced in ADR
0009) and ADR 0009's own `BinId`-as-`LocationCode` simplification are both
"state the shortcut plainly, in the ADR and in the code, rather than
silently approximate."

## Decision

**We add an optional, class-level (1-9) `DOTHazardClass` to
`ProductClassification`, and enforce same-bin DOT segregation in
`StowStock`, using a class-level incompatibility matrix conservatively
derived from 49 CFR §177.848 under four explicit simplification rules.**

1. **New value object `product.DOTHazardClass` (`int`, 1-9), with
   `ParseDOTHazardClass` validating the range.** Unlike `HandlingTag` and
   `TemperatureClass`, this is not a closed enum of named Go constants —
   the real regulation's classes are just the integers 1 through 9, and a
   validated `int` is more honest than nine placeholder identifiers this
   domain does not otherwise name. Zero (`DOTHazardClassUnspecified`) is
   the "no class recorded" value.

2. **Invariant, mirroring `TemperatureSensitive`/`TemperatureClass`
   exactly, with one deliberate asymmetry.** `DOTHazardClass` is
   meaningful — and settable — **only** when `HandlingTags` contains
   `Hazmat`; a non-zero `DOTHazardClass` without `Hazmat` is rejected with
   `ErrDOTHazardClassNotApplicable`, the same shape as
   `ErrTemperatureClassNotApplicable`. The asymmetry: `TemperatureClass`
   is *required* once `TemperatureSensitive` is set (empty is rejected);
   `DOTHazardClass` remains **optional even when `Hazmat` is set** — the
   zero value validates. This is required for backward compatibility:
   every SKU classified as `Hazmat` under ADR 0009, before this field
   existed, has no `DOTHazardClass` on file, and re-validating those rows
   must not start failing. New Hazmat classifications are free to specify
   a class; existing ones are not retroactively forced to.

3. **New domain file `internal/domain/product/segregation.go`** carrying
   the derived 9x9 boolean incompatibility matrix as a Go-level, heavily
   commented, auditable table, and `Incompatible(a, b DOTHazardClass)
   bool`. `DOTHazardClassUnspecified` (zero) is always compatible with
   anything — fail-open, the same default posture as the rest of this
   service's classification design (ADR 0009: "unclassified SKUs carry no
   constraints").

4. **Four explicit, deliberate simplification rules** bridge the real
   §177.848 division-level table down to this service's class-level,
   single-bin model. These are stated here AND as a Go doc comment in
   `segregation.go`, so the derivation is auditable from either side:

   - **Rule 1 — collapse to class.** Every division/zone row and column
     in the real table (1.1, 1.2, ..., 2.3 zone A, 2.3 zone B, 4.1, 4.2,
     4.3, ...) collapses to its parent top-level class (1-9), turning the
     division-level table into a 9x9 class-level matrix.
   - **Rule 2 — `X` and `O` both block co-storage.** A discrete bin
     cannot express "keep these two pallets apart by a safe distance
     within the same slot" — there is no partial/graduated storage
     relationship in this domain. Both `X` (never together) and `O`
     (distance-separated) from the real table are therefore treated as
     "incompatible for same-bin storage." This is conservative by
     construction: the real regulation never requires *more* separation
     than an `O` calls for, so upgrading `O` to a hard block never
     under-protects.
   - **Rule 3 — Class 1 is maximally restrictive.** §177.848(f)'s
     Class-1 compatibility-group table (groups A-L/S) is explicitly OUT
     OF SCOPE for this round — too granular for a study project, and it
     would require tracking compatibility groups as additional SKU
     master data this service does not otherwise carry. Instead, Class 1
     is modeled as incompatible with **every** class, including a
     **different** Class-1 SKU (an unknown/different compatibility
     group). This is the single safest simplification available without
     compatibility-group data: a warehouse actually co-storing multiple
     explosives divisions/groups needs the real §177.848(f) table, not
     this matrix.
   - **Rule 4 — most-restrictive-entry-wins.** Where Rule 1's collapse
     merges multiple real table entries that disagree (some blank, some
     `O`, some `X`, across different divisions folded into the same
     class), the most restrictive value present anywhere in the
     collapsed cell decides the class-level entry (`X` beats `O` beats
     blank). Information is only ever lost in the conservative direction.

   Class 9 (miscellaneous dangerous goods) does not appear in the real
   §177.848(d) table at all — it postdates the table's structure — and is
   modeled per commercial/DOT-guidance convention as compatible with
   every class except Class 1 (Rule 3 already covers that pairing).

5. **`StowStock.Execute` gains a same-bin segregation check, AFTER the
   existing hazmat-zone/temperature-class checks from ADR 0009**, so this
   is strictly additive: nothing about the prior checks is removed or
   weakened. It only runs when `Classifications` is wired, and only when
   the incoming SKU's classification carries a non-zero `DOTHazardClass`.
   For every OTHER SKU already occupying the target bin (via the
   already-owned `ports.StockRepo.FindByBin`), it looks up that
   occupant's classification (via the already-owned
   `ports.ProductClassificationRepo.FindBySKU`) and rejects with the new
   `ErrHazmatClassIncompatible` if `product.Incompatible` reports the two
   classes as incompatible. An occupant with no classification, or a
   classification with no `DOTHazardClass` recorded, never blocks the
   stow (fail-open, consistent with `Incompatible`'s own zero-value
   behaviour).

6. **This check is entirely local to this service — no new outbound
   port, no cross-context call.** Unlike ADR 0009's hazmat/temperature
   placement check (which reads facility-layout's zone data over HTTP),
   bin occupancy and product classification are both already this
   service's own aggregates. There is no "unavailable lookup" failure
   mode to design around here, unlike ADR 0009's fail-open/fail-closed
   asymmetry.

7. **New typed error `ErrHazmatClassIncompatible`, mapped to `409
   Conflict` via RFC 7807**, in the same `usecases/errors.go` +
   `adapters/inbound/http/errors.go` pattern as `ErrHazmatZoneRequired`
   and `ErrTemperatureClassMismatch`.

8. **REST surface is unchanged in shape**: the existing `PUT
   /products/{sku}/classification` request/response gains an optional
   `dotHazardClass` field (integer 1-9, nullable/omittable). No new
   endpoints. `/stock/stow`'s existing `409` response documents the new
   `hazmat-class-incompatible` case alongside the existing ADR 0009
   cases.

9. **Migration**: a new nullable `dot_hazard_class SMALLINT` column on
   `product_classifications` (`migrations/0003_dot_hazard_class.{up,down}.sql`),
   the next-numbered golang-migrate file after ADR 0009's `0002`. NULL
   round-trips to `DOTHazardClassUnspecified`, matching the domain's
   "unspecified is the zero value" model exactly (the same non-DB-enforced
   conditional-field pattern `temperature_class`'s `NOT NULL DEFAULT ''`
   already uses for its own conditional invariant).

## Consequences

### Easier

- **Real, class-level hazmat co-storage safety at stow time**, grounded
  in a citable federal regulation rather than an invented compatibility
  list — the single biggest gap the boolean `Hazmat` tag left open after
  ADR 0009.
- **Fully additive and backward compatible.** Every SKU classified as
  `Hazmat` under ADR 0009 continues to validate with
  `DOTHazardClassUnspecified` and is never retroactively blocked from
  stowing anywhere; the segregation check only engages once a SKU is
  explicitly given a DOT class.
- **No new cross-context dependency.** Unlike ADR 0009's facility-layout
  read, this feature adds zero new failure modes to `StowStock`'s
  availability — everything it needs, this service already owns.
- **The derivation is auditable, not asserted.** `segregation.go`'s doc
  comment traces every "X" in the derived matrix back to specific real
  §177.848(d) division-pair cells, so a reviewer can check the collapse
  by hand rather than trust it blind.

### Harder

- **Explosives sub-compatibility (§177.848(f), groups A-L/S) is a known,
  deliberate gap — not modeled at all.** Rule 3's "Class 1 is
  incompatible with everything, including another Class 1" is
  conservative (it never under-protects), but it is also strictly more
  restrictive than the real regulation for two explosives that ARE
  compatible under the real compatibility-group table (e.g. two items
  both in group C). A warehouse that genuinely needs to co-store
  multiple explosives divisions/groups in one bin will find this matrix
  over-blocks and needs the real §177.848(f) table — the same class of
  gap ADR 0003 documents for lazy reservation expiry (resolved on read,
  not swept — see its "Lazy expiry" section):
  recorded here rather than silently assumed away.
- **The class-level collapse (Rules 1 and 4) necessarily loses
  precision relative to the real division-level table**, always in the
  conservative direction. A warehouse operating exactly at the boundary
  of a real `O`-vs-blank distinction within one collapsed class (e.g. two
  different Class 4 sub-divisions that the real table treats
  differently) will see this matrix block a co-storage the real
  regulation might permit with distance separation. That is the accepted
  cost of Rule 2's "no partial separation in a discrete bin" decision.
- **`DOTHazardClass`'s optionality means a Hazmat SKU with no class
  recorded is invisible to this check entirely**, exactly the same
  "unclassified SKUs carry no constraints" fail-open posture ADR 0009
  already established — but it does mean two genuinely incompatible
  hazmat SKUs, if neither has been given a DOT class, can still land in
  the same bin. This feature raises the ceiling on what safety this
  service can enforce; it does not retroactively audit or backfill
  existing classifications.
- **The matrix is a compile-time constant, not configurable.** Any
  future correction to the derivation (a mistake in the collapse, a
  regulatory amendment) requires a code change and a new PR, not a data
  migration — the same tradeoff every closed-enum decision in this
  service (ADR 0009's `HandlingTag`) already accepts.
