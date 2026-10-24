---
id: 0047-bin-handling-unit-declared-event
slug: /adr/0047
title: "ADR 0047: The bin handling unit arrives on LocationSlotRegistered and LocationSlotHandlingUnitDeclared (amends 0045 section 4)"
sidebar_label: "47. Bin handling unit events (amends 0045 s4)"
sidebar_position: 47
description: "ADR 0047 — amends ADR 0045 section 4: facility-layout now publishes the bin handling unit (optional handlingUnit on LocationSlotRegistered, new LocationSlotHandlingUnitDeclared event); the facility cache reads both, so a sealed case can finally be stowed. The fail-closed rule is unchanged."
---

# 0047. The bin handling unit arrives on two facility-layout events

## Status

Accepted (2026-10-24). Amends [ADR 0045](./0045-container-lpn-sealed-cases-breakcase-owner-segregated-stock.md)
section 4 only; the rest of 0045 stands. ADR 0045 is history and is not edited.

## Context

ADR 0045 section 4 made `StowContainer` refuse a sealed case unless the target
bin's `handlingUnit` is `CASE` or `PALLET`, and said the value would come from
facility-layout "once it ships the attribute". The facilitycache consumer read
an optional `handlingUnit` string from `LocationSlotRegistered`. facility-layout
published no such field, so a live run showed every stow of a case answered
`409 bin-handling-unit-mismatch`: no bin could take a case.

facility-layout now ships it (its ADR 0040, additive):

- `LocationSlotRegistered` gains an optional `handlingUnit`
  (`EACH | CASE | PALLET`), **omitted** when none was recorded.
- A new `com.warehouse.wms.facility-layout.locationslot.LocationSlotHandlingUnitDeclared`
  with `data` `{locationCode, handlingUnit, declaredAt}` announces a value set
  or changed later by `PUT /locations/{locationCode}/handling-unit`.

Both use facility-layout's camelCase (`locationCode`, `handlingUnit`), which the
existing consumer tag `handlingUnit` already matched, so no field was renamed.

## Decision

1. `internal/adapters/outbound/facilitycache/consumer.go` dispatches the new
   full type on the same topic and the same per-process consumer group (no new
   consumer, group, env var or chart value). It sets the bin's entry in its
   event-fed copy: an idempotent last-write-wins upsert keyed by
   `locationCode`, tolerant of unknown fields.
2. Section 4 of ADR 0045 is otherwise unchanged and still fail-closed: a bin
   with no recorded unit (never declared, an unrecognised value, or not yet
   replayed) is not case-compatible, so `StowContainer` answers `409
   bin-handling-unit-mismatch`. A `LocationSlotRegistered` that carries no unit
   still clears a previously held value; `LocationSlotDecommissioned` still
   forgets the bin.
3. An event with an empty `locationCode` is dead-lettered like any malformed
   facility event. An unrecognised unit value is not an error: it reads as "none
   recorded" (fail closed).

## Consequences

- Existing bins have no handling unit until facility-layout declares one, so an
  operator must declare the case and pallet bins (one `PUT` each there) before
  sealed cases can be stowed in them; loose-each stow is unaffected.
- Declaration order is safe: both events share the `locationCode` partition key
  in facility-layout, and the cache is replayed from the first offset on every
  start, so a restart reproduces the same last-write-wins result.
- Tested: unit tests on the dispatch (set, change, idempotent redelivery, unknown
  fields, bad value, missing code, decommission), a Kafka testcontainers test on
  the cache, and the real-Postgres/Kafka end-to-end run (`cmd/inventory`), where
  a stow into a bin without a unit is `409` and, after the declaration event,
  `200`.
