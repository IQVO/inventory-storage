---
id: 0025-bin-registration-endpoint-and-pick-location
slug: /adr/0025-bin-registration-endpoint-and-pick-location
title: "25. Declarative bin registration over REST, and the pick location on every reservation allocation"
sidebar_label: "25. Bin registration & pick location"
sidebar_position: 25
description: "ADR 0025: add an idempotent, declarative PUT /bins/{binId} (plus GET /bins/{binId}) so inventory control can register and resize bins over the public API instead of seeding Postgres directly, and record each reservation allocation's bin (allocations[].binId, reservation_allocations.bin_id, migration 0008) so a picker's RF gun knows where to go. No new domain or integration event; apis/asyncapi.yaml is unchanged."
---

# 25. Declarative bin registration over REST, and the pick location on every reservation allocation

## Status

Accepted. Implemented in the change that introduces this record.

## Context

The fleet is getting a warehouse-day simulator that drives every service only
through its public REST API, the way real associates do with RF guns. Two
gaps in this service blocked that.

1. **Bins could not be created over HTTP.** A `Bin` (a coded slot with a
   capacity) had to exist before `StowStock` would accept stock, but no
   endpoint created one. The README said to seed bins straight into
   Postgres, and `e2e-tests` inserted rows into `bins` directly. The
   service's own BDD suite did the same through the `LocationRepo`. Bin
   provisioning was described as "an infrastructure/seed-data concern", but
   registering and re-sizing slots is ordinary inventory-control work, and
   doing it by writing to another service's database breaks the hexagonal
   boundary (ADR 0001). It also skips the aggregate's invariants and the
   optimistic-concurrency version (ADR 0019).
2. **Reservations did not say where to pick.** `POST /reservations` and
   `GET /reservations?demandRef=` returned allocations as
   `{stockUnitId, quantity}`. The `StockUnit` knows its bin
   (`StockUnit.BinID()`), but the caller would need one extra lookup per
   stock unit to find it, and no endpoint offers that lookup. A picker's RF
   gun therefore had no pick location to display.

## Decision

### `PUT /bins/{binId}`: declarative, idempotent registration

A new `RegisterBin` use case backs `PUT /bins/{binId}` with body
`{"capacity": <int > 0>}`. The caller states the capacity it wants, and the
bin converges to that state:

| Current state | Result | Status |
| --- | --- | --- |
| No bin with this id | `location.NewBin(id, capacity)`, saved | `201 Created` + `Location: /bins/{binId}` |
| Bin exists, same capacity | no-op, **nothing written** (no version bump) | `200 OK` |
| Bin exists, different capacity ≥ occupied | `Bin.Resize(capacity)`, saved | `200 OK` |
| Bin exists, capacity < occupied | rejected, `location.ErrCapacityBelowOccupancy` | `409` `capacity-below-occupancy` |

Every response body is `{binId, capacity, occupied, available}`. The same
shape comes back from the new read endpoint `GET /bins/{binId}`, which
returns `404 bin-not-found` for an unknown bin.

- **The invariant lives in the aggregate.** `Bin.Resize` rejects a capacity
  of zero or less (`ErrInvalidCapacity`) and any capacity below current
  occupancy (`ErrCapacityBelowOccupancy`). Shrinking a bin below what it
  holds would break `sum(stock in bin) <= capacity` for stock that is
  physically in the slot. Resizing to exactly the occupancy is allowed;
  the bin simply becomes full. `Resize` never changes occupancy, which
  moves only through stow and pick.
- **Why PUT and not POST.** The caller supplies the identity: a bin id is a
  physical label on the rack. Repeating the request must be safe, because
  the simulator and real RF guns retry after a dropped response. So `PUT`
  is the natural verb, and it needs no `Idempotency-Key` middleware
  (ADR 0018 limits that to the two POSTs that create server-identified
  resources). The 201-vs-200 split matches `PUT /products/{sku}/classification`.
- **Persistence.** Writes go through the existing version-guarded
  `LocationRepo.Save` (ADR 0019), inside the optional `UnitOfWork`
  (ADR 0017). If a resize races a concurrent stow on the same bin, it gets
  `409 concurrent-modification` and does not overwrite the stow's
  occupancy.
- **Validation mapping** follows the existing 400/422 split (ADR 0005):
  - malformed JSON is `400 malformed-request-body`;
  - an omitted `capacity` is `400 capacity-required`;
  - `0` is `422 invalid-bin-capacity` (the existing `ErrInvalidCapacity`
    mapping);
  - a negative value is `422 negative-quantity`;
  - a value above int32 (the wire format and the `INTEGER` column) is
    `422 capacity-out-of-range`.
- **No domain or integration event.** Bin registration is local topology
  master data. No consumer in the fleet needs it today, and adding an event
  would widen the Kafka contract (ADR 0004) for no reader.
  `apis/asyncapi.yaml` is deliberately unchanged.

### `allocations[].binId`: the pick location

- `reservation.Allocation` gains `BinID`. `ReserveStock.allocate` captures
  it from `unit.BinID()` when it draws from a unit. A `StockUnit` never
  changes bin: it is created by a stow into one bin, and nothing moves it.
  So the bin recorded at reserve time is still the right pick location
  when the pick happens.
- Migration **`0008_reservation_allocation_bin_id`** adds a nullable
  `reservation_allocations.bin_id TEXT` and backfills it from
  `stock_units.bin_id`. Because units never move, the backfill is exact.
  The column stays nullable so that a row the backfill cannot resolve
  still reads (as an empty `BinID`) instead of failing. On upsert the
  Postgres repo writes `bin_id` with `COALESCE`, so a later status change
  can never erase it.
- The HTTP DTO exposes it as `allocations[].binId` (`omitempty`) on both
  `POST /reservations` and `GET /reservations?demandRef=`. For any
  allocation made by this version it is always present.

### Hardening found along the way

When `apis/openapi.yaml` changed, the Schemathesis contract job
(`scripts/contract-test.sh`) found that `POST /reservations` accepted an
empty `demandRef`, even though the spec says `minLength: 1`. An empty
`demandRef` is useless: it is the key that `ReserveStock`'s retry guard
and `GET /reservations?demandRef=` both look up by, and the GET handler
already rejects it as `400 missing-demand-ref`. The POST handler now
rejects it the same way.

## Consequences

**Easier**

- Callers can register, inspect and resize bins entirely over REST,
  including the simulator, `e2e-tests`, and this repo's own BDD suite
  (`features/bin_registration.feature`). Nobody needs to write to
  `bins` directly any more.
- A reservation response now answers "where do I pick?" without a second
  lookup. `GET /reservations?demandRef=` already backs the Order Lifecycle
  console, so that screen gets the pick location as well.
- The resize rule is unit-tested once, in the aggregate, and every caller
  shares it.

**Harder / accepted trade-offs**

- `PUT /bins/{binId}` cannot delete or decommission a bin. A bin that is
  no longer used just stays registered. Decommissioning would need its own
  decision about what happens to any stock still in the bin.
- There is no authorization on who may register or resize bins: the whole
  API is unauthenticated (ADR 0015). Inventory control and every other
  caller have the same reach.
- `binId` on an allocation is a snapshot taken when the reservation was
  made. If stock could ever move between bins (re-slotting, consolidation),
  the snapshot could go stale. That would need a new decision, either
  re-resolving the bin at read time or re-allocating.
- Registering bins here does not tell facility-layout about them. The two
  bin registries stay separate: facility-layout owns the physical layout
  and zone attributes (ADR 0009/0013), and this service owns capacity and
  occupancy. A bin unknown to facility-layout still fails open at stow,
  exactly as before.
