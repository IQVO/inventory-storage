---
title: Why Chaotic Storage
sidebar_label: Why Chaotic Storage
description: The reasoning behind random stow, and what it forces the software to guarantee.
---

# Why Chaotic Storage

**Chaotic (random) stow**: there is no assigned location per product. An item
goes wherever there is free space, the associate scans item + scans location,
and the system records the exact bin.

This is a **first-class domain rule, not an accident** of how the warehouse
happens to be organised. It is the single decision that shapes this bounded
context most.

## What it buys

From `fulfillment-reference-ddd.md`:

| Benefit | Why it follows from randomness |
| --- | --- |
| **Space utilisation** | Every free slot is usable by every SKU. Fixed slotting reserves space for a product whether or not it is currently in stock; chaotic stow has no reserved-but-empty space. |
| **Shorter pick paths** | A picker's next item is likely near the last, because popular SKUs end up spread everywhere rather than concentrated in one "hot" aisle. |
| **No zone congestion** | Fixed slotting concentrates activity for a fast-moving SKU on one aisle; randomness spreads it across the floor. |
| **Redundancy** | One seller's 500 units are spread across ~400 locations on multiple floors. A blocked pod, a jammed aisle, or a damaged unit takes out a *fraction* of the availability, not all of it. |

That last row is the one this codebase leans on hardest. Redundancy at the
physical level is what makes a revocable reservation *useful*: when a specific
pick fails, there is almost always a different holding of the same SKU to
re-satisfy the demand from. Chaotic storage and
[revocable reservations](./revocable-reservations.md) are two halves of one
idea.

## What it costs

Randomness moves the entire "where is it" burden into software. In a fixed-slot
warehouse, a human can find `SKU-1` by walking to the `SKU-1` shelf even if the
system is down. Under chaotic stow there is no such fallback: **if the record
is wrong, the item is lost**, even though it is physically sitting on a shelf
twenty feet away.

Hence the two hard rules the domain layer enforces.

### Rule 1 — a stow needs both scans

```go
// internal/domain/stock/stock_unit.go
func NewStockUnit(id string, sku shared.SKU, binID shared.BinId, qty shared.Quantity) (*StockUnit, error) {
	if sku == "" || binID == "" {
		return nil, ErrStowRequiresItemAndLocation
	}
	...
}
```

Item-scan without location-scan means "we know we have it, somewhere" — which
under chaotic storage is worth nothing. Location-scan without item-scan means
"something is in that bin" — worth even less. The aggregate refuses to
construct rather than record a half-truth, and the HTTP adapter maps that
refusal to `400 Bad Request` with problem type `stow-requires-item-and-location`.

### Rule 2 — capacity is real

```go
// internal/domain/location/bin.go
func (b *Bin) Occupy(qty shared.Quantity) error {
	if b.occupied.Add(qty).GreaterThan(b.capacity) {
		return ErrBinFull
	}
	...
}
```

"Any SKU may occupy any free bin" only works if "free" is accurate. If a bin
were allowed to overflow, the physical overflow would end up in a neighbouring
bin — unscanned, and therefore lost. `ErrBinFull` maps to `409 Conflict`; the
operator is expected to pick another bin, which under chaotic storage costs
nothing.

## What it explicitly does *not* mean

Chaotic stow is not "we don't care where things go." It is "we don't
*pre-assign* where things go, and in exchange we record with total precision
where they actually went."

Two things follow:

- **No SKU affinity in the model.** `Bin` has no notion of which SKU belongs in
  it — deliberately. A "preferred SKU per bin" field would be fixed slotting
  wearing a disguise.
- **Slotting policy lives elsewhere.** Rules about *which* free bin an item
  should go to (temperature class, hazmat, size) are placement rules, and they
  belong to `facility-layout`'s `PlacementRule` model, not here. This service
  accepts the bin it was told about and enforces capacity. The one exception
  is a *veto*, never a choice: for SKUs classified `Hazmat` or
  `TemperatureSensitive`, `StowStock` rejects a bin whose zone is not
  hazmat-rated or has the wrong temperature class (ADR 0009, zone data from
  facility-layout via ADR 0013), and it rejects a DOT-incompatible neighbour
  in the same bin (ADR 0010). It still never *picks* a bin.

## Cycle counting is the audit that makes it survivable

Because the record *is* reality under chaotic storage, the record has to be
audited. `RunCycleCount` compares a bin's counted quantity to its system
quantity and does one of three things:

| Case | Behaviour |
| --- | --- |
| `counted == system` | `CycleCountCompleted{discrepancy: false}` — clean. |
| `counted < system` (shortfall) | `DiscrepancyDetected`, then affected `StockUnit`s are marked `UNLOCATED` and stop counting as usable, then `CycleCountCompleted{discrepancy: true}`. |
| `counted > system` (overage) | `DiscrepancyDetected` + `CycleCountCompleted{discrepancy: true}`, but **no** automatic upward reconciliation — goods appearing without a receipt is a receiving/audit problem, and inventing stock here would corrupt the ledger. |

The shortfall path is where the "OR is flagged Unlocated" half of the core
invariant actually gets exercised. Note the deliberate simplification recorded
in the code: a unit touched by a shortfall is marked *fully* unlocated rather
than split into located and lost portions, leaving finer-grained
reconciliation to a follow-up stow or count. That is a conservative choice —
it under-reports usable rather than over-reporting it.

See [ADR 0002](/docs/adr/0002-chaotic-storage-over-fixed-slotting).
