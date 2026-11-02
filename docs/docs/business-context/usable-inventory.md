---
title: Usable Inventory
sidebar_label: Usable Inventory
description: The read model that constrains release — on-hand minus active reservations minus held/unlocated stock.
---

# Usable Inventory

> **Usable inventory** — stock immediately available to fulfil (on-hand minus
> active reservations minus held/damaged). Usable, not total, is what
> constrains release. Expose this explicitly.

The last sentence is a design instruction, and this context follows it
literally: `GET /inventory/{sku}/usable` is a first-class endpoint, and
`Usable()` is a method on the aggregate rather than a calculation each caller
reinvents.

## The formula

```text
usable(SKU) = Σ over StockUnits of SKU:
                0                          if state ∈ {UNLOCATED, REMOVED}
                quantity − reserved        otherwise
```

In code:

```go
// internal/domain/stock/stock_unit.go
func (u *StockUnit) Usable() shared.Quantity {
	if u.state == StateUnlocated || u.state == StateRemoved {
		return 0
	}
	usable, err := u.quantity.Sub(u.reserved)
	if err != nil {
		return 0
	}
	return usable
}
```

Two details worth noticing:

- **`UNLOCATED` contributes zero regardless of recorded quantity.** A lost unit
  still has a quantity on record — that number matters for reconciliation and
  audit — but it must never be promised to a customer. Excluding it here is
  what makes "lost" mean something operationally rather than just being a flag.
- **The `Sub` error path returns 0, not a negative number.** `Quantity.Sub`
  refuses to go negative by design; if persisted state were ever inconsistent
  (`reserved > quantity`), this degrades to "promise nothing" rather than
  propagating a negative into a sum. Under-promising is recoverable;
  over-promising is not.

## Why on-hand is the wrong number

On-hand answers "how many units are in the building." Release needs to know
"how many units can I commit to a *new* demand right now," and those differ by
everything already spoken for or unavailable:

| Excluded from usable | Why |
| --- | --- |
| Active reservations | Already bound to other demand. Counting them twice over-promises. |
| `UNLOCATED` stock | Physically present somewhere, but not findable. Promising it strands an order. |
| `REMOVED` stock | Already picked out; quantity is zero anyway. |

## No SKU balance aggregate

There is deliberately no `SkuBalance` aggregate holding a running total.
Usable is a **projection**, computed by summing `StockUnit`s:

```go
// internal/application/usecases/get_usable.go
units, err := uc.Stock.FindBySKU(ctx, sku)
total := shared.Quantity(0)
for _, unit := range units {
	total = total.Add(unit.Usable())
}
```

That is a conscious trade. A cached per-SKU counter would read faster, but it
becomes a contention point every stow, reserve, revoke and pick has to update,
and it introduces a second place where the truth lives — which under chaotic
storage is exactly the kind of divergence that loses inventory. The
`StockUnit`s *are* the ledger; usable is derived from them, so it cannot drift.

The same reasoning is why `ReserveStock` re-derives total usable at reserve
time rather than trusting a cached figure:

```go
if qty.GreaterThan(totalUsable) {
	return nil, ErrInsufficientUsable   // → 409 Conflict
}
```

**Reserved quantity ≤ usable quantity at reserve time** is one of the four
named invariants of the context, and it is checked twice — once across the SKU
in the use case, and again per-unit inside `StockUnit.Reserve`.

## Read models are projections

`CLAUDE.md` states it as a rule for the whole context:

> Read models (usable-by-SKU, bin occupancy) are PROJECTIONS from events.

Usable-by-SKU is the projection this service exposes over HTTP. Downstream,
`wes-work-planning` builds its *own* projection —
`UsableInventoryObserved`, in its `inventoryview` package — from the
`StockReserved` / `ReservationRevoked` events on
`warehouse.inventory.events`. That is the same idea applied across a context
boundary: the WES tier gets a derived view of stock reality, and never write
access to the ledger it was derived from.

## Worked example

Starting from an empty bin `A-1-1` with capacity 20:

| Step | Action | On-hand | Reserved | Usable |
| --- | --- | --- | --- | --- |
| 1 | `POST /stock-receipts` `SKU-1` ×10 | 0 | 0 | 0 |
| 2 | `POST /stowages` `SKU-1` ×10 → `A-1-1` | 10 | 0 | **10** |
| 3 | `POST /reservations` `SKU-1` ×6 for `order-42` | 10 | 6 | **4** |
| 4a | `DELETE /reservations/{id}` (pick failed) | 10 | 0 | **10** |
| 4b | *or* `POST /reservations/{id}/pick-confirmations` | 4 | 0 | **4** |
| 5 | `POST /bins/A-1-1/cycle-counts` counted 3 (system 4) | 4 | 0 | **0** — unit marked `UNLOCATED` |

Step 1 changes nothing measurable: a receipt stages goods but creates no
`StockUnit`, because nothing has been located yet. That is why the endpoint
returns `202 Accepted` rather than `201 Created` — there is no addressable
resource to point at.

Step 5 shows the conservative shortfall rule: a unit touched by the shortfall
is marked fully `UNLOCATED` rather than split, so usable drops to 0 rather
than 3. The stock is not deleted — it is flagged, so a follow-up count or stow
can bring it back.
