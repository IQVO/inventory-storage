// Package allocation holds the AllocationOrder policy: the single place that
// decides, for one SKU, in which order its StockUnits are drawn on when a
// reservation is made (ADR 0050).
//
// The policy is a pure function of the units and of the SKU's forward pick
// faces. It reads no clock, no repository and no randomness, so the same
// inputs always give the same allocation.
package allocation

import (
	"sort"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// BinRole is the role a bin plays for ONE SKU. A bin is not forward or
// reserve in itself: it is the forward pick face of the SKUs the approved slot
// plan assigns to it, and plain reserve storage for every other SKU.
type BinRole string

const (
	// RoleForward: the bin is the SKU's pick face in the site's approved slot
	// plan (slotting-optimization's SlotPlanApproved assignment).
	RoleForward BinRole = "FORWARD"
	// RoleReserve: any other bin holding the SKU (bulk, overstock, or a SKU
	// no approved plan assigns anywhere).
	RoleReserve BinRole = "RESERVE"
)

// ForwardFaces is the set of bins that are one SKU's forward pick faces. The
// zero value is the empty set: every bin is then RoleReserve and the order
// degrades to the deterministic tie-breakers only.
type ForwardFaces struct {
	bins map[shared.BinId]struct{}
}

// NewForwardFaces builds the set from bins.
func NewForwardFaces(bins ...shared.BinId) ForwardFaces {
	set := make(map[shared.BinId]struct{}, len(bins))
	for _, b := range bins {
		set[b] = struct{}{}
	}
	return ForwardFaces{bins: set}
}

// RoleOf reports the role of bin for the SKU these faces belong to.
func (f ForwardFaces) RoleOf(bin shared.BinId) BinRole {
	if _, ok := f.bins[bin]; ok {
		return RoleForward
	}
	return RoleReserve
}

// Order returns units in the order a reservation draws on them. It does not
// modify units. THE ORDERING RULE (the one place it is written down), a strict
// total order, compared in this sequence:
//
//  1. role: units in a FORWARD bin of the SKU before units in any RESERVE bin;
//  2. usable quantity ascending: the unit closest to fully consumed first, so
//     a draw empties (and frees) small holdings before it breaks a big one;
//  3. bin id ascending (byte order);
//  4. stock unit id ascending (byte order), the final tie-breaker, so two
//     units in one bin with equal usable quantity still have one order.
//
// A request larger than the first unit's usable quantity takes that unit
// whole and spills to the next unit in this same order (partial forward: the
// forward remainder first, then reserve). StockUnit carries no lot, expiry or
// receipt date today, so there is no FEFO/FIFO rule to preserve; when one is
// added it slots in between 1 and 2.
func Order(units []*stock.StockUnit, faces ForwardFaces) []*stock.StockUnit {
	ordered := make([]*stock.StockUnit, len(units))
	copy(ordered, units)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if ra, rb := faces.RoleOf(a.BinID()) == RoleForward, faces.RoleOf(b.BinID()) == RoleForward; ra != rb {
			return ra
		}
		if ua, ub := a.Usable().Int(), b.Usable().Int(); ua != ub {
			return ua < ub
		}
		if a.BinID().String() != b.BinID().String() {
			return a.BinID().String() < b.BinID().String()
		}
		return a.ID() < b.ID()
	})
	return ordered
}
