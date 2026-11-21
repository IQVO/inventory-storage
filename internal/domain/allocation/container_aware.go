package allocation

import (
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// OrderFor is Order made container-aware (ADR 0054, amending ADR 0050 for
// units held in a container only). The forward pick faces stay first,
// exactly as Order. When they do not cover qty, the first unit in Order's
// sequence after them that is in a container and whose usable quantity
// covers the whole remainder is moved to the front of the rest: a pick from
// automated storage then needs one container retrieval instead of several.
// Order sorts by usable ascending, so that unit is the smallest container
// that covers. With no such unit (or no container stock) the result is
// Order's. It does not modify units.
func OrderFor(units []*stock.StockUnit, faces ForwardFaces, qty shared.Quantity) []*stock.StockUnit {
	ordered := Order(units, faces)
	split, forward := forwardPrefix(ordered, faces)
	if !qty.GreaterThan(forward) {
		return ordered
	}
	remainder, err := qty.Sub(forward)
	if err != nil {
		return ordered
	}
	best := firstCoveringContainer(ordered, split, remainder)
	if best == -1 {
		return ordered
	}
	out := make([]*stock.StockUnit, 0, len(ordered))
	out = append(out, ordered[:split]...)
	out = append(out, ordered[best])
	out = append(out, ordered[split:best]...)
	return append(out, ordered[best+1:]...)
}

// forwardPrefix returns how many leading units are in a forward face and
// their combined usable quantity.
func forwardPrefix(ordered []*stock.StockUnit, faces ForwardFaces) (int, shared.Quantity) {
	usable := shared.Quantity(0)
	n := 0
	for n < len(ordered) && faces.RoleOf(ordered[n].BinID()) == RoleForward {
		usable = usable.Add(ordered[n].Usable())
		n++
	}
	return n, usable
}

// firstCoveringContainer returns the index of the first unit at or after from
// that is in a container and covers remainder, or -1.
func firstCoveringContainer(ordered []*stock.StockUnit, from int, remainder shared.Quantity) int {
	for i := from; i < len(ordered); i++ {
		if ordered[i].InContainer() && !remainder.GreaterThan(ordered[i].Usable()) {
			return i
		}
	}
	return -1
}
