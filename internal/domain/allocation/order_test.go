package allocation_test

import (
	"strings"
	"testing"

	"github.com/claudioed/inventory-storage/internal/domain/allocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// u builds a stowed unit of SKU-1 on-hand qty with reserved already bound,
// so its usable quantity is qty - reserved.
func u(t *testing.T, id, bin string, qty, reserved int) *stock.StockUnit {
	t.Helper()
	return stock.RehydrateStockUnit(id, shared.SKU("SKU-1"), shared.BinId(bin),
		shared.Quantity(qty), shared.Quantity(reserved), stock.StateAvailable, 1)
}

func ids(units []*stock.StockUnit) string {
	out := make([]string, 0, len(units))
	for _, x := range units {
		out = append(out, x.ID())
	}
	return strings.Join(out, ",")
}

// The ordering rule as a table: forward before reserve; then smaller usable
// first; then bin id ascending; then stock unit id ascending.
func TestOrder_Table(t *testing.T) {
	fwd := allocation.NewForwardFaces(shared.BinId("F-1"), shared.BinId("F-2"))
	cases := []struct {
		name  string
		faces allocation.ForwardFaces
		units func(t *testing.T) []*stock.StockUnit
		want  string
	}{
		{
			name:  "forward bin first even when the reserve bin is listed first and is smaller",
			faces: fwd,
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "r", "R-1", 5, 0), u(t, "f", "F-1", 50, 0)}
			},
			want: "f,r",
		},
		{
			name:  "forward bin first even when the reserve bin id sorts lower",
			faces: fwd,
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "r", "A-0", 10, 0), u(t, "f", "F-1", 10, 0)}
			},
			want: "f,r",
		},
		{
			name:  "within reserve the smaller usable is drawn first",
			faces: fwd,
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "big", "R-1", 40, 0), u(t, "small", "R-2", 4, 0), u(t, "mid", "R-3", 12, 0)}
			},
			want: "small,mid,big",
		},
		{
			name:  "usable, not on-hand, decides: a big bin that is mostly reserved counts as small",
			faces: fwd,
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "plain", "R-1", 10, 0), u(t, "mostly-reserved", "R-2", 100, 95)}
			},
			want: "mostly-reserved,plain",
		},
		{
			name:  "within forward the smaller usable is drawn first",
			faces: fwd,
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "f1", "F-1", 20, 0), u(t, "f2", "F-2", 8, 0), u(t, "r", "R-1", 1, 0)}
			},
			want: "f2,f1,r",
		},
		{
			name:  "equal usable falls back to bin id ascending",
			faces: fwd,
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "z", "R-9", 10, 0), u(t, "a", "R-2", 10, 0), u(t, "m", "R-5", 10, 0)}
			},
			want: "a,m,z",
		},
		{
			name:  "same bin and same usable falls back to stock unit id ascending",
			faces: fwd,
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "su-2", "R-1", 10, 0), u(t, "su-1", "R-1", 10, 0)}
			},
			want: "su-1,su-2",
		},
		{
			name:  "no forward faces known: every bin is reserve and the tie-breakers alone order the units",
			faces: allocation.ForwardFaces{},
			units: func(t *testing.T) []*stock.StockUnit {
				return []*stock.StockUnit{u(t, "f", "F-1", 50, 0), u(t, "r", "R-1", 5, 0)}
			},
			want: "r,f",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := allocation.Order(tc.units(t), tc.faces)
			if ids(got) != tc.want {
				t.Fatalf("order = %s, want %s", ids(got), tc.want)
			}
		})
	}
}

// Determinism: any permutation of the input yields one and the same order.
func TestOrder_IsIndependentOfInputOrder(t *testing.T) {
	faces := allocation.NewForwardFaces(shared.BinId("F-1"))
	base := []*stock.StockUnit{
		u(t, "a", "F-1", 10, 0), u(t, "b", "R-1", 10, 0), u(t, "c", "R-1", 10, 0), u(t, "d", "R-2", 3, 0), u(t, "e", "F-1", 10, 0),
	}
	want := ids(allocation.Order(base, faces))
	for _, p := range [][]int{{4, 3, 2, 1, 0}, {2, 0, 4, 1, 3}, {1, 2, 3, 4, 0}, {3, 4, 0, 2, 1}} {
		shuffled := make([]*stock.StockUnit, 0, len(base))
		for _, i := range p {
			shuffled = append(shuffled, base[i])
		}
		if got := ids(allocation.Order(shuffled, faces)); got != want {
			t.Fatalf("permutation %v ordered %s, want %s", p, got, want)
		}
	}
}

func TestOrder_DoesNotModifyItsInput(t *testing.T) {
	in := []*stock.StockUnit{u(t, "r", "R-1", 5, 0), u(t, "f", "F-1", 50, 0)}
	allocation.Order(in, allocation.NewForwardFaces(shared.BinId("F-1")))
	if ids(in) != "r,f" {
		t.Fatalf("input was reordered: %s", ids(in))
	}
}

func TestForwardFaces_RoleOf(t *testing.T) {
	faces := allocation.NewForwardFaces(shared.BinId("F-1"))
	if got := faces.RoleOf(shared.BinId("F-1")); got != allocation.RoleForward {
		t.Fatalf("F-1 role = %s", got)
	}
	if got := faces.RoleOf(shared.BinId("R-1")); got != allocation.RoleReserve {
		t.Fatalf("R-1 role = %s", got)
	}
}
