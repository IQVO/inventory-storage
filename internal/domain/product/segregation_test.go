package product

import "testing"

// TestIncompatible_TruthTable covers the segregation matrix's required
// cases: class-1-vs-everything (including a second, different class-1
// SKU — rule 3's maximal restriction), a same-class pair, a known
// incompatible cross-class pair, a known compatible cross-class pair, and
// class 9's broad compatibility (everything except class 1).
func TestIncompatible_TruthTable(t *testing.T) {
	tests := []struct {
		name string
		a, b DOTHazardClass
		want bool
	}{
		// Class 1 vs everything, including itself — rule 3.
		{"class1 vs class1 (different explosives)", 1, 1, true},
		{"class1 vs class2", 1, 2, true},
		{"class1 vs class3", 1, 3, true},
		{"class1 vs class4", 1, 4, true},
		{"class1 vs class5", 1, 5, true},
		{"class1 vs class6", 1, 6, true},
		{"class1 vs class7", 1, 7, true},
		{"class1 vs class8", 1, 8, true},
		{"class1 vs class9", 1, 9, true},

		// Same-class pair: class 2 (gases) is self-incompatible per the
		// derived matrix (2.1 flammable vs 2.3A poison gas collapse into
		// the same cell as "X").
		{"class2 vs class2 (same class, self-incompatible)", 2, 2, true},
		// Same-class pair that IS compatible: class 3 (flammable
		// liquids) has no self-conflicting division entry.
		{"class3 vs class3 (same class, compatible)", 3, 3, false},

		// Known cross-class incompatible pair: class 3 (flammable
		// liquids) vs class 6 (poison liquids) is "X" in the real table.
		{"class3 vs class6 (known incompatible cross-class)", 3, 6, true},
		{"class6 vs class3 (symmetric)", 6, 3, true},

		// Known cross-class compatible pair: class 3 (flammable liquids)
		// vs class 4 (flammable solids) has no restriction in the real
		// table.
		{"class3 vs class4 (known compatible cross-class)", 3, 4, false},
		{"class4 vs class3 (symmetric)", 4, 3, false},

		// Class 9: compatible with every class except class 1.
		{"class9 vs class2", 9, 2, false},
		{"class9 vs class3", 9, 3, false},
		{"class9 vs class4", 9, 4, false},
		{"class9 vs class5", 9, 5, false},
		{"class9 vs class6", 9, 6, false},
		{"class9 vs class7", 9, 7, false},
		{"class9 vs class8", 9, 8, false},
		{"class9 vs class9", 9, 9, false},
		{"class9 vs class1", 9, 1, true},

		// Fail-open: unspecified (zero value) is always compatible.
		{"unspecified vs class1", DOTHazardClassUnspecified, 1, false},
		{"class1 vs unspecified", 1, DOTHazardClassUnspecified, false},
		{"unspecified vs unspecified", DOTHazardClassUnspecified, DOTHazardClassUnspecified, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Incompatible(tt.a, tt.b)
			if got != tt.want {
				t.Fatalf("Incompatible(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// TestIncompatible_OutOfRangeClasses_FailSafe pins the defensive bounds
// branch: a DOTHazardClass outside 1-9 can only exist behind a Rehydrate
// (ParseDOTHazardClass rejects it at the boundary), and if one ever does
// reach Incompatible it must fail safe — report compatible rather than
// index out of range — never panic.
func TestIncompatible_OutOfRangeClasses_FailSafe(t *testing.T) {
	tests := []struct {
		name string
		a, b DOTHazardClass
	}{
		{"above range in first position", DOTHazardClass(10), 3},
		{"above range in second position", 3, DOTHazardClass(10)},
		{"negative in first position", DOTHazardClass(-1), 3},
		{"negative in second position", 3, DOTHazardClass(-1)},
		{"both out of range", DOTHazardClass(42), DOTHazardClass(-7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Incompatible(tt.a, tt.b); got {
				t.Fatalf("Incompatible(%v, %v) = true, want false (out-of-range fails safe)", tt.a, tt.b)
			}
		})
	}
}

// TestIncompatible_Symmetric verifies the derived matrix is symmetric for
// every pair in the valid 1-9 range, as required by the real regulation
// (segregation is a property of a PAIR, not directional).
func TestIncompatible_Symmetric(t *testing.T) {
	for a := DOTHazardClass(1); a <= 9; a++ {
		for b := DOTHazardClass(1); b <= 9; b++ {
			if Incompatible(a, b) != Incompatible(b, a) {
				t.Fatalf("matrix not symmetric for (%v, %v): Incompatible(a,b)=%v, Incompatible(b,a)=%v",
					a, b, Incompatible(a, b), Incompatible(b, a))
			}
		}
	}
}
