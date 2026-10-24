package stock

// PackState says whether a StockUnit holds loose eaches or one sealed case
// (ADR 0045). A unit with no recorded pack state is LOOSE, so every row that
// predates containers keeps its meaning.
type PackState string

const (
	// PackLoose: ordinary eaches, reservable by quantity.
	PackLoose PackState = "LOOSE"
	// PackSealedCase: the eaches of one unopened case, identified by a
	// Container. Reserved only as a whole case (ReserveFullCases) and never
	// split by a quantity reservation or a relocation.
	PackSealedCase PackState = "SEALED_CASE"
)

// ParsePackState reads a persisted value; "" reads as PackLoose.
func ParsePackState(v string) PackState {
	if v == string(PackSealedCase) {
		return PackSealedCase
	}
	return PackLoose
}
