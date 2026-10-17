package shared

// BinLevel is the absolute stock level of one SKU in one bin as the ledger
// sees it, plus the version that identifies it (ADR 0040). It is a value
// returned by the BinLevelLedger port and carried onto BinStockLevelChanged.
type BinLevel struct {
	// OnHand is the quantity physically in the bin: the sum over the SKU's
	// located, still-present units (not UNLOCATED, not REMOVED).
	OnHand Quantity
	// Reserved is the part of OnHand bound to active reservations.
	Reserved Quantity
	// Version is the per-(bin, sku) strictly increasing counter, 1 for the
	// first change ever recorded for the pair.
	Version int64
}
