package shared

import "time"

// StockHeld: a received quantity was held out of usable stock (ADR 0052).
// Reason is the closed hold reason (DAMAGED, QC_PENDING, SUSPECT, OTHER).
// The occurrence time is the held_at instant.
type StockHeld struct {
	base
	HeldUnitID string
	SKU        SKU
	Quantity   Quantity
	Reason     string
	ReceiptID  string
	ASNNumber  string
	LineNo     int
	OwnerRef   OwnerRef
	SSCC       string
}

func NewStockHeld(heldAt time.Time, heldUnitID string, sku SKU, qty Quantity, reason, receiptID, asnNumber string, lineNo int, owner OwnerRef, sscc string) StockHeld {
	return StockHeld{
		base: newBase("StockHeld", heldAt), HeldUnitID: heldUnitID, SKU: sku, Quantity: qty, Reason: reason,
		ReceiptID: receiptID, ASNNumber: asnNumber, LineNo: lineNo, OwnerRef: owner, SSCC: sscc,
	}
}

// StockReleased: a held quantity went back to normal stock, as a fresh staged
// receipt (ADR 0052). The occurrence time is the decision instant.
type StockReleased struct {
	base
	HeldUnitID string
	SKU        SKU
	Quantity   Quantity
	ReceiptID  string
	ASNNumber  string
	LineNo     int
}

func NewStockReleased(releasedAt time.Time, heldUnitID string, sku SKU, qty Quantity, receiptID, asnNumber string, lineNo int) StockReleased {
	return StockReleased{
		base: newBase("StockReleased", releasedAt), HeldUnitID: heldUnitID, SKU: sku, Quantity: qty,
		ReceiptID: receiptID, ASNNumber: asnNumber, LineNo: lineNo,
	}
}

// StockDispositioned: a disposition was decided for a held quantity (ADR
// 0052). Disposition is one of RELEASE, RETURN_TO_VENDOR, SCRAP, REWORK;
// Reason is the hold reason after the decision (changed only by REWORK).
type StockDispositioned struct {
	base
	HeldUnitID  string
	SKU         SKU
	Quantity    Quantity
	Disposition string
	Reason      string
	ReceiptID   string
	ASNNumber   string
	LineNo      int
}

func NewStockDispositioned(decidedAt time.Time, heldUnitID string, sku SKU, qty Quantity, disposition, reason, receiptID, asnNumber string, lineNo int) StockDispositioned {
	return StockDispositioned{
		base: newBase("StockDispositioned", decidedAt), HeldUnitID: heldUnitID, SKU: sku, Quantity: qty,
		Disposition: disposition, Reason: reason, ReceiptID: receiptID, ASNNumber: asnNumber, LineNo: lineNo,
	}
}
