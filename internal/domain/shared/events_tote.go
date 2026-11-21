package shared

import "time"

// ToteRegistered: a reusable tote with this license plate now exists, in no
// bin and empty (ADR 0054).
type ToteRegistered struct {
	base
	LPN string
}

func NewToteRegistered(occurredAt time.Time, lpn string) ToteRegistered {
	return ToteRegistered{base: newBase("ToteRegistered", occurredAt), LPN: lpn}
}

// ToteFilled: loose eaches were stowed into a tote at its current bin. The
// stock unit holding them is StockUnitID.
type ToteFilled struct {
	base
	LPN         string
	SKU         SKU
	Quantity    Quantity
	OwnerRef    OwnerRef
	SiteID      SiteID
	BinID       BinId
	StockUnitID string
}

func NewToteFilled(occurredAt time.Time, lpn string, sku SKU, qty Quantity, owner OwnerRef, siteID SiteID, binID BinId, stockUnitID string) ToteFilled {
	return ToteFilled{base: newBase("ToteFilled", occurredAt), LPN: lpn, SKU: sku, Quantity: qty, OwnerRef: owner, SiteID: siteID, BinID: binID, StockUnitID: stockUnitID}
}

// ToteLine is one (sku, owner) share of a tote's contents, in eaches.
type ToteLine struct {
	SKU      SKU
	Quantity Quantity
	OwnerRef OwnerRef
}

// ToteStowed: a tote and all its contents now have ToBin as their logical
// location. FromBin is empty on the first placement.
type ToteStowed struct {
	base
	LPN      string
	SiteID   SiteID
	FromBin  BinId
	ToBin    BinId
	Contents []ToteLine
}

func NewToteStowed(occurredAt time.Time, lpn string, siteID SiteID, from, to BinId, contents []ToteLine) ToteStowed {
	return ToteStowed{base: newBase("ToteStowed", occurredAt), LPN: lpn, SiteID: siteID, FromBin: from, ToBin: to, Contents: contents}
}
