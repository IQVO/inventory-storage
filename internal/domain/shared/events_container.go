package shared

import "time"

// ContainerRegistered: a sealed carton received on a receipt line became a
// Container (ADR 0045). It carries no site: the receipt names none, the site
// is set when the container is stowed. No quantity is booked by this event.
type ContainerRegistered struct {
	base
	LPN          string
	SKU          SKU
	GTIN         string
	UnitsPerPack int
	Quantity     Quantity
	OwnerRef     OwnerRef
	Lot          string
	Expiry       string
	ReceiptID    string
	LineNo       int
}

func NewContainerRegistered(occurredAt time.Time, lpn string, sku SKU, gtin string, unitsPerPack int, qty Quantity, owner OwnerRef, lot, expiry, receiptID string, lineNo int) ContainerRegistered {
	return ContainerRegistered{
		base: newBase("ContainerRegistered", occurredAt), LPN: lpn, SKU: sku, GTIN: gtin, UnitsPerPack: unitsPerPack,
		Quantity: qty, OwnerRef: owner, Lot: lot, Expiry: expiry, ReceiptID: receiptID, LineNo: lineNo,
	}
}

// ContainerStowed: a sealed container was placed in a bin; its site is the
// bin's site.
type ContainerStowed struct {
	base
	LPN      string
	SKU      SKU
	Quantity Quantity
	OwnerRef OwnerRef
	SiteID   SiteID
	BinID    BinId
}

func NewContainerStowed(occurredAt time.Time, lpn string, sku SKU, qty Quantity, owner OwnerRef, siteID SiteID, binID BinId) ContainerStowed {
	return ContainerStowed{base: newBase("ContainerStowed", occurredAt), LPN: lpn, SKU: sku, Quantity: qty, OwnerRef: owner, SiteID: siteID, BinID: binID}
}

// CaseBroken: a sealed case was opened and its eaches became loose stock in
// LooseBin. The eaches are conserved: Quantity left FromBin and arrived in
// LooseBin.
type CaseBroken struct {
	base
	LPN      string
	SKU      SKU
	Quantity Quantity
	OwnerRef OwnerRef
	FromBin  BinId
	LooseBin BinId
}

func NewCaseBroken(occurredAt time.Time, lpn string, sku SKU, qty Quantity, owner OwnerRef, fromBin, looseBin BinId) CaseBroken {
	return CaseBroken{base: newBase("CaseBroken", occurredAt), LPN: lpn, SKU: sku, Quantity: qty, OwnerRef: owner, FromBin: fromBin, LooseBin: looseBin}
}

// Why a container became EMPTY (ContainerEmptied.reason).
const (
	ContainerEmptiedBroken    = "BROKEN"
	ContainerEmptiedPicked    = "PICKED"
	ContainerEmptiedUnlocated = "UNLOCATED"
)

// ContainerEmptied: a container no longer holds eaches.
type ContainerEmptied struct {
	base
	LPN      string
	SKU      SKU
	OwnerRef OwnerRef
	SiteID   SiteID
	Reason   string
}

func NewContainerEmptied(occurredAt time.Time, lpn string, sku SKU, owner OwnerRef, siteID SiteID, reason string) ContainerEmptied {
	return ContainerEmptied{base: newBase("ContainerEmptied", occurredAt), LPN: lpn, SKU: sku, OwnerRef: owner, SiteID: siteID, Reason: reason}
}
