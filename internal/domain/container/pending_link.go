package container

import "github.com/claudioed/inventory-storage/internal/domain/shared"

// Pending link statuses.
const (
	// LinkPending: waiting for the receipt line to be booked (enough), or for
	// the carton's GTIN to appear in the packaging view.
	LinkPending = "PENDING"
	// LinkRejected: can never become a container; kept for audit.
	LinkRejected = "REJECTED"
)

// PendingLink is a sealed carton waiting to become a Container, keyed by
// (ReceiptID, LineNo, LPN). Provenance only; it holds no stock.
type PendingLink struct {
	ReceiptID  string
	LineNo     int
	LPN        string
	SSCC       string
	ParentSSCC string
	SKU        shared.SKU
	GTIN       string
	Quantity   shared.Quantity
	OwnerRef   shared.OwnerRef
	Lot        string
	Expiry     string
	Status     string
	Reason     string
}
