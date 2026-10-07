// Package usecases implements the application's use cases: one struct per
// use case, depending only on the domain and on application/ports.
package usecases

import "errors"

var (
	ErrStockUnitNotFound   = errors.New("stock unit not found")
	ErrBinNotFound         = errors.New("bin not found")
	ErrReservationNotFound = errors.New("reservation not found")
	ErrInsufficientUsable  = errors.New("requested quantity exceeds usable inventory")

	// ErrProductClassificationNotFound is returned when a GetProductClassification
	// (or similar) lookup finds no registered classification for the SKU.
	ErrProductClassificationNotFound = errors.New("product classification not found")

	// ErrHazmatZoneRequired is returned by StowStock when a Hazmat SKU is
	// stowed into a bin whose zone is not hazmat-rated, per facility-layout's
	// location-classification lookup.
	ErrHazmatZoneRequired = errors.New("hazmat sku requires a hazmat-rated zone")

	// ErrTemperatureClassMismatch is returned by StowStock when a
	// TemperatureSensitive SKU is stowed into a bin whose zone temperature
	// class does not match the SKU's required temperature class.
	ErrTemperatureClassMismatch = errors.New("bin temperature class does not match the sku's required temperature class")

	// ErrLocationClassificationUnavailable is returned by StowStock when the
	// synchronous facility-layout lookup fails (transport/5xx) for a SKU
	// that carries Hazmat or TemperatureSensitive tags. Unclassified SKUs
	// are never blocked by lookup unavailability — see ADR 0009's
	// fail-open/fail-closed asymmetry.
	ErrLocationClassificationUnavailable = errors.New("location classification lookup unavailable")

	// ErrHazmatClassIncompatible is returned by StowStock when the SKU
	// being stowed carries a DOT hazard class that is incompatible (per
	// product.Incompatible, the derived 49 CFR §177.848-grounded matrix)
	// with the DOT hazard class of another SKU already occupying the
	// target bin. This is a same-bin-only, local check — see ADR 0010.
	ErrHazmatClassIncompatible = errors.New("dot hazard class incompatible with another sku already stowed in this bin")

	// ErrConcurrentModification is returned by a repo's Save when the
	// version-guarded write affected zero rows against an EXISTING row:
	// another writer already modified (and incremented the version of)
	// the same aggregate since this caller last read it (ADR 0019,
	// optimistic concurrency). The caller must re-fetch and retry rather
	// than treat this as success or as a generic internal error — the
	// inbound HTTP adapter maps it to 409 Conflict.
	ErrConcurrentModification = errors.New("aggregate was concurrently modified by another writer; re-fetch and retry")

	// ErrTransferLineAlreadyDecided is returned by
	// TransferAllocationRepo.Save when the transfer_line_id already has a
	// decided ledger row (the DB unique constraint fired). The use case
	// treats it as "replay — return the original outcome", never as a
	// write failure.
	ErrTransferLineAlreadyDecided = errors.New("transfer line already decided")

	// ErrTransferReceiptAlreadyStaged is returned by
	// TransferReceiptRepo.Save when the transfer_line_id already has a
	// receipt (the DB unique constraint fired). The use case treats it
	// as "replay — return the original receipt", never as a failure.
	ErrTransferReceiptAlreadyStaged = errors.New("transfer receipt already staged")

	// ErrTransferReceiptAlreadyStowed is returned by
	// TransferReceiptRepo.SaveStowed when the receipt is already in its
	// STOWED terminal state. The use case routes it to the replay path
	// (return the original stow outcome, create no second StockUnit,
	// publish nothing new).
	ErrTransferReceiptAlreadyStowed = errors.New("transfer receipt already stowed")

	// ErrTransferLineUnrecognized is returned by StageTransferReceipt
	// when the scanned transfer_line_id has NO decided allocation row —
	// this service never allocated stock for that line. The scan is
	// quarantined as an inventory exception; NO stock changes.
	ErrTransferLineUnrecognized = errors.New("transfer line was never allocated by this service")

	// ErrTransferNotAllocated is returned by StageTransferReceipt when a
	// ledger row exists but its outcome is not ALLOCATED (it was
	// REJECTED at allocation time), or the scanned transfer_id does not
	// match the allocated row's. The scan is quarantined; NO stock
	// changes.
	ErrTransferNotAllocated = errors.New("transfer line is not in ALLOCATED state for this scan")

	// ErrTransferWrongDestination is returned by StageTransferReceipt
	// when the scan names a destination site that differs from the
	// allocated row's origin/destination expectations cannot be
	// reconciled — in v1 the allocation carries the ORIGIN site and the
	// destination is whatever site receives; a destination equal to the
	// ORIGIN site is physically impossible (a transfer never ships back
	// to its donor), so it is quarantined as unrecognized.
	ErrTransferWrongDestination = errors.New("destination site cannot be the transfer's own origin site")

	// ErrReceiptNotStaged is returned by StowTransferStock when the
	// transfer line has no STAGED receipt — either nothing was ever
	// staged, or the receipt was already stowed and the replay is
	// handled separately. Stowing before staging is rejected.
	ErrReceiptNotStaged = errors.New("transfer receipt is not in STAGED state; stage the receipt before stowing")

	// ErrBinWrongSite is returned by StowTransferStock when a requested
	// bin is not in the receipt's destination site's custody — the
	// site-custody fact (ADR 0030/0031) fails closed.
	ErrBinWrongSite = errors.New("bin does not belong to the transfer's destination site")

	// ErrStowQuantityMismatch is returned by StowTransferStock when the
	// requested bin quantities do not sum exactly to the receipt's
	// received quantity — a partial or padded stow would strand or
	// invent physical stock.
	ErrStowQuantityMismatch = errors.New("stow quantities must sum exactly to the receipt's received quantity")

	// ErrTransferExceptionScanExists is returned by
	// InventoryExceptionRepo.Save when the exact same scan tuple was
	// already quarantined (the unique constraint fired). The use case
	// treats it as a benign replay, never a failure.
	ErrTransferExceptionScanExists = errors.New("this exact scan was already quarantined")

	// Closed rejection reasons for a transfer allocation (wire-stable —
	// these exact strings ride the TransferStockAllocationRejected event).
	RejectionOriginSiteUnknown   = "ORIGIN_SITE_UNKNOWN"
	RejectionInsufficientUsable  = "INSUFFICIENT_USABLE"
	RejectionIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
)
