package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// statusFor maps a typed domain/application error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, usecases.ErrStockUnitNotFound),
		errors.Is(err, usecases.ErrBinNotFound),
		errors.Is(err, usecases.ErrReservationNotFound),
		errors.Is(err, usecases.ErrProductClassificationNotFound):
		return http.StatusNotFound

	case errors.Is(err, shared.ErrEmptySKU),
		errors.Is(err, shared.ErrEmptyBinID),
		errors.Is(err, reservation.ErrInvalidLineNo),
		errors.Is(err, stock.ErrStowRequiresItemAndLocation),
		errors.Is(err, product.ErrUnknownHandlingTag),
		errors.Is(err, product.ErrUnknownTemperatureClass),
		errors.Is(err, product.ErrNoHandlingTags),
		errors.Is(err, product.ErrTemperatureClassRequired),
		errors.Is(err, product.ErrTemperatureClassNotApplicable),
		errors.Is(err, product.ErrDuplicateHandlingTag),
		errors.Is(err, product.ErrInvalidDOTHazardClass),
		errors.Is(err, product.ErrDOTHazardClassNotApplicable):
		return http.StatusBadRequest

	case errors.Is(err, shared.ErrNegativeQuantity),
		errors.Is(err, shared.ErrZeroQuantity),
		errors.Is(err, location.ErrInvalidCapacity):
		return http.StatusUnprocessableEntity

	case errors.Is(err, location.ErrBinFull),
		errors.Is(err, location.ErrCapacityBelowOccupancy),
		errors.Is(err, location.ErrReleaseExceedsOccupancy),
		errors.Is(err, usecases.ErrInsufficientUsable),
		errors.Is(err, stock.ErrInsufficientUsable),
		errors.Is(err, stock.ErrInsufficientReserved),
		errors.Is(err, stock.ErrUnitUnlocated),
		errors.Is(err, reservation.ErrAlreadyResolved),
		errors.Is(err, reservation.ErrExpired),
		errors.Is(err, reservation.ErrNoAllocations),
		errors.Is(err, usecases.ErrHazmatZoneRequired),
		errors.Is(err, usecases.ErrTemperatureClassMismatch),
		errors.Is(err, usecases.ErrLocationClassificationUnavailable),
		errors.Is(err, usecases.ErrHazmatClassIncompatible),
		errors.Is(err, usecases.ErrConcurrentModification),
		errors.Is(err, usecases.ErrTransferReceiptConflict),
		errors.Is(err, usecases.ErrReceiptNotStaged),
		errors.Is(err, usecases.ErrBinWrongSite),
		errors.Is(err, usecases.ErrStowQuantityMismatch):
		return http.StatusConflict

	default:
		return http.StatusInternalServerError
	}
}

// problemBaseURI is the namespace for this service's RFC 7807 "type" URIs.
// It does not need to resolve to a real page — it's an identifier, unique
// per distinct error category in this service.
const problemBaseURI = "https://errors.inventory-storage.warehouse-systems.dev/"

// problemInfo is the fixed, category-level (type, title) pair for an RFC
// 7807 problem response. slug becomes the last path segment of "type";
// title is a fixed human string for the category (the dynamic detail comes
// from err.Error() at write time, not from this table).
type problemInfo struct {
	slug  string
	title string
}

// problemCatalog pins each typed domain/application error to its RFC 7807
// (type, title) pair. It is a slice, not a map, because order matters:
// problemFor matches the FIRST entry whose target equals (or wraps) the
// error, exactly as the switch it replaced evaluated errors.Is top-to-bottom.
func problemCatalog() []struct {
	err  error
	info problemInfo
} {
	return []struct {
		err  error
		info problemInfo
	}{
		{usecases.ErrStockUnitNotFound, problemInfo{"stock-unit-not-found", "Stock unit not found"}},
		{usecases.ErrBinNotFound, problemInfo{"bin-not-found", "Bin not found"}},
		{usecases.ErrReservationNotFound, problemInfo{"reservation-not-found", "Reservation not found"}},
		{usecases.ErrProductClassificationNotFound, problemInfo{"product-classification-not-found", "Product classification not found"}},

		{shared.ErrEmptySKU, problemInfo{"empty-sku", "SKU must not be empty"}},
		{shared.ErrEmptyBinID, problemInfo{"empty-bin-id", "Bin ID must not be empty"}},
		{reservation.ErrInvalidLineNo, problemInfo{"invalid-line-no", "lineNo must be at least 1"}},
		{stock.ErrStowRequiresItemAndLocation, problemInfo{"stow-requires-item-and-location", "Stow requires both an item scan and a location scan"}},
		{product.ErrUnknownHandlingTag, problemInfo{"unknown-handling-tag", "Unknown handling tag"}},
		{product.ErrUnknownTemperatureClass, problemInfo{"unknown-temperature-class", "Unknown temperature class"}},
		{product.ErrNoHandlingTags, problemInfo{"no-handling-tags", "Classification requires at least one handling tag"}},
		{product.ErrTemperatureClassRequired, problemInfo{"temperature-class-required", "Temperature-sensitive classification requires a temperature class"}},
		{product.ErrTemperatureClassNotApplicable, problemInfo{"temperature-class-not-applicable", "Temperature class is only meaningful when the temperature-sensitive tag is present"}},
		{product.ErrDuplicateHandlingTag, problemInfo{"duplicate-handling-tag", "Duplicate handling tag"}},
		{product.ErrInvalidDOTHazardClass, problemInfo{"invalid-dot-hazard-class", "DOT hazard class must be between 1 and 9"}},
		{product.ErrDOTHazardClassNotApplicable, problemInfo{"dot-hazard-class-not-applicable", "DOT hazard class is only meaningful when the hazmat tag is present"}},

		{shared.ErrNegativeQuantity, problemInfo{"negative-quantity", "Quantity must not be negative"}},
		{shared.ErrZeroQuantity, problemInfo{"zero-quantity", "Quantity must be greater than zero"}},
		{location.ErrInvalidCapacity, problemInfo{"invalid-bin-capacity", "Bin capacity must be greater than zero"}},

		{location.ErrBinFull, problemInfo{"bin-full", "Bin is full: capacity exceeded"}},
		{location.ErrCapacityBelowOccupancy, problemInfo{"capacity-below-occupancy", "Bin capacity cannot be set below current occupancy"}},
		{location.ErrReleaseExceedsOccupancy, problemInfo{"release-exceeds-occupancy", "Release exceeds bin occupancy"}},
		{usecases.ErrInsufficientUsable, problemInfo{"insufficient-usable", "Requested quantity exceeds usable inventory"}},
		{stock.ErrInsufficientUsable, problemInfo{"insufficient-usable", "Requested quantity exceeds usable inventory"}},
		{stock.ErrInsufficientReserved, problemInfo{"insufficient-reserved", "Pick quantity exceeds reserved quantity"}},
		{stock.ErrUnitUnlocated, problemInfo{"unit-unlocated", "Stock unit is unlocated"}},
		{reservation.ErrAlreadyResolved, problemInfo{"reservation-already-resolved", "Reservation is already resolved"}},
		{reservation.ErrExpired, problemInfo{"reservation-expired", "Reservation has expired"}},
		{reservation.ErrNoAllocations, problemInfo{"reservation-no-allocations", "Reservation requires at least one allocation"}},
		{usecases.ErrHazmatZoneRequired, problemInfo{"hazmat-zone-required", "Hazmat SKU requires a hazmat-rated zone"}},
		{usecases.ErrTemperatureClassMismatch, problemInfo{"temperature-class-mismatch", "Bin temperature class does not match the SKU's required temperature class"}},
		{usecases.ErrLocationClassificationUnavailable, problemInfo{"location-classification-unavailable", "Location classification lookup unavailable"}},
		{usecases.ErrHazmatClassIncompatible, problemInfo{"hazmat-class-incompatible", "DOT hazard class incompatible with another SKU already stowed in this bin"}},
		{usecases.ErrConcurrentModification, problemInfo{"concurrent-modification", "The resource was modified by another request; re-fetch the latest version and retry"}},
		{usecases.ErrTransferReceiptConflict, problemInfo{"transfer-receipt-conflict", "This transfer line already has a receipt for a different scan"}},
		{usecases.ErrReceiptNotStaged, problemInfo{"receipt-not-staged", "Transfer receipt is not in STAGED state; stage the receipt before stowing"}},
		{usecases.ErrBinWrongSite, problemInfo{"bin-wrong-site", "Bin does not belong to the transfer's destination site"}},
		{usecases.ErrStowQuantityMismatch, problemInfo{"stow-quantity-mismatch", "Stow quantities must sum exactly to the receipt's received quantity"}},
	}
}

// problemFor maps a typed domain/application error to its RFC 7807
// (type, title) pair by walking problemCatalog in order. Mirrors statusFor's
// error groupings one-for-one — statusFor itself is untouched; this only
// decides what goes in the body.
func problemFor(err error) problemInfo {
	for _, entry := range problemCatalog() {
		if errors.Is(err, entry.err) {
			return entry.info
		}
	}
	return problemInfo{"internal-error", "An unexpected internal error occurred"}
}
