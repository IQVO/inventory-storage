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
		errors.Is(err, usecases.ErrConcurrentModification):
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

// problemFor maps a typed domain/application error to its RFC 7807
// (type, title) pair. Mirrors statusFor's error groupings one-for-one —
// statusFor itself is untouched; this only decides what goes in the body.
func problemFor(err error) problemInfo {
	switch {
	case errors.Is(err, usecases.ErrStockUnitNotFound):
		return problemInfo{"stock-unit-not-found", "Stock unit not found"}
	case errors.Is(err, usecases.ErrBinNotFound):
		return problemInfo{"bin-not-found", "Bin not found"}
	case errors.Is(err, usecases.ErrReservationNotFound):
		return problemInfo{"reservation-not-found", "Reservation not found"}
	case errors.Is(err, usecases.ErrProductClassificationNotFound):
		return problemInfo{"product-classification-not-found", "Product classification not found"}

	case errors.Is(err, shared.ErrEmptySKU):
		return problemInfo{"empty-sku", "SKU must not be empty"}
	case errors.Is(err, shared.ErrEmptyBinID):
		return problemInfo{"empty-bin-id", "Bin ID must not be empty"}
	case errors.Is(err, stock.ErrStowRequiresItemAndLocation):
		return problemInfo{"stow-requires-item-and-location", "Stow requires both an item scan and a location scan"}
	case errors.Is(err, product.ErrUnknownHandlingTag):
		return problemInfo{"unknown-handling-tag", "Unknown handling tag"}
	case errors.Is(err, product.ErrUnknownTemperatureClass):
		return problemInfo{"unknown-temperature-class", "Unknown temperature class"}
	case errors.Is(err, product.ErrNoHandlingTags):
		return problemInfo{"no-handling-tags", "Classification requires at least one handling tag"}
	case errors.Is(err, product.ErrTemperatureClassRequired):
		return problemInfo{"temperature-class-required", "Temperature-sensitive classification requires a temperature class"}
	case errors.Is(err, product.ErrTemperatureClassNotApplicable):
		return problemInfo{"temperature-class-not-applicable", "Temperature class is only meaningful when the temperature-sensitive tag is present"}
	case errors.Is(err, product.ErrDuplicateHandlingTag):
		return problemInfo{"duplicate-handling-tag", "Duplicate handling tag"}
	case errors.Is(err, product.ErrInvalidDOTHazardClass):
		return problemInfo{"invalid-dot-hazard-class", "DOT hazard class must be between 1 and 9"}
	case errors.Is(err, product.ErrDOTHazardClassNotApplicable):
		return problemInfo{"dot-hazard-class-not-applicable", "DOT hazard class is only meaningful when the hazmat tag is present"}

	case errors.Is(err, shared.ErrNegativeQuantity):
		return problemInfo{"negative-quantity", "Quantity must not be negative"}
	case errors.Is(err, shared.ErrZeroQuantity):
		return problemInfo{"zero-quantity", "Quantity must be greater than zero"}
	case errors.Is(err, location.ErrInvalidCapacity):
		return problemInfo{"invalid-bin-capacity", "Bin capacity must be greater than zero"}

	case errors.Is(err, location.ErrBinFull):
		return problemInfo{"bin-full", "Bin is full: capacity exceeded"}
	case errors.Is(err, location.ErrReleaseExceedsOccupancy):
		return problemInfo{"release-exceeds-occupancy", "Release exceeds bin occupancy"}
	case errors.Is(err, usecases.ErrInsufficientUsable), errors.Is(err, stock.ErrInsufficientUsable):
		return problemInfo{"insufficient-usable", "Requested quantity exceeds usable inventory"}
	case errors.Is(err, stock.ErrInsufficientReserved):
		return problemInfo{"insufficient-reserved", "Pick quantity exceeds reserved quantity"}
	case errors.Is(err, stock.ErrUnitUnlocated):
		return problemInfo{"unit-unlocated", "Stock unit is unlocated"}
	case errors.Is(err, reservation.ErrAlreadyResolved):
		return problemInfo{"reservation-already-resolved", "Reservation is already resolved"}
	case errors.Is(err, reservation.ErrExpired):
		return problemInfo{"reservation-expired", "Reservation has expired"}
	case errors.Is(err, reservation.ErrNoAllocations):
		return problemInfo{"reservation-no-allocations", "Reservation requires at least one allocation"}
	case errors.Is(err, usecases.ErrHazmatZoneRequired):
		return problemInfo{"hazmat-zone-required", "Hazmat SKU requires a hazmat-rated zone"}
	case errors.Is(err, usecases.ErrTemperatureClassMismatch):
		return problemInfo{"temperature-class-mismatch", "Bin temperature class does not match the SKU's required temperature class"}
	case errors.Is(err, usecases.ErrLocationClassificationUnavailable):
		return problemInfo{"location-classification-unavailable", "Location classification lookup unavailable"}
	case errors.Is(err, usecases.ErrHazmatClassIncompatible):
		return problemInfo{"hazmat-class-incompatible", "DOT hazard class incompatible with another SKU already stowed in this bin"}
	case errors.Is(err, usecases.ErrConcurrentModification):
		return problemInfo{"concurrent-modification", "The resource was modified by another request; re-fetch the latest version and retry"}

	default:
		return problemInfo{"internal-error", "An unexpected internal error occurred"}
	}
}
