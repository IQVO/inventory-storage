package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
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
		errors.Is(err, usecases.ErrRelocationNotFound),
		errors.Is(err, usecases.ErrContainerNotFound),
		errors.Is(err, usecases.ErrToteNotFound),
		errors.Is(err, usecases.ErrHeldUnitNotFound),
		errors.Is(err, usecases.ErrProductClassificationNotFound):
		return http.StatusNotFound

	case errors.Is(err, shared.ErrEmptySKU),
		errors.Is(err, shared.ErrEmptySiteID),
		errors.Is(err, shared.ErrEmptyBinID),
		errors.Is(err, shared.ErrInvalidOwnerRef),
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
		errors.Is(err, relocation.ErrSameBin),
		errors.Is(err, relocation.ErrUnknownReason),
		errors.Is(err, relocation.ErrSourceRefTooLong),
		errors.Is(err, stock.ErrInvalidDisposition),
		errors.Is(err, stock.ErrInvalidHoldReason),
		errors.Is(err, location.ErrInvalidCapacity):
		return http.StatusUnprocessableEntity

	case errors.Is(err, location.ErrBinFull),
		errors.Is(err, location.ErrCapacityBelowOccupancy),
		errors.Is(err, location.ErrReleaseExceedsOccupancy),
		errors.Is(err, usecases.ErrInsufficientUsable),
		errors.Is(err, stock.ErrInsufficientUsable),
		errors.Is(err, stock.ErrInsufficientReserved),
		errors.Is(err, stock.ErrUnitUnlocated),
		errors.Is(err, stock.ErrUnitHeld),
		errors.Is(err, stock.ErrHeldUnitClosed),
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
		errors.Is(err, relocation.ErrBinNotAtSite),
		errors.Is(err, usecases.ErrStowQuantityMismatch),
		errors.Is(err, shared.ErrOwnerMismatch),
		errors.Is(err, container.ErrContainerNotSealed),
		errors.Is(err, container.ErrContainerNotStowed),
		errors.Is(err, container.ErrContainerNotAtSite),
		errors.Is(err, container.ErrContainerReserved),
		errors.Is(err, container.ErrBreakNotConserving),
		errors.Is(err, container.ErrContainerQuantityMismatch),
		errors.Is(err, container.ErrBinHandlingUnitMismatch),
		errors.Is(err, usecases.ErrLicensePlateTaken),
		errors.Is(err, container.ErrToteNotPlaced),
		errors.Is(err, container.ErrToteReserved),
		errors.Is(err, usecases.ErrAutomatedStorageRequiresContainer):
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
	// class is the replay class of the problem type (replay_class.go): what the Idempotency-Key
	// middleware does with this answer when the request is retried. Every catalogue entry
	// declares one; TestEveryProblemTypeDeclaresAReplayClass fails on one that does not.
	class replayClass
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
		{usecases.ErrStockUnitNotFound, problemInfo{"stock-unit-not-found", "Stock unit not found", classRetryable}},
		{usecases.ErrBinNotFound, problemInfo{"bin-not-found", "Bin not found", classRetryable}},
		{usecases.ErrReservationNotFound, problemInfo{"reservation-not-found", "Reservation not found", classFinal}},
		{usecases.ErrRelocationNotFound, problemInfo{"relocation-not-found", "Relocation not found", classFinal}},
		{usecases.ErrProductClassificationNotFound, problemInfo{"product-classification-not-found", "Product classification not found", classRetryable}},
		{usecases.ErrContainerNotFound, problemInfo{"container-not-found", "Container not found", classRetryable}},
		{usecases.ErrHeldUnitNotFound, problemInfo{"held-unit-not-found", "Held unit not found", classFinal}},

		{shared.ErrEmptySKU, problemInfo{"empty-sku", "SKU must not be empty", classFinal}},
		{shared.ErrEmptySiteID, problemInfo{"empty-site-id", "Site ID must not be empty", classFinal}},
		{shared.ErrEmptyBinID, problemInfo{"empty-bin-id", "Bin ID must not be empty", classFinal}},
		{shared.ErrInvalidOwnerRef, problemInfo{"invalid-owner-ref", "ownerRef must be at most 64 characters of [A-Za-z0-9._-]", classFinal}},
		{reservation.ErrInvalidLineNo, problemInfo{"invalid-line-no", "lineNo must be between 1 and 2147483647", classFinal}},
		{stock.ErrStowRequiresItemAndLocation, problemInfo{"stow-requires-item-and-location", "Stow requires both an item scan and a location scan", classFinal}},
		{product.ErrUnknownHandlingTag, problemInfo{"unknown-handling-tag", "Unknown handling tag", classFinal}},
		{product.ErrUnknownTemperatureClass, problemInfo{"unknown-temperature-class", "Unknown temperature class", classFinal}},
		{product.ErrNoHandlingTags, problemInfo{"no-handling-tags", "Classification requires at least one handling tag", classFinal}},
		{product.ErrTemperatureClassRequired, problemInfo{"temperature-class-required", "Temperature-sensitive classification requires a temperature class", classFinal}},
		{product.ErrTemperatureClassNotApplicable, problemInfo{"temperature-class-not-applicable", "Temperature class is only meaningful when the temperature-sensitive tag is present", classFinal}},
		{product.ErrDuplicateHandlingTag, problemInfo{"duplicate-handling-tag", "Duplicate handling tag", classFinal}},
		{product.ErrInvalidDOTHazardClass, problemInfo{"invalid-dot-hazard-class", "DOT hazard class must be between 1 and 9", classFinal}},
		{product.ErrDOTHazardClassNotApplicable, problemInfo{"dot-hazard-class-not-applicable", "DOT hazard class is only meaningful when the hazmat tag is present", classFinal}},

		{shared.ErrNegativeQuantity, problemInfo{"negative-quantity", "Quantity must not be negative", classFinal}},
		{shared.ErrZeroQuantity, problemInfo{"zero-quantity", "Quantity must be greater than zero", classFinal}},
		{location.ErrInvalidCapacity, problemInfo{"invalid-bin-capacity", "Bin capacity must be greater than zero", classFinal}},
		{relocation.ErrSameBin, problemInfo{"same-bin", "Relocation source and destination bin must differ", classFinal}},
		{relocation.ErrUnknownReason, problemInfo{"invalid-relocation-reason", "Relocation reason must be SLOTTING, CONSOLIDATION or MANUAL", classFinal}},
		{relocation.ErrSourceRefTooLong, problemInfo{"source-ref-too-long", "Relocation source reference must be at most 200 characters", classFinal}},
		{stock.ErrInvalidDisposition, problemInfo{"invalid-disposition", "Disposition must be RELEASE, RETURN_TO_VENDOR, SCRAP or REWORK", classFinal}},
		{stock.ErrInvalidHoldReason, problemInfo{"invalid-hold-reason", "Hold reason must be DAMAGED, QC_PENDING, SUSPECT or OTHER", classFinal}},

		{location.ErrBinFull, problemInfo{"bin-full", "Bin is full: capacity exceeded", classRetryable}},
		{location.ErrCapacityBelowOccupancy, problemInfo{"capacity-below-occupancy", "Bin capacity cannot be set below current occupancy", classRetryable}},
		{location.ErrReleaseExceedsOccupancy, problemInfo{"release-exceeds-occupancy", "Release exceeds bin occupancy", classRetryable}},
		{usecases.ErrInsufficientUsable, problemInfo{"insufficient-usable", "Requested quantity exceeds usable inventory", classRetryable}},
		{stock.ErrInsufficientUsable, problemInfo{"insufficient-usable", "Requested quantity exceeds usable inventory", classRetryable}},
		{stock.ErrInsufficientReserved, problemInfo{"insufficient-reserved", "Pick quantity exceeds reserved quantity", classRetryable}},
		{stock.ErrUnitUnlocated, problemInfo{"unit-unlocated", "Stock unit is unlocated", classRetryable}},
		{stock.ErrUnitHeld, problemInfo{"unit-held", "Stock unit is held and cannot be used until a disposition releases it", classRetryable}},
		{stock.ErrHeldUnitClosed, problemInfo{"held-unit-closed", "Held unit already has a final disposition", classRetryable}},
		{reservation.ErrAlreadyResolved, problemInfo{"reservation-already-resolved", "Reservation is already resolved", classOutcome}},
		{reservation.ErrExpired, problemInfo{"reservation-expired", "Reservation has expired", classRetryable}},
		{reservation.ErrNoAllocations, problemInfo{"reservation-no-allocations", "Reservation requires at least one allocation", classRetryable}},
		{usecases.ErrHazmatZoneRequired, problemInfo{"hazmat-zone-required", "Hazmat SKU requires a hazmat-rated zone", classRetryable}},
		{usecases.ErrTemperatureClassMismatch, problemInfo{"temperature-class-mismatch", "Bin temperature class does not match the SKU's required temperature class", classRetryable}},
		{usecases.ErrLocationClassificationUnavailable, problemInfo{"location-classification-unavailable", "Location classification lookup unavailable", classRetryable}},
		{usecases.ErrHazmatClassIncompatible, problemInfo{"hazmat-class-incompatible", "DOT hazard class incompatible with another SKU already stowed in this bin", classRetryable}},
		{usecases.ErrConcurrentModification, problemInfo{"concurrent-modification", "The resource was modified by another request; re-fetch the latest version and retry", classRetryable}},
		{usecases.ErrTransferReceiptConflict, problemInfo{"transfer-receipt-conflict", "This transfer line already has a receipt for a different scan", classRetryable}},
		{usecases.ErrReceiptNotStaged, problemInfo{"receipt-not-staged", "Transfer receipt is not in STAGED state; stage the receipt before stowing", classRetryable}},
		{usecases.ErrBinWrongSite, problemInfo{"bin-wrong-site", "Bin does not belong to the transfer's destination site", classRetryable}},
		{relocation.ErrBinNotAtSite, problemInfo{"bin-not-at-site", "Bin is not in the relocation's site", classRetryable}},
		{usecases.ErrStowQuantityMismatch, problemInfo{"stow-quantity-mismatch", "Stow quantities must sum exactly to the receipt's received quantity", classRetryable}},
		{shared.ErrOwnerMismatch, problemInfo{"owner-mismatch", "Stock owner does not match the reservation owner", classRetryable}},
		{container.ErrContainerNotSealed, problemInfo{"container-not-sealed", "Container is not sealed", classRetryable}},
		{container.ErrContainerNotStowed, problemInfo{"container-not-stowed", "Container is not stowed in a bin", classRetryable}},
		{container.ErrContainerNotAtSite, problemInfo{"container-not-at-site", "Container and bin are not at the same site", classRetryable}},
		{container.ErrContainerReserved, problemInfo{"container-reserved", "Container is reserved", classRetryable}},
		{container.ErrBreakNotConserving, problemInfo{"break-not-conserving", "A case break must produce exactly the container quantity", classRetryable}},
		{container.ErrContainerQuantityMismatch, problemInfo{"container-quantity-mismatch", "A sealed container must hold exactly one case", classRetryable}},
		{container.ErrBinHandlingUnitMismatch, problemInfo{"bin-handling-unit-mismatch", "Bin does not accept this kind of handling unit", classRetryable}},
		{usecases.ErrToteNotFound, problemInfo{"tote-not-found", "Tote not found", classRetryable}},
		{usecases.ErrLicensePlateTaken, problemInfo{"license-plate-taken", "The license plate already names another container", classFinal}},
		{container.ErrToteNotPlaced, problemInfo{"tote-not-placed", "Tote is not in a bin; stow it before filling it", classRetryable}},
		{container.ErrToteReserved, problemInfo{"tote-reserved", "Tote holds reserved stock and cannot be stowed elsewhere", classRetryable}},
		{usecases.ErrAutomatedStorageRequiresContainer, problemInfo{"automated-storage-requires-container", "An automated storage location holds container stock only", classRetryable}},
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
	return problemInternalError
}
