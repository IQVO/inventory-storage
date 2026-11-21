package usecases

import "errors"

var (
	// ErrToteNotFound: no tote has this license plate.
	ErrToteNotFound = errors.New("tote not found")
	// ErrLicensePlateTaken: the license plate already names another
	// container (a sealed case, or a tote registered concurrently).
	ErrLicensePlateTaken = errors.New("license plate is already registered")
	// ErrAutomatedStorageRequiresContainer: loose stock was sent into a bin
	// whose facility-layout LocationType is AutomatedStorage (ADR 0054).
	ErrAutomatedStorageRequiresContainer = errors.New("an automated storage location holds container stock only")
)
