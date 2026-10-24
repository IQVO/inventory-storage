package shared

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidOwnerRef: an owner reference is longer than MaxOwnerRefLen or
	// holds a character outside [A-Za-z0-9._-] (contract G4).
	ErrInvalidOwnerRef = errors.New("owner ref must be at most 64 characters of [A-Za-z0-9._-]")
	// ErrOwnerMismatch: a reservation (or an allocation under it) would span
	// stock of two different owners. Stock and reservations are segregated by
	// owner (ADR 0045); a reservation is bound to exactly one.
	ErrOwnerMismatch = errors.New("stock owner does not match the reservation owner")
)

// MaxOwnerRefLen is the longest owner reference accepted (contract G4).
const MaxOwnerRefLen = 64

// OwnerRef is the opaque tenant tag stock is segregated by (ADR 0045, contract
// G4). On the fleet it arrives from the Vendor API and is never interpreted.
// The empty OwnerRef means fleet-owned stock, which is all pre-existing data:
// an absent owner on a request means fleet-owned stock ONLY, never "any owner".
type OwnerRef string

// NewOwnerRef validates value. The empty string is valid (fleet-owned).
func NewOwnerRef(value string) (OwnerRef, error) {
	if len(value) > MaxOwnerRefLen {
		return "", fmt.Errorf("%w: %d characters", ErrInvalidOwnerRef, len(value))
	}
	for i := 0; i < len(value); i++ {
		if !isOwnerRefChar(value[i]) {
			return "", fmt.Errorf("%w: %q", ErrInvalidOwnerRef, value)
		}
	}
	return OwnerRef(value), nil
}

func isOwnerRefChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return c == '.' || c == '_' || c == '-'
}

// String returns the raw value.
func (o OwnerRef) String() string { return string(o) }

// IsFleet reports whether this is fleet-owned stock (no owner).
func (o OwnerRef) IsFleet() bool { return o == "" }
