package shared

import (
	"strings"

	"errors"
)

// ErrEmptySiteID is returned by NewSiteID for a missing site identifier:
// site custody is only meaningful when the site is named.
var ErrEmptySiteID = errors.New("site id must not be empty")

// SiteID identifies one warehouse site in the fulfillment network. A
// StockUnit carrying a SiteID is in that site's custody: transfer
// allocation may only draw from units whose custody matches the
// requested origin site (a global usable count cannot prevent the wrong
// facility from donating stock — see the network-inventory transfer
// design). The empty SiteID means "site not recorded" (every row
// persisted before site custody existed); such units stay allocatable
// for ordinary demand but are NEVER allocatable for transfers.
type SiteID string

// NewSiteID validates and constructs a SiteID. The value must be a
// non-empty, non-whitespace string.
func NewSiteID(value string) (SiteID, error) {
	if strings.TrimSpace(value) == "" {
		return "", ErrEmptySiteID
	}
	return SiteID(value), nil
}

func (s SiteID) String() string { return string(s) }
