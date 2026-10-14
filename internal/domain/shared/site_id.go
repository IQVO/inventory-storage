package shared

import (
	"errors"
	"strings"
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

// NewSiteID validates and constructs a SiteID. The value must contain at
// least one non-whitespace character, where "whitespace" is exactly the
// ECMA-262 \s set the OpenAPI contract's `pattern: '\S'` uses (JSON Schema
// patterns are ECMA-262). Go's unicode.IsSpace differs from that set: it
// counts U+0085 (NEL) as space and U+FEFF as not, so a TrimSpace check
// rejected "\u0085", a value the schema accepts (found by Schemathesis on
// POST /relocations), and accepted "\uFEFF", one the schema rejects. Using the
// contract's own definition keeps the domain and the API in agreement.
func NewSiteID(value string) (SiteID, error) {
	if strings.TrimFunc(value, isContractWhitespace) == "" {
		return "", ErrEmptySiteID
	}
	return SiteID(value), nil
}

// isContractWhitespace reports whether r is in ECMA-262's \s class
// (WhiteSpace + LineTerminator): TAB, LF, VT, FF, CR, SPACE, NBSP, ZWNBSP
// (U+FEFF), U+1680, U+2000-U+200A, U+2028, U+2029, U+202F, U+205F, U+3000.
func isContractWhitespace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', '\u00a0', '\ufeff', '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000':
		return true
	}
	return r >= '\u2000' && r <= '\u200a'
}

func (s SiteID) String() string { return string(s) }
