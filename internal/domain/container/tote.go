package container

import (
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var (
	// ErrToteNotPlaced: the operation needs the tote to be in a bin (a fill
	// happens where the tote is).
	ErrToteNotPlaced = errors.New("tote is not in a bin")
	// ErrToteReserved: a tote holding reserved stock cannot be stowed
	// elsewhere, because each allocation names its pick location (ADR 0025).
	ErrToteReserved = errors.New("tote holds reserved stock")
)

// LicensePlateKind says which aggregate holds a license plate: the LPN
// namespace is shared by sealed cases and totes (ADR 0054).
type LicensePlateKind string

const (
	// KindCase: a sealed case (Container, ADR 0045).
	KindCase LicensePlateKind = "CASE"
	// KindTote: a reusable tote (Tote, ADR 0054).
	KindTote LicensePlateKind = "TOTE"
)

// ValidLPN reports whether lpn satisfies the license-plate rule every
// container shares: 1 to 64 characters of [A-Za-z0-9._-].
func ValidLPN(lpn string) bool { return lpnPattern.MatchString(lpn) }

// Tote is the aggregate root for one reusable, license-plated container that
// the WCS moves (ADR 0054). It holds no quantity itself: its contents are the
// LOOSE stock units tagged with its LPN. Its logical location is the bin it
// is stowed in; rack positions never reach this context (WCS seam decision 2).
type Tote struct {
	lpn    string
	siteID shared.SiteID
	binID  shared.BinId
	// version is optimistic-concurrency metadata (ADR 0019).
	version int
}

// RegisterTote creates a tote in no bin. The LPN is the tote's permanent
// label; this service never mints one (ADR 0054 decision 2).
func RegisterTote(lpn string) (*Tote, error) {
	if !ValidLPN(lpn) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidLPN, lpn)
	}
	return &Tote{lpn: lpn, version: 1}, nil
}

// ToteSnapshot is the persisted state of a Tote.
type ToteSnapshot struct {
	LPN     string
	SiteID  shared.SiteID
	BinID   shared.BinId
	Version int
}

// RehydrateTote rebuilds a Tote from a row without re-running creation rules.
func RehydrateTote(s ToteSnapshot) *Tote {
	return &Tote{lpn: s.LPN, siteID: s.SiteID, binID: s.BinID, version: s.Version}
}

// Snapshot returns the tote's persisted state.
func (t *Tote) Snapshot() ToteSnapshot {
	return ToteSnapshot{LPN: t.lpn, SiteID: t.siteID, BinID: t.binID, Version: t.version}
}

func (t *Tote) LPN() string           { return t.lpn }
func (t *Tote) SiteID() shared.SiteID { return t.siteID }
func (t *Tote) BinID() shared.BinId   { return t.binID }
func (t *Tote) Version() int          { return t.version }
func (t *Tote) IsPlaced() bool        { return t.binID != "" }

// PlaceIn records the tote's logical location. The first placement sets the
// site from the bin; a later one must stay in that site. A refused placement
// changes nothing.
func (t *Tote) PlaceIn(bin shared.BinId, site shared.SiteID) error {
	if bin == "" {
		return shared.ErrEmptyBinID
	}
	if site == "" || (t.siteID != "" && t.siteID != site) {
		return ErrContainerNotAtSite
	}
	t.siteID = site
	t.binID = bin
	return nil
}
