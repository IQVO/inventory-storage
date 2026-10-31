// Package container holds the Container aggregate: the identity of one
// physical sealed case in storage, keyed by its LPN (the carton's SSCC, or an
// internal LPN minted by inbound-receiving for an unlabelled carton).
//
// A Container does not hold stock. The eaches stay on a StockUnit in the
// ledger (ADR 0045): the Container adds identity and a pack state on top of
// it, linked 1:1 through StockUnitID while the case is stowed.
package container

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/shared/gs1"
)

var (
	// ErrContainerQuantityMismatch: a sealed container must hold exactly one
	// case: qtyEaches == unitsPerPack * cases (cases is 1 in v1).
	ErrContainerQuantityMismatch = errors.New("sealed container quantity must equal units per pack times cases")
	// ErrBreakNotConserving: a case break must produce exactly the eaches the
	// container holds, no more and no fewer.
	ErrBreakNotConserving = errors.New("case break must produce exactly the container quantity")
	// ErrContainerNotSealed: the operation needs a SEALED container.
	ErrContainerNotSealed = errors.New("container is not sealed")
	// ErrContainerNotOpen: the operation needs an OPEN container.
	ErrContainerNotOpen = errors.New("container is not open")
	// ErrContainerNotStowed: the container has no bin yet (it was registered
	// from a receipt but never stowed).
	ErrContainerNotStowed = errors.New("container is not stowed in a bin")
	// ErrContainerNotAtSite: the container, or the bin asked for, is at a
	// different site than the one the operation needs.
	ErrContainerNotAtSite = errors.New("container and bin are not at the same site")
	// ErrContainerReserved: a reserved case cannot be moved or broken because
	// the reservation names the case, not its eaches.
	ErrContainerReserved = errors.New("container is reserved")
	// ErrBinHandlingUnitMismatch: the bin does not accept this kind of
	// handling unit (a sealed case needs a CASE or PALLET bin; loose eaches
	// need an EACH bin). A bin with no recorded handling unit fails closed
	// for a case.
	ErrBinHandlingUnitMismatch = errors.New("bin handling unit does not accept this stock")
	// ErrInvalidLPN: the license plate is empty, too long or has an illegal character.
	ErrInvalidLPN = errors.New("lpn must be 1 to 64 characters of [A-Za-z0-9._-]")
	// ErrInvalidContainer: a container identity field is invalid.
	ErrInvalidContainer = errors.New("invalid container")
)

const (
	maxLPNLen    = 64
	maxLotLen    = 20
	maxExpiryLen = 32
)

var lpnPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// State is the pack state of a container.
type State string

const (
	// StateSealed: the case is unopened and holds its full quantity.
	StateSealed State = "SEALED"
	// StateOpen: the case was opened and its eaches are being moved out. In
	// v1 it exists only inside one break transaction.
	StateOpen State = "OPEN"
	// StateEmpty: the case no longer holds eaches (broken or picked whole).
	StateEmpty State = "EMPTY"
)

// Container is the aggregate root for one sealed case.
type Container struct {
	lpn          string
	sku          shared.SKU
	gtin         string
	unitsPerPack int
	quantity     shared.Quantity
	ownerRef     shared.OwnerRef
	siteID       shared.SiteID
	binID        shared.BinId
	state        State
	lot          string
	expiry       string
	stockUnitID  string
	receiptID    string
	lineNo       int
	// version is optimistic-concurrency metadata (ADR 0019).
	version int
}

// RegisterSpec is what a received sealed carton contributes. Quantity is the
// carton's eaches; the container holds one case, so it must equal UnitsPerPack.
type RegisterSpec struct {
	LPN          string
	SKU          string
	GTIN         string
	UnitsPerPack int
	Quantity     int
	OwnerRef     string
	Lot          string
	// Expiry is the carton's expiry date as received (AI 17); carried, not interpreted.
	Expiry    string
	ReceiptID string
	LineNo    int
}

// Register creates a SEALED container with no location. The site is not known
// here: it is set at StowContainer from the bin's site.
func Register(s RegisterSpec) (*Container, error) {
	if !lpnPattern.MatchString(s.LPN) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidLPN, s.LPN)
	}
	sku, err := shared.NewSKU(s.SKU)
	if err != nil {
		return nil, err
	}
	gtin, err := gs1.ParseGTIN(s.GTIN)
	if err != nil {
		return nil, err
	}
	owner, err := shared.NewOwnerRef(s.OwnerRef)
	if err != nil {
		return nil, err
	}
	if err := validateProvenance(s); err != nil {
		return nil, err
	}
	if s.UnitsPerPack < 1 || s.Quantity != s.UnitsPerPack {
		return nil, fmt.Errorf("%w: %d eaches in a case of %d", ErrContainerQuantityMismatch, s.Quantity, s.UnitsPerPack)
	}
	return &Container{
		lpn: s.LPN, sku: sku, gtin: gtin.String(), unitsPerPack: s.UnitsPerPack,
		quantity: shared.Quantity(s.Quantity), ownerRef: owner, state: StateSealed,
		lot: s.Lot, expiry: s.Expiry, receiptID: s.ReceiptID, lineNo: s.LineNo, version: 1,
	}, nil
}

func validateProvenance(s RegisterSpec) error {
	switch {
	case s.ReceiptID == "":
		return fmt.Errorf("%w: receipt id is required", ErrInvalidContainer)
	case s.LineNo < 1:
		return fmt.Errorf("%w: line no must be at least 1", ErrInvalidContainer)
	case len(s.Lot) > maxLotLen:
		return fmt.Errorf("%w: lot is longer than %d", ErrInvalidContainer, maxLotLen)
	case len(s.Expiry) > maxExpiryLen:
		return fmt.Errorf("%w: expiry is longer than %d", ErrInvalidContainer, maxExpiryLen)
	}
	return nil
}

// Snapshot is the persisted state of a Container.
type Snapshot struct {
	LPN          string
	SKU          shared.SKU
	GTIN         string
	UnitsPerPack int
	Quantity     shared.Quantity
	OwnerRef     shared.OwnerRef
	SiteID       shared.SiteID
	BinID        shared.BinId
	State        State
	Lot          string
	Expiry       string
	StockUnitID  string
	ReceiptID    string
	LineNo       int
	Version      int
}

// Rehydrate rebuilds a Container from a row without re-running creation rules.
func Rehydrate(s Snapshot) *Container {
	return &Container{
		lpn: s.LPN, sku: s.SKU, gtin: s.GTIN, unitsPerPack: s.UnitsPerPack, quantity: s.Quantity,
		ownerRef: s.OwnerRef, siteID: s.SiteID, binID: s.BinID, state: s.State, lot: s.Lot, expiry: s.Expiry,
		stockUnitID: s.StockUnitID, receiptID: s.ReceiptID, lineNo: s.LineNo, version: s.Version,
	}
}

func (c *Container) LPN() string               { return c.lpn }
func (c *Container) SKU() shared.SKU           { return c.sku }
func (c *Container) GTIN() string              { return c.gtin }
func (c *Container) UnitsPerPack() int         { return c.unitsPerPack }
func (c *Container) Quantity() shared.Quantity { return c.quantity }
func (c *Container) OwnerRef() shared.OwnerRef { return c.ownerRef }
func (c *Container) SiteID() shared.SiteID     { return c.siteID }
func (c *Container) BinID() shared.BinId       { return c.binID }
func (c *Container) State() State              { return c.state }
func (c *Container) Lot() string               { return c.lot }
func (c *Container) Expiry() string            { return c.expiry }
func (c *Container) StockUnitID() string       { return c.stockUnitID }
func (c *Container) ReceiptID() string         { return c.receiptID }
func (c *Container) LineNo() int               { return c.lineNo }

// Version is the optimistic-concurrency version this aggregate was loaded at.
func (c *Container) Version() int { return c.version }

// Stow places the sealed container in a bin of siteID, backed by the
// SEALED_CASE stock unit stockUnitID. The first stow sets the container's
// site from the bin (the receipt carries none); a later stow must stay in
// that site.
func (c *Container) Stow(bin shared.BinId, siteID shared.SiteID, stockUnitID string) error {
	if c.state != StateSealed {
		return ErrContainerNotSealed
	}
	if siteID == "" || (c.siteID != "" && c.siteID != siteID) {
		return ErrContainerNotAtSite
	}
	c.siteID = siteID
	c.binID = bin
	c.stockUnitID = stockUnitID
	return nil
}

// Break opens a stowed, sealed container whose eaches are being moved out.
// produced is the quantity the caller actually moved to loose stock; the
// break is rejected unless it equals the container quantity (conservation).
func (c *Container) Break(produced shared.Quantity) error {
	if c.state != StateSealed {
		return ErrContainerNotSealed
	}
	if c.binID == "" {
		return ErrContainerNotStowed
	}
	if produced != c.quantity {
		return fmt.Errorf("%w: produced %d of %d", ErrBreakNotConserving, produced, c.quantity)
	}
	c.state = StateOpen
	return nil
}

// Decant empties an OPEN container once its eaches are loose stock.
func (c *Container) Decant() error {
	if c.state != StateOpen {
		return ErrContainerNotOpen
	}
	c.state = StateEmpty
	c.binID = ""
	return nil
}

// EmptyWhole empties a SEALED container whose whole case left storage (a
// confirmed whole-case pick, or a cycle count that could not find it).
func (c *Container) EmptyWhole() error {
	if c.state != StateSealed {
		return ErrContainerNotSealed
	}
	c.state = StateEmpty
	c.binID = ""
	return nil
}

// HandlingUnit is what a bin accepts, from facility-layout's bin attribute
// (ADR 0045 section 4).
type HandlingUnit string

const (
	HandlingEach    HandlingUnit = "EACH"
	HandlingCase    HandlingUnit = "CASE"
	HandlingPallet  HandlingUnit = "PALLET"
	HandlingUnknown HandlingUnit = ""
)

// ParseHandlingUnit reads the wire value case-insensitively; anything else is
// HandlingUnknown.
func ParseHandlingUnit(v string) HandlingUnit {
	switch HandlingUnit(upper(v)) {
	case HandlingEach:
		return HandlingEach
	case HandlingCase:
		return HandlingCase
	case HandlingPallet:
		return HandlingPallet
	}
	return HandlingUnknown
}

func upper(v string) string {
	b := []byte(v)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
	}
	return string(b)
}

// AcceptsSealedCase reports whether a sealed case may be stowed in such a bin.
// An unknown handling unit fails closed.
func (h HandlingUnit) AcceptsSealedCase() bool { return h == HandlingCase || h == HandlingPallet }

// AcceptsLoose reports whether loose eaches may be put in such a bin: an EACH
// bin, or a bin with no recorded handling unit (every bin registered before
// facility-layout shipped the attribute).
func (h HandlingUnit) AcceptsLoose() bool { return h == HandlingEach || h == HandlingUnknown }
