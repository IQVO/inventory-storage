// Package relocation holds the Relocation aggregate: one intra-site move of
// a quantity of one SKU from a source bin to a destination bin of the same
// site (ADR 0039). It is the stock ledger's record of slot-plan execution
// (slotting-optimization ADR 0010) and of manual re-slotting: both bins and
// every touched StockUnit change in one transaction, the ledger never goes
// negative, and the record answers GET /relocations/{id}.
package relocation

import (
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// Reason is the closed set of reasons a relocation is made for. The values
// are wire-stable: they ride StockRelocated.reason and the REST body.
type Reason string

const (
	// ReasonSlotting: executing a move of an approved slot plan
	// (slotting-optimization ADR 0010), normally via a RELOCATE task.
	ReasonSlotting Reason = "SLOTTING"
	// ReasonConsolidation: merging partial quantities of a SKU into fewer bins.
	ReasonConsolidation Reason = "CONSOLIDATION"
	// ReasonManual: any other operator-initiated move.
	ReasonManual Reason = "MANUAL"
	// ReasonReplenishment: refilling a pick face from reserve storage,
	// executed by a REPLENISH task (ADR 0041). It is set by the system only:
	// ParseReason, the operator-facing parser behind POST /relocations,
	// rejects it, so the REST contract is unchanged.
	ReasonReplenishment Reason = "REPLENISHMENT"
)

// MaxSourceRefLength bounds the caller's correlation reference, in characters.
const MaxSourceRefLength = 200

var (
	// ErrEmptyID: a relocation (or its destination stock unit) needs an id.
	ErrEmptyID = errors.New("relocation id must not be empty")
	// ErrSameBin: moving stock into the bin it already sits in is not a
	// relocation.
	ErrSameBin = errors.New("relocation source and destination bin must differ")
	// ErrUnknownReason: the reason is not one an operator may request
	// (SLOTTING, CONSOLIDATION, MANUAL), or, for New, not a known reason.
	ErrUnknownReason = errors.New("unknown relocation reason (want SLOTTING, CONSOLIDATION or MANUAL)")
	// ErrSourceRefTooLong: the source reference exceeds MaxSourceRefLength.
	ErrSourceRefTooLong = errors.New("relocation source reference must be at most 200 characters")
	// ErrBinNotAtSite: a bin is not in the relocation's site (its recorded
	// custody, or the site segment of its location code, differs or is
	// unknown). A relocation never crosses sites.
	ErrBinNotAtSite = errors.New("bin is not in the relocation's site")
	// ErrBinMismatch: Apply was handed bins other than the ones the
	// relocation names (a programming error in the caller).
	ErrBinMismatch = errors.New("relocation applied to bins other than the ones it names")
	// ErrAlreadyApplied: a relocation moves stock exactly once.
	ErrAlreadyApplied = errors.New("relocation was already applied")
)

// ParseReason validates a reason an OPERATOR may request (POST /relocations):
// SLOTTING, CONSOLIDATION or MANUAL. Reasons are case-sensitive.
// ReasonReplenishment is deliberately not accepted here (see New).
func ParseReason(value string) (Reason, error) {
	switch r := Reason(value); r {
	case ReasonSlotting, ReasonConsolidation, ReasonManual:
		return r, nil
	}
	return "", ErrUnknownReason
}

// known reports whether r is any persisted reason: the operator-facing ones
// plus the system-set ReasonReplenishment.
func known(r Reason) bool {
	if r == ReasonReplenishment {
		return true
	}
	_, err := ParseReason(string(r))
	return err == nil
}

// Spec is the requested relocation.
type Spec struct {
	ID        string
	SiteID    shared.SiteID
	SKU       shared.SKU
	FromBin   shared.BinId
	ToBin     shared.BinId
	Quantity  shared.Quantity
	Reason    Reason
	SourceRef string
}

// Leg is one source StockUnit's contribution to the move.
type Leg struct {
	StockUnitID string
	Quantity    shared.Quantity
}

// Relocation is the aggregate root: the requested move plus, once applied,
// its outcome (the source legs, the destination unit, the time).
type Relocation struct {
	spec                   Spec
	legs                   []Leg
	destinationStockUnitID string
	occurredAt             time.Time
}

// New validates spec and returns an unapplied relocation.
func New(spec Spec) (*Relocation, error) {
	if err := validate(spec); err != nil {
		return nil, err
	}
	return &Relocation{spec: spec}, nil
}

func validate(spec Spec) error {
	if strings.TrimSpace(spec.ID) == "" {
		return ErrEmptyID
	}
	if _, err := shared.NewSiteID(spec.SiteID.String()); err != nil {
		return err
	}
	if _, err := shared.NewSKU(spec.SKU.String()); err != nil {
		return err
	}
	if err := validateBins(spec.FromBin, spec.ToBin); err != nil {
		return err
	}
	if spec.Quantity.Int() <= 0 {
		return shared.ErrZeroQuantity
	}
	if !known(spec.Reason) {
		return ErrUnknownReason
	}
	if utf8.RuneCountInString(spec.SourceRef) > MaxSourceRefLength {
		return ErrSourceRefTooLong
	}
	return nil
}

func validateBins(from, to shared.BinId) error {
	if _, err := shared.NewBinId(from.String()); err != nil {
		return err
	}
	if _, err := shared.NewBinId(to.String()); err != nil {
		return err
	}
	if from == to {
		return ErrSameBin
	}
	return nil
}

// Rehydrate rebuilds a persisted relocation without re-running New's checks.
func Rehydrate(spec Spec, legs []Leg, destinationStockUnitID string, occurredAt time.Time) *Relocation {
	return &Relocation{spec: spec, legs: append([]Leg(nil), legs...), destinationStockUnitID: destinationStockUnitID, occurredAt: occurredAt}
}

func (r *Relocation) ID() string                     { return r.spec.ID }
func (r *Relocation) SiteID() shared.SiteID          { return r.spec.SiteID }
func (r *Relocation) SKU() shared.SKU                { return r.spec.SKU }
func (r *Relocation) FromBin() shared.BinId          { return r.spec.FromBin }
func (r *Relocation) ToBin() shared.BinId            { return r.spec.ToBin }
func (r *Relocation) Quantity() shared.Quantity      { return r.spec.Quantity }
func (r *Relocation) Reason() Reason                 { return r.spec.Reason }
func (r *Relocation) SourceRef() string              { return r.spec.SourceRef }
func (r *Relocation) DestinationStockUnitID() string { return r.destinationStockUnitID }
func (r *Relocation) OccurredAt() time.Time          { return r.occurredAt }

// Legs returns a copy of the source legs (empty until applied).
func (r *Relocation) Legs() []Leg { return append([]Leg(nil), r.legs...) }

// Outcome is what Apply changed: the new destination unit and the source
// units it drew from (in draw order), all of which the caller must save.
type Outcome struct {
	Destination *stock.StockUnit
	Touched     []*stock.StockUnit
}

// Apply performs the move on the given aggregates, in memory: it draws the
// quantity from the USABLE quantity of the SKU's units in the source bin
// (ascending unit id, so concurrent movers lock rows alike), releases the
// source bin's occupancy, occupies the destination bin, and creates the
// destination StockUnit (destUnitID) in the relocation's site custody.
//
// Every rule is checked before anything changes, so a rejected Apply leaves
// the units, the bins and the relocation untouched: both bins must be the
// ones named and belong to the site; the usable quantity must cover the
// move (stock.ErrInsufficientUsable); the destination must have room
// (location.ErrBinFull) and the source bin's occupancy must cover the move
// (location.ErrReleaseExceedsOccupancy).
func (r *Relocation) Apply(units []*stock.StockUnit, from, to *location.Bin, destUnitID string, at time.Time) (Outcome, error) {
	if err := r.checkApplicable(from, to, destUnitID); err != nil {
		return Outcome{}, err
	}
	plan, err := r.drawPlan(units)
	if err != nil {
		return Outcome{}, err
	}
	dest, err := stock.NewStockUnitAtSite(destUnitID, r.spec.SKU, r.spec.ToBin, r.spec.Quantity, r.spec.SiteID)
	if err != nil {
		return Outcome{}, err
	}
	if err := moveOccupancy(from, to, r.spec.Quantity); err != nil {
		return Outcome{}, err
	}

	touched := make([]*stock.StockUnit, 0, len(plan))
	legs := make([]Leg, 0, len(plan))
	for _, p := range plan {
		if err := p.unit.RelocateOut(p.take); err != nil {
			return Outcome{}, err
		}
		touched = append(touched, p.unit)
		legs = append(legs, Leg{StockUnitID: p.unit.ID(), Quantity: p.take})
	}
	r.legs = legs
	r.destinationStockUnitID = destUnitID
	r.occurredAt = at
	return Outcome{Destination: dest, Touched: touched}, nil
}

func (r *Relocation) checkApplicable(from, to *location.Bin, destUnitID string) error {
	if r.destinationStockUnitID != "" {
		return ErrAlreadyApplied
	}
	if strings.TrimSpace(destUnitID) == "" {
		return ErrEmptyID
	}
	if from.ID() != r.spec.FromBin || to.ID() != r.spec.ToBin {
		return ErrBinMismatch
	}
	if !from.BelongsToSite(r.spec.SiteID) || !to.BelongsToSite(r.spec.SiteID) {
		return ErrBinNotAtSite
	}
	return nil
}

// draw is one planned take from one source unit.
type draw struct {
	unit *stock.StockUnit
	take shared.Quantity
}

// drawPlan picks, without changing anything, how much to take from which
// unit: the SKU's units in the source bin with usable quantity, ascending id.
func (r *Relocation) drawPlan(units []*stock.StockUnit) ([]draw, error) {
	candidates := make([]*stock.StockUnit, 0, len(units))
	for _, u := range units {
		if u.SKU() == r.spec.SKU && u.BinID() == r.spec.FromBin && u.Usable().Int() > 0 {
			candidates = append(candidates, u)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID() < candidates[j].ID() })

	remaining := r.spec.Quantity
	plan := make([]draw, 0, len(candidates))
	for _, u := range candidates {
		if remaining.Int() == 0 {
			break
		}
		take := u.Usable()
		if take.GreaterThan(remaining) {
			take = remaining
		}
		plan = append(plan, draw{unit: u, take: take})
		remaining -= take
	}
	if remaining.Int() > 0 {
		return nil, stock.ErrInsufficientUsable
	}
	return plan, nil
}

// moveOccupancy occupies the destination and releases the source; when the
// release fails the destination is restored, so neither bin changes.
func moveOccupancy(from, to *location.Bin, qty shared.Quantity) error {
	if err := to.Occupy(qty); err != nil {
		return err
	}
	if err := from.Release(qty); err != nil {
		// Undo the occupy just made: it cannot fail, the space was just taken.
		_ = to.Release(qty)
		return err
	}
	return nil
}

// Event is the StockRelocated fact of an applied relocation.
func (r *Relocation) Event() shared.StockRelocated {
	return shared.NewStockRelocated(r.occurredAt, r.spec.ID, r.spec.SiteID, r.spec.SKU, r.spec.FromBin, r.spec.ToBin,
		r.spec.Quantity, string(r.spec.Reason), r.spec.SourceRef)
}
