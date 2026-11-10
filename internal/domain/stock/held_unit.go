package stock

import (
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

var (
	// ErrUnitHeld: the unit is held (ADR 0052): it has no usable quantity and
	// cannot be reserved, moved or added to.
	ErrUnitHeld = errors.New("stock unit is held")
	// ErrHeldUnitClosed: the held unit already left the hold through a
	// disposition; it accepts no further decision.
	ErrHeldUnitClosed = errors.New("held unit already has a final disposition")
	// ErrInvalidHoldReason: a hold reason outside the closed set.
	ErrInvalidHoldReason = errors.New("invalid hold reason")
	// ErrInvalidDisposition: a disposition outside the closed set.
	ErrInvalidDisposition = errors.New("invalid disposition")
	// ErrHeldSourceRequired: a hold must name its receipt, line and SKU.
	ErrHeldSourceRequired = errors.New("held unit requires a receipt id, a line number, a sku and a positive quantity")
)

// HoldReason is why a unit is held. The set is closed; the values are
// wire-stable.
type HoldReason string

const (
	ReasonDamaged   HoldReason = "DAMAGED"
	ReasonQCPending HoldReason = "QC_PENDING"
	ReasonSuspect   HoldReason = "SUSPECT"
	ReasonOther     HoldReason = "OTHER"
)

// ParseHoldReason validates a wire value against the closed set.
func ParseHoldReason(v string) (HoldReason, error) {
	switch r := HoldReason(v); r {
	case ReasonDamaged, ReasonQCPending, ReasonSuspect, ReasonOther:
		return r, nil
	}
	return "", fmt.Errorf("%w: %q (want DAMAGED, QC_PENDING, SUSPECT or OTHER)", ErrInvalidHoldReason, v)
}

// Disposition is the decision that settles a held unit. The set is closed and
// borrows the vocabulary of returns-processing's DispositionDecided.
type Disposition string

const (
	// DispositionRelease: back to normal stock, as a fresh (staged) receipt.
	DispositionRelease Disposition = "RELEASE"
	// DispositionReturnToVendor: the units leave the building to the vendor.
	DispositionReturnToVendor Disposition = "RETURN_TO_VENDOR"
	// DispositionScrap: the units are destroyed.
	DispositionScrap Disposition = "SCRAP"
	// DispositionRework: the units stay held for rework, with an updated reason.
	DispositionRework Disposition = "REWORK"
)

// ParseDisposition validates a wire value against the closed set.
func ParseDisposition(v string) (Disposition, error) {
	switch d := Disposition(v); d {
	case DispositionRelease, DispositionReturnToVendor, DispositionScrap, DispositionRework:
		return d, nil
	}
	return "", fmt.Errorf("%w: %q (want RELEASE, RETURN_TO_VENDOR, SCRAP or REWORK)", ErrInvalidDisposition, v)
}

// HeldSource says where a held quantity came from: the receipt line that
// delivered it, with the line's pack provenance.
type HeldSource struct {
	ReceiptID string
	ASNNumber string
	LineNo    int
	SKU       shared.SKU
	Quantity  shared.Quantity
	// OwnerRef is empty for fleet-owned stock; SSCC is the optional pack label.
	OwnerRef shared.OwnerRef
	SSCC     string
}

// HeldQuery filters a read of the held units. Zero values mean "any".
type HeldQuery struct {
	Reason HoldReason
	SKU    shared.SKU
	// HeldBefore keeps units held strictly before this instant (the age filter).
	HeldBefore time.Time
	After      *HeldPageAfter
	// Limit is the maximum number of rows to return; 0 means no limit.
	Limit int
}

// HeldPageAfter is the keyset position of the last row of the previous page:
// units sort by (HeldAt, ID).
type HeldPageAfter struct {
	HeldAt time.Time
	ID     string
}

// HeldUnit is a quantity of one SKU received but held out of usable stock
// (ADR 0052). It has no bin: the goods wait in the receiving or quarantine
// area, so it is never a stock_units row and no reader of usable quantity can
// see it. State is HELD until a final disposition makes it REMOVED.
type HeldUnit struct {
	id          string
	source      HeldSource
	reason      HoldReason
	state       State
	disposition Disposition
	decidedAt   time.Time
	heldAt      time.Time
	reworkCount int
	version     int
	lpns        []string
}

// NewHeldUnit holds the quantity of source for reason at heldAt.
func NewHeldUnit(id string, source HeldSource, reason HoldReason, heldAt time.Time) (*HeldUnit, error) {
	if id == "" || source.ReceiptID == "" || source.LineNo < 1 || source.SKU == "" || source.Quantity.Int() <= 0 {
		return nil, ErrHeldSourceRequired
	}
	if _, err := ParseHoldReason(string(reason)); err != nil {
		return nil, err
	}
	return &HeldUnit{id: id, source: source, reason: reason, state: StateHeld, heldAt: heldAt, version: 1}, nil
}

// HeldRecord is the persisted shape of a HeldUnit.
type HeldRecord struct {
	ID          string
	Source      HeldSource
	Reason      HoldReason
	State       State
	Disposition Disposition
	DecidedAt   time.Time
	HeldAt      time.Time
	ReworkCount int
	Version     int
	LPNs        []string
}

// RehydrateHeldUnit rebuilds a HeldUnit from a persisted row without
// re-validating it.
func RehydrateHeldUnit(r HeldRecord) *HeldUnit {
	return &HeldUnit{
		id: r.ID, source: r.Source, reason: r.Reason, state: r.State, disposition: r.Disposition,
		decidedAt: r.DecidedAt, heldAt: r.HeldAt, reworkCount: r.ReworkCount, version: r.Version, lpns: r.LPNs,
	}
}

func (h *HeldUnit) ID() string                { return h.id }
func (h *HeldUnit) SKU() shared.SKU           { return h.source.SKU }
func (h *HeldUnit) Quantity() shared.Quantity { return h.source.Quantity }
func (h *HeldUnit) Reason() HoldReason        { return h.reason }
func (h *HeldUnit) State() State              { return h.state }
func (h *HeldUnit) ReceiptID() string         { return h.source.ReceiptID }
func (h *HeldUnit) ASNNumber() string         { return h.source.ASNNumber }
func (h *HeldUnit) LineNo() int               { return h.source.LineNo }
func (h *HeldUnit) OwnerRef() shared.OwnerRef { return h.source.OwnerRef }
func (h *HeldUnit) SSCC() string              { return h.source.SSCC }
func (h *HeldUnit) HeldAt() time.Time         { return h.heldAt }
func (h *HeldUnit) DecidedAt() time.Time      { return h.decidedAt }
func (h *HeldUnit) ReworkCount() int          { return h.reworkCount }
func (h *HeldUnit) LPNs() []string            { return h.lpns }

// WithLPNs returns a copy of h carrying the LPNs of the Damaged cartons
// recorded for its receipt line. Repositories use it when loading.
func (h *HeldUnit) WithLPNs(lpns []string) *HeldUnit {
	c := *h
	c.lpns = lpns
	return &c
}

// Disposition is the last decision taken, empty while none was.
func (h *HeldUnit) Disposition() Disposition { return h.disposition }

// Version is the optimistic-concurrency version (ADR 0019), infrastructure
// only: no business rule reads it.
func (h *HeldUnit) Version() int { return h.version }

// Usable is always zero: held stock is never promised to demand.
func (h *HeldUnit) Usable() shared.Quantity { return 0 }

// Reserve always fails with ErrUnitHeld: a held unit cannot be reserved.
func (h *HeldUnit) Reserve(shared.Quantity) error { return ErrUnitHeld }

// Decide settles the unit with d at decidedAt. RELEASE, RETURN_TO_VENDOR and
// SCRAP close it (state REMOVED, disposition recorded for audit); REWORK keeps
// it held with reason updated (empty means OTHER) and counts the rework. The
// whole quantity is always disposed: it is conserved, never split. A closed
// unit accepts no further decision.
func (h *HeldUnit) Decide(d Disposition, reason HoldReason, decidedAt time.Time) error {
	if h.state != StateHeld {
		return ErrHeldUnitClosed
	}
	if _, err := ParseDisposition(string(d)); err != nil {
		return err
	}
	if d == DispositionRework {
		if reason == "" {
			reason = ReasonOther
		}
		if _, err := ParseHoldReason(string(reason)); err != nil {
			return err
		}
		h.reason = reason
		h.reworkCount++
	} else {
		h.state = StateRemoved
	}
	h.disposition = d
	h.decidedAt = decidedAt
	return nil
}
