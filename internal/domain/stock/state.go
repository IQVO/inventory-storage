package stock

// State is the lifecycle stage of a StockUnit.
type State string

const (
	// StateAvailable: on-hand, stowed, no active reservation.
	StateAvailable State = "AVAILABLE"
	// StateReserved: at least part of the unit's quantity is bound to demand.
	StateReserved State = "RESERVED"
	// StatePicked: part of the unit's quantity was physically removed; some
	// quantity remains at the bin.
	StatePicked State = "PICKED"
	// StateRemoved: the unit's quantity reached zero — fully picked out.
	StateRemoved State = "REMOVED"
	// StateUnlocated: a cycle count could not account for this quantity; the
	// physical item exists somewhere but the system no longer knows where.
	StateUnlocated State = "UNLOCATED"
	// StateHeld: received but not fit for sale (damaged, QC pending, suspect)
	// and waiting for a disposition (ADR 0052). A held unit has no usable
	// quantity and can never be reserved.
	StateHeld State = "HELD"
)
