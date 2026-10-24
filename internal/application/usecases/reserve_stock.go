package usecases

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/reservation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// DefaultReservationTimeout is used when a use case is constructed with a
// zero Timeout.
const DefaultReservationTimeout = 30 * time.Minute

// ReserveStock revocably binds a quantity of a SKU's usable inventory to
// demand. It does not bind to one specific StockUnit permanently: the
// allocation it draws from is recorded on the Reservation so a later revoke
// returns exactly that quantity, but a future reserve is free to draw from a
// different holding.
type ReserveStock struct {
	Stock        ports.StockRepo
	Reservations ports.ReservationRepo
	Events       ports.EventPublisher
	Clock        ports.Clock
	Metrics      ports.ReservationMetrics
	Timeout      time.Duration
	// UnitOfWork brackets every Save/Publish this use case makes
	// atomically (ADR 0017), including the ones expireAllIfDue/expireIfDue
	// make internally. Optional: nil means "no transactional backing".
	UnitOfWork ports.UnitOfWork
	// Levels raises BinStockLevelChanged (RESERVE, and REVOKE for lazily
	// expired reservations) in the same transaction (ADR 0040). Optional:
	// nil means "not emitted".
	Levels *BinLevelEmitter
}

func (uc *ReserveStock) Execute(ctx context.Context, sku shared.SKU, qty shared.Quantity, demandRef string) (*reservation.Reservation, error) {
	return uc.ExecuteForLine(ctx, sku, qty, demandRef, nil)
}

// ExecuteForLine is Execute for a reservation that names the order line it
// serves (decision 18, ADR 0036). lineNo is optional (nil = unknown, exactly
// Execute); a non-nil value below 1 is rejected with
// reservation.ErrInvalidLineNo before anything is read or written. The line
// is stored on the Reservation so the TaskCompleted consumer can confirm
// exactly the picked line.
func (uc *ReserveStock) ExecuteForLine(ctx context.Context, sku shared.SKU, qty shared.Quantity, demandRef string, lineNo *int) (*reservation.Reservation, error) {
	return uc.ExecuteFor(ctx, ReserveRequest{SKU: sku, Quantity: qty, DemandRef: demandRef, LineNo: lineNo})
}

// ReserveRequest is one quantity reservation. OwnerRef selects whose stock
// may be drawn on (ADR 0045): empty means fleet-owned stock ONLY, never "any
// owner", so a legacy caller can never allocate a seller's stock.
type ReserveRequest struct {
	SKU       shared.SKU
	Quantity  shared.Quantity
	DemandRef string
	LineNo    *int
	OwnerRef  shared.OwnerRef
}

// ExecuteFor reserves req.Quantity eaches of req.SKU from loose stock of
// req.OwnerRef. A sealed case is never drawn on: it is reserved whole by
// ReserveFullCases. Stock of another owner never satisfies the request.
func (uc *ReserveStock) ExecuteFor(ctx context.Context, req ReserveRequest) (*reservation.Reservation, error) {
	if err := validateReserveInput(req.Quantity, req.LineNo); err != nil {
		return nil, err
	}

	res, err := uc.activeReservationFor(ctx, req)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return res, nil
	}

	units, err := uc.Stock.FindBySKU(ctx, req.SKU)
	if err != nil {
		return nil, err
	}

	allocations, touched, err := uc.allocate(units, req.Quantity, req.OwnerRef)
	if err != nil {
		return nil, err
	}

	id, err := uc.Reservations.NextID(ctx)
	if err != nil {
		return nil, err
	}

	now := uc.Clock.Now()
	res, err = reservation.NewForLine(id, req.SKU, req.Quantity, req.DemandRef, req.LineNo, allocations, now, uc.timeout())
	if err != nil {
		return nil, err
	}
	res.ForOwner(req.OwnerRef)

	if err := uc.commit(ctx, res, touched, now); err != nil {
		return nil, err
	}
	return res, nil
}

func (uc *ReserveStock) timeout() time.Duration {
	if uc.Timeout <= 0 {
		return DefaultReservationTimeout
	}
	return uc.Timeout
}

// commit persists a freshly decided reservation: every mutated StockUnit
// save, the Reservation save AND the event publish run inside ONE UnitOfWork
// scope (the audit's atomicity fix): a failure after the first stock
// decrement rolls back all of them, so a half-drawn reservation can never
// survive. Atomicity therefore does not depend on an outer transaction (e.g.
// the HTTP idempotency middleware's) already being on ctx.
func (uc *ReserveStock) commit(ctx context.Context, res *reservation.Reservation, touched []*stock.StockUnit, now time.Time) error {
	err := uc.Levels.Atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		for _, unit := range touched {
			if err := uc.Stock.Save(ctx, unit); err != nil {
				return err
			}
			if err := uc.Levels.TrackFor(ctx, shared.BinLevelReserve, unit.BinID(), res.SKU(), unit.OwnerRef()); err != nil {
				return err
			}
		}
		if err := uc.Reservations.Save(ctx, res); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewStockReserved(now, res.ID(), res.SKU(), res.Quantity(), res.DemandRef()))
	})
	if err != nil {
		return err
	}
	if uc.Metrics != nil {
		uc.Metrics.ReservationCreated(ctx)
	}
	return nil
}

// validateReserveInput rejects a reserve request that can never succeed, before
// anything is read or written.
func validateReserveInput(qty shared.Quantity, lineNo *int) error {
	if qty.Int() <= 0 {
		return shared.ErrZeroQuantity
	}
	if lineNo != nil && !reservation.ValidLineNo(*lineNo) {
		return reservation.ErrInvalidLineNo
	}
	return nil
}

// activeReservationFor implements the idempotency guard for Execute:
// a client-side retry after a dropped response (e.g. the first call's
// Reservation was created and stock allocated, but the response never
// reached the caller) must not create a second Reservation and
// double-reserve stock for the same demandRef. If an ACTIVE reservation
// already exists for this demandRef AND the same (sku, quantity) -- see
// isReplayOf -- treat this call as the retry and hand back that same
// reservation instead of allocating again. A different SKU or quantity
// under the same demandRef is a different line of the same demand, never
// a retry. A demandRef
// legitimately has multiple reservations across its lifetime (revoke +
// retry), so this only short-circuits when an unresolved one is still open —
// once it's revoked/confirmed/expired, a new demandRef call is a genuine new
// reservation attempt, not a retry.
//
// This is a best-effort, not a hard uniqueness guarantee: two concurrent
// first-attempts for the same demandRef racing this check could still both
// pass it before either has saved. Closing that window would need a DB-level
// constraint; see REST_AUDIT.md's idempotency notes for the accepted scope
// here. It returns nil when no ACTIVE reservation remains for demandRef.
func (uc *ReserveStock) activeReservationFor(ctx context.Context, req ReserveRequest) (*reservation.Reservation, error) {
	existing, err := uc.Reservations.FindByDemandRef(ctx, req.DemandRef)
	if err != nil {
		return nil, err
	}
	// Lazy expiry: this is a read of every reservation ever created
	// against demandRef, so it is a point where a timed-out ACTIVE
	// reservation must be resolved before being reasoned about — a
	// caller retrying against a demandRef whose only "active" match has
	// actually timed out must fall through to a genuine new reservation
	// attempt, not be handed back an expired one as if it were live.
	existing, err = expireAllIfDue(ctx, uc.UnitOfWork, uc.Levels, uc.Stock, uc.Reservations, uc.Events, uc.Clock, existing)
	if err != nil {
		return nil, err
	}
	for _, res := range existing {
		if res.Status() == reservation.StatusActive && isReplayOf(res, req.SKU, req.Quantity, req.LineNo, req.OwnerRef) {
			return res, nil
		}
	}
	return nil, nil
}

// isReplayOf reports whether res is what a replay of reserve(sku, qty,
// lineNo) would have created. A demandRef names a DEMAND (e.g. an order), and one
// demand legitimately holds several ACTIVE reservations at once -- one per
// order line / SKU. Matching on demandRef alone handed line 2's request
// back line 1's reservation (a different SKU), so every line after the
// first silently reserved nothing: its stock was never held, a backorder
// was reported as allocated, and cancelling the order revoked the same
// reservation twice (observed live in the warehouse-day simulation).
// Only an identical (sku, quantity) request is treated as the retry.
//
// When the request names its line (ADR 0036) the line must also agree, so
// two lines of one order that share SKU and quantity stay two reservations.
// An ACTIVE reservation with NO line (made before the field existed) still
// answers a line-aware retry: handing it back is the conservative choice
// over holding the same stock twice. A request without a line matches as it
// always did.
func isReplayOf(res *reservation.Reservation, sku shared.SKU, qty shared.Quantity, lineNo *int, owner shared.OwnerRef) bool {
	if res.SKU() != sku || res.Quantity() != qty || res.OwnerRef() != owner {
		return false
	}
	have := res.LineNo()
	if lineNo == nil || have == nil {
		return true
	}
	return *have == *lineNo
}

// allocate reserves qty across the given stock units, greedily drawing from
// each unit's usable quantity until the full amount is covered. It returns
// the allocations recorded and the units touched (mutated but not yet
// persisted; persisting is Execute's job, inside its UnitOfWork, so a later failure never leaves a
// partially-drawn reservation behind). It fails with ErrInsufficientUsable —
// before any caller-visible state has been persisted — when the units'
// combined usable quantity cannot cover qty.
func (uc *ReserveStock) allocate(all []*stock.StockUnit, qty shared.Quantity, owner shared.OwnerRef) ([]reservation.Allocation, []*stock.StockUnit, error) {
	units := reservableUnits(all, owner)
	totalUsable := shared.Quantity(0)
	for _, unit := range units {
		totalUsable = totalUsable.Add(unit.Usable())
	}
	if qty.GreaterThan(totalUsable) {
		return nil, nil, ErrInsufficientUsable
	}

	remaining := qty
	var allocations []reservation.Allocation
	var touched []*stock.StockUnit
	for _, unit := range units {
		if remaining.Int() == 0 {
			break
		}
		usable := unit.Usable()
		if usable.Int() == 0 {
			continue
		}
		take := usable
		if !remaining.GreaterThan(usable) {
			take = remaining
		}
		if err := unit.ReserveFor(owner, take); err != nil {
			return nil, nil, err
		}
		allocations = append(allocations, reservation.Allocation{StockUnitID: unit.ID(), BinID: unit.BinID(), Quantity: take})
		touched = append(touched, unit)
		remaining, _ = remaining.Sub(take)
	}

	if remaining.Int() > 0 {
		return nil, nil, ErrInsufficientUsable
	}

	return allocations, touched, nil
}

// reservableUnits keeps the loose units of owner: stock of another owner never
// satisfies a reservation, and a sealed case is never split by a quantity
// reservation (ADR 0045). An empty owner keeps fleet-owned stock only.
func reservableUnits(units []*stock.StockUnit, owner shared.OwnerRef) []*stock.StockUnit {
	out := make([]*stock.StockUnit, 0, len(units))
	for _, unit := range units {
		if unit.ReservableBy(owner) {
			out = append(out, unit)
		}
	}
	return out
}
