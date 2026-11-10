package usecases

import (
	"context"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// DecideHeldDispositionCommand is one disposition decision for a held unit.
type DecideHeldDispositionCommand struct {
	HeldUnitID  string
	Disposition stock.Disposition
	// Reason is the new hold reason of a REWORK (empty means OTHER); the other
	// dispositions ignore it.
	Reason stock.HoldReason
}

// DecideHeldDisposition settles a held unit (ADR 0052). RELEASE returns the
// whole quantity to the normal flow through the same staged-receipt seam as a
// fresh receipt (ReceiveStock: not usable until stowed); RETURN_TO_VENDOR and
// SCRAP remove it; REWORK keeps it held with its reason updated.
//
// The decision, the unit save and every event commit in ONE UnitOfWork. The
// held-unit gauge moves after the commit, so a rolled-back attempt is never
// counted.
type DecideHeldDisposition struct {
	Held    ports.HeldStockRepo
	Receive stockReceiver
	Events  ports.EventPublisher
	Clock   ports.Clock
	// UnitOfWork brackets the save and the events atomically (ADR 0017).
	// Optional.
	UnitOfWork ports.UnitOfWork
	// Metrics moves inventory.held_units. Optional.
	Metrics ports.HeldStockMetrics
}

// Execute decides cmd. It returns ErrHeldUnitNotFound for an unknown unit,
// stock.ErrHeldUnitClosed for a unit that already left the hold, and an error
// wrapping stock.ErrInvalidDisposition or stock.ErrInvalidHoldReason for a
// value outside the closed sets; none of them changes anything.
func (uc *DecideHeldDisposition) Execute(ctx context.Context, cmd DecideHeldDispositionCommand) (*stock.HeldUnit, error) {
	var held *stock.HeldUnit
	var before stock.HoldReason
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		var err error
		held, err = uc.Held.FindByID(ctx, cmd.HeldUnitID)
		if err != nil {
			return err
		}
		if held == nil {
			return ErrHeldUnitNotFound
		}
		before = held.Reason()
		now := uc.Clock.Now()
		if err := held.Decide(cmd.Disposition, cmd.Reason, now); err != nil {
			return err
		}
		if err := uc.Held.Save(ctx, held); err != nil {
			return err
		}
		return uc.publish(ctx, held, cmd.Disposition, now)
	})
	if err != nil {
		return nil, err
	}
	uc.count(ctx, held, before, cmd.Disposition)
	return held, nil
}

// publish raises the decision events: StockDispositioned for every
// disposition, plus StockReleased and the staged receipt on RELEASE.
func (uc *DecideHeldDisposition) publish(ctx context.Context, h *stock.HeldUnit, d stock.Disposition, now time.Time) error {
	if err := uc.Events.Publish(ctx, shared.NewStockDispositioned(now, h.ID(), h.SKU(), h.Quantity(), string(d), string(h.Reason()),
		h.ReceiptID(), h.ASNNumber(), h.LineNo())); err != nil {
		return err
	}
	if d != stock.DispositionRelease {
		return nil
	}
	if err := uc.Events.Publish(ctx, shared.NewStockReleased(now, h.ID(), h.SKU(), h.Quantity(), h.ReceiptID(), h.ASNNumber(), h.LineNo())); err != nil {
		return err
	}
	_, err := uc.Receive.Execute(ctx, h.SKU(), h.Quantity())
	return err
}

// count moves the held-unit series: units leave their reason on RELEASE,
// RETURN_TO_VENDOR and SCRAP, and move to the new reason on REWORK.
func (uc *DecideHeldDisposition) count(ctx context.Context, h *stock.HeldUnit, before stock.HoldReason, d stock.Disposition) {
	if uc.Metrics == nil {
		return
	}
	qty := h.Quantity().Int()
	uc.Metrics.HeldUnits(ctx, string(before), -qty)
	if d == stock.DispositionRework {
		uc.Metrics.HeldUnits(ctx, string(h.Reason()), qty)
	}
}
