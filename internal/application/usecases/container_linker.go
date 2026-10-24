package usecases

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/container"
	"github.com/claudioed/inventory-storage/internal/domain/packaging"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// SettleResult counts what one settle pass did with the pending links it
// looked at.
type SettleResult struct {
	// Registered is the number of containers created.
	Registered int
	// Pending is the number of links left waiting (line not booked enough, or
	// GTIN not in the packaging view yet).
	Pending int
	// Rejected is the number of links that can never become a container.
	Rejected int
}

func (r *SettleResult) add(o SettleResult) {
	r.Registered += o.Registered
	r.Pending += o.Pending
	r.Rejected += o.Rejected
}

// Reasons a pending link is rejected (kept on the row for audit).
const (
	rejectQuantityMismatch = "QUANTITY_MISMATCH"
	rejectGTINOtherSKU     = "GTIN_BELONGS_TO_OTHER_SKU"
	rejectNotACaseGTIN     = "GTIN_IS_NOT_A_CASE"
	rejectLPNTaken         = "LPN_REGISTERED_AT_ANOTHER_RECEIPT_LINE"
	rejectInvalid          = "INVALID_CONTAINER"
)

// ContainerLinker turns pending carton links into Containers once the
// ledger has counted them (ADR 0045 section 6, the double-count rule):
//
//	sum(container qty) per (receipt id, line no) <= booked qty
//
// A container can therefore never exist for units the ledger has not
// counted, whichever of ReceiptLineReceived and ReceiptCartonReceived arrives
// first. The linker books NO quantity itself. Every method must run inside
// the caller's UnitOfWork.
type ContainerLinker struct {
	Containers ports.ContainerRepo
	Pending    ports.PendingLinkRepo
	Bookings   ports.ReceiptBookingRepo
	Packaging  ports.PackagingViewRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
}

// Settle re-evaluates the pending links of one receipt line.
func (l *ContainerLinker) Settle(ctx context.Context, receiptID string, lineNo int) (SettleResult, error) {
	booked, err := l.Bookings.Booked(ctx, receiptID, lineNo)
	if err != nil {
		return SettleResult{}, err
	}
	linked, err := l.Containers.SumQuantity(ctx, receiptID, lineNo)
	if err != nil {
		return SettleResult{}, err
	}
	links, err := l.Pending.FindPendingByLine(ctx, receiptID, lineNo)
	if err != nil {
		return SettleResult{}, err
	}
	var result SettleResult
	for _, link := range links {
		outcome, err := l.settleOne(ctx, link, booked, linked)
		if err != nil {
			return SettleResult{}, err
		}
		if outcome.Registered > 0 {
			linked = linked.Add(link.Quantity)
		}
		result.add(outcome)
	}
	return result, nil
}

// SettleSKU re-evaluates every pending link of sku, line by line (a pack
// level was declared, so a carton waiting for its GTIN may resolve now).
func (l *ContainerLinker) SettleSKU(ctx context.Context, sku shared.SKU) (SettleResult, error) {
	links, err := l.Pending.FindPendingBySKU(ctx, sku)
	if err != nil {
		return SettleResult{}, err
	}
	seen := make(map[string]bool, len(links))
	var result SettleResult
	for _, link := range links {
		key := fmt.Sprintf("%s/%d", link.ReceiptID, link.LineNo)
		if seen[key] {
			continue
		}
		seen[key] = true
		r, err := l.Settle(ctx, link.ReceiptID, link.LineNo)
		if err != nil {
			return SettleResult{}, err
		}
		result.add(r)
	}
	return result, nil
}

// settleOne decides one pending link against the line's booked and already
// linked eaches.
func (l *ContainerLinker) settleOne(ctx context.Context, link container.PendingLink, booked, linked shared.Quantity) (SettleResult, error) {
	existing, err := l.Containers.FindByLPN(ctx, link.LPN)
	if err != nil {
		return SettleResult{}, err
	}
	if existing != nil {
		return l.alreadyRegistered(ctx, link, existing)
	}
	gtin, unitsPerPack, verdict, err := l.resolvePack(ctx, link)
	if err != nil {
		return SettleResult{}, err
	}
	if verdict.reject != "" {
		return l.reject(ctx, link, verdict.reject)
	}
	if verdict.wait {
		return SettleResult{Pending: 1}, nil
	}
	c, err := container.Register(container.RegisterSpec{
		LPN: link.LPN, SKU: link.SKU.String(), GTIN: gtin, UnitsPerPack: unitsPerPack, Quantity: link.Quantity.Int(),
		OwnerRef: link.OwnerRef.String(), Lot: link.Lot, Expiry: link.Expiry, ReceiptID: link.ReceiptID, LineNo: link.LineNo,
	})
	if err != nil {
		return l.reject(ctx, link, rejectReasonFor(err))
	}
	if linked.Add(c.Quantity()).GreaterThan(booked) {
		return SettleResult{Pending: 1}, nil
	}
	return l.register(ctx, link, c)
}

func rejectReasonFor(err error) string {
	if errors.Is(err, container.ErrContainerQuantityMismatch) {
		return rejectQuantityMismatch
	}
	return rejectInvalid
}

// alreadyRegistered handles a link whose LPN already has a container: the
// same receipt line means a redelivery under another event id (drop the
// link); anywhere else it is a duplicate SSCC and is never merged.
func (l *ContainerLinker) alreadyRegistered(ctx context.Context, link container.PendingLink, existing *container.Container) (SettleResult, error) {
	if existing.ReceiptID() == link.ReceiptID && existing.LineNo() == link.LineNo {
		return SettleResult{}, l.Pending.Remove(ctx, link.ReceiptID, link.LineNo, link.LPN)
	}
	return l.reject(ctx, link, rejectLPNTaken)
}

func (l *ContainerLinker) reject(ctx context.Context, link container.PendingLink, reason string) (SettleResult, error) {
	if err := l.Pending.Reject(ctx, link.ReceiptID, link.LineNo, link.LPN, reason); err != nil {
		return SettleResult{}, err
	}
	return SettleResult{Rejected: 1}, nil
}

func (l *ContainerLinker) register(ctx context.Context, link container.PendingLink, c *container.Container) (SettleResult, error) {
	created, err := l.Containers.Add(ctx, c)
	if err != nil {
		return SettleResult{}, err
	}
	if err := l.Pending.Remove(ctx, link.ReceiptID, link.LineNo, link.LPN); err != nil {
		return SettleResult{}, err
	}
	if !created {
		return SettleResult{}, nil
	}
	ev := shared.NewContainerRegistered(l.Clock.Now(), c.LPN(), c.SKU(), c.GTIN(), c.UnitsPerPack(), c.Quantity(), c.OwnerRef(), c.Lot(), c.Expiry(), c.ReceiptID(), c.LineNo())
	if err := l.Events.Publish(ctx, ev); err != nil {
		return SettleResult{}, err
	}
	return SettleResult{Registered: 1}, nil
}

// packVerdict is the outcome of resolving a carton's GTIN: wait for the
// packaging view, or reject for good, or neither (resolved).
type packVerdict struct {
	wait   bool
	reject string
}

// resolvePack finds the case GTIN and units per pack of a carton from the
// local packaging view. The carton's content GTIN is either the case GTIN
// itself or the each GTIN of the SKU, in which case the SKU's active CASE
// level is used. An unknown GTIN waits: a pack level is never guessed.
func (l *ContainerLinker) resolvePack(ctx context.Context, link container.PendingLink) (string, int, packVerdict, error) {
	level, err := l.Packaging.FindByGTIN(ctx, link.GTIN)
	if err != nil {
		return "", 0, packVerdict{}, err
	}
	if level == nil {
		return "", 0, packVerdict{wait: true}, nil
	}
	if level.SKU != link.SKU {
		return "", 0, packVerdict{reject: rejectGTINOtherSKU}, nil
	}
	switch level.Level {
	case packaging.LevelCase:
		return level.GTIN.String(), level.UnitsPerPack, packVerdict{}, nil
	case packaging.LevelEach:
		return l.activeCase(ctx, link.SKU)
	}
	return "", 0, packVerdict{reject: rejectNotACaseGTIN}, nil
}

func (l *ContainerLinker) activeCase(ctx context.Context, sku shared.SKU) (string, int, packVerdict, error) {
	active, err := l.Packaging.FindActive(ctx, sku, packaging.LevelCase, l.Clock.Now())
	if err != nil {
		return "", 0, packVerdict{}, err
	}
	if active == nil {
		return "", 0, packVerdict{wait: true}, nil
	}
	return active.GTIN.String(), active.UnitsPerPack, packVerdict{}, nil
}
