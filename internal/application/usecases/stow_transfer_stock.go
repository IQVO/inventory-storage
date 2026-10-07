package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
	"github.com/claudioed/inventory-storage/internal/domain/transfer"
)

// StowTransferStock places a STAGED transfer receipt's goods into
// destination bins (ADR 0031, Phase 3) — the ONLY path that raises the
// destination site's usable stock for a transfer, exactly once per
// transfer_line_id.
//
// Rules, in order:
//
//  1. The receipt must exist and be STAGED (stow-before-stage is
//     rejected with ErrReceiptNotStaged; an already-STOWED receipt is
//     the replay case below).
//  2. Every requested bin must be registered and belong to the
//     receipt's destination_site_id — the site-custody fact (ADR
//     0030/0031) fails closed: a bin at another site, or a legacy
//     site-less bin, rejects with ErrBinWrongSite.
//  3. The bin quantities must sum EXACTLY to the receipt's
//     received_quantity (ErrStowQuantityMismatch otherwise): a partial
//     stow would strand dock stock, a padded one would invent it.
//  4. One StockUnit per bin leg is created AT the destination site
//     (stock.NewStockUnitAtSite), the bin's occupancy is taken, the
//     receipt moves STAGED -> STOWED with its stow legs recorded, and
//     TransferStockStowed is published — all in ONE UnitOfWork.
//
// Idempotency is DATABASE-level: a replayed stow for a STOWED receipt
// returns the ORIGINAL outcome (the recorded stow legs and their
// StockUnits) with Replay=true, creates NO second StockUnit, and
// publishes nothing new. The unique transfer_receipts row plus the
// STOWED-state guard (SaveStowed refuses a non-STAGED receipt) back
// this at the database level — a concurrent double-stow loses the
// state race and is routed to the replay path.
type StowTransferStock struct {
	Receipts   ports.TransferReceiptRepo
	Stock      ports.StockRepo
	Locations  ports.LocationRepo
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

// StowBin is one requested destination placement: bin_id plus the
// quantity to place there.
type StowBin struct {
	BinID    shared.BinId
	Quantity shared.Quantity
}

// ErrMalformedStowCommand marks a stow command that failed validation.
var ErrMalformedStowCommand = errors.New("transfer stow command failed validation")

// ValidateStowCommand checks the command's own shape: the line id and
// at least one positive-quantity bin leg.
func ValidateStowCommand(lineID string, bins []StowBin) error {
	if lineID == "" {
		return fmt.Errorf("%w: transfer_line_id is empty", ErrMalformedStowCommand)
	}
	if len(bins) == 0 {
		return fmt.Errorf("%w: at least one bin placement is required", ErrMalformedStowCommand)
	}
	for i, bin := range bins {
		if _, err := shared.NewBinId(bin.BinID.String()); err != nil {
			return fmt.Errorf("%w: bins[%d].bin_id is empty", ErrMalformedStowCommand, i)
		}
		if bin.Quantity.Int() <= 0 {
			return fmt.Errorf("%w: bins[%d].quantity must be positive", ErrMalformedStowCommand, i)
		}
	}
	return nil
}

// StowResult is the outcome of one stow: the STOWED receipt, the
// destination StockUnits created (one per leg), and whether this
// execution was a replay of an earlier stow.
type StowResult struct {
	Receipt    *transfer.Receipt
	StockUnits []*stock.StockUnit
	Replay     bool
}

// Execute places one staged receipt's goods. See the type doc.
func (uc *StowTransferStock) Execute(ctx context.Context, lineID string, bins []StowBin) (*StowResult, error) {
	if err := ValidateStowCommand(lineID, bins); err != nil {
		return nil, err
	}

	receipt, err := uc.Receipts.FindByTransferLineID(ctx, lineID)
	if err != nil {
		return nil, err
	}
	if receipt == nil {
		return nil, ErrReceiptNotStaged
	}
	if receipt.State() == transfer.ReceiptStowed {
		// Idempotent replay: return the original outcome, create no
		// second StockUnit, publish nothing new.
		return uc.replayResult(ctx, receipt)
	}

	var decided *StowResult
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		return uc.decide(ctx, lineID, bins, &decided)
	})
	if err != nil {
		// Lost the state race to a concurrent stow: its outcome stands;
		// answer this caller with the original.
		if errors.Is(err, ErrTransferReceiptAlreadyStowed) {
			current, ferr := uc.Receipts.FindByTransferLineID(ctx, lineID)
			if ferr == nil && current != nil && current.State() == transfer.ReceiptStowed {
				return uc.replayResult(ctx, current)
			}
		}
		return nil, err
	}
	if decided != nil {
		return decided, nil
	}

	// Happy path committed; re-read for the persisted terminal state.
	return uc.committedResult(ctx, lineID)
}

// decide runs inside the caller's UnitOfWork: it re-reads the receipt
// (a concurrent stow may have completed since the outer read) and
// either replays, refuses, or performs the stow.
func (uc *StowTransferStock) decide(ctx context.Context, lineID string, bins []StowBin, decided **StowResult) error {
	current, err := uc.Receipts.FindByTransferLineID(ctx, lineID)
	if err != nil {
		return err
	}
	if current == nil {
		return ErrReceiptNotStaged
	}
	if current.State() == transfer.ReceiptStowed {
		res, replayErr := uc.replayResult(ctx, current)
		*decided = res
		return replayErr
	}
	return uc.stow(ctx, current, bins)
}

// committedResult rebuilds the happy-path answer from the persisted
// terminal state: the STOWED receipt plus its stow legs' StockUnits.
func (uc *StowTransferStock) committedResult(ctx context.Context, lineID string) (*StowResult, error) {
	final, err := uc.Receipts.FindByTransferLineID(ctx, lineID)
	if err != nil {
		return nil, err
	}
	if final == nil || final.State() != transfer.ReceiptStowed {
		return nil, fmt.Errorf("stow for line %q committed but the receipt is not STOWED", lineID)
	}
	units := make([]*stock.StockUnit, 0, len(final.StowLegs()))
	for _, leg := range final.StowLegs() {
		unit, err := uc.Stock.FindByID(ctx, leg.StockUnitID)
		if err != nil {
			return nil, err
		}
		if unit == nil {
			return nil, fmt.Errorf("stow leg %q committed but its stock unit is missing", leg.StockUnitID)
		}
		units = append(units, unit)
	}
	return &StowResult{Receipt: final, StockUnits: units, Replay: false}, nil
}

// stowBinLeg is one validated destination placement: the loaded Bin
// aggregate plus the quantity to place in it.
type stowBinLeg struct {
	bin *location.Bin
	qty shared.Quantity
}

// loadStowLegs loads and site-checks every requested bin BEFORE any
// write happens: no partial stow may ever commit. Every bin must be
// registered and belong to the receipt's destination site's custody,
// and the quantities must sum exactly to the received quantity.
func (uc *StowTransferStock) loadStowLegs(ctx context.Context, receipt *transfer.Receipt, bins []StowBin) ([]stowBinLeg, error) {
	legs := make([]stowBinLeg, 0, len(bins))
	total := shared.Quantity(0)
	for _, req := range bins {
		bin, err := uc.Locations.FindByID(ctx, req.BinID)
		if err != nil {
			return nil, err
		}
		if bin == nil {
			return nil, ErrBinNotFound
		}
		if !bin.IsAtSite(receipt.DestinationSiteID()) {
			return nil, fmt.Errorf("%w: bin %q is not in destination site %q's custody", ErrBinWrongSite, req.BinID, receipt.DestinationSiteID())
		}
		legs = append(legs, stowBinLeg{bin: bin, qty: req.Quantity})
		total = total.Add(req.Quantity)
	}
	if total != receipt.ReceivedQuantity() {
		return nil, fmt.Errorf("%w: bins sum to %d but the receipt received %d", ErrStowQuantityMismatch, total.Int(), receipt.ReceivedQuantity().Int())
	}
	return legs, nil
}

// stow runs inside the caller's UnitOfWork: validates the bins against
// the destination site's custody, creates the StockUnits, marks the
// receipt STOWED and queues TransferStockStowed — one transaction.
func (uc *StowTransferStock) stow(ctx context.Context, receipt *transfer.Receipt, bins []StowBin) error {
	legs, err := uc.loadStowLegs(ctx, receipt, bins)
	if err != nil {
		return err
	}

	now := uc.Clock.Now()
	stowLegs := make([]transfer.StowLeg, 0, len(legs))
	for _, leg := range legs {
		placed, err := uc.placeLeg(ctx, receipt, leg)
		if err != nil {
			return err
		}
		stowLegs = append(stowLegs, placed)
	}

	if err := receipt.MarkStowed(stowLegs, now); err != nil {
		return err
	}
	if err := uc.Receipts.SaveStowed(ctx, receipt); err != nil {
		return err
	}
	return uc.publishStowed(ctx, now, receipt, stowLegs)
}

// placeLeg occupies the bin, creates the destination-site StockUnit and
// persists both, returning the recorded stow leg.
func (uc *StowTransferStock) placeLeg(ctx context.Context, receipt *transfer.Receipt, leg stowBinLeg) (transfer.StowLeg, error) {
	if err := leg.bin.Occupy(leg.qty); err != nil {
		return transfer.StowLeg{}, err
	}
	id, err := uc.Stock.NextID(ctx)
	if err != nil {
		return transfer.StowLeg{}, err
	}
	unit, err := stock.NewStockUnitAtSite(id, receipt.SKU(), leg.bin.ID(), leg.qty, receipt.DestinationSiteID())
	if err != nil {
		return transfer.StowLeg{}, err
	}
	if err := uc.Locations.Save(ctx, leg.bin); err != nil {
		return transfer.StowLeg{}, err
	}
	if err := uc.Stock.Save(ctx, unit); err != nil {
		return transfer.StowLeg{}, err
	}
	return transfer.StowLeg{StockUnitID: unit.ID(), BinID: leg.bin.ID(), Quantity: leg.qty}, nil
}

// publishStowed queues the TransferStockStowed event with the created
// allocations and the summed stowed quantity.
func (uc *StowTransferStock) publishStowed(ctx context.Context, now time.Time, receipt *transfer.Receipt, stowLegs []transfer.StowLeg) error {
	wireLegs := make([]shared.TransferAllocationLeg, 0, len(stowLegs))
	stowedQty := shared.Quantity(0)
	for _, leg := range stowLegs {
		wireLegs = append(wireLegs, shared.TransferAllocationLeg{StockUnitID: leg.StockUnitID, BinID: leg.BinID, Quantity: leg.Quantity})
		stowedQty = stowedQty.Add(leg.Quantity)
	}
	return uc.Events.Publish(ctx, shared.NewTransferStockStowed(
		now, receipt.TransferID(), receipt.TransferLineID(), receipt.DestinationSiteID(), receipt.SKU(),
		receipt.ReceivedQuantity(), stowedQty, wireLegs,
	))
}

// replayResult loads the ORIGINAL stow outcome for an already-STOWED
// receipt: the recorded stow legs' StockUnits. No new unit, no event.
func (uc *StowTransferStock) replayResult(ctx context.Context, receipt *transfer.Receipt) (*StowResult, error) {
	units := make([]*stock.StockUnit, 0, len(receipt.StowLegs()))
	for _, leg := range receipt.StowLegs() {
		unit, err := uc.Stock.FindByID(ctx, leg.StockUnitID)
		if err != nil {
			return nil, err
		}
		if unit == nil {
			return nil, fmt.Errorf("stowed receipt %q references missing stock unit %q", receipt.TransferLineID(), leg.StockUnitID)
		}
		units = append(units, unit)
	}
	return &StowResult{Receipt: receipt, StockUnits: units, Replay: true}, nil
}
