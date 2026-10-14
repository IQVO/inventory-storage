package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
	"github.com/claudioed/inventory-storage/internal/domain/stock"
)

// RelocateTaskType is the task_type fulfillment-execution publishes on
// TaskCompleted for a relocation task (decision E1, ADR 0039).
const RelocateTaskType = "RELOCATE"

// RelocateTaskConsumer is the processed_events consumer name under which
// ApplyRelocationTask claims CloudEvents ids (separate from confirm-pick's).
const RelocateTaskConsumer = "task-completed-relocate"

// ErrMalformedRelocationTask marks a RELOCATE completion that can never be
// applied: missing or invalid fields, or a move the ledger deterministically
// rejects (unknown bin, bin not in the site, same bin, not enough usable
// stock, destination full). The consumer dead-letters it immediately.
var ErrMalformedRelocationTask = errors.New("malformed relocation task")

// RelocationTaskCompletion is one fulfillment-execution TaskCompleted for a
// RELOCATE task, as decoded by the inbound adapter (wire values). SKU,
// FromBin, ToBin, Quantity and RelocationRef are the ADDITIVE TaskCompleted
// fields fulfillment-execution emits for RELOCATE tasks only.
type RelocationTaskCompletion struct {
	EventID  string
	TaskID   string
	TaskType string
	SKU      string
	FromBin  string
	ToBin    string
	// Quantity is the operator-confirmed count; nil when absent.
	Quantity      *int
	RelocationRef string
}

// RelocationTaskOutcome says what ApplyRelocationTask did with one event.
type RelocationTaskOutcome string

const (
	// RelocationIgnored: not a RELOCATE task. Nothing was claimed.
	RelocationIgnored RelocationTaskOutcome = "IGNORED"
	// RelocationDuplicate: this CloudEvents id was already applied.
	RelocationDuplicate RelocationTaskOutcome = "DUPLICATE"
	// RelocationApplied: the move was applied (see Relocation).
	RelocationApplied RelocationTaskOutcome = "APPLIED"
)

// RelocationTaskResult is the per-event summary.
type RelocationTaskResult struct {
	Outcome    RelocationTaskOutcome
	Relocation *relocation.Relocation
}

// relocator is the move this use case drives; satisfied by *RelocateStock.
type relocator interface {
	Execute(ctx context.Context, cmd RelocateStockCommand) (*relocation.Relocation, error)
}

// ApplyRelocationTask applies a completed RELOCATE task to the ledger
// (ADR 0039): the CloudEvents id claim and the whole move commit in ONE
// UnitOfWork, so a redelivery is a no-op and a failure un-claims the id.
// The site is the source bin's site (TaskCompleted carries none); the reason
// is SLOTTING (RELOCATE tasks execute slot plans, slotting-optimization
// ADR 0010); the source ref is relocation_ref, or the task id when absent.
type ApplyRelocationTask struct {
	Relocate        relocator
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and the move. Optional: nil means "no
	// transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
}

// Execute handles one completion. A deterministic problem returns an error
// wrapping ErrMalformedRelocationTask; a transient one (database,
// ErrConcurrentModification, cancellation) is returned unchanged.
func (uc *ApplyRelocationTask) Execute(ctx context.Context, in RelocationTaskCompletion) (RelocationTaskResult, error) {
	if in.TaskType != RelocateTaskType {
		return RelocationTaskResult{Outcome: RelocationIgnored}, nil
	}
	cmd, err := relocationCommand(in)
	if err != nil {
		return RelocationTaskResult{}, err
	}

	var result RelocationTaskResult
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		result = RelocationTaskResult{}
		claimed, err := uc.ProcessedEvents.Claim(ctx, RelocateTaskConsumer, in.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			result.Outcome = RelocationDuplicate
			return nil
		}
		rel, err := uc.Relocate.Execute(ctx, cmd)
		if err != nil {
			return classifyRelocationError(err)
		}
		result = RelocationTaskResult{Outcome: RelocationApplied, Relocation: rel}
		return nil
	})
	if err != nil {
		return RelocationTaskResult{}, err
	}
	return result, nil
}

// relocationCommand validates the wire fields and builds the command.
func relocationCommand(in RelocationTaskCompletion) (RelocateStockCommand, error) {
	if in.EventID == "" {
		return RelocateStockCommand{}, fmt.Errorf("%w: empty event id", ErrMalformedRelocationTask)
	}
	sku, err := shared.NewSKU(strings.TrimSpace(in.SKU))
	if err != nil {
		return RelocateStockCommand{}, fmt.Errorf("%w: sku: %v", ErrMalformedRelocationTask, err)
	}
	from, err := shared.NewBinId(strings.TrimSpace(in.FromBin))
	if err != nil {
		return RelocateStockCommand{}, fmt.Errorf("%w: from_bin: %v", ErrMalformedRelocationTask, err)
	}
	to, err := shared.NewBinId(strings.TrimSpace(in.ToBin))
	if err != nil {
		return RelocateStockCommand{}, fmt.Errorf("%w: to_bin: %v", ErrMalformedRelocationTask, err)
	}
	if in.Quantity == nil {
		return RelocateStockCommand{}, fmt.Errorf("%w: quantity is missing", ErrMalformedRelocationTask)
	}
	qty, err := shared.NewPositiveQuantity(*in.Quantity)
	if err != nil {
		return RelocateStockCommand{}, fmt.Errorf("%w: quantity %d: %v", ErrMalformedRelocationTask, *in.Quantity, err)
	}
	ref := strings.TrimSpace(in.RelocationRef)
	if ref == "" {
		ref = in.TaskID
	}
	return RelocateStockCommand{SKU: sku, FromBin: from, ToBin: to, Quantity: qty, Reason: relocation.ReasonSlotting, SourceRef: ref}, nil
}

// relocationRejections are the deterministic ledger rejections of a move:
// retrying the same completion can never succeed.
var relocationRejections = []error{
	ErrBinNotFound,
	relocation.ErrBinNotAtSite,
	relocation.ErrSameBin,
	relocation.ErrSourceRefTooLong,
	relocation.ErrUnknownReason,
	stock.ErrInsufficientUsable,
	location.ErrBinFull,
	location.ErrReleaseExceedsOccupancy,
	shared.ErrZeroQuantity,
}

// classifyRelocationError wraps a deterministic rejection in
// ErrMalformedRelocationTask and returns anything else unchanged.
func classifyRelocationError(err error) error {
	for _, rejection := range relocationRejections {
		if errors.Is(err, rejection) {
			return fmt.Errorf("%w: %w", ErrMalformedRelocationTask, err)
		}
	}
	return err
}
