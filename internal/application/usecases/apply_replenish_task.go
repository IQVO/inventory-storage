package usecases

import (
	"context"
	"fmt"
	"strings"

	"github.com/claudioed/inventory-storage/internal/application/ports"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
)

// ReplenishTaskType is the task_type fulfillment-execution publishes on
// TaskCompleted for a pick-face replenishment task (ADR 0041).
const ReplenishTaskType = "REPLENISH"

// ReplenishTaskConsumer is the processed_events consumer name under which
// ApplyReplenishTask claims CloudEvents ids (separate from the RELOCATE and
// confirm-pick consumers').
const ReplenishTaskConsumer = "task-completed-replenish"

// A REPLENISH completion that can never be applied (missing or invalid fields,
// request_ref included, or a deterministic ledger rejection) is reported with
// ErrMalformedRelocationTask, the same error a RELOCATE completion uses: it is
// the same ledger move, so the consumer dead-letters both alike.

// ReplenishTaskCompletion is one fulfillment-execution TaskCompleted for a
// REPLENISH task, as decoded by the inbound adapter (wire values). RequestRef
// is the replenishment REQUEST id (pick-face-replenishment); it is REQUIRED.
type ReplenishTaskCompletion struct {
	EventID  string
	TaskID   string
	TaskType string
	SKU      string
	FromBin  string
	ToBin    string
	// Quantity is the operator-confirmed count; nil when absent.
	Quantity   *int
	RequestRef string
}

// ReplenishTaskOutcome says what ApplyReplenishTask did with one event.
type ReplenishTaskOutcome string

const (
	// ReplenishIgnored: not a REPLENISH task. Nothing was claimed.
	ReplenishIgnored ReplenishTaskOutcome = "IGNORED"
	// ReplenishDuplicate: this CloudEvents id was already applied.
	ReplenishDuplicate ReplenishTaskOutcome = "DUPLICATE"
	// ReplenishApplied: the move was applied (see Relocation).
	ReplenishApplied ReplenishTaskOutcome = "APPLIED"
)

// ReplenishTaskResult is the per-event summary.
type ReplenishTaskResult struct {
	Outcome    ReplenishTaskOutcome
	Relocation *relocation.Relocation
}

// ApplyReplenishTask applies a completed REPLENISH task to the ledger
// (ADR 0041) exactly like ApplyRelocationTask applies a RELOCATE one: the
// CloudEvents id claim and the whole move commit in ONE UnitOfWork, so a
// redelivery is a no-op and a failure un-claims the id. The site is the
// source bin's site; the reason is REPLENISHMENT; the source ref is the
// request_ref (required, no task-id fallback: pick-face-replenishment closes
// a request by source_ref == request id).
type ApplyReplenishTask struct {
	Relocate        relocator
	ProcessedEvents ports.ProcessedEventRepo
	// UnitOfWork brackets the claim and the move. Optional: nil means "no
	// transactional backing" (in-memory runs).
	UnitOfWork ports.UnitOfWork
}

// Execute handles one completion. A deterministic problem returns an error
// wrapping ErrMalformedRelocationTask; a transient one (database,
// ErrConcurrentModification, cancellation) is returned unchanged.
func (uc *ApplyReplenishTask) Execute(ctx context.Context, in ReplenishTaskCompletion) (ReplenishTaskResult, error) {
	if in.TaskType != ReplenishTaskType {
		return ReplenishTaskResult{Outcome: ReplenishIgnored}, nil
	}
	cmd, err := replenishCommand(in)
	if err != nil {
		return ReplenishTaskResult{}, err
	}

	var result ReplenishTaskResult
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		result = ReplenishTaskResult{}
		claimed, err := uc.ProcessedEvents.Claim(ctx, ReplenishTaskConsumer, in.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			result.Outcome = ReplenishDuplicate
			return nil
		}
		rel, err := uc.Relocate.Execute(ctx, cmd)
		if err != nil {
			return classifyRelocationError(err)
		}
		result = ReplenishTaskResult{Outcome: ReplenishApplied, Relocation: rel}
		return nil
	})
	if err != nil {
		return ReplenishTaskResult{}, err
	}
	return result, nil
}

// replenishCommand validates the wire fields and builds the command. The
// shared field checks are those of a RELOCATE completion; what differs is the
// required request_ref (the source ref) and the REPLENISHMENT reason.
func replenishCommand(in ReplenishTaskCompletion) (RelocateStockCommand, error) {
	ref := strings.TrimSpace(in.RequestRef)
	if ref == "" {
		return RelocateStockCommand{}, fmt.Errorf("%w: request_ref is missing", ErrMalformedRelocationTask)
	}
	cmd, err := relocationCommand(RelocationTaskCompletion{
		EventID: in.EventID, TaskID: in.TaskID, TaskType: in.TaskType,
		SKU: in.SKU, FromBin: in.FromBin, ToBin: in.ToBin, Quantity: in.Quantity,
		RelocationRef: ref,
	})
	if err != nil {
		return RelocateStockCommand{}, err
	}
	cmd.Reason = relocation.ReasonReplenishment
	return cmd, nil
}
