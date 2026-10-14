package usecases_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/memory"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
	"github.com/claudioed/inventory-storage/internal/domain/location"
	"github.com/claudioed/inventory-storage/internal/domain/relocation"
)

func intPtr(v int) *int { return &v }

func relocateTask() usecases.RelocationTaskCompletion {
	return usecases.RelocationTaskCompletion{
		EventID: "evt-1", TaskID: "task-1", TaskType: usecases.RelocateTaskType,
		SKU: "SKU-1", FromBin: string(relFrom), ToBin: string(relTo), Quantity: intPtr(4),
		RelocationRef: "plan-7/move-2",
	}
}

func newApplyRelocationTask(t *testing.T) (relocationEnv, *usecases.ApplyRelocationTask) {
	t.Helper()
	e := newRelocationEnv(t)
	return e, &usecases.ApplyRelocationTask{Relocate: e.uc, ProcessedEvents: memory.NewProcessedEventRepo()}
}

func TestApplyRelocationTask_AppliesTheMove(t *testing.T) {
	e, uc := newApplyRelocationTask(t)

	res, err := uc.Execute(context.Background(), relocateTask())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Outcome != usecases.RelocationApplied || res.Relocation == nil {
		t.Fatalf("outcome %q relocation %v, want APPLIED", res.Outcome, res.Relocation)
	}
	rel := res.Relocation
	if rel.SiteID() != "WH1" || rel.Reason() != relocation.ReasonSlotting || rel.SourceRef() != "plan-7/move-2" || rel.Quantity() != 4 {
		t.Fatalf("relocation wrong: site %q reason %q ref %q qty %d", rel.SiteID(), rel.Reason(), rel.SourceRef(), rel.Quantity())
	}
	if e.source.Quantity() != 6 {
		t.Fatalf("source quantity %d, want 6", e.source.Quantity())
	}
}

func TestApplyRelocationTask_SourceRefFallsBackToTheTaskID(t *testing.T) {
	_, uc := newApplyRelocationTask(t)
	in := relocateTask()
	in.RelocationRef = "  "

	res, err := uc.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Relocation.SourceRef() != "task-1" {
		t.Fatalf("source ref %q, want the task id", res.Relocation.SourceRef())
	}
}

func TestApplyRelocationTask_RedeliveryIsADuplicate(t *testing.T) {
	e, uc := newApplyRelocationTask(t)
	ctx := context.Background()
	if _, err := uc.Execute(ctx, relocateTask()); err != nil {
		t.Fatalf("first: %v", err)
	}

	res, err := uc.Execute(ctx, relocateTask())
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if res.Outcome != usecases.RelocationDuplicate || res.Relocation != nil {
		t.Fatalf("outcome %q, want DUPLICATE", res.Outcome)
	}
	if e.source.Quantity() != 6 || len(e.Events.Events()) != 1 {
		t.Fatalf("a redelivery moved stock again: source %d events %d", e.source.Quantity(), len(e.Events.Events()))
	}
}

func TestApplyRelocationTask_IgnoresOtherTaskTypes(t *testing.T) {
	e, uc := newApplyRelocationTask(t)
	for _, tt := range []string{"PICK", "", "relocate"} {
		in := relocateTask()
		in.TaskType = tt
		res, err := uc.Execute(context.Background(), in)
		if err != nil || res.Outcome != usecases.RelocationIgnored {
			t.Fatalf("task type %q: outcome %q err %v, want IGNORED", tt, res.Outcome, err)
		}
	}
	if e.source.Quantity() != 10 {
		t.Fatal("an ignored completion moved stock")
	}
}

func TestApplyRelocationTask_MalformedCompletionsAreDeterministic(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*usecases.RelocationTaskCompletion)
	}{
		{"no event id", func(c *usecases.RelocationTaskCompletion) { c.EventID = "" }},
		{"no sku", func(c *usecases.RelocationTaskCompletion) { c.SKU = " " }},
		{"no from bin", func(c *usecases.RelocationTaskCompletion) { c.FromBin = "" }},
		{"no to bin", func(c *usecases.RelocationTaskCompletion) { c.ToBin = "" }},
		{"no quantity", func(c *usecases.RelocationTaskCompletion) { c.Quantity = nil }},
		{"zero quantity", func(c *usecases.RelocationTaskCompletion) { c.Quantity = intPtr(0) }},
		{"negative quantity", func(c *usecases.RelocationTaskCompletion) { c.Quantity = intPtr(-3) }},
		{"same bin", func(c *usecases.RelocationTaskCompletion) { c.ToBin = c.FromBin }},
		{"unknown bin", func(c *usecases.RelocationTaskCompletion) { c.ToBin = "WH1-STOR-AMB-Z99-01-01-A" }},
		{"bin in another site", func(c *usecases.RelocationTaskCompletion) { c.ToBin = string(relOther) }},
		{"more than usable", func(c *usecases.RelocationTaskCompletion) { c.Quantity = intPtr(11) }},
		{"relocation ref too long", func(c *usecases.RelocationTaskCompletion) { c.RelocationRef = strings.Repeat("r", 201) }},
	}
	for _, tc := range cases {
		e, uc := newApplyRelocationTask(t)
		in := relocateTask()
		tc.mut(&in)

		_, err := uc.Execute(context.Background(), in)
		if !errors.Is(err, usecases.ErrMalformedRelocationTask) {
			t.Fatalf("%s: err = %v, want ErrMalformedRelocationTask", tc.name, err)
		}
		if e.source.Quantity() != 10 {
			t.Fatalf("%s: a rejected completion moved stock", tc.name)
		}
	}
}

func TestApplyRelocationTask_DestinationFullIsDeterministic(t *testing.T) {
	e, uc := newApplyRelocationTask(t)
	small, _ := location.NewBin("WH1-STOR-AMB-A03-01-01-A", 3)
	_ = e.Locations.Save(context.Background(), small)
	in := relocateTask()
	in.ToBin = string(small.ID())

	_, err := uc.Execute(context.Background(), in)
	if !errors.Is(err, usecases.ErrMalformedRelocationTask) || !errors.Is(err, location.ErrBinFull) {
		t.Fatalf("err = %v, want ErrMalformedRelocationTask wrapping ErrBinFull", err)
	}
}

func TestApplyRelocationTask_TransientFailuresAreNotMalformed(t *testing.T) {
	for _, transient := range []error{errFake, usecases.ErrConcurrentModification, context.Canceled} {
		_, uc := newApplyRelocationTask(t)
		uc.Relocate = failingRelocator{err: transient}

		_, err := uc.Execute(context.Background(), relocateTask())
		if !errors.Is(err, transient) || errors.Is(err, usecases.ErrMalformedRelocationTask) {
			t.Fatalf("err = %v, want %v unwrapped as transient", err, transient)
		}
	}
}

func TestApplyRelocationTask_ClaimFailureIsReturned(t *testing.T) {
	_, uc := newApplyRelocationTask(t)
	uc.ProcessedEvents = failingClaims{}

	if _, err := uc.Execute(context.Background(), relocateTask()); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the claim's error", err)
	}
}

func TestApplyRelocationTask_ClaimsUnderItsOwnConsumerName(t *testing.T) {
	_, uc := newApplyRelocationTask(t)
	claims := memory.NewProcessedEventRepo()
	uc.ProcessedEvents = claims
	// The same CloudEvent id claimed by confirm-pick must not block the
	// relocation branch (and vice versa).
	if _, err := claims.Claim(context.Background(), usecases.ConfirmPicksConsumer, "evt-1"); err != nil {
		t.Fatal(err)
	}

	res, err := uc.Execute(context.Background(), relocateTask())
	if err != nil || res.Outcome != usecases.RelocationApplied {
		t.Fatalf("outcome %q err %v, want APPLIED", res.Outcome, err)
	}
	again, _ := claims.Claim(context.Background(), usecases.RelocateTaskConsumer, "evt-1")
	if again {
		t.Fatalf("the event id was not claimed under %q", usecases.RelocateTaskConsumer)
	}
}

func TestApplyRelocationTask_RunsClaimAndMoveInOneUnitOfWork(t *testing.T) {
	_, uc := newApplyRelocationTask(t)
	uow := &countingUoW{}
	uc.UnitOfWork = uow

	if _, err := uc.Execute(context.Background(), relocateTask()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork scopes %d, want 1", uow.calls)
	}
}

// failingRelocator stands in for RelocateStock and fails with err.
type failingRelocator struct{ err error }

func (f failingRelocator) Execute(context.Context, usecases.RelocateStockCommand) (*relocation.Relocation, error) {
	return nil, f.err
}
