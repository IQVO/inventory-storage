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

func replenishTask() usecases.ReplenishTaskCompletion {
	return usecases.ReplenishTaskCompletion{
		EventID: "evt-r1", TaskID: "task-r1", TaskType: usecases.ReplenishTaskType,
		SKU: "SKU-1", FromBin: string(relFrom), ToBin: string(relTo), Quantity: intPtr(4),
		RequestRef: "repl-3b0e8d2a",
	}
}

func newApplyReplenishTask(t *testing.T) (relocationEnv, *usecases.ApplyReplenishTask) {
	t.Helper()
	e := newRelocationEnv(t)
	return e, &usecases.ApplyReplenishTask{Relocate: e.uc, ProcessedEvents: memory.NewProcessedEventRepo()}
}

func TestApplyReplenishTask_AppliesTheMoveAsReplenishment(t *testing.T) {
	e, uc := newApplyReplenishTask(t)

	res, err := uc.Execute(context.Background(), replenishTask())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Outcome != usecases.ReplenishApplied || res.Relocation == nil {
		t.Fatalf("outcome %q relocation %v, want APPLIED", res.Outcome, res.Relocation)
	}
	rel := res.Relocation
	if rel.SiteID() != "WH1" || rel.Reason() != relocation.ReasonReplenishment || rel.SourceRef() != "repl-3b0e8d2a" || rel.Quantity() != 4 {
		t.Fatalf("relocation wrong: site %q reason %q ref %q qty %d", rel.SiteID(), rel.Reason(), rel.SourceRef(), rel.Quantity())
	}
	if e.source.Quantity() != 6 {
		t.Fatalf("source quantity %d, want 6", e.source.Quantity())
	}
}

func TestApplyReplenishTask_RequestRefIsTrimmedAndNeverFallsBackToTheTaskID(t *testing.T) {
	_, uc := newApplyReplenishTask(t)
	in := replenishTask()
	in.RequestRef = "  repl-9  "

	res, err := uc.Execute(context.Background(), in)
	if err != nil || res.Relocation.SourceRef() != "repl-9" {
		t.Fatalf("source ref %v err %v, want the trimmed request_ref", res.Relocation, err)
	}
	for _, blank := range []string{"", "   "} {
		_, uc := newApplyReplenishTask(t)
		in.RequestRef = blank
		if _, err := uc.Execute(context.Background(), in); !errors.Is(err, usecases.ErrMalformedRelocationTask) {
			t.Fatalf("request_ref %q: err = %v, want malformed (no task-id fallback)", blank, err)
		}
	}
}

func TestApplyReplenishTask_RedeliveryIsADuplicate(t *testing.T) {
	e, uc := newApplyReplenishTask(t)
	ctx := context.Background()
	if _, err := uc.Execute(ctx, replenishTask()); err != nil {
		t.Fatalf("first: %v", err)
	}

	res, err := uc.Execute(ctx, replenishTask())
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if res.Outcome != usecases.ReplenishDuplicate || res.Relocation != nil {
		t.Fatalf("outcome %q, want DUPLICATE", res.Outcome)
	}
	if e.source.Quantity() != 6 || len(e.Events.Events()) != 1 {
		t.Fatalf("a redelivery moved stock again: source %d events %d", e.source.Quantity(), len(e.Events.Events()))
	}
}

func TestApplyReplenishTask_IgnoresOtherTaskTypes(t *testing.T) {
	e, uc := newApplyReplenishTask(t)
	for _, tt := range []string{"PICK", "RELOCATE", "", "replenish"} {
		in := replenishTask()
		in.TaskType = tt
		res, err := uc.Execute(context.Background(), in)
		if err != nil || res.Outcome != usecases.ReplenishIgnored {
			t.Fatalf("task type %q: outcome %q err %v, want IGNORED", tt, res.Outcome, err)
		}
	}
	if e.source.Quantity() != 10 {
		t.Fatal("an ignored completion moved stock")
	}
}

func TestApplyReplenishTask_MalformedCompletionsAreDeterministic(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*usecases.ReplenishTaskCompletion)
	}{
		{"no event id", func(c *usecases.ReplenishTaskCompletion) { c.EventID = "" }},
		{"no request ref", func(c *usecases.ReplenishTaskCompletion) { c.RequestRef = "" }},
		{"no sku", func(c *usecases.ReplenishTaskCompletion) { c.SKU = " " }},
		{"no from bin", func(c *usecases.ReplenishTaskCompletion) { c.FromBin = "" }},
		{"no to bin", func(c *usecases.ReplenishTaskCompletion) { c.ToBin = "" }},
		{"no quantity", func(c *usecases.ReplenishTaskCompletion) { c.Quantity = nil }},
		{"zero quantity", func(c *usecases.ReplenishTaskCompletion) { c.Quantity = intPtr(0) }},
		{"negative quantity", func(c *usecases.ReplenishTaskCompletion) { c.Quantity = intPtr(-3) }},
		{"same bin", func(c *usecases.ReplenishTaskCompletion) { c.ToBin = c.FromBin }},
		{"unknown bin", func(c *usecases.ReplenishTaskCompletion) { c.ToBin = "WH1-STOR-AMB-Z99-01-01-A" }},
		{"bin in another site", func(c *usecases.ReplenishTaskCompletion) { c.ToBin = string(relOther) }},
		{"more than usable", func(c *usecases.ReplenishTaskCompletion) { c.Quantity = intPtr(11) }},
		{"request ref too long", func(c *usecases.ReplenishTaskCompletion) { c.RequestRef = strings.Repeat("r", 201) }},
	}
	for _, tc := range cases {
		e, uc := newApplyReplenishTask(t)
		in := replenishTask()
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

func TestApplyReplenishTask_DestinationFullIsDeterministic(t *testing.T) {
	e, uc := newApplyReplenishTask(t)
	small, _ := location.NewBin("WH1-STOR-AMB-A03-01-01-A", 3)
	_ = e.Locations.Save(context.Background(), small)
	in := replenishTask()
	in.ToBin = string(small.ID())

	_, err := uc.Execute(context.Background(), in)
	if !errors.Is(err, usecases.ErrMalformedRelocationTask) || !errors.Is(err, location.ErrBinFull) {
		t.Fatalf("err = %v, want ErrMalformedRelocationTask wrapping ErrBinFull", err)
	}
}

func TestApplyReplenishTask_TransientFailuresAreNotMalformed(t *testing.T) {
	for _, transient := range []error{errFake, usecases.ErrConcurrentModification, context.Canceled} {
		_, uc := newApplyReplenishTask(t)
		uc.Relocate = failingRelocator{err: transient}

		_, err := uc.Execute(context.Background(), replenishTask())
		if !errors.Is(err, transient) || errors.Is(err, usecases.ErrMalformedRelocationTask) {
			t.Fatalf("err = %v, want %v unwrapped as transient", err, transient)
		}
	}
}

func TestApplyReplenishTask_ClaimFailureIsReturned(t *testing.T) {
	_, uc := newApplyReplenishTask(t)
	uc.ProcessedEvents = failingClaims{}

	if _, err := uc.Execute(context.Background(), replenishTask()); !errors.Is(err, errFake) {
		t.Fatalf("err = %v, want the claim's error", err)
	}
}

func TestApplyReplenishTask_ClaimsUnderItsOwnConsumerName(t *testing.T) {
	_, uc := newApplyReplenishTask(t)
	claims := memory.NewProcessedEventRepo()
	uc.ProcessedEvents = claims
	// The same CloudEvent id claimed by the RELOCATE branch must not block the
	// replenish branch (and vice versa).
	if _, err := claims.Claim(context.Background(), usecases.RelocateTaskConsumer, "evt-r1"); err != nil {
		t.Fatal(err)
	}

	res, err := uc.Execute(context.Background(), replenishTask())
	if err != nil || res.Outcome != usecases.ReplenishApplied {
		t.Fatalf("outcome %q err %v, want APPLIED", res.Outcome, err)
	}
	again, _ := claims.Claim(context.Background(), usecases.ReplenishTaskConsumer, "evt-r1")
	if again {
		t.Fatalf("the event id was not claimed under %q", usecases.ReplenishTaskConsumer)
	}
	if usecases.ReplenishTaskConsumer != "task-completed-replenish" {
		t.Fatalf("consumer name %q is part of the dedupe contract", usecases.ReplenishTaskConsumer)
	}
}

func TestApplyReplenishTask_RunsClaimAndMoveInOneUnitOfWork(t *testing.T) {
	_, uc := newApplyReplenishTask(t)
	uow := &countingUoW{}
	uc.UnitOfWork = uow

	if _, err := uc.Execute(context.Background(), replenishTask()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if uow.calls != 1 {
		t.Fatalf("UnitOfWork scopes %d, want 1", uow.calls)
	}
}
