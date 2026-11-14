package kafka

import (
	"context"
	"log/slog"
	"testing"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// Pinned-contract tests for the network-inventory-planning edge (the
// transfer-allocation COMMAND consumer). The pinned copy is byte-equal to
// IQVO/network-inventory-planning develop (contracts/pinned.json).
// Fixtures: this producer ships no message examples, so E2 fixtures are
// DERIVED from the pinned schema's own consts and required data fields
// (pinnedDerivedFixture) — provenance: derived-from-schema.
const nipPinnedSpec = "testdata/network-inventory-planning.asyncapi.yaml"

type pinnedAllocator struct{ cmds []usecases.TransferCommand }

func (a *pinnedAllocator) Execute(_ context.Context, cmd usecases.TransferCommand) (*usecases.Result, error) {
	a.cmds = append(a.cmds, cmd)
	return &usecases.Result{}, nil
}

type pinnedReleaser struct {
	cmds []usecases.TransferReleaseCommand
}

func (r *pinnedReleaser) Execute(_ context.Context, cmd usecases.TransferReleaseCommand) (*usecases.ReleaseResult, error) {
	r.cmds = append(r.cmds, cmd)
	return &usecases.ReleaseResult{}, nil
}

func nipConsumer(a *pinnedAllocator, r *pinnedReleaser) *Consumer {
	return &Consumer{Allocate: a, Release: r, Logger: slog.Default()}
}

// E1: both command types our consumer dispatches on stay in the producer's
// channel catalog (this pinned spec uses channel-level messages:, not
// subscribe/publish operations — the collector handles both shapes).
func TestTransferPinCoversOurSubscription(t *testing.T) {
	doc := loadPinnedSpec(t, nipPinnedSpec)
	requirePinnedDefaultContentType(t, doc)
	types := pinnedChannelTypes(t, doc, Topic)
	for _, typ := range []string{typeTransferAllocationRequested, typeTransferAllocationReleaseRequested} {
		key, ok := types[typ]
		if !ok {
			t.Fatalf("pinned NIP spec no longer documents %s on %s", typ, Topic)
		}
		requirePinnedMessageContentType(t, doc, key)
	}
}

// E2: schema-derived fixtures of both command types decode through the real
// consumer and reach the matching handler port.
func TestTransferPinFixturesFlowThroughConsumer(t *testing.T) {
	doc := loadPinnedSpec(t, nipPinnedSpec)
	alloc, release := &pinnedAllocator{}, &pinnedReleaser{}
	c := nipConsumer(alloc, release)
	for _, typ := range []string{typeTransferAllocationRequested, typeTransferAllocationReleaseRequested} {
		fixture := pinnedDerivedFixture(t, doc, Topic, typ)
		if err := c.Handle(t.Context(), pinnedKafkaMessage(t, Topic, fixture)); err != nil {
			t.Fatalf("handle derived %s fixture: %v", typ, err)
		}
	}
	if len(alloc.cmds) != 1 || alloc.cmds[0].Quantity != 1 {
		t.Fatalf("allocate handler saw %+v", alloc.cmds)
	}
	if len(release.cmds) != 1 {
		t.Fatalf("release handler saw %+v", release.cmds)
	}
}

// E3: the derived envelopes carry the fleet CloudEvents 1.0 invariants (the
// consts they were built from are exactly what the pin declares).
func TestTransferPinEnvelopeInvariants(t *testing.T) {
	doc := loadPinnedSpec(t, nipPinnedSpec)
	for _, typ := range []string{typeTransferAllocationRequested, typeTransferAllocationReleaseRequested} {
		requirePinnedEnvelope(t, pinnedDerivedFixture(t, doc, Topic, typ))
	}
}

// E4: unknown data keys are tolerated; a documented-but-unhandled type is
// skipped; a non-CloudEvent body is skipped (never retried, never poisoned).
func TestTransferPinTolerancePolicy(t *testing.T) {
	doc := loadPinnedSpec(t, nipPinnedSpec)
	fixture := pinnedDerivedFixture(t, doc, Topic, typeTransferAllocationRequested)
	alloc, release := &pinnedAllocator{}, &pinnedReleaser{}
	c := nipConsumer(alloc, release)
	if err := c.Handle(t.Context(), pinnedKafkaMessage(t, Topic, pinnedWithUnknownDataKey(t, fixture))); err != nil {
		t.Fatalf("unknown data key must not fail: %v", err)
	}
	if len(alloc.cmds) != 1 {
		t.Fatal("unknown data key must still reach the handler")
	}

	for typ := range pinnedChannelTypes(t, doc, Topic) {
		if typ == typeTransferAllocationRequested || typ == typeTransferAllocationReleaseRequested {
			continue
		}
		skipAlloc := &pinnedAllocator{}
		cs := nipConsumer(skipAlloc, &pinnedReleaser{})
		if err := cs.Handle(t.Context(), pinnedKafkaMessage(t, Topic, pinnedDerivedFixture(t, doc, Topic, typ))); err != nil {
			t.Fatalf("documented-but-unhandled type %s must be skipped, got %v", typ, err)
		}
		if len(skipAlloc.cmds) != 0 {
			t.Fatalf("unhandled type %s reached the handler", typ)
		}
	}

	if err := nipConsumer(&pinnedAllocator{}, &pinnedReleaser{}).Handle(
		t.Context(), kafkaMessage{Topic: Topic, Value: []byte("not-a-cloudevent")}); err != nil {
		t.Fatalf("non-CloudEvent must be skipped, got %v", err)
	}
}
