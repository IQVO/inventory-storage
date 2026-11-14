package kafka

import (
	"errors"
	"log/slog"
	"testing"
)

// Pinned-contract tests for the fulfillment-execution edge. The pinned copy
// is byte-equal to IQVO/fulfillment-execution develop (contracts/pinned.json).
// Fixtures: the spec's own examples (producer ships 3 for TaskCompleted).
const fulfillmentPinnedSpec = "testdata/fulfillment-execution.asyncapi.yaml"

// E1: the topic our reader config pins (FulfillmentTopic) and the CloudEvents
// type our consumer handles must stay in the producer's message catalog.
func TestFulfillmentPinCoversOurSubscription(t *testing.T) {
	doc := loadPinnedSpec(t, fulfillmentPinnedSpec)
	requirePinnedDefaultContentType(t, doc)
	key, ok := pinnedChannelTypes(t, doc, FulfillmentTopic)[typeTaskCompleted]
	if !ok {
		t.Fatalf("pinned fulfillment spec no longer documents %s on %s", typeTaskCompleted, FulfillmentTopic)
	}
	requirePinnedMessageContentType(t, doc, key)
}

// E2: every spec example of the handled type decodes through the real
// consumer and reaches the real handler with the envelope's id and payload.
func TestFulfillmentPinExamplesFlowThroughConsumer(t *testing.T) {
	doc := loadPinnedSpec(t, fulfillmentPinnedSpec)
	for _, payload := range pinnedExamples(t, doc, FulfillmentTopic, typeTaskCompleted) {
		fake := &fakeConfirmer{}
		c := &TaskCompletedConsumer{Confirm: fake, Logger: slog.Default()}
		if err := c.Handle(t.Context(), pinnedKafkaMessage(t, FulfillmentTopic, payload)); err != nil {
			t.Fatalf("handle spec example %v: %v", payload["id"], err)
		}
		if len(fake.got) != 1 {
			t.Fatalf("example %v: handler called %d times, want 1", payload["id"], len(fake.got))
		}
		data := payload["data"].(map[string]any)
		if fake.got[0].EventID != payload["id"] || fake.got[0].TaskID != data["task_id"] {
			t.Fatalf("example %v: decoded completion = %+v", payload["id"], fake.got[0])
		}
	}
}

// E3: the pinned examples carry the fleet CloudEvents 1.0 invariants, and
// their dataschema matches the const the pinned schema declares.
func TestFulfillmentPinEnvelopeInvariants(t *testing.T) {
	doc := loadPinnedSpec(t, fulfillmentPinnedSpec)
	key := pinnedChannelTypes(t, doc, FulfillmentTopic)[typeTaskCompleted]
	schema := pinnedMessageSchema(t, doc, key)
	for _, payload := range pinnedExamples(t, doc, FulfillmentTopic, typeTaskCompleted) {
		requirePinnedEnvelope(t, payload)
		if c, ok := pinnedSchemaConst(schema, "dataschema"); ok && payload["dataschema"] != c {
			t.Errorf("example dataschema %v != pinned const %q", payload["dataschema"], c)
		}
	}
}

// E4: additive evolution (unknown data key) is tolerated; a documented type
// we do not handle is skipped untouched; an undecodable payload of OUR type
// is poison and surfaces errPoison (settle dead-letters it).
func TestFulfillmentPinToleranceAndPoisonPolicy(t *testing.T) {
	doc := loadPinnedSpec(t, fulfillmentPinnedSpec)
	examples := pinnedExamples(t, doc, FulfillmentTopic, typeTaskCompleted)
	if len(examples) == 0 {
		t.Fatal("no TaskCompleted examples to perturb")
	}
	fake := &fakeConfirmer{}
	c := &TaskCompletedConsumer{Confirm: fake, Logger: slog.Default()}
	if err := c.Handle(t.Context(), pinnedKafkaMessage(t, FulfillmentTopic, pinnedWithUnknownDataKey(t, examples[0]))); err != nil {
		t.Fatalf("unknown data key must not fail: %v", err)
	}
	if len(fake.got) != 1 {
		t.Fatal("unknown data key must still reach the handler")
	}

	for typ := range pinnedChannelTypes(t, doc, FulfillmentTopic) {
		if typ == typeTaskCompleted {
			continue
		}
		skip := &fakeConfirmer{}
		cs := &TaskCompletedConsumer{Confirm: skip, Logger: slog.Default()}
		payload := pinnedDerivedFixture(t, doc, FulfillmentTopic, typ)
		if err := cs.Handle(t.Context(), pinnedKafkaMessage(t, FulfillmentTopic, payload)); err != nil {
			t.Fatalf("documented-but-unhandled type %s must be skipped, got %v", typ, err)
		}
		if len(skip.got) != 0 {
			t.Fatalf("unhandled type %s reached the handler", typ)
		}
	}

	poison := pinnedWithUnknownDataKey(t, examples[0])
	data := poison["data"].(map[string]any)
	data["task_id"] = map[string]any{"nested": true}
	cp := &TaskCompletedConsumer{Confirm: &fakeConfirmer{}, Logger: slog.Default()}
	if err := cp.Handle(t.Context(), pinnedKafkaMessage(t, FulfillmentTopic, poison)); !errors.Is(err, errPoison) {
		t.Fatalf("undecodable TaskCompleted must be poison, got %v", err)
	}
}
