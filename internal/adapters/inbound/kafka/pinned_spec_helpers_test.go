package kafka

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Shared machinery for the per-producer pinned-AsyncAPI contract tests
// (archetypes E1-E4). Each producer this service consumes has its own
// spec_test file listing its pinned copy, its reader-config topic constant
// and the CloudEvents types its consumer handles; this file only knows how
// to read an AsyncAPI document and its CloudEvents envelope conventions.
//
// Fleet reality encoded here: most pinned specs are channel-perspective
// (producer's own topics under subscribe:, consumed topics under publish:),
// but NOT uniformly — network-inventory-planning declares channel-level
// `messages:` maps instead. Channel message collection therefore accepts
// every shape; the pinned spec stays the message catalog either way.

const (
	pinnedCEContentType = "application/cloudevents+json"
	pinnedJSONBody      = "application/json"
	pinnedURNPrefix     = "urn:warehouse:"
	pinnedURNSuffix     = ":v1"
)

type pinnedSpecDoc struct {
	DefaultContentType string                   `yaml:"defaultContentType"`
	Channels           map[string]pinnedChannel `yaml:"channels"`
	Components         struct {
		Messages map[string]pinnedSpecMessage `yaml:"messages"`
		Schemas  map[string]any               `yaml:"schemas"`
	} `yaml:"components"`
}

type pinnedChannel struct {
	Messages  map[string]pinnedRef `yaml:"messages"`
	Subscribe *pinnedOp            `yaml:"subscribe"`
	Publish   *pinnedOp            `yaml:"publish"`
}

type pinnedOp struct {
	Message *pinnedOpMessage `yaml:"message"`
}

type pinnedOpMessage struct {
	OneOf []pinnedRef `yaml:"oneOf"`
	Ref   string      `yaml:"$ref"`
}

type pinnedRef struct {
	Ref string `yaml:"$ref"`
}

type pinnedSpecMessage struct {
	ContentType string              `yaml:"contentType"`
	Payload     any                 `yaml:"payload"`
	Examples    []pinnedSpecExample `yaml:"examples"`
}

type pinnedSpecExample struct {
	Name    string         `yaml:"name"`
	Payload map[string]any `yaml:"payload"`
}

func loadPinnedSpec(t *testing.T, file string) *pinnedSpecDoc {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read pinned spec %s: %v", file, err)
	}
	var doc pinnedSpecDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", file, err)
	}
	return &doc
}

// pinnedMessageKeys lists the components.messages keys a channel declares,
// across the fleet's AsyncAPI shapes (channel.messages map, or the 2.x
// subscribe/publish operation message oneOf).
func pinnedMessageKeys(t *testing.T, doc *pinnedSpecDoc, channel string) []string {
	t.Helper()
	ch, ok := doc.Channels[channel]
	if !ok {
		t.Fatalf("pinned spec does not document channel %q", channel)
	}
	var keys []string
	for _, ref := range ch.Messages {
		keys = append(keys, pinnedRefName(ref.Ref))
	}
	for _, op := range []*pinnedOp{ch.Subscribe, ch.Publish} {
		if op == nil || op.Message == nil {
			continue
		}
		for _, ref := range op.Message.OneOf {
			keys = append(keys, pinnedRefName(ref.Ref))
		}
		if op.Message.Ref != "" {
			keys = append(keys, pinnedRefName(op.Message.Ref))
		}
	}
	if len(keys) == 0 {
		t.Fatalf("channel %q declares no message catalog", channel)
	}
	return keys
}

func pinnedRefName(ref string) string {
	return strings.TrimPrefix(ref, "#/components/messages/")
}

// pinnedMessageSchema resolves a message's payload: either inline or a
// $ref into components.schemas (network-inventory-planning's shape).
func pinnedMessageSchema(t *testing.T, doc *pinnedSpecDoc, key string) map[string]any {
	t.Helper()
	msg, ok := doc.Components.Messages[key]
	if !ok {
		t.Fatalf("message %q is not in components.messages", key)
	}
	if payload, ok := msg.Payload.(map[string]any); ok {
		if ref, ok := payload["$ref"].(string); ok {
			name := strings.TrimPrefix(ref, "#/components/schemas/")
			schema, ok := doc.Components.Schemas[name].(map[string]any)
			if !ok {
				t.Fatalf("payload $ref %q does not resolve", ref)
			}
			return schema
		}
		return payload
	}
	t.Fatalf("message %q has no object payload", key)
	return nil
}

// pinnedSchemaConst reads properties.<attr>.const out of a schema's allOf
// branches (the fleet's CloudEvents envelope narrowing shape).
func pinnedSchemaConst(schema map[string]any, attr string) (string, bool) {
	branches, _ := schema["allOf"].([]any)
	branches = append(branches, schema)
	for _, b := range branches {
		branch, ok := b.(map[string]any)
		if !ok {
			continue
		}
		props, _ := branch["properties"].(map[string]any)
		node, _ := props[attr].(map[string]any)
		if c, ok := node["const"].(string); ok {
			return c, true
		}
	}
	return "", false
}

// pinnedMessageType returns the full CloudEvents `type` a channel message
// carries, from the schema const (falling back to its first example).
func pinnedMessageType(t *testing.T, doc *pinnedSpecDoc, key string) string {
	t.Helper()
	if c, ok := pinnedSchemaConst(pinnedMessageSchema(t, doc, key), "type"); ok {
		return c
	}
	for _, ex := range doc.Components.Messages[key].Examples {
		if ty, _ := ex.Payload["type"].(string); ty != "" {
			return ty
		}
	}
	t.Fatalf("message %q declares no CloudEvents type const or example type", key)
	return ""
}

// pinnedChannelTypes maps every CloudEvents type on a channel to its
// message key: the E1 view of "what the pinned catalog offers".
func pinnedChannelTypes(t *testing.T, doc *pinnedSpecDoc, channel string) map[string]string {
	t.Helper()
	types := make(map[string]string)
	for _, key := range pinnedMessageKeys(t, doc, channel) {
		types[pinnedMessageType(t, doc, key)] = key
	}
	return types
}

// pinnedExamples returns every example payload of the given type on the
// channel (spec examples are the first-choice fixtures, per the kit).
func pinnedExamples(t *testing.T, doc *pinnedSpecDoc, channel, wantType string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, key := range pinnedMessageKeys(t, doc, channel) {
		if pinnedMessageType(t, doc, key) != wantType {
			continue
		}
		for _, ex := range doc.Components.Messages[key].Examples {
			if ex.Payload["type"] == wantType {
				out = append(out, ex.Payload)
			}
		}
	}
	return out
}

// requirePinnedEnvelope (E3) asserts the CloudEvents 1.0 invariants every
// fleet envelope carries, on a concrete payload.
func requirePinnedEnvelope(t *testing.T, payload map[string]any) {
	t.Helper()
	str := func(key string) string { v, _ := payload[key].(string); return v }
	if str("specversion") != "1.0" {
		t.Errorf("specversion = %q, want 1.0", str("specversion"))
	}
	for _, attr := range []string{"id", "source", "type", "subject", "time"} {
		if str(attr) == "" {
			t.Errorf("envelope attribute %q is missing/empty", attr)
		}
	}
	if str("datacontenttype") != pinnedJSONBody {
		t.Errorf("datacontenttype = %q, want %s", str("datacontenttype"), pinnedJSONBody)
	}
	if ds := str("dataschema"); !strings.HasPrefix(ds, pinnedURNPrefix) || !strings.HasSuffix(ds, pinnedURNSuffix) {
		t.Errorf("dataschema = %q, want a %s...%s urn", ds, pinnedURNPrefix, pinnedURNSuffix)
	}
	if _, ok := payload["data"].(map[string]any); !ok {
		t.Errorf("data is not an object: %T", payload["data"])
	}
}

// requirePinnedMessageContentType (E3) asserts the wire content type the
// producer documents for every handled message.
func requirePinnedMessageContentType(t *testing.T, doc *pinnedSpecDoc, key string) {
	t.Helper()
	if ct := doc.Components.Messages[key].ContentType; ct != pinnedCEContentType {
		t.Errorf("message %q contentType = %q, want %s", key, ct, pinnedCEContentType)
	}
}

func requirePinnedDefaultContentType(t *testing.T, doc *pinnedSpecDoc) {
	t.Helper()
	if doc.DefaultContentType != pinnedCEContentType {
		t.Errorf("defaultContentType = %q, want %s", doc.DefaultContentType, pinnedCEContentType)
	}
}

// pinnedWithUnknownDataKey (E4) deep-ish-copies a payload and injects a
// data field no pinned schema declares: additive producer evolution the
// decoders must tolerate (encoding/json ignores unknown keys).
func pinnedWithUnknownDataKey(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	const unknownKey = "x_future_contract_field"
	clone := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		clone[k] = v
	}
	data, _ := payload["data"].(map[string]any)
	if data == nil {
		t.Fatal("payload has no data object to perturb")
	}
	dataClone := make(map[string]any, len(data)+1)
	for k, v := range data {
		dataClone[k] = v
	}
	dataClone[unknownKey] = "added-after-pinning"
	clone["data"] = dataClone
	return clone
}

// pinnedKafkaMessage marshals a payload into the bytes a consumer sees.
func pinnedKafkaMessage(t *testing.T, topic string, payload map[string]any) kafkaMessage {
	t.Helper()
	value, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal pinned payload: %v", err)
	}
	return kafkaMessage{Topic: topic, Partition: 0, Offset: 1, Value: value}
}

// pinnedDerivedFixture builds an envelope from the pinned schema itself for
// producers that ship no examples (network-inventory-planning today):
// envelope consts from the schema, data synthesized from its required
// properties. Provenance: derived-from-schema, not a spec example.
func pinnedDerivedFixture(t *testing.T, doc *pinnedSpecDoc, channel, wantType string) map[string]any {
	t.Helper()
	types := pinnedChannelTypes(t, doc, channel)
	key, ok := types[wantType]
	if !ok {
		t.Fatalf("type %s not documented on %s", wantType, channel)
	}
	schema := pinnedMessageSchema(t, doc, key)
	fixture := map[string]any{
		"specversion": "1.0",
		"id":          "derived-from-pinned-schema-0001",
		"subject":     "derived-from-pinned-schema",
		"time":        "2026-11-01T00:00:00Z",
		"type":        wantType,
	}
	for _, attr := range []string{"source", "dataschema"} {
		c, ok := pinnedSchemaConst(schema, attr)
		if !ok {
			t.Fatalf("%s %q declares no %s const", channel, key, attr)
		}
		fixture[attr] = c
	}
	if c, ok := pinnedSchemaConst(schema, "datacontenttype"); ok {
		fixture["datacontenttype"] = c
	} else {
		fixture["datacontenttype"] = pinnedJSONBody
	}
	var fixtureData map[string]any
	if data := pinnedOptionalDataNode(schema); data != nil {
		props, _ := data["properties"].(map[string]any)
		fixtureData = make(map[string]any, max(len(props), 1))
		for _, field := range pinnedRequiredList(t, data) {
			fixtureData[field] = pinnedSynthesizeValue(props[field])
		}
	} else {
		// no data.required declared: a decodable envelope with empty data is
		// enough for documented-but-unhandled types (the consumer skips them
		// before reading data)
		fixtureData = map[string]any{}
	}
	fixture["data"] = fixtureData
	return fixture
}

// pinnedOptionalDataNode finds the payload's data node WITH a required
// list, tolerating the fleet's schema shapes; nil when none declares one.
func pinnedOptionalDataNode(schema map[string]any) map[string]any {
	branches, _ := schema["allOf"].([]any)
	for _, b := range branches {
		branch, ok := b.(map[string]any)
		if !ok {
			continue
		}
		// two fleet shapes: a bare {data: {...}} branch, or a
		// {properties: {data: {...}}} branch (CloudEventEnvelope style)
		props, _ := branch["properties"].(map[string]any)
		for _, node := range []any{branch["data"], mapIndex(props, "data")} {
			if node, ok := node.(map[string]any); ok && node["required"] != nil {
				return node
			}
		}
	}
	// payload schemas without allOf may declare data inline
	if props, ok := schema["properties"].(map[string]any); ok {
		if node, ok := mapIndex(props, "data").(map[string]any); ok && node["required"] != nil {
			return node
		}
	}
	return nil
}

// mapIndex returns m[k] as any (nil when m is nil), keeping call sites terse.
func mapIndex(m map[string]any, k string) any {
	if m == nil {
		return nil
	}
	return m[k]
}

func pinnedRequiredList(t *testing.T, data map[string]any) []string {
	t.Helper()
	raw, _ := data["required"].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, _ := v.(string)
		if s == "" {
			t.Fatal("data.required carries a non-string entry")
		}
		out = append(out, s)
	}
	return out
}

func pinnedSynthesizeValue(prop any) any {
	node, ok := prop.(map[string]any)
	if !ok {
		return "derived"
	}
	switch node["type"] {
	case "integer":
		return 1
	case "number":
		return 1.5
	case "boolean":
		return true
	default:
		return "derived-from-pinned-schema"
	}
}
