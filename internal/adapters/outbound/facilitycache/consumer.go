// Package facilitycache is the outbound adapter that keeps a local,
// in-memory view of facility-layout's location classifications up to date
// by consuming that context's warehouse.facility.events topic, replacing
// the synchronous per-stow HTTP call the facilitylayout.Client makes. It
// satisfies ports.LocationClassificationLookup — the same port every
// existing use case already depends on — plus a Ready() readiness gate so
// this service can refuse traffic until its initial replay completes.
//
// Why an event-fed cache rather than the HTTP client:
//
// StowStock consults the location classification on its hot path. With
// the HTTP client that makes facility-layout a RUNTIME dependency of
// every stow: if facility-layout is down, slow, or mid-redeploy, stows
// fail (or stall for DefaultTimeout) even though the classification data
// itself changes perhaps a handful of times a week. Facility layout is
// reference data with an extremely low change rate and an extremely high
// read rate — the textbook shape for a locally-maintained read model fed
// by the owning context's Published Language.
//
// This adapter makes inventory-storage a Conformist to facility-layout's
// published events (ADR-0009 over there): facility-layout remains the sole
// source of truth for zone attributes, and this context never writes them,
// only mirrors them. The trade is availability and latency in exchange for
// an eventual-consistency window — see this repo's ADR for that decision
// and its rollback.
//
// Why the readiness gate exists: an empty cache is indistinguishable from
// "this location is not modeled in facility-layout," which
// GetSlotAttributes reports as Known=false — a FAIL-OPEN answer. Serving
// traffic before the initial replay finished would therefore silently
// wave through stows that should have been classified, which is a
// correctness regression rather than a cosmetic one. Ready() blocks
// readiness (not process startup — a transient Kafka outage should not be
// fatal) until this consumer has itself replayed everything that existed
// in the topic when it started.
package facilitycache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/domain/product"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// Topic is facility-layout's integration topic. This service has no
// business knowing anything else about that context beyond this topic name
// and the CloudEvents types/payload shapes below.
const Topic = "warehouse.facility.events"

// consumerGroupPrefix names this service's dedicated, PER-PROCESS consumer
// group on Topic. It is deliberately NOT a fixed shared name: this
// consumer rebuilds a complete in-memory cache from Topic's full history
// on every start (an event-sourced read model, not a work queue). Kafka
// consumer group offsets are shared infrastructure state, so a brand-new
// process joining a group an earlier instance already consumed would
// resume from THAT instance's committed offset and be marked ready with an
// empty cache, having replayed nothing. Every NewConsumer call therefore
// mints a fresh unique group id built on this prefix.
const consumerGroupPrefix = "inventory-storage-facility-location-cache"

// WaitReadyTimeout bounds how long the composition root waits for the
// initial replay before giving up and failing startup loudly, rather than
// hanging forever on a broker that will never deliver.
const WaitReadyTimeout = 60 * time.Second

// CloudEvents `type` strings this consumer acts on: facility-layout's
// Published Language (ADR-0024 cross-service catalogue). Dispatch is on the
// FULL, byte-identical type string — never a suffix match — so an
// unrelated event that merely shares a trailing name can never be applied.
const (
	typeZoneRegistered             = "com.warehouse.wms.facility-layout.zone.ZoneRegistered"
	typeLocationSlotRegistered     = "com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered"
	typeLocationSlotDecommissioned = "com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned"
)

// zoneData is facility-layout's ZoneRegistered payload. Only the
// classification attributes matter here; the rest is decoded and ignored.
type zoneData struct {
	ZoneID           string `json:"zoneId"`
	TemperatureClass string `json:"temperatureClass"`
	Hazmat           bool   `json:"hazmat"`
}

// slotData is facility-layout's LocationSlot{Registered,Decommissioned}
// payload.
type slotData struct {
	LocationCode string `json:"locationCode"`
	ZoneID       string `json:"zoneId"`
}

// Reader is the subset of *kafkago.Reader this Consumer needs, so tests
// can substitute a fake without a live broker.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// dlqTopicSuffix names the dead-letter topic this consumer publishes an
// unparseable/malformed message to, relative to its OWN source topic
// (never a fixed constant): NewConsumerForTopic's isolated test topics
// each get their own matching "<topic>.dlq".
//
// Unlike order-management's RepromiseConsumer (ADR-0025 §DLQ over
// there), this consumer does NOT retry apply() before dead-lettering:
// apply()'s only failure mode is a permanently-malformed JSON payload
// (bad envelope/data shape, an empty required field) — a deterministic
// parse failure that would fail identically on every retry attempt,
// never a transient infrastructure error. Retrying it would just burn
// CPU for zero benefit. The value this DLQ still adds (ADR-0020) is
// pure visibility/replay: today a malformed message is logged and
// silently skipped forever; now it is ALSO preserved on
// Topic+dlqTopicSuffix (raw payload plus x-dlq-* headers) so an operator
// can inspect and, if facility-layout ever fixes its own publish bug,
// replay it. Readiness still advances past it either way (observe()
// runs unconditionally) — this consumer was already fail-open/
// non-blocking on a bad message before this change, unlike
// RepromiseConsumer's redelivery-blocking risk that originally
// motivated retry-then-DLQ there.
const dlqTopicSuffix = ".dlq"

// Consumer maintains the local location-classification view by replaying
// Topic from its earliest offset and applying facility-layout's events.
//
// Zones and slots are kept in SEPARATE maps and joined at lookup time
// rather than denormalized on write. That is deliberate: a slot and its
// zone are different aggregates, so they are published under different
// partition keys and carry no cross-aggregate ordering guarantee. A
// LocationSlotRegistered can legitimately be replayed before the
// ZoneRegistered it refers to; joining lazily means such a slot simply
// resolves once its zone arrives, instead of being permanently cached with
// missing attributes.
type Consumer struct {
	Reader Reader
	Logger *slog.Logger

	mu    sync.RWMutex
	zones map[string]product.SlotAttributes
	slots map[string]string // locationCode -> zoneID

	ready   bool
	readyCh chan struct{}
	target  targetOffsets

	// topic is the source topic this Consumer reads (Topic in
	// production, an isolated test topic in integration tests) — kept
	// so dlqPublish can derive topic+dlqTopicSuffix without a second
	// constructor parameter.
	topic string
	// dlqWriter publishes a malformed message (ADR-0020 §DLQ) to
	// topic+dlqTopicSuffix. nil in the zero-value struct every existing
	// unit test builds directly via newTestConsumer (they never reach
	// Run's DLQ path, only apply()/observe() directly) — dlqPublish
	// itself guards against a nil writer so those tests keep compiling
	// unchanged.
	dlqWriter *kafkago.Writer
}

// targetOffsets is the per-partition "caught up" watermark captured once at
// startup, so Ready() means "has this consumer seen everything that existed
// in the topic when it started" rather than "will it ever catch up to a
// topic that keeps growing" — the latter never settles on a live topic.
type targetOffsets map[int]int64

// NewConsumer constructs a Consumer reading Topic from brokers under a
// fresh, process-unique consumer group, starting at the earliest offset. It
// dials the topic directly to capture each partition's current last offset
// as this instance's readiness target BEFORE consuming, so Ready() reports
// true exactly once THIS consumer has processed every message that existed
// at startup — never skipped because another instance already committed
// past them.
func NewConsumer(ctx context.Context, brokers []string, logger *slog.Logger) (*Consumer, error) {
	return NewConsumerForTopic(ctx, brokers, Topic, logger)
}

// NewConsumerForTopic is NewConsumer with an explicit topic. Production
// code should call NewConsumer (which pins facility-layout's real topic);
// this variant exists so integration tests can drive the identical
// replay/readiness logic against a throwaway topic instead of polluting
// the real one.
func NewConsumerForTopic(ctx context.Context, brokers []string, topic string, logger *slog.Logger) (*Consumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	target, err := newTargetOffsets(ctx, brokers, topic)
	if err != nil {
		return nil, fmt.Errorf("facilitycache: determine readiness target: %w", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     uniqueConsumerGroup(),
		StartOffset: kafkago.FirstOffset,
	})

	c := &Consumer{
		Reader:  reader,
		Logger:  logger,
		zones:   make(map[string]product.SlotAttributes),
		slots:   make(map[string]string),
		readyCh: make(chan struct{}),
		target:  target,
		topic:   topic,
		dlqWriter: &kafkago.Writer{
			Addr:  kafkago.TCP(brokers...),
			Topic: topic + dlqTopicSuffix,
		},
	}
	if len(target) == 0 {
		// The topic has no partitions carrying any messages yet (brand
		// new topic, or facility-layout has never published). There is
		// nothing to catch up to, so this consumer is trivially ready.
		//
		// NOTE this is a genuine fail-open: every lookup will report
		// Known=false until events arrive. That matches the HTTP
		// client's own 404 behaviour for an unmodeled location, and the
		// composition root logs the empty-cache case loudly at startup.
		c.markReady()
	}
	return c, nil
}

// uniqueConsumerGroup builds a group id unique to this process instance
// (hostname + PID + nanosecond timestamp, so two processes on one host
// started in the same second still never collide). The group is abandoned
// on every restart; Kafka garbage-collects unused group metadata on its own
// retention schedule, so this needs no explicit cleanup.
func uniqueConsumerGroup() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%s-%d-%d", consumerGroupPrefix, host, os.Getpid(), time.Now().UnixNano())
}

// newTargetOffsets dials Topic directly (no consumer group) and reads each
// partition's current last offset, returning only partitions that actually
// hold at least one message — an empty partition contributes no readiness
// requirement.
func newTargetOffsets(ctx context.Context, brokers []string, topic string) (targetOffsets, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("facilitycache: no brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("facilitycache: dial %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		if isUnknownTopic(err) {
			// The topic does not exist yet. That is a legitimate startup
			// state, not a failure: facility-layout creates it on its
			// first publish, so a consumer that happens to start first
			// (a fresh cluster, or before that service is deployed) must
			// wait for it rather than crash. Treating it as fatal here
			// produced a real CrashLoopBackOff on a first rollout --
			// Kubernetes eventually recovered it, but only because the
			// topic appeared between restarts.
			//
			// No partitions means no readiness target, so the consumer
			// comes up trivially ready with an empty cache (fail-open,
			// loudly warned about by the composition root) and picks the
			// topic up live once it is created.
			return targetOffsets{}, nil
		}
		return nil, fmt.Errorf("facilitycache: read partitions for %s: %w", topic, err)
	}

	out := make(targetOffsets, len(partitions))
	for _, p := range partitions {
		pconn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("facilitycache: dial leader for partition %d: %w", p.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		closeErr := pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("facilitycache: read offsets for partition %d: %w", p.ID, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("facilitycache: close leader conn for partition %d: %w", p.ID, closeErr)
		}
		if last > first {
			// last is the offset of the NEXT message to be written
			// (exclusive), so a consumer has caught up once it has
			// processed offset last-1.
			out[p.ID] = last
		}
	}
	return out, nil
}

// isUnknownTopic reports whether err is Kafka's UnknownTopicOrPartition,
// i.e. "that topic does not exist (yet)" rather than a real transport or
// broker failure. Checked via errors.Is against the typed protocol error so
// this does not depend on message wording.
func isUnknownTopic(err error) bool {
	return errors.Is(err, kafkago.UnknownTopicOrPartition)
}

// Close releases the underlying Kafka reader and, if configured, the DLQ
// writer.
func (c *Consumer) Close() error {
	readerErr := c.Reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
}

// Ready reports whether this consumer has processed every message that
// existed in Topic when it started. Safe for concurrent use (e.g. from a
// healthz handler).
func (c *Consumer) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// WaitReady blocks until Ready() would return true or ctx is done.
func (c *Consumer) WaitReady(ctx context.Context) error {
	if c.Ready() {
		return nil
	}
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) markReady() {
	c.mu.Lock()
	alreadyReady := c.ready
	c.ready = true
	c.mu.Unlock()
	if !alreadyReady {
		close(c.readyCh)
	}
}

// Slots reports how many location slots are currently cached — used by the
// composition root to log the replay result, and by tests.
func (c *Consumer) Slots() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.slots)
}

// Zones reports how many zones are currently cached.
func (c *Consumer) Zones() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.zones)
}

// GetSlotAttributes satisfies ports.LocationClassificationLookup from the
// local cache. It never performs I/O, so it cannot fail and never returns
// an error — StowStock's ErrLocationClassificationUnavailable path is
// simply unreachable through this adapter, which is the entire point of
// the migration.
//
// Semantics deliberately mirror facilitylayout.Client exactly:
//
//   - an unknown/decommissioned slot yields Known=false (fail-open), the
//     same answer the HTTP client derives from a 404;
//   - a slot whose zone has not been seen also yields Known=false rather
//     than a half-populated answer;
//   - an unparseable/empty temperature class falls back to the zero value
//     rather than failing the lookup, since the zone may simply carry no
//     temperature constraint.
func (c *Consumer) GetSlotAttributes(_ context.Context, binID shared.BinId) (product.SlotAttributes, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	zoneID, ok := c.slots[binID.String()]
	if !ok {
		return product.SlotAttributes{Known: false}, nil
	}
	attrs, ok := c.zones[zoneID]
	if !ok {
		return product.SlotAttributes{Known: false}, nil
	}
	return attrs, nil
}

// Run consumes Topic until ctx is cancelled or the reader fails, applying
// every recognized event to the local cache. It is intended to run in its
// own goroutine for the life of the process.
//
// A message apply() cannot parse (not a valid CloudEvents 1.0 event —
// including the retired flat envelope — malformed data JSON, or a required
// field missing) is dead-lettered to topic+dlqTopicSuffix
// (ADR-0020 §DLQ) — the raw payload plus x-dlq-* headers preserved for
// operator inspection/replay — then observe() still runs, exactly as
// before this change, so readiness and the read model keep advancing
// unaffected by one bad message. A dlqPublish failure is logged but does
// NOT stop the consume loop: this consumer's whole reason to exist is
// availability of the read model, and a broker hiccup writing to the DLQ
// must never become a reason to stop reading the SOURCE topic.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		if err := c.apply(msg.Value); err != nil {
			// A single malformed message must never stop the cache from
			// tracking everything else — log and continue, exactly as a
			// tolerant reader should.
			c.Logger.ErrorContext(ctx, "facility event handling failed",
				"error", err, "partition", msg.Partition, "offset", msg.Offset)
			if dlqErr := c.dlqPublish(ctx, msg, err); dlqErr != nil {
				c.Logger.ErrorContext(ctx, "facility event dead-letter publish failed",
					"error", dlqErr, "partition", msg.Partition, "offset", msg.Offset)
			}
		}
		c.observe(msg.Partition, msg.Offset)
	}
}

// dlqPublish writes the raw, unmodified message payload plus error
// context (as headers, so the raw body stays byte-identical for a
// manual replay tool) to the dead-letter topic. A nil dlqWriter (the
// zero-value Consumer every existing unit test constructs directly via
// newTestConsumer, which never exercises this path) is a documented
// no-op rather than a nil-pointer panic.
func (c *Consumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// observe records that a message was processed and flips readiness once
// every tracked partition has reached its captured startup target.
func (c *Consumer) observe(partition int, offset int64) {
	c.mu.Lock()
	if target, tracked := c.target[partition]; tracked && offset+1 >= target {
		delete(c.target, partition)
	}
	done := len(c.target) == 0
	c.mu.Unlock()
	if done {
		c.markReady()
	}
}

// apply decodes one CloudEvents 1.0 event and updates the cache. Anything
// cloudevents.Decode rejects (the retired flat envelope, bad JSON, missing
// required attributes) is returned as an error so Run dead-letters it —
// it is never parsed as a legacy shape. Every update is an idempotent
// upsert/delete keyed by the aggregate, so a redelivered event (same
// CloudEvents id) re-applies harmlessly and needs no separate id dedupe.
func (c *Consumer) apply(value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		return fmt.Errorf("facilitycache: %w", err)
	}

	switch e.Type() {
	case typeZoneRegistered:
		return c.applyZoneRegistered(e.DataAs)
	case typeLocationSlotRegistered:
		return c.applySlotRegistered(e.DataAs)
	case typeLocationSlotDecommissioned:
		var data slotData
		if err := e.DataAs(&data); err != nil {
			return fmt.Errorf("facilitycache: decode %s: %w", typeLocationSlotDecommissioned, err)
		}
		c.mu.Lock()
		delete(c.slots, data.LocationCode)
		c.mu.Unlock()
	default:
		// Every other event on this topic (SiteRegistered, AisleRegistered,
		// LocationTypeRegistered, PlacementRuleDefined, FacilityLayoutImported)
		// carries nothing this cache needs. Ignoring them is correct, not a
		// gap: they still count toward the readiness target because observe()
		// runs for every message regardless.
	}
	return nil
}

// applyZoneRegistered caches a zone's classification attributes.
func (c *Consumer) applyZoneRegistered(dataAs func(any) error) error {
	var data zoneData
	if err := dataAs(&data); err != nil {
		return fmt.Errorf("facilitycache: decode %s: %w", typeZoneRegistered, err)
	}
	if data.ZoneID == "" {
		return fmt.Errorf("facilitycache: %s with empty zoneId", typeZoneRegistered)
	}
	temperatureClass, err := product.ParseTemperatureClass(data.TemperatureClass)
	if err != nil {
		temperatureClass = ""
	}
	c.mu.Lock()
	c.zones[data.ZoneID] = product.SlotAttributes{
		Hazmat:           data.Hazmat,
		TemperatureClass: temperatureClass,
		Known:            true,
	}
	c.mu.Unlock()
	return nil
}

// applySlotRegistered caches a location slot -> zone binding.
func (c *Consumer) applySlotRegistered(dataAs func(any) error) error {
	var data slotData
	if err := dataAs(&data); err != nil {
		return fmt.Errorf("facilitycache: decode %s: %w", typeLocationSlotRegistered, err)
	}
	if data.LocationCode == "" {
		return fmt.Errorf("facilitycache: %s with empty locationCode", typeLocationSlotRegistered)
	}
	zoneID := data.ZoneID
	if zoneID == "" {
		// Derive the zone identity from the code itself, mirroring
		// facility-layout's own LocationCode.ZoneID() (site-area-zone,
		// the first three of seven hyphen-separated segments), so a
		// payload that omits the field still resolves.
		zoneID = zoneIDFromLocationCode(data.LocationCode)
	}
	c.mu.Lock()
	c.slots[data.LocationCode] = zoneID
	c.mu.Unlock()
	return nil
}

// zoneIDFromLocationCode mirrors facility-layout's LocationCode.ZoneID():
// the site, area and zone segments joined, e.g.
// "WH1-STOR-AMB-A07-03-02-B" -> "WH1-STOR-AMB". A code that does not have
// at least three segments yields "", which resolves to Known=false.
func zoneIDFromLocationCode(code string) string {
	parts := strings.Split(code, "-")
	if len(parts) < 3 {
		return ""
	}
	return strings.Join(parts[:3], "-")
}
