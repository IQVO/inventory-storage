package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// ProductMasterTopic is product-master's integration topic, its Published
// Language for SKU master data (product-master ADR 0001, this repo's
// ADR 0034).
const ProductMasterTopic = "warehouse.product-master.events"

// typeProductMasterClassified is the FULL CloudEvents type this consumer
// dispatches on, byte-identical to product-master's apis/asyncapi.yaml.
// The other types on the topic (ProductRegistered,
// ProductDescriptionChanged, ProductDimensionsDeclared, ProductMeasured)
// are committed past untouched.
const typeProductMasterClassified = "com.warehouse.wms.product-master.product.ProductClassified"

// The pack-level types feed the local PackagingView (contract R8). Their
// payloads use product-master's snake_case casing.
const (
	typeProductPackLevelDeclared = "com.warehouse.wms.product-master.product.ProductPackLevelDeclared"
	typeProductPackLevelRetired  = "com.warehouse.wms.product-master.product.ProductPackLevelRetired"
)

// productPackLevelDeclaredData is ProductPackLevelDeclared v1. Fields this
// service does not use (dims, is_orderable, ...) are ignored.
type productPackLevelDeclaredData struct {
	SKU           string    `json:"sku"`
	Level         string    `json:"level"`
	GTIN          string    `json:"gtin"`
	ChildGTIN     string    `json:"child_gtin"`
	UnitsPerPack  int       `json:"units_per_pack"`
	EffectiveFrom time.Time `json:"effective_from"`
	Version       int64     `json:"version"`
}

// productPackLevelRetiredData is ProductPackLevelRetired v1.
type productPackLevelRetiredData struct {
	SKU         string    `json:"sku"`
	Level       string    `json:"level"`
	GTIN        string    `json:"gtin"`
	EffectiveTo time.Time `json:"effective_to"`
	Version     int64     `json:"version"`
}

// PackLevelApplier is the use-case port the consumer drives for pack levels,
// satisfied by *usecases.ApplyPackLevel.
type PackLevelApplier interface {
	Declared(ctx context.Context, d usecases.PackLevelDeclared) (usecases.PackLevelOutcome, error)
	Retired(ctx context.Context, r usecases.PackLevelRetired) (usecases.PackLevelOutcome, error)
}

// productMasterClassifiedData is product-master's ProductClassified v1
// payload: full-state replacement of the SKU's classification plus the
// product version that guards the local copy.
type productMasterClassifiedData struct {
	SKU                  string   `json:"sku"`
	HandlingTags         []string `json:"handling_tags"`
	TemperatureClass     string   `json:"temperature_class,omitempty"`
	DOTHazardClass       int      `json:"dot_hazard_class,omitempty"`
	ClassificationSource string   `json:"classification_source"`
	Version              int64    `json:"version"`
}

// ProductClassificationApplier is the use-case port the consumer drives,
// satisfied by *usecases.ApplyProductClassification.
type ProductClassificationApplier interface {
	Execute(ctx context.Context, u usecases.ProductClassificationUpdate) (usecases.ApplyOutcome, error)
}

// ProductMasterConsumer keeps the local product_classifications copy in step
// with product-master (ADR 0034). Fixed consumer group from configuration
// (PRODUCT_MASTER_CONSUMER_GROUP), state-mutating, at-least-once:
// FetchMessage, handler, then CommitMessages; a transient handler error
// retries the SAME message with capped backoff; deterministic poison is
// logged and committed past.
type ProductMasterConsumer struct {
	Reader Reader
	// Apply is the handler. Required.
	Apply ProductClassificationApplier
	// Packaging projects pack levels into the local PackagingView (contract
	// R8). Optional: nil commits those events past untouched.
	Packaging PackLevelApplier
	Logger    *slog.Logger
	// Backoff is the wait before retry attempt n; nil means retryBackoff.
	Backoff func(attempt int) time.Duration
}

// NewProductMasterConsumer builds the reader on ProductMasterTopic.
// groupID comes from configuration; the composition root never starts the
// consumer without one.
func NewProductMasterConsumer(brokers []string, groupID string, apply ProductClassificationApplier, logger *slog.Logger) *ProductMasterConsumer {
	return NewProductMasterConsumerForTopic(ProductMasterTopic, brokers, groupID, apply, logger)
}

// NewProductMasterConsumerForTopic is NewProductMasterConsumer with an
// explicit topic, so integration tests drive the same semantics against a
// throwaway topic.
func NewProductMasterConsumerForTopic(topic string, brokers []string, groupID string, apply ProductClassificationApplier, logger *slog.Logger) *ProductMasterConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// A brand-new group replays the topic from the start, so the local
		// copy is rebuilt from product-master's full history; once the
		// group has committed, its offsets take over.
		StartOffset: kafkago.FirstOffset,
	})
	return &ProductMasterConsumer{Reader: reader, Apply: apply, Logger: logger}
}

// Run consumes until ctx is cancelled or the reader fails. A cancellation is a
// clean stop and returns nil, even when it lands mid-handle: the in-flight
// message stays uncommitted and is redelivered on the next start.
func (c *ProductMasterConsumer) Run(ctx context.Context) error {
	backoff := c.Backoff
	if backoff == nil {
		backoff = retryBackoff
	}
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		attempt := 0
		for {
			err := c.Handle(ctx, msg)
			if err == nil {
				break
			}
			attempt++
			c.Logger.WarnContext(ctx, "product-master classification handling failed; retrying same message",
				"error", err, "attempt", attempt, "partition", msg.Partition, "offset", msg.Offset)
			select {
			case <-ctx.Done():
				// Shutdown landed mid-handle: a clean stop. The offset was not
				// committed, so the message is redelivered and deduplicated.
				return nil
			case <-time.After(backoff(attempt)):
			}
		}

		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil // shutdown: the uncommitted message is redelivered and deduplicated
			}
			return fmt.Errorf("commit product-master message: %w", err)
		}
	}
}

// Handle decodes and applies one message. It returns an error ONLY for a
// transient failure; anything deterministic (not a CloudEvent, other type,
// undecodable payload, invalid classification) is logged and returns nil.
func (c *ProductMasterConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping product-master message that is not a valid CloudEvent",
			"partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	switch e.Type() {
	case typeProductMasterClassified:
		return c.handleClassified(ctx, msg, e)
	case typeProductPackLevelDeclared:
		return c.handlePackDeclared(ctx, msg, e)
	case typeProductPackLevelRetired:
		return c.handlePackRetired(ctx, msg, e)
	}
	return nil
}

func (c *ProductMasterConsumer) handleClassified(ctx context.Context, msg kafkago.Message, e event.Event) error {
	var data productMasterClassifiedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping product-master ProductClassified with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}

	outcome, err := c.Apply.Execute(ctx, usecases.ProductClassificationUpdate{
		EventID:              e.ID(),
		SKU:                  data.SKU,
		HandlingTags:         data.HandlingTags,
		TemperatureClass:     data.TemperatureClass,
		DOTHazardClass:       data.DOTHazardClass,
		ClassificationSource: data.ClassificationSource,
		Version:              data.Version,
	})
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedProductClassification) {
			c.Logger.WarnContext(ctx, "skipping invalid product-master classification",
				"id", e.ID(), "sku", data.SKU, "version", data.Version, "error", err)
			return nil
		}
		return err
	}
	c.Logger.InfoContext(ctx, "product-master classification handled",
		"id", e.ID(), "sku", data.SKU, "version", data.Version, "outcome", string(outcome))
	return nil
}

func (c *ProductMasterConsumer) handlePackDeclared(ctx context.Context, msg kafkago.Message, e event.Event) error {
	if c.Packaging == nil {
		return nil
	}
	var data productPackLevelDeclaredData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping product-master ProductPackLevelDeclared with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	outcome, err := c.Packaging.Declared(ctx, usecases.PackLevelDeclared{
		EventID: e.ID(), SKU: data.SKU, Level: data.Level, GTIN: data.GTIN, ChildGTIN: data.ChildGTIN,
		UnitsPerPack: data.UnitsPerPack, EffectiveFrom: data.EffectiveFrom, Version: data.Version,
	})
	return c.packLevelDone(ctx, e.ID(), data.SKU, data.GTIN, data.Version, outcome, err)
}

func (c *ProductMasterConsumer) handlePackRetired(ctx context.Context, msg kafkago.Message, e event.Event) error {
	if c.Packaging == nil {
		return nil
	}
	var data productPackLevelRetiredData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping product-master ProductPackLevelRetired with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	outcome, err := c.Packaging.Retired(ctx, usecases.PackLevelRetired{
		EventID: e.ID(), SKU: data.SKU, Level: data.Level, GTIN: data.GTIN, EffectiveTo: data.EffectiveTo, Version: data.Version,
	})
	return c.packLevelDone(ctx, e.ID(), data.SKU, data.GTIN, data.Version, outcome, err)
}

// packLevelDone logs the result of a pack-level event: a malformed one is
// skipped (nil), a transient failure is returned for retry.
func (c *ProductMasterConsumer) packLevelDone(ctx context.Context, id, sku, gtin string, version int64, outcome usecases.PackLevelOutcome, err error) error {
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedPackLevel) {
			c.Logger.WarnContext(ctx, "skipping invalid product-master pack level",
				"id", id, "sku", sku, "gtin", gtin, "version", version, "error", err)
			return nil
		}
		return err
	}
	c.Logger.InfoContext(ctx, "product-master pack level handled",
		"id", id, "sku", sku, "gtin", gtin, "version", version, "outcome", string(outcome))
	return nil
}

// Close releases the underlying reader.
func (c *ProductMasterConsumer) Close() error {
	return c.Reader.Close()
}
