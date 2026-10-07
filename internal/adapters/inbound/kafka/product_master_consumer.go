package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

// ProductMasterTopic is product-master's integration topic, its Published
// Language for SKU master data (product-master ADR 0001, this repo's
// ADR 0033).
const ProductMasterTopic = "warehouse.product-master.events"

// typeProductMasterClassified is the FULL CloudEvents type this consumer
// dispatches on, byte-identical to product-master's apis/asyncapi.yaml.
// The other types on the topic (ProductRegistered,
// ProductDescriptionChanged, ProductDimensionsDeclared, ProductMeasured)
// are committed past untouched.
const typeProductMasterClassified = "com.warehouse.wms.product-master.product.ProductClassified"

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
// with product-master (ADR 0033). Fixed consumer group from configuration
// (PRODUCT_MASTER_CONSUMER_GROUP), state-mutating, at-least-once:
// FetchMessage, handler, then CommitMessages; a transient handler error
// retries the SAME message with capped backoff; deterministic poison is
// logged and committed past.
type ProductMasterConsumer struct {
	Reader Reader
	// Apply is the handler. Required.
	Apply  ProductClassificationApplier
	Logger *slog.Logger
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

// Run consumes until ctx is cancelled or the reader fails.
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
				return ctx.Err()
			case <-time.After(backoff(attempt)):
			}
		}

		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
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
	if e.Type() != typeProductMasterClassified {
		return nil
	}

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

// Close releases the underlying reader.
func (c *ProductMasterConsumer) Close() error {
	return c.Reader.Close()
}
