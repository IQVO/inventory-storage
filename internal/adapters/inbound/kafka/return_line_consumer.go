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

// ReturnsProcessingTopic is returns-processing's integration topic, its
// Published Language for the reverse flow of goods (returns-processing
// ADR 0001, this repo's ADR 0044).
const ReturnsProcessingTopic = "warehouse.returns-processing.events"

// typeReturnLineStaged is the FULL CloudEvents type this consumer
// dispatches on, byte-identical to returns-processing's
// apis/asyncapi.yaml. The other types on the topic (ReturnAuthorization*,
// ReturnReceipt*, ReturnLineInspected, UnexpectedReturnReceived,
// DispositionDecided) are committed past untouched: only a RESTOCK
// disposition stages a line.
const typeReturnLineStaged = "com.warehouse.wms.returns-processing.disposition.ReturnLineStaged"

// returnLineStagedData is returns-processing's ReturnLineStaged v1
// payload: inbound-receiving's ReceiptLineReceived fields plus rma_id,
// disposition_id and site_id. received_at is not needed: StockReceived
// is stamped by this service's clock, exactly like the dock-receipt
// path; the return context (receipt, RMA, disposition, site) is logged,
// never stored on the stock event.
type returnLineStagedData struct {
	ReceiptID     string `json:"receipt_id"`
	LineNo        int    `json:"line_no"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	Condition     string `json:"condition"`
	ReceivedAt    string `json:"received_at"`
	RmaID         string `json:"rma_id"`
	DispositionID string `json:"disposition_id"`
	SiteID        string `json:"site_id"`
}

// ReturnLineBooker is the use-case port the consumer drives, satisfied
// by *usecases.BookReturnLine.
type ReturnLineBooker interface {
	Execute(ctx context.Context, l usecases.ReturnLine) (usecases.ReturnBookOutcome, error)
}

// ReturnLineConsumer books returns-processing's ReturnLineStaged lines
// as staged stock through the SAME staged-receipt use case an inbound
// receipt line uses (ADR 0044). Fixed consumer group from configuration
// (RETURN_LINE_CONSUMER_GROUP), state-mutating, at-least-once:
// FetchMessage, handler, then CommitMessages; a transient handler error
// retries the SAME message with capped backoff; deterministic poison is
// logged and committed past.
type ReturnLineConsumer struct {
	Reader Reader
	// Book is the handler. Required.
	Book   ReturnLineBooker
	Logger *slog.Logger
	// Backoff is the wait before retry attempt n; nil means retryBackoff.
	Backoff func(attempt int) time.Duration
}

// NewReturnLineConsumer builds the reader on ReturnsProcessingTopic.
// groupID comes from configuration; the composition root never starts
// the consumer without one.
func NewReturnLineConsumer(brokers []string, groupID string, book ReturnLineBooker, logger *slog.Logger) *ReturnLineConsumer {
	return NewReturnLineConsumerForTopic(ReturnsProcessingTopic, brokers, groupID, book, logger)
}

// NewReturnLineConsumerForTopic is NewReturnLineConsumer with an
// explicit topic, so integration tests drive the same semantics against
// a throwaway topic.
func NewReturnLineConsumerForTopic(topic string, brokers []string, groupID string, book ReturnLineBooker, logger *slog.Logger) *ReturnLineConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// A brand-new group starts at the EARLIEST offset: a line staged
		// while this consumer was down must still be booked. Replaying
		// already-booked history is harmless (processed_events dedupes on
		// the CloudEvents id).
		StartOffset: kafkago.FirstOffset,
	})
	return &ReturnLineConsumer{Reader: reader, Book: book, Logger: logger}
}

// Run consumes until ctx is cancelled or the reader fails. A
// cancellation is a clean stop and returns nil, even when it lands
// mid-handle: the in-flight message stays uncommitted and is redelivered
// on the next start.
func (c *ReturnLineConsumer) Run(ctx context.Context) error {
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
			c.Logger.WarnContext(ctx, "returns-processing return-line handling failed; retrying same message",
				"error", err, "attempt", attempt, "partition", msg.Partition, "offset", msg.Offset)
			select {
			case <-ctx.Done():
				// Shutdown landed mid-handle: a clean stop. The offset was
				// not committed, so the message is redelivered and
				// deduplicated.
				return nil
			case <-time.After(backoff(attempt)):
			}
		}

		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil // shutdown: the uncommitted message is redelivered and deduplicated
			}
			return fmt.Errorf("commit returns-processing message: %w", err)
		}
	}
}

// Handle decodes and applies one message. It returns an error ONLY for
// a transient failure; anything deterministic (not a CloudEvent, other
// type, undecodable payload, invalid line) is logged and returns nil.
func (c *ReturnLineConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping returns-processing message that is not a valid CloudEvent",
			"partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	if e.Type() != typeReturnLineStaged {
		return nil
	}

	var data returnLineStagedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping returns-processing ReturnLineStaged with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}

	outcome, err := c.Book.Execute(ctx, usecases.ReturnLine{
		EventID:       e.ID(),
		DispositionID: data.DispositionID,
		ReceiptID:     data.ReceiptID,
		RmaID:         data.RmaID,
		SiteID:        data.SiteID,
		LineNo:        data.LineNo,
		SKU:           data.SKU,
		Quantity:      data.Quantity,
		Condition:     data.Condition,
		ReceivedAt:    data.ReceivedAt,
	})
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedReturnLine) {
			c.Logger.WarnContext(ctx, "skipping invalid returns-processing return line",
				"id", e.ID(), "disposition_id", data.DispositionID, "receipt_id", data.ReceiptID, "sku", data.SKU,
				"quantity", data.Quantity, "condition", data.Condition, "error", err)
			return nil
		}
		return err
	}
	c.logOutcome(ctx, e.ID(), data, outcome)
	return nil
}

// logOutcome logs what the use case did with one settled line. The
// return context attributes are the provenance record of the flow:
// StockReceived itself carries only {sku, quantity} (ADR 0037's frozen
// analytics contract), so receipt_id/rma_id/disposition_id/site_id live
// in the log and in the processed_events claim row.
func (c *ReturnLineConsumer) logOutcome(ctx context.Context, id string, data returnLineStagedData, outcome usecases.ReturnBookOutcome) {
	attrs := []any{
		"id", id, "disposition_id", data.DispositionID, "receipt_id", data.ReceiptID, "rma_id", data.RmaID,
		"site_id", data.SiteID, "line_no", data.LineNo, "sku", data.SKU, "quantity", data.Quantity,
	}
	switch outcome {
	case usecases.ReturnLineDuplicate:
		c.Logger.InfoContext(ctx, "returns-processing return line already handled; redelivery ignored", attrs...)
	case usecases.ReturnLineBooked:
		c.Logger.InfoContext(ctx, "returns-processing return line booked as staged stock", attrs...)
	}
}

// Close releases the underlying reader.
func (c *ReturnLineConsumer) Close() error {
	return c.Reader.Close()
}
