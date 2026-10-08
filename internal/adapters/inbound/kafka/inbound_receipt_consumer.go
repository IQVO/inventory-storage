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

// InboundReceivingTopic is inbound-receiving's integration topic, its
// Published Language for receipts (inbound-receiving ADR 0003, this repo's
// ADR 0037).
const InboundReceivingTopic = "warehouse.inbound-receiving.events"

// typeReceiptLineReceived is the FULL CloudEvents type this consumer
// dispatches on, byte-identical to inbound-receiving's apis/asyncapi.yaml.
// The other types on the topic (ASN*, DockAppointment*, ReceiptOpened,
// ReceiptClosed) are committed past untouched.
const typeReceiptLineReceived = "com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived"

// receiptLineReceivedData is inbound-receiving's ReceiptLineReceived v1
// payload. received_at is not needed: StockReceived is stamped by this
// service's clock, exactly like POST /stock/receive.
type receiptLineReceivedData struct {
	ReceiptID string `json:"receipt_id"`
	ASNNumber string `json:"asn_number"`
	LineNo    int    `json:"line_no"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	Condition string `json:"condition"`
}

// InboundReceiptBooker is the use-case port the consumer drives, satisfied by
// *usecases.BookInboundReceiptLine.
type InboundReceiptBooker interface {
	Execute(ctx context.Context, l usecases.InboundReceiptLine) (usecases.BookOutcome, error)
}

// InboundReceiptConsumer books the Good lines of inbound-receiving's
// ReceiptLineReceived as staged stock (ADR 0037). Fixed consumer group from
// configuration (INBOUND_RECEIPT_CONSUMER_GROUP), state-mutating,
// at-least-once: FetchMessage, handler, then CommitMessages; a transient
// handler error retries the SAME message with capped backoff; deterministic
// poison is logged and committed past.
type InboundReceiptConsumer struct {
	Reader Reader
	// Book is the handler. Required.
	Book   InboundReceiptBooker
	Logger *slog.Logger
	// Backoff is the wait before retry attempt n; nil means retryBackoff.
	Backoff func(attempt int) time.Duration
}

// NewInboundReceiptConsumer builds the reader on InboundReceivingTopic.
// groupID comes from configuration; the composition root never starts the
// consumer without one.
func NewInboundReceiptConsumer(brokers []string, groupID string, book InboundReceiptBooker, logger *slog.Logger) *InboundReceiptConsumer {
	return NewInboundReceiptConsumerForTopic(InboundReceivingTopic, brokers, groupID, book, logger)
}

// NewInboundReceiptConsumerForTopic is NewInboundReceiptConsumer with an
// explicit topic, so integration tests drive the same semantics against a
// throwaway topic.
func NewInboundReceiptConsumerForTopic(topic string, brokers []string, groupID string, book InboundReceiptBooker, logger *slog.Logger) *InboundReceiptConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// A brand-new group starts at the EARLIEST offset: a line received
		// while this consumer was down must still be booked. Replaying
		// already-booked history is harmless (processed_events dedupes on
		// the CloudEvents id).
		StartOffset: kafkago.FirstOffset,
	})
	return &InboundReceiptConsumer{Reader: reader, Book: book, Logger: logger}
}

// Run consumes until ctx is cancelled or the reader fails.
func (c *InboundReceiptConsumer) Run(ctx context.Context) error {
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
			c.Logger.WarnContext(ctx, "inbound-receiving receipt handling failed; retrying same message",
				"error", err, "attempt", attempt, "partition", msg.Partition, "offset", msg.Offset)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(attempt)):
			}
		}

		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit inbound-receiving message: %w", err)
		}
	}
}

// Handle decodes and applies one message. It returns an error ONLY for a
// transient failure; anything deterministic (not a CloudEvent, other type,
// undecodable payload, invalid line) is logged and returns nil.
func (c *InboundReceiptConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping inbound-receiving message that is not a valid CloudEvent",
			"partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	if e.Type() != typeReceiptLineReceived {
		return nil
	}

	var data receiptLineReceivedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping inbound-receiving ReceiptLineReceived with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}

	outcome, err := c.Book.Execute(ctx, usecases.InboundReceiptLine{
		EventID:   e.ID(),
		ReceiptID: data.ReceiptID,
		ASNNumber: data.ASNNumber,
		LineNo:    data.LineNo,
		SKU:       data.SKU,
		Quantity:  data.Quantity,
		Condition: data.Condition,
	})
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedInboundReceiptLine) {
			c.Logger.WarnContext(ctx, "skipping invalid inbound-receiving receipt line",
				"id", e.ID(), "receipt_id", data.ReceiptID, "sku", data.SKU, "quantity", data.Quantity,
				"condition", data.Condition, "error", err)
			return nil
		}
		return err
	}
	c.logOutcome(ctx, e.ID(), data, outcome)
	return nil
}

// logOutcome logs what the use case did with one settled line. A Damaged line
// is an INFO: it is expected, recorded on the receipt, and counted in
// inventory.inbound_receipt_units{outcome=damaged_not_booked}.
func (c *InboundReceiptConsumer) logOutcome(ctx context.Context, id string, data receiptLineReceivedData, outcome usecases.BookOutcome) {
	attrs := []any{
		"id", id, "receipt_id", data.ReceiptID, "asn_number", data.ASNNumber, "line_no", data.LineNo,
		"sku", data.SKU, "quantity", data.Quantity, "condition", data.Condition,
	}
	switch outcome {
	case usecases.LineDamagedNotBooked:
		c.Logger.InfoContext(ctx, "inbound-receiving receipt line is Damaged; not booked as stock (v1)", attrs...)
	case usecases.LineDuplicate:
		c.Logger.InfoContext(ctx, "inbound-receiving receipt line already handled; redelivery ignored", attrs...)
	case usecases.LineBooked:
		c.Logger.InfoContext(ctx, "inbound-receiving receipt line booked as staged stock", attrs...)
	}
}

// Close releases the underlying reader.
func (c *InboundReceiptConsumer) Close() error {
	return c.Reader.Close()
}
