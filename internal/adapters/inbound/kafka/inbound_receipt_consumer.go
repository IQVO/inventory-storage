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

// InboundReceivingTopic is inbound-receiving's integration topic, its
// Published Language for receipts (inbound-receiving ADR 0003, this repo's
// ADR 0037).
const InboundReceivingTopic = "warehouse.inbound-receiving.events"

// typeReceiptLineReceived is the FULL CloudEvents type this consumer
// dispatches on, byte-identical to inbound-receiving's apis/asyncapi.yaml.
// The other types on the topic (ASN*, DockAppointment*, ReceiptOpened,
// ReceiptClosed) are committed past untouched.
const typeReceiptLineReceived = "com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived"

// typeReceiptCartonReceived is the carton identity event (ADR 0045): it
// creates or links Containers and never books a quantity.
const typeReceiptCartonReceived = "com.warehouse.wms.inbound-receiving.receipt.ReceiptCartonReceived"

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
	// The optional pack provenance (ADR 0045). Unknown fields are ignored.
	OwnerRef string `json:"owner_ref"`
	PackGTIN string `json:"pack_gtin"`
	Cases    int    `json:"cases"`
	SSCC     string `json:"sscc"`
}

// receiptCartonReceivedData is inbound-receiving's ReceiptCartonReceived v1
// payload (contract section 2 with R6/R7 applied): snake_case, NO site. The
// decode is tolerant: fields this service does not know are ignored, and the
// carton's received_at is not needed.
type receiptCartonReceivedData struct {
	ReceiptID  string                 `json:"receipt_id"`
	ASNNumber  string                 `json:"asn_number"`
	SSCC       string                 `json:"sscc"`
	ParentSSCC string                 `json:"parent_sscc"`
	Sealed     bool                   `json:"sealed"`
	LPN        string                 `json:"lpn"`
	OwnerRef   string                 `json:"owner_ref"`
	Contents   []receiptCartonContent `json:"contents"`
}

type receiptCartonContent struct {
	LineNo    int    `json:"line_no"`
	SKU       string `json:"sku"`
	GTIN      string `json:"gtin"`
	Qty       int    `json:"qty"`
	Lot       string `json:"lot"`
	Expiry    string `json:"expiry"`
	Condition string `json:"condition"`
}

// InboundReceiptBooker is the use-case port the consumer drives, satisfied by
// *usecases.BookInboundReceiptLine.
type InboundReceiptBooker interface {
	Execute(ctx context.Context, l usecases.InboundReceiptLine) (usecases.BookOutcome, error)
}

// InboundCartonLinker is the use-case port the consumer drives for carton
// events, satisfied by *usecases.LinkReceiptCarton.
type InboundCartonLinker interface {
	Execute(ctx context.Context, c usecases.ReceiptCarton) (usecases.CartonOutcome, usecases.SettleResult, error)
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
	Book InboundReceiptBooker
	// Carton links ReceiptCartonReceived to Containers (ADR 0045). Optional:
	// nil commits carton events past untouched.
	Carton InboundCartonLinker
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

// Run consumes until ctx is cancelled or the reader fails. A cancellation is a
// clean stop and returns nil, even when it lands mid-handle: the in-flight
// message stays uncommitted and is redelivered on the next start.
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
	switch e.Type() {
	case typeReceiptLineReceived:
		return c.handleLine(ctx, msg, e)
	case typeReceiptCartonReceived:
		return c.handleCarton(ctx, msg, e)
	}
	return nil
}

func (c *InboundReceiptConsumer) handleLine(ctx context.Context, msg kafkago.Message, e event.Event) error {
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
		OwnerRef:  data.OwnerRef,
		PackGTIN:  data.PackGTIN,
		Cases:     data.Cases,
		SSCC:      data.SSCC,
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

// handleCarton links one ReceiptCartonReceived. It never books quantity.
func (c *InboundReceiptConsumer) handleCarton(ctx context.Context, msg kafkago.Message, e event.Event) error {
	if c.Carton == nil {
		return nil
	}
	var data receiptCartonReceivedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping inbound-receiving ReceiptCartonReceived with undecodable payload",
			"id", e.ID(), "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	}
	outcome, settled, err := c.Carton.Execute(ctx, data.carton(e.ID()))
	if err != nil {
		if errors.Is(err, usecases.ErrMalformedReceiptCarton) {
			c.Logger.WarnContext(ctx, "skipping invalid inbound-receiving receipt carton",
				"id", e.ID(), "receipt_id", data.ReceiptID, "sscc", data.SSCC, "error", err)
			return nil
		}
		return err
	}
	c.Logger.InfoContext(ctx, "inbound-receiving receipt carton handled",
		"id", e.ID(), "receipt_id", data.ReceiptID, "lpn", data.LPN, "outcome", string(outcome),
		"registered", settled.Registered, "pending", settled.Pending, "rejected", settled.Rejected)
	return nil
}

// carton maps the wire payload onto the use case's command.
func (d receiptCartonReceivedData) carton(eventID string) usecases.ReceiptCarton {
	contents := make([]usecases.ReceiptCartonContent, 0, len(d.Contents))
	for _, ct := range d.Contents {
		contents = append(contents, usecases.ReceiptCartonContent{
			LineNo: ct.LineNo, SKU: ct.SKU, GTIN: ct.GTIN, Quantity: ct.Qty, Lot: ct.Lot, Expiry: ct.Expiry, Condition: ct.Condition,
		})
	}
	return usecases.ReceiptCarton{
		EventID: eventID, ReceiptID: d.ReceiptID, ASNNumber: d.ASNNumber, SSCC: d.SSCC, ParentSSCC: d.ParentSSCC,
		LPN: d.LPN, Sealed: d.Sealed, OwnerRef: d.OwnerRef, Contents: contents,
	}
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
