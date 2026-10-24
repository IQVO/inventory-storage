package kafka

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/inventory-storage/internal/application/usecases"
)

const typeCartonForTest = "com.warehouse.wms.inbound-receiving.receipt.ReceiptCartonReceived"

// cartonRecorder records the cartons handed to the use case.
type cartonRecorder struct {
	got []usecases.ReceiptCarton
	err error
}

func (r *cartonRecorder) Execute(_ context.Context, c usecases.ReceiptCarton) (usecases.CartonOutcome, usecases.SettleResult, error) {
	r.got = append(r.got, c)
	return usecases.CartonRecorded, usecases.SettleResult{Registered: 1}, r.err
}

// cartonPayload is ReceiptCartonReceived exactly as the contract names it
// (section 2 with R6/R7): snake_case, NO site, plus fields this service does
// not know, which the decode must tolerate.
func cartonPayload() map[string]any {
	return map[string]any{
		"receipt_id": "rcpt-1", "asn_number": "ASN-1001", "sscc": "003400000000000017", "parent_sscc": "003400000000000024",
		"sealed": true, "lpn": "003400000000000017", "owner_ref": "seller-a", "received_at": "2026-10-08T14:00:00Z",
		"future_field": map[string]any{"x": 1},
		"contents": []map[string]any{{
			"line_no": 2, "sku": "SKU-1", "gtin": "24006381333935", "qty": 12, "lot": "L1", "expiry": "2027-03-01",
			"condition": "Good", "another_future_field": "ignored",
		}},
	}
}

func cartonConsumer(rec *cartonRecorder) *InboundReceiptConsumer {
	return &InboundReceiptConsumer{Carton: rec, Logger: quietLogger()}
}

func TestInboundReceiptConsumer_CartonIsDecodedToleratingUnknownFields(t *testing.T) {
	rec := &cartonRecorder{}
	msg := receiptMsg(0, receiptCE(t, "ce-carton-1", typeCartonForTest, cartonPayload()))
	if err := cartonConsumer(rec).Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(rec.got) != 1 {
		t.Fatalf("cartons = %d, want 1", len(rec.got))
	}
	c := rec.got[0]
	want := usecases.ReceiptCarton{
		EventID: "ce-carton-1", ReceiptID: "rcpt-1", ASNNumber: "ASN-1001", SSCC: "003400000000000017", ParentSSCC: "003400000000000024",
		LPN: "003400000000000017", Sealed: true, OwnerRef: "seller-a",
		Contents: []usecases.ReceiptCartonContent{{LineNo: 2, SKU: "SKU-1", GTIN: "24006381333935", Quantity: 12, Lot: "L1", Expiry: "2027-03-01", Condition: "Good"}},
	}
	if c.EventID != want.EventID || c.ReceiptID != want.ReceiptID || c.SSCC != want.SSCC || c.ParentSSCC != want.ParentSSCC ||
		c.LPN != want.LPN || !c.Sealed || c.OwnerRef != want.OwnerRef || len(c.Contents) != 1 || c.Contents[0] != want.Contents[0] {
		t.Fatalf("decoded carton = %+v, want %+v", c, want)
	}
}

func TestInboundReceiptConsumer_CartonErrors(t *testing.T) {
	msg := receiptMsg(0, receiptCE(t, "ce-carton-1", typeCartonForTest, cartonPayload()))

	transient := &cartonRecorder{err: errors.New("db is down")}
	if err := cartonConsumer(transient).Handle(context.Background(), msg); err == nil {
		t.Fatal("a transient failure must be returned for retry")
	}
	malformed := &cartonRecorder{err: usecases.ErrMalformedReceiptCarton}
	if err := cartonConsumer(malformed).Handle(context.Background(), msg); err != nil {
		t.Fatalf("a malformed carton must be skipped, got %v", err)
	}
	undecodable := receiptMsg(1, receiptCE(t, "ce-2", typeCartonForTest, map[string]any{"contents": "not-a-list"}))
	rec := &cartonRecorder{}
	if err := cartonConsumer(rec).Handle(context.Background(), undecodable); err != nil || len(rec.got) != 0 {
		t.Fatalf("an undecodable payload must be skipped: err %v, handled %d", err, len(rec.got))
	}
}

func TestInboundReceiptConsumer_CartonEventsAreIgnoredWithoutALinker(t *testing.T) {
	msg := receiptMsg(0, receiptCE(t, "ce-carton-1", typeCartonForTest, cartonPayload()))
	if err := (&InboundReceiptConsumer{Logger: quietLogger()}).Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
}

// lineBooker records the line the use case receives, to pin the new optional
// fields.
type lineBooker struct{ got []usecases.InboundReceiptLine }

func (b *lineBooker) Execute(_ context.Context, l usecases.InboundReceiptLine) (usecases.BookOutcome, error) {
	b.got = append(b.got, l)
	return usecases.LineBooked, nil
}

func TestInboundReceiptConsumer_LineCarriesTheOptionalPackFields(t *testing.T) {
	booker := &lineBooker{}
	data := lineData("SKU-1", 36, "Good")
	data["owner_ref"], data["pack_gtin"], data["cases"], data["sscc"] = "seller-a", "24006381333935", 3, "003400000000000017"
	data["a_field_from_the_future"] = true
	c := &InboundReceiptConsumer{Book: booker, Logger: quietLogger()}
	if err := c.Handle(context.Background(), receiptMsg(0, receiptCE(t, "ce-1", typeReceiptLineReceived, data))); err != nil {
		t.Fatal(err)
	}
	if len(booker.got) != 1 {
		t.Fatalf("lines = %d", len(booker.got))
	}
	l := booker.got[0]
	if l.Quantity != 36 || l.OwnerRef != "seller-a" || l.PackGTIN != "24006381333935" || l.Cases != 3 || l.SSCC != "003400000000000017" {
		t.Fatalf("line = %+v", l)
	}
}

func TestInboundReceiptConsumer_LineWithoutOptionalFieldsStillDecodes(t *testing.T) {
	booker := &lineBooker{}
	c := &InboundReceiptConsumer{Book: booker, Logger: quietLogger()}
	if err := c.Handle(context.Background(), kafkago.Message{Value: receiptCE(t, "ce-1", typeReceiptLineReceived, lineData("SKU-1", 5, "Good"))}); err != nil {
		t.Fatal(err)
	}
	if l := booker.got[0]; l.OwnerRef != "" || l.PackGTIN != "" || l.Cases != 0 || l.SSCC != "" || l.Quantity != 5 {
		t.Fatalf("legacy line = %+v", l)
	}
}
