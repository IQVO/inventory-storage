package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// TestProductMasterConsumerLogsNoEventDerivedLineBreaks feeds the consumer a payload that is not a CloudEvent and a
// CloudEvent of a consumed type whose id, subject and data carry CR/LF and
// whose data does not decode: no log attribute may carry the line break.
func TestProductMasterConsumerLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	c := &ProductMasterConsumer{Logger: slog.New(rec)}
	ctx := context.Background()
	_ = c.Handle(ctx, kafkago.Message{Topic: "logsafe.topic", Value: []byte("not a CloudEvent" + crlfForged)})
	b, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "id" + crlfForged, "type": typeProductMasterClassified, "source": "urn:logsafe",
		"subject": "subject" + crlfForged, "datacontenttype": "application/json",
		"data": []string{"not an object" + crlfForged},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Handle(ctx, kafkago.Message{Topic: "logsafe.topic", Key: []byte("key" + crlfForged), Value: b})
	rec.assertNoLineBreaks(t, 2)
}
