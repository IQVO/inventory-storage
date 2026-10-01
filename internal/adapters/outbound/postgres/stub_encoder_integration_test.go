//go:build integration

package postgres_test

import (
	"context"

	"github.com/claudioed/inventory-storage/internal/adapters/outbound/kafka"
	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// stubEncoder is a minimal kafka.Encoder used by integration tests that
// only need to prove an outbox row was (or was not) written for a given
// event — not to exercise the real Kafka wire format (kafka.Publisher /
// kafka.AnalyticsPublisher's own Encode methods are covered directly by
// their own unit tests in the kafka package). It encodes every event to
// exactly one row on a fixed test topic, keyed by the event's
// ReservationID field when present (via a small type switch), so a test
// can filter outbox_events by key to isolate its own fixture from
// whatever earlier test runs left behind.
type stubEncoder struct{}

const stubEncoderTopic = "test.outbox"

func (stubEncoder) Encode(_ context.Context, event shared.DomainEvent) ([]kafka.Encoded, error) {
	var key []byte
	switch e := event.(type) {
	case shared.ReservationExpired:
		key = []byte(e.ReservationID)
	case shared.ReservationRevoked:
		key = []byte(e.ReservationID)
	}
	return []kafka.Encoded{{
		Topic:     stubEncoderTopic,
		EventType: event.EventName(),
		Key:       key,
		Value:     []byte(`{"stub":"` + event.EventName() + `"}`),
	}}, nil
}
