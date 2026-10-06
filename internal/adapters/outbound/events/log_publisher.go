// Package events provides outbound EventPublisher implementations. The
// interface is intentionally the shape a Kafka producer would satisfy
// (Publish(ctx, event) error), so a kafka.Publisher can be dropped in later
// without touching the application layer.
package events

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/claudioed/inventory-storage/internal/domain/shared"
)

// LogPublisher publishes domain events by logging them as JSON. Useful for
// local development and as a default when no broker is configured.
type LogPublisher struct {
	logger *slog.Logger
}

func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	payload, err := json.Marshal(logPayload(event))
	if err != nil {
		return err
	}
	p.logger.InfoContext(ctx, "domain event published", "event_name", event.EventName(), "payload", json.RawMessage(payload))
	return nil
}
