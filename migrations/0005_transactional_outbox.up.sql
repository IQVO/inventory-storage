-- Replaces the dead-end `events` table (a write-only sink with no relay,
-- see the retired postgres.EventPublisher) with a real transactional
-- outbox: one already-encoded Kafka message per (event x topic), so the
-- outbox relay can drain it onto the broker (ADR 0017).
DROP TABLE IF EXISTS events;

CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    key          BYTEA,
    value        BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '[]', -- [{"key":..,"value":..}] (W3C trace headers)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);

CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
