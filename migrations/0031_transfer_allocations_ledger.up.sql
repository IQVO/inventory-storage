-- Transfer allocation ledger (transfer-safe allocation, Phase 2).
--
-- One row PER transfer line this service has ever decided, keyed by the
-- planning context's transfer_line_id (DB-unique). The ledger is what
-- makes a replayed allocation command idempotent at the DATABASE level,
-- not just the use-case level: a second command for the same
-- transfer_line_id hits the unique constraint and is routed to the
-- "return the original outcome" path instead of touching stock again.
--
-- outcome is ALLOCATED or REJECTED. reservation_id is NULL exactly when
-- outcome is REJECTED (no stock was held). rejection_reason carries the
-- closed reason code (ORIGIN_SITE_UNKNOWN, INSUFFICIENT_USABLE,
-- IDEMPOTENCY_CONFLICT) for a rejection.

CREATE TABLE transfer_allocations (
    id                BIGSERIAL PRIMARY KEY,
    transfer_id       TEXT NOT NULL,
    transfer_line_id  TEXT NOT NULL,
    origin_site_id    TEXT NOT NULL,
    sku               TEXT NOT NULL,
    requested_quantity INTEGER NOT NULL CHECK (requested_quantity > 0),
    reservation_id    TEXT,
    outcome           TEXT NOT NULL CHECK (outcome IN ('ALLOCATED', 'REJECTED')),
    rejection_reason  TEXT,
    decided_at        TIMESTAMPTZ NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT transfer_allocations_line_unique UNIQUE (transfer_line_id),
    CONSTRAINT transfer_allocations_rejection_shape CHECK (
        (outcome = 'ALLOCATED' AND reservation_id IS NOT NULL AND rejection_reason IS NULL)
        OR
        (outcome = 'REJECTED' AND reservation_id IS NULL AND rejection_reason IS NOT NULL)
    )
);

CREATE INDEX idx_transfer_allocations_transfer ON transfer_allocations (transfer_id);
