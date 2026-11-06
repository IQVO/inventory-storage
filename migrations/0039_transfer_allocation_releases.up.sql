-- Transfer allocation RELEASE ledger (cancel-after-allocation
-- compensation, ADR 0050).
--
-- One row PER transfer line this service has ever released, keyed by the
-- planning context's transfer_line_id (DB-unique) — the mirror of the
-- transfer_allocations ledger for the release leg. It makes a replayed
-- TransferAllocationReleaseRequested command idempotent at the DATABASE
-- level: a second command for the same transfer_line_id hits the unique
-- constraint and is routed to the "return the original outcome" path
-- instead of resolving the reservation again.
--
-- outcome is RELEASED (the reservation still held the stock; this
-- command revoked it and returned the quantity to usable) or
-- ALREADY_RESOLVED (the reservation had already expired or been
-- confirmed — or the line's allocation was REJECTED, so no hold ever
-- existed; either way the hold is gone). Both settle the saga as
-- success. reason carries the command's cancellation reason.

CREATE TABLE transfer_allocation_releases (
    id                BIGSERIAL PRIMARY KEY,
    transfer_id       TEXT NOT NULL,
    transfer_line_id  TEXT NOT NULL,
    reservation_id    TEXT NOT NULL,
    origin_site_id    TEXT NOT NULL,
    sku               TEXT NOT NULL,
    quantity          INTEGER NOT NULL CHECK (quantity > 0),
    outcome           TEXT NOT NULL CHECK (outcome IN ('RELEASED', 'ALREADY_RESOLVED')),
    reason            TEXT NOT NULL,
    decided_at        TIMESTAMPTZ NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT transfer_allocation_releases_line_unique UNIQUE (transfer_line_id)
);

CREATE INDEX idx_transfer_allocation_releases_transfer ON transfer_allocation_releases (transfer_id);
