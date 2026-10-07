-- Destination transfer receipt custody (ADR 0031, Phase 3).
--
-- transfer_receipts is the destination-side twin of the origin-side
-- transfer_allocations ledger: ONE row per transfer line that arrived,
-- keyed DB-UNIQUE on transfer_line_id. The receipt records what the
-- ledger promised (expected_quantity), what was physically counted at
-- the dock (received_quantity), and the SIGNED variance (received -
-- expected; positive = over, negative = short, 0 = exact). Over/short
-- is explicit on the row and in the TransferReceiptStaged event — never
-- silently absorbed.
--
-- state is STAGED (counted against a recognized ALLOCATED line, no
-- usable stock yet) then STOWED (destination StockUnits exist; usable
-- rose exactly once). stow_legs records the destination StockUnits the
-- stow created so an idempotent replay returns the ORIGINAL outcome
-- without creating a second unit or republishing the event.
--
-- inventory_exceptions is the quarantine table: a destination scan that
-- could NOT be matched to a recognized ALLOCATED transfer line lands
-- here instead of raising stock. No stock row is created, no usable
-- quantity changes, no availability-raising event is published.

CREATE TABLE transfer_receipts (
    id                  BIGSERIAL PRIMARY KEY,
    transfer_id         TEXT NOT NULL,
    transfer_line_id    TEXT NOT NULL,
    destination_site_id TEXT NOT NULL,
    sku                 TEXT NOT NULL,
    reservation_id      TEXT NOT NULL,
    expected_quantity   INTEGER NOT NULL CHECK (expected_quantity > 0),
    received_quantity   INTEGER NOT NULL CHECK (received_quantity > 0),
    variance            INTEGER NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('STAGED', 'STOWED')),
    stow_legs           JSONB,
    staged_at           TIMESTAMPTZ NOT NULL,
    stowed_at           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT transfer_receipts_line_unique UNIQUE (transfer_line_id),
    CONSTRAINT transfer_receipts_variance_shape CHECK (variance = received_quantity - expected_quantity),
    CONSTRAINT transfer_receipts_stowed_shape CHECK (
        (state = 'STAGED' AND stowed_at IS NULL AND stow_legs IS NULL)
        OR
        (state = 'STOWED' AND stowed_at IS NOT NULL AND stow_legs IS NOT NULL)
    )
);

CREATE INDEX idx_transfer_receipts_transfer ON transfer_receipts (transfer_id);

CREATE TABLE inventory_exceptions (
    id                  BIGSERIAL PRIMARY KEY,
    transfer_id         TEXT NOT NULL DEFAULT '',
    transfer_line_id    TEXT NOT NULL,
    destination_site_id TEXT NOT NULL,
    sku                 TEXT NOT NULL,
    received_quantity   INTEGER NOT NULL CHECK (received_quantity >= 0),
    kind                TEXT NOT NULL CHECK (kind IN ('UNKNOWN_TRANSFER', 'UNRECOGNIZED_TRANSFER')),
    detail              TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL,
    CONSTRAINT inventory_exceptions_scan_unique UNIQUE (transfer_line_id, destination_site_id, sku, received_quantity)
);

CREATE INDEX idx_inventory_exceptions_site ON inventory_exceptions (destination_site_id);
