-- Held stock and disposition (ADR 0052). Additive: two new tables, nothing
-- existing is altered, so every row already persisted is untouched.
--
-- A held unit is a received quantity kept out of usable stock (a Damaged
-- inbound line today). It has no bin, so it is NOT a stock_units row and no
-- usable-quantity read can see it. state is HELD until a final disposition
-- (RELEASE, RETURN_TO_VENDOR, SCRAP) makes it REMOVED; REWORK keeps it HELD
-- with the reason updated. The disposition and its instant stay on the row as
-- the audit trail.
CREATE TABLE held_stock (
    id           TEXT        PRIMARY KEY,
    receipt_id   TEXT        NOT NULL,
    asn_number   TEXT        NOT NULL DEFAULT '',
    line_no      INTEGER     NOT NULL CHECK (line_no >= 1),
    sku          TEXT        NOT NULL,
    quantity     INTEGER     NOT NULL CHECK (quantity > 0),
    owner_ref    TEXT,
    sscc         TEXT,
    reason       TEXT        NOT NULL CHECK (reason IN ('DAMAGED', 'QC_PENDING', 'SUSPECT', 'OTHER')),
    state        TEXT        NOT NULL CHECK (state IN ('HELD', 'REMOVED')),
    disposition  TEXT        CHECK (disposition IS NULL OR disposition IN ('RELEASE', 'RETURN_TO_VENDOR', 'SCRAP', 'REWORK')),
    rework_count INTEGER     NOT NULL DEFAULT 0 CHECK (rework_count >= 0),
    held_at      TIMESTAMPTZ NOT NULL,
    decided_at   TIMESTAMPTZ,
    version      INTEGER     NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A REMOVED unit has always been dispositioned.
    CONSTRAINT held_stock_removed_has_disposition CHECK (state <> 'REMOVED' OR (disposition IS NOT NULL AND decided_at IS NOT NULL))
);

-- The list read: units still held, oldest first (the keyset order), by reason or SKU.
CREATE INDEX idx_held_stock_open_age ON held_stock (held_at, id) WHERE state = 'HELD';
CREATE INDEX idx_held_stock_open_reason ON held_stock (reason, held_at, id) WHERE state = 'HELD';
CREATE INDEX idx_held_stock_open_sku ON held_stock (sku, held_at, id) WHERE state = 'HELD';
CREATE INDEX idx_held_stock_receipt_line ON held_stock (receipt_id, line_no);

-- The carton provenance of a held line: a Damaged carton content marks its LPN
-- on the (receipt, line). It books no quantity (the line event is the sole
-- quantity authority), so it is independent of the order the events arrive in.
CREATE TABLE held_stock_carton_marks (
    receipt_id TEXT        NOT NULL,
    line_no    INTEGER     NOT NULL CHECK (line_no >= 1),
    lpn        TEXT        NOT NULL,
    sscc       TEXT,
    marked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (receipt_id, line_no, lpn)
);
