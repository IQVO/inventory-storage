-- Containers (LPN), sealed cases, owner-segregated stock and the packaging
-- view (ADR 0045, 0046). Additive and dormant: every new column is nullable
-- (or defaulted to the fleet-owned / loose meaning) and every new table is
-- empty until containers or owners appear on the wire, so behaviour with no
-- cartons and no ownerRef is identical to before.

-- Stock is segregated by owner. NULL owner_ref = fleet-owned (all existing
-- rows); NULL pack_state = LOOSE.
ALTER TABLE stock_units ADD COLUMN owner_ref  TEXT;
ALTER TABLE stock_units ADD COLUMN pack_state TEXT CHECK (pack_state IS NULL OR pack_state IN ('LOOSE', 'SEALED_CASE'));
CREATE INDEX idx_stock_units_sku_owner ON stock_units (sku, owner_ref);

-- A reservation is bound to exactly one owner (NULL = fleet-owned).
ALTER TABLE reservations ADD COLUMN owner_ref TEXT;

-- BinStockLevelChanged versions are per (bin, sku, owner): '' = fleet-owned.
ALTER TABLE bin_sku_levels ADD COLUMN owner_ref TEXT NOT NULL DEFAULT '';
ALTER TABLE bin_sku_levels DROP CONSTRAINT bin_sku_levels_pkey;
ALTER TABLE bin_sku_levels ADD PRIMARY KEY (bin_id, sku, owner_ref);

-- Local projection of product-master's ProductPackLevelDeclared/Retired
-- (the PackagingView, contract R8). Keyed by GTIN; a retired level stays
-- resolvable by GTIN. version is product-master's product version.
CREATE TABLE pack_levels (
    gtin           TEXT        PRIMARY KEY,
    sku            TEXT        NOT NULL,
    level          TEXT        NOT NULL CHECK (level IN ('EACH', 'INNER', 'CASE', 'PALLET')),
    units_per_pack INTEGER     NOT NULL CHECK (units_per_pack >= 1),
    child_gtin     TEXT,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,
    version        BIGINT      NOT NULL CHECK (version >= 1),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_pack_levels_sku_level ON pack_levels (sku, level);

-- The Container aggregate. lpn is the carton SSCC or an internal LPN and is
-- unique; site_id and bin_id stay NULL until StowContainer (the receipt carries
-- no site: the site is the bin's). stock_unit_id names the 1:1 SEALED_CASE unit.
CREATE TABLE containers (
    lpn            TEXT        PRIMARY KEY,
    receipt_id     TEXT        NOT NULL,
    line_no        INTEGER     NOT NULL CHECK (line_no >= 1),
    sku            TEXT        NOT NULL,
    gtin           TEXT        NOT NULL,
    units_per_pack INTEGER     NOT NULL CHECK (units_per_pack >= 1),
    qty_eaches     INTEGER     NOT NULL CHECK (qty_eaches >= 1),
    owner_ref      TEXT,
    site_id        TEXT,
    bin_id         TEXT,
    state          TEXT        NOT NULL CHECK (state IN ('SEALED', 'OPEN', 'EMPTY')),
    lot            TEXT,
    expiry         TEXT,
    stock_unit_id  TEXT,
    version        INTEGER     NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_containers_receipt_line ON containers (receipt_id, line_no);
CREATE INDEX idx_containers_bin ON containers (bin_id) WHERE bin_id IS NOT NULL;
CREATE INDEX idx_containers_sku_stowed ON containers (sku, owner_ref, created_at) WHERE state = 'SEALED' AND bin_id IS NOT NULL;
CREATE UNIQUE INDEX uq_containers_stock_unit ON containers (stock_unit_id) WHERE stock_unit_id IS NOT NULL;

-- What ReceiptLineReceived booked per (receipt_id, line_no): the minimum of
-- the receipt kept to bound containers (ADR 0045 section 6). Summed when a
-- line is received in more than one event; owner_ref is the line's owner.
CREATE TABLE receipt_line_bookings (
    receipt_id    TEXT        NOT NULL,
    line_no       INTEGER     NOT NULL CHECK (line_no >= 1),
    sku           TEXT        NOT NULL,
    owner_ref     TEXT,
    booked_eaches INTEGER     NOT NULL CHECK (booked_eaches >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (receipt_id, line_no)
);

-- A sealed carton that could not become a Container yet: its line is not
-- booked (or not booked enough), or its GTIN is not in the packaging view.
-- Re-evaluated when the booking or the pack level arrives. REJECTED rows are
-- kept for audit (quantity mismatch, GTIN of another SKU, LPN taken elsewhere).
CREATE TABLE container_pending_links (
    receipt_id   TEXT        NOT NULL,
    line_no      INTEGER     NOT NULL CHECK (line_no >= 1),
    lpn          TEXT        NOT NULL,
    sscc         TEXT,
    parent_sscc  TEXT,
    sku          TEXT        NOT NULL,
    gtin         TEXT        NOT NULL,
    qty_eaches   INTEGER     NOT NULL CHECK (qty_eaches >= 1),
    owner_ref    TEXT,
    lot          TEXT,
    expiry       TEXT,
    status       TEXT        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'REJECTED')),
    reason       TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (receipt_id, line_no, lpn)
);
CREATE INDEX idx_container_pending_sku ON container_pending_links (sku) WHERE status = 'PENDING';
