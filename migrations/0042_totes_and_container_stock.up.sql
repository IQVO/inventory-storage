-- Totes, stock per container and allocations naming their container (ADR 0054).
-- Additive: every new column is nullable, every existing row keeps its meaning
-- (no tote exists until one is registered; existing SEALED_CASE units are tagged
-- with their case, which is what they already were).

-- One license-plate namespace across sealed cases (containers) and totes.
CREATE TABLE license_plates (
    lpn        TEXT        PRIMARY KEY,
    kind       TEXT        NOT NULL CHECK (kind IN ('CASE', 'TOTE')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO license_plates (lpn, kind)
SELECT lpn, 'CASE' FROM containers
ON CONFLICT (lpn) DO NOTHING;

-- The Tote aggregate: its logical location is bin_id (NULL until placed).
CREATE TABLE totes (
    lpn        TEXT        PRIMARY KEY REFERENCES license_plates (lpn),
    site_id    TEXT,
    bin_id     TEXT,
    version    INTEGER     NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_totes_bin ON totes (bin_id, lpn) WHERE bin_id IS NOT NULL;

-- Stock per container: NULL = stock in no container.
ALTER TABLE stock_units ADD COLUMN container_lpn TEXT;
UPDATE stock_units su
SET container_lpn = c.lpn
FROM containers c
WHERE c.stock_unit_id = su.id
  AND su.container_lpn IS NULL;
CREATE INDEX idx_stock_units_container ON stock_units (container_lpn) WHERE container_lpn IS NOT NULL;

-- The container an allocation drew from (REST allocations[].containerId).
ALTER TABLE reservation_allocations ADD COLUMN container_lpn TEXT;
UPDATE reservation_allocations ra
SET container_lpn = su.container_lpn
FROM stock_units su
WHERE su.id = ra.stock_unit_id
  AND su.container_lpn IS NOT NULL
  AND ra.container_lpn IS NULL;
