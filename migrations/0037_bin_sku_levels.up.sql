-- Per-(bin, sku) level version (ADR 0040): the strictly increasing counter that
-- rides every BinStockLevelChanged integration event, so a consumer keeps the
-- highest version it has seen and ignores lower ones.
--
-- The row is bumped with an upsert INSIDE the transaction of the ledger change
-- that moves the level, and the absolute levels are read after that upsert: the
-- row lock it takes serialises every concurrent change of the same (bin, sku)
-- until commit, so versions are gapless (a rolled-back change rolls its bump
-- back too), never duplicated, and survive restarts. Additive: nothing reads
-- this table except the level emitter; rows are created on first use.
CREATE TABLE bin_sku_levels (
    bin_id     TEXT        NOT NULL,
    sku        TEXT        NOT NULL,
    version    BIGINT      NOT NULL CHECK (version >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bin_id, sku)
);

-- The level read sums a (bin, sku) slice of the stock ledger.
CREATE INDEX idx_stock_units_bin_sku ON stock_units (bin_id, sku);
