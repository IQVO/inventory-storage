-- Site custody on stock units (transfer-safe allocation, Phase 2).
--
-- A StockUnit now records WHICH warehouse site it physically sits at.
-- The column is nullable on purpose: every row persisted before this
-- migration has no recorded site, and guessing one (defaulting to a
-- 'primary' site, say) would let the wrong facility donate stock to a
-- transfer. Legacy site-less rows stay unallocatable for transfers —
-- they keep serving ordinary demand unchanged.
--
-- Bin registration (PUT /bins/{binId}) can carry a site, so bins get the
-- same nullable custody column: a bin's site is the natural place a
-- stowed unit's custody comes from, making new stock site-scoped at stow
-- time without touching the REST contract for existing callers.

ALTER TABLE stock_units ADD COLUMN site_id TEXT;

ALTER TABLE bins ADD COLUMN site_id TEXT;

CREATE INDEX idx_stock_units_site_sku ON stock_units (site_id, sku) WHERE site_id IS NOT NULL;
