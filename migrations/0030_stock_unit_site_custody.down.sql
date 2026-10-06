DROP INDEX IF EXISTS idx_stock_units_site_sku;

ALTER TABLE bins DROP COLUMN IF EXISTS site_id;

ALTER TABLE stock_units DROP COLUMN IF EXISTS site_id;
