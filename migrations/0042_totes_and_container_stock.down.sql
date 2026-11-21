ALTER TABLE reservation_allocations DROP COLUMN IF EXISTS container_lpn;
DROP INDEX IF EXISTS idx_stock_units_container;
ALTER TABLE stock_units DROP COLUMN IF EXISTS container_lpn;
DROP TABLE IF EXISTS totes;
DROP TABLE IF EXISTS license_plates;
