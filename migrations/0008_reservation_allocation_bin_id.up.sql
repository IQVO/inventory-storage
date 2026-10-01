-- Pick location on allocations (ADR 0025). A reservation's allocations
-- record WHICH stock units it drew from; this adds the bin each of those
-- units sits in, so GET/POST /reservations can tell a picker where to go
-- without a second lookup per stock unit.
--
-- Nullable: allocations are written by ReserveStock, which now always
-- supplies bin_id, but rows persisted before this migration only gain one
-- via the backfill below. A StockUnit never changes bin (stow is the only
-- way one is created, and nothing moves it), so the backfill from
-- stock_units.bin_id is exact, not an approximation.
ALTER TABLE reservation_allocations ADD COLUMN bin_id TEXT;

UPDATE reservation_allocations ra
SET bin_id = su.bin_id
FROM stock_units su
WHERE su.id = ra.stock_unit_id
  AND ra.bin_id IS NULL;
