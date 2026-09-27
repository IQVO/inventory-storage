-- Adds a version column to every mutable aggregate table whose repo Save
-- previously did a blind full-column `ON CONFLICT (id) DO UPDATE SET ...`
-- with no WHERE/version guard (stock_units, bins, reservations) — a
-- confirmed lost-update race: two concurrent writers reading the same
-- row can silently clobber each other with no error to either caller.
-- See ADR 0018 (optimistic concurrency).
ALTER TABLE stock_units ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE bins ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE reservations ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
