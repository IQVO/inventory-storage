-- Down migration for 0032: drop the destination receipt and quarantine
-- tables. StockUnits created by a completed transfer stow are NOT
-- removed — they are ordinary stock rows and rolling a schema migration
-- back must never destroy physical-custody facts.
DROP TABLE IF EXISTS inventory_exceptions;
DROP TABLE IF EXISTS transfer_receipts;
