DROP TABLE IF EXISTS processed_events;

ALTER TABLE product_classifications DROP COLUMN IF EXISTS classification_source;

ALTER TABLE product_classifications DROP COLUMN IF EXISTS version;
