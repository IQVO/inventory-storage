-- product-master owns product classification (ADR 0034, product-master ADR
-- 0003 stage C). product_classifications becomes a LOCAL COPY fed by
-- product-master's ProductClassified events on warehouse.product-master.events.
--
-- version: product-master's Product.version carried on every event. A
-- message is applied only when its version is greater than the stored one.
-- Rows written here before the cutover keep 0, so the first product-master
-- version (>= 1) always replaces them.
--
-- classification_source: who wrote the row. 'inventory-storage' for the
-- legacy rows written by the retired PUT endpoint; product-master's own
-- label ('native' | 'legacy-import') for rows applied from its events.
ALTER TABLE product_classifications
    ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE product_classifications
    ADD COLUMN classification_source TEXT NOT NULL DEFAULT 'inventory-storage';

-- Inbound CloudEvents dedupe. One row per (consumer, CloudEvents id) ever
-- applied; the claim is inserted in the SAME transaction as the effect, so
-- a rolled-back handler un-claims it. consumer names the inbound flow
-- (e.g. 'product-master-classification') so later consumers can share the
-- table without colliding on ids.
CREATE TABLE processed_events (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
