-- Intra-site relocations (ADR 0039): one row per applied RelocateStock,
-- written in the SAME transaction as the bin/stock-unit updates and the
-- StockRelocated outbox row. A row is immutable once written (no UPDATE
-- path): it is the ledger's record of a move and answers
-- GET /relocations/{id}. legs is the JSON array of source contributions
-- [{"stock_unit_id": "...", "quantity": n}].
CREATE TABLE relocations (
    id                        TEXT        PRIMARY KEY,
    site_code                 TEXT        NOT NULL,
    sku                       TEXT        NOT NULL,
    from_bin                  TEXT        NOT NULL,
    to_bin                    TEXT        NOT NULL,
    quantity                  INTEGER     NOT NULL CHECK (quantity > 0),
    reason                    TEXT        NOT NULL,
    source_ref                TEXT        NOT NULL DEFAULT '',
    legs                      JSONB       NOT NULL,
    destination_stock_unit_id TEXT        NOT NULL,
    occurred_at               TIMESTAMPTZ NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT relocations_distinct_bins CHECK (from_bin <> to_bin)
);

CREATE INDEX relocations_site_sku_idx ON relocations (site_code, sku, occurred_at DESC);
CREATE INDEX relocations_source_ref_idx ON relocations (source_ref) WHERE source_ref <> '';
