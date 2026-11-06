-- Forward pick faces: a LOCAL read model of slotting-optimization's approved
-- slot plans (ADR 0050). ReserveStock reads it to draw a SKU's forward pick
-- face before its reserve bins, without any call to a sibling at reservation
-- time.
--
-- forward_pick_face_plans holds, per site, the plan whose map is currently
-- applied (plan id + approval instant, the supersession guard).
-- forward_pick_faces holds that plan's FULL assignment map; it is replaced
-- wholesale per site when a later plan is approved.
CREATE TABLE forward_pick_face_plans (
    site_id     TEXT        PRIMARY KEY,
    plan_id     TEXT        NOT NULL,
    approved_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE forward_pick_faces (
    site_id TEXT NOT NULL REFERENCES forward_pick_face_plans (site_id) ON DELETE CASCADE,
    sku     TEXT NOT NULL,
    bin_id  TEXT NOT NULL,
    PRIMARY KEY (site_id, sku, bin_id)
);

CREATE INDEX forward_pick_faces_sku_idx ON forward_pick_faces (sku);
