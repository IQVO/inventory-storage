-- Per-order count of completed PICK tasks (ADR 0035, last-pick confirmation).
--
-- fulfillment-execution creates one PICK task per order LINE and every one
-- publishes TaskCompleted with the same order_ref (= the reservation's
-- demand_ref). A Reservation has no line identity, so the only correlation
-- available is a count: inventory-storage confirms the order's reservations
-- when picked_tasks reaches the number of confirmable reservations.
--
-- The row is incremented in the SAME transaction as the processed_events
-- claim, so a redelivered event cannot count twice and a rolled-back handling
-- un-counts. updated_at is what the housekeeping sweeper (ADR 0026) ages out
-- (ORDER_PICK_PROGRESS_RETENTION), so the table does not grow forever.
CREATE TABLE order_pick_progress (
    demand_ref   TEXT        PRIMARY KEY,
    picked_tasks INTEGER     NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX order_pick_progress_updated_at_idx ON order_pick_progress (updated_at);
