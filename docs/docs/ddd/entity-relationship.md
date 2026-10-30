---
title: Entity-Relationship Diagram
sidebar_label: ER Diagram
sidebar_position: 9
description: The final OLTP and analytical Postgres schemas of inventory-storage, from migrations applied in order — every table, column, key and real foreign key, with the table-to-aggregate mapping.
---

# Entity-Relationship Diagram

The schema after applying `migrations/0001`…`0008`, `0030`…`0033`
(site custody, transfer ledger, transfer receipts and exceptions, `processed_events`),
`0034` (`order_pick_progress`), `0035` (`reservations.line_no`), `0036` (`relocations`),
`0037` (`bin_sku_levels`) and `0038` (containers, owners, packaging) (OLTP database,
`DATABASE_URL`) and `migrations/analytics/0001` (analytical database,
`ANALYTICS_DATABASE_URL`) in order. Relationship lines are drawn **only**
where a real `FOREIGN KEY` (`REFERENCES`) exists in the migrations.

## OLTP database

```mermaid
erDiagram
    bins {
        TEXT id PK
        INTEGER capacity "CHECK capacity > 0"
        INTEGER occupied "CHECK occupied >= 0"
        INTEGER version "optimistic concurrency, default 1"
    }
    stock_units {
        TEXT id PK
        TEXT sku "indexed"
        TEXT bin_id FK "REFERENCES bins, indexed"
        INTEGER quantity "CHECK quantity >= 0"
        INTEGER reserved "CHECK reserved >= 0"
        TEXT state "AVAILABLE RESERVED PICKED REMOVED UNLOCATED"
        INTEGER version "default 1"
    }
    reservations {
        TEXT id PK
        TEXT sku
        INTEGER quantity "CHECK quantity > 0"
        TEXT demand_ref "indexed, not unique"
        TEXT status "ACTIVE CONFIRMED REVOKED EXPIRED"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ expires_at
        INTEGER version "default 1"
        INTEGER line_no "nullable order line, 32-bit: 1..2147483647 (CHECK >= 1, 0035, ADR 0036)"
    }
    reservation_allocations {
        TEXT reservation_id PK,FK "REFERENCES reservations"
        TEXT stock_unit_id PK,FK "REFERENCES stock_units"
        INTEGER quantity "CHECK quantity > 0"
        TEXT bin_id "nullable pick location, no FK"
    }
    product_classifications {
        TEXT sku PK
        TEXT_ARRAY handling_tags "TEXT array"
        TEXT temperature_class "default empty string"
        SMALLINT dot_hazard_class "nullable, 1 to 9"
    }
    outbox_events {
        BIGSERIAL id PK
        TEXT topic
        TEXT event_type "full CloudEvents type"
        BYTEA key "Kafka key"
        BYTEA value "CloudEvent JSON"
        JSONB headers "content-type and W3C trace"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ published_at "null until relayed, partial index"
        INTEGER attempts
        TEXT last_error
    }
    idempotency_keys {
        TEXT key PK
        TEXT method
        TEXT path
        TEXT request_hash "SHA-256 of body"
        INTEGER status_code
        BYTEA response_body
        JSONB response_headers
        TIMESTAMPTZ created_at "indexed"
        TIMESTAMPTZ completed_at
    }
    schema_migrations {
        BIGINT version PK
        BOOLEAN dirty
    }
    processed_events {
        TEXT consumer PK "inbound flow, e.g. task-completed-confirm-pick"
        TEXT event_id PK "CloudEvents id"
        TIMESTAMPTZ processed_at
    }
    order_pick_progress {
        TEXT demand_ref PK "an OrderId, = reservations.demand_ref, no FK"
        INTEGER picked_tasks "completed PICK tasks, one per order line"
        TIMESTAMPTZ updated_at "indexed, what the sweeper ages out"
    }
    relocations {
        TEXT id PK "one row per applied RelocateStock, immutable (ADR 0039)"
        TEXT site_code
        TEXT sku
        TEXT from_bin "no FK, CHECK from_bin <> to_bin"
        TEXT to_bin "no FK"
        INTEGER quantity "CHECK quantity > 0"
        TEXT reason
        TEXT source_ref "default empty, indexed when set"
        JSONB legs "source contributions [{stock_unit_id, quantity}]"
        TEXT destination_stock_unit_id "no FK"
        TIMESTAMPTZ occurred_at
        TIMESTAMPTZ created_at
    }
    bin_sku_levels {
        TEXT bin_id PK "no FK"
        TEXT sku PK
        TEXT owner_ref PK "0038: '' = fleet-owned"
        BIGINT version "CHECK version >= 1, rides every BinStockLevelChanged (ADR 0040)"
        TIMESTAMPTZ updated_at
    }
    pack_levels {
        TEXT gtin PK "product-master GTIN, no FK"
        TEXT sku "indexed with level"
        TEXT level "EACH INNER CASE PALLET"
        INTEGER units_per_pack "CHECK units_per_pack >= 1"
        TEXT child_gtin "nullable, no FK"
        TIMESTAMPTZ effective_from
        TIMESTAMPTZ effective_to "null while current"
        BIGINT version "CHECK version >= 1, product-master product version"
        TIMESTAMPTZ updated_at
    }
    containers {
        TEXT lpn PK "carton SSCC or internal LPN"
        TEXT receipt_id "inbound-receiving identity, no FK"
        INTEGER line_no "CHECK line_no >= 1"
        TEXT sku
        TEXT gtin
        INTEGER units_per_pack "CHECK units_per_pack >= 1"
        INTEGER qty_eaches "CHECK qty_eaches >= 1"
        TEXT owner_ref "null = fleet-owned"
        TEXT site_id "null until StowContainer"
        TEXT bin_id "null until StowContainer, no FK"
        TEXT state "SEALED OPEN EMPTY"
        TEXT lot
        TEXT expiry
        TEXT stock_unit_id "1:1 SEALED_CASE unit, unique index, no FK"
        INTEGER version "default 1"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }
    receipt_line_bookings {
        TEXT receipt_id PK "inbound-receiving identity, no FK"
        INTEGER line_no PK "CHECK line_no >= 1"
        TEXT sku
        TEXT owner_ref "the line's owner"
        INTEGER booked_eaches "CHECK booked_eaches >= 0, summed across events"
        TIMESTAMPTZ updated_at
    }
    container_pending_links {
        TEXT receipt_id PK "no FK"
        INTEGER line_no PK
        TEXT lpn PK
        TEXT sscc
        TEXT parent_sscc
        TEXT sku "partial index while PENDING"
        TEXT gtin
        INTEGER qty_eaches "CHECK qty_eaches >= 1"
        TEXT owner_ref
        TEXT lot
        TEXT expiry
        TEXT status "PENDING REJECTED, default PENDING"
        TEXT reason
        TIMESTAMPTZ first_seen_at
    }
    transfer_allocations {
        BIGSERIAL id PK
        TEXT transfer_id "indexed"
        TEXT transfer_line_id "UNIQUE, planning context's identity"
        TEXT origin_site_id
        TEXT sku
        INTEGER requested_quantity "CHECK requested_quantity > 0"
        TEXT reservation_id "null on REJECTED, no FK"
        TEXT outcome "ALLOCATED REJECTED"
        TEXT rejection_reason "set exactly on REJECTED"
        TIMESTAMPTZ decided_at
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }
    transfer_receipts {
        BIGSERIAL id PK
        TEXT transfer_id "indexed"
        TEXT transfer_line_id "UNIQUE"
        TEXT destination_site_id
        TEXT sku
        TEXT reservation_id "the allocation's hold, no FK"
        INTEGER expected_quantity "CHECK expected_quantity > 0"
        INTEGER received_quantity "CHECK received_quantity > 0"
        INTEGER variance "CHECK variance = received - expected"
        TEXT state "STAGED STOWED"
        JSONB stow_legs "destination StockUnits the stow created"
        TIMESTAMPTZ staged_at
        TIMESTAMPTZ stowed_at "null until STOWED"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }
    inventory_exceptions {
        BIGSERIAL id PK
        TEXT transfer_id "default empty"
        TEXT transfer_line_id
        TEXT destination_site_id "indexed"
        TEXT sku
        INTEGER received_quantity "CHECK received_quantity >= 0"
        TEXT kind "UNKNOWN_TRANSFER UNRECOGNIZED_TRANSFER"
        TEXT detail
        TIMESTAMPTZ created_at
    }

    bins ||--o{ stock_units : "bin_id"
    reservations ||--|{ reservation_allocations : "reservation_id"
    stock_units ||--o{ reservation_allocations : "stock_unit_id"
```

Source: `migrations/0001_init.up.sql` … `migrations/0008_reservation_allocation_bin_id.up.sql`,
plus `0030_stock_unit_site_custody.up.sql` (`stock_units.site_id`, `bins.site_id`),
`0031_transfer_allocations_ledger.up.sql` (`transfer_allocations`),
`0032_transfer_receipts_and_exceptions.up.sql` (`transfer_receipts`, `inventory_exceptions`, ADR 0031),
`0033_product_master_local_copy.up.sql` (`processed_events`),
`0034_order_pick_progress.up.sql` (`order_pick_progress`, ADR 0035),
`0035_reservation_line_no.up.sql` (`reservations.line_no`, ADR 0036),
`0036_relocations.up.sql` (`relocations`, ADR 0039), `0037_bin_sku_levels.up.sql` (`bin_sku_levels`, ADR 0040) and
`0038_containers_owner_stock_packaging.up.sql` (`pack_levels`, `containers`, `receipt_line_bookings`,
`container_pending_links`, owner/pack columns on `stock_units`, `reservations`, `bin_sku_levels`, ADR 0045, 0046);
`schema_migrations` is created by golang-migrate (`internal/adapters/outbound/postgres/migrate.go`).
Omitted: the `events` table created by `0001` — `0005` drops it, so it is
not part of the final schema. `TEXT_ARRAY` stands for Postgres `TEXT[]`
(kept as a plain token so the Mermaid type parses).

### Links with no foreign key (aggregate boundaries)

| Column | Points at | Why there is no FK |
| --- | --- | --- |
| `reservation_allocations.bin_id` | `bins.id` | Denormalised pick location (ADR 0025), copied from the stock unit at reserve time; nullable for rows the 0008 backfill could not resolve. |
| `stock_units.sku`, `reservations.sku` | `product_classifications.sku` | A SKU does not need a classification — unclassified SKUs are valid and fail open (ADR 0009). |
| `reservations.demand_ref` | an order line in `order-management` | Another bounded context's identity; stored opaquely, never parsed. |
| `reservations.line_no` | the order's line number in `order-management` | Another bounded context's identity, sent as `lineNo` on `POST /reservations` (ADR 0036); nullable (NULL for reservations made before migration 0035 or by a client that does not send it); with `demand_ref` it identifies the line the consumer confirms. |
| `order_pick_progress.demand_ref` | `reservations.demand_ref` | Not unique on `reservations` (one reservation per order line), and the counter row is swept independently of the reservations; correlated by value only (ADR 0035). Only written by the counting fallback for events or reservations without `line_no` (ADR 0036). |
| `outbox_events`, `idempotency_keys`, `processed_events`, `order_pick_progress` | any aggregate row | Infrastructure tables; written in the same transaction as the aggregate change, never joined to it. |
| `transfer_allocations.reservation_id` | `reservations.id` | Ledger row written in the same transaction as the hold; NULL exactly when `outcome = REJECTED`; a replayed command is answered from the ledger, so the reference is never joined (transfer-safe allocation, Phase 2). |
| `transfer_receipts.reservation_id` | `reservations.id` | Copied from the allocation ledger; the receipt stages and stows independently of the reservation lifecycle (ADR 0031). |
| `transfer_allocations.transfer_id` / `transfer_line_id`, `transfer_receipts.transfer_id` / `transfer_line_id` | a transfer in `network-inventory-planning` | Another bounded context's identity; stored opaquely, `transfer_line_id` made DB-unique so replays return the original outcome. |
| `containers.stock_unit_id` | `stock_units.id` | The 1:1 SEALED_CASE unit, enforced by a unique index, not a FK; NULL until the container's stock unit exists (ADR 0045). |
| `containers.receipt_id` / `line_no`, `receipt_line_bookings.receipt_id` / `line_no`, `container_pending_links.receipt_id` / `line_no` | a receipt line in `inbound-receiving` | Another bounded context's identity, carried on `ReceiptLineReceived` events; stored opaquely (ADR 0045). |
| `pack_levels.gtin`, `pack_levels.child_gtin` | a GTIN in `product-master` | Local projection of `ProductPackLevelDeclared`/`Retired`; another bounded context's identity, resolvable after retirement (ADR 0046). |

## Analytical database

```mermaid
erDiagram
    analytics_processed_events {
        TEXT event_id PK "CloudEvents id"
        TIMESTAMPTZ occurred_at "indexed desc, for freshness lag"
        TIMESTAMPTZ applied_at
    }
    analytics_consumed_events {
        TEXT event_id PK "consumer-level dedupe"
        TIMESTAMPTZ processed_at
    }
    flow_accuracy_rollup {
        TEXT sku PK "empty for bin-only events"
        TEXT bin_id PK "empty for SKU-only events"
        TIMESTAMPTZ hour_bucket PK "indexed"
        BIGINT received_quantity
        BIGINT stowed_count
        BIGINT picked_quantity
        BIGINT reservations_created
        BIGINT reservations_expired
        BIGINT reservations_revoked
        BIGINT cycle_counts_completed
        BIGINT discrepancies_detected
        BIGINT unlocated_count
    }
    schema_migrations {
        BIGINT version PK
        BOOLEAN dirty
    }
```

Source: `migrations/analytics/0001_report.up.sql`. This database is written
only by `cmd/inventory-projector` and read only by `cmd/inventory-reports`
(ADR 0011). It has no foreign keys at all: the rollup is keyed by SKU, bin
and hour, and the two event-id tables are idempotency ledgers.

## Table ≠ aggregate

| Table | Kind | Maps to |
| --- | --- | --- |
| `stock_units` | aggregate root | `stock.StockUnit` |
| `bins` | aggregate root | `location.Bin` |
| `reservations` | aggregate root | `reservation.Reservation` |
| `reservation_allocations` | part of an aggregate | `reservation.Allocation` values inside `Reservation` — saved and loaded with it by `postgres.ReservationRepo` |
| `product_classifications` | aggregate root | `product.ProductClassification` |
| `outbox_events` | infrastructure — transactional outbox | `postgres.OutboxPublisher` / `OutboxRelay` (ADR 0017); published rows pruned by the sweeper after `OUTBOX_RETENTION` (ADR 0026) |
| `idempotency_keys` | infrastructure — HTTP idempotency | `RequireIdempotencyKey` middleware (ADR 0018); pruned after `IDEMPOTENCY_KEY_TTL` |
| `processed_events` | infrastructure — inbound CloudEvents dedupe | `postgres.ProcessedEventRepo` (ADR 0034, ADR 0035); claim inserted in the same transaction as the effect; not swept |
| `order_pick_progress` | infrastructure — per-order completed-PICK counter, the ADR 0035 counting fallback | `postgres.OrderPickProgressRepo` (ADR 0035); written only for a PICK without `line_no` or one served by line-less reservations (ADR 0036), never on the per-line path; incremented in the same transaction as the `processed_events` claim; rows not updated for `ORDER_PICK_PROGRESS_RETENTION` (default 30 days) pruned by the sweeper (ADR 0026) |
| `schema_migrations` | infrastructure — golang-migrate | migration version per database |
| `pack_levels` | read model — product-master packaging projection | the PackagingView, fed by `ProductPackLevelDeclared`/`Retired` (ADR 0046); keyed by GTIN, retired levels stay resolvable |
| `containers` | aggregate root | `container.Container` — the LPN aggregate (ADR 0045); 1:1 with a SEALED_CASE `stock_units` row |
| `receipt_line_bookings` | infrastructure — booked-quantity record | what `ReceiptLineReceived` booked per (receipt, line); bounds container creation (ADR 0045 section 6) |
| `container_pending_links` | infrastructure — quarantine | sealed cartons not yet linkable to a Container; re-evaluated when the booking or pack level arrives; REJECTED rows kept for audit |
| `transfer_allocations` | infrastructure — idempotent transfer-decision ledger | one row per `transfer_line_id`; the unique constraint routes replays to the original outcome (transfer-safe allocation, Phase 2) |
| `transfer_receipts` | infrastructure — destination receipt ledger | one row per arrived transfer line; STAGED → STOWED; over/short variance explicit on the row (ADR 0031) |
| `inventory_exceptions` | infrastructure — quarantine | destination scans not matched to an ALLOCATED line; no stock row, no usable change, no availability event |
| `analytics_processed_events` | analytics projection | projection idempotency + freshness (`GET /reports/flow-accuracy/freshness`) |
| `analytics_consumed_events` | analytics projection | consumer-level dedupe (`report.ProcessedEvents`) |
| `flow_accuracy_rollup` | analytics projection | `report.Row`, the Inventory Flow & Accuracy report |

The OLTP `processed_events` table (migration 0033) is the inbox of the Kafka
consumers that must be exactly-once (`ApplyProductClassification`,
`ConfirmPicksForOrder`); the facility location cache, by contrast, is in memory
and rebuilt from offset 0 on every start, with idempotent upserts instead of an
id ledger.
