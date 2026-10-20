---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
description: Symptom, cause, check and fix for the failure modes visible in the inventory-storage code — startup, readiness, Kafka consumers, DLQs, the outbox, and the RFC 7807 problem types.
---

# Troubleshooting

Each row starts from what you observe. Problem types are the last path segment
of `https://errors.inventory-storage.warehouse-systems.dev/<slug>`
(`internal/adapters/inbound/http/errors.go`). Log messages are quoted exactly as
the code writes them. Variables are explained in
[Configuration](./configuration.md), procedures in [Runbook](./runbook.md).

## Startup and readiness

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| API pod restarts before ever answering `/healthz` | Something before the listener failed: migrations, DB ping, broker dial or the facility cache replay. Each dial is retried 5 times (1 s → 8 s) and then the process exits 1. | Pod logs: last `retrying` line names the `op` (`run migrations`, `ping database`, `dial facility-layout kafka topic`, `dial fulfillment kafka brokers`). | Fix connectivity / credentials for that dependency; the startup probe allows 60 s. |
| `facility location cache did not become ready within 1m0s` | `LOCATION_LOOKUP_MODE=kafka` and the replay of `warehouse.facility.events` did not reach the watermark in 60 s. | Broker reachable? Topic size? | Restore the broker, or temporarily set `LOCATION_LOOKUP_MODE=http` with `FACILITY_LAYOUT_BASE_URL` (the documented rollback; `permissive` no longer exists). |
| `facility location cache is ready but EMPTY; every location will report Known=false (fail-open) ...` | facility-layout has not published any slot yet. | Topic `warehouse.facility.events` has messages? | Not fatal. Stows are not zone-checked until facility-layout publishes. |
| The process exits at once with `DATABASE_URL is required`, `KAFKA_BROKERS is required`, `LOCATION_LOOKUP_MODE=... is not supported` or `<NAME>_CONSUMER_GROUP is required` | Postgres and Kafka are the only runtime ([ADR 0042](../adr/0042-postgres-and-kafka-are-the-only-runtime.md)): the variable named in the message is mandatory and has no default (no in-memory mode, no `localhost:9092`, no `permissive`). `loadConfig` reports the first missing one, before telemetry or any dial. | The pod's environment; for the chart, the rendered ConfigMap and Secret. | Set the variable ([Configuration](./configuration.md)). The chart refuses to render such a release (`helm template` names the value). |
| The process exits with `EVENT_PUBLISHER=... is not supported`, `TRANSFER_ALLOCATION_CONSUMER_MODE=... is not supported` or `TASK_COMPLETED_CONSUMER_MODE=... is not supported` | A legacy toggle was set to `log` / `off` / anything but `kafka`. They are kept only as guards. | The deployment's `extraEnv` and chart `config.*` values. | Unset it or set `kafka`. |
| The process exits non-zero after startup; the last ERROR is `<worker> stopped` (for example `task-completed confirm-pick consumer stopped`, `product-master classification consumer stopped`, `facility location cache consumer stopped`) or, in the projector, `analytics consumer stopped` | A consumer returned without a shutdown signal; `run` drains and returns its error so the orchestrator restarts the pod ([ADR 0042](../adr/0042-postgres-and-kafka-are-the-only-runtime.md)). | The ERROR line carries the cause. | Fix that cause (broker, database, credentials); the restart resumes from the committed offsets. |
| `/readyz` returns `503` with problem type `dependency-unavailable` | Postgres did not answer a ping, or the Kafka broker did not answer a metadata request (topics are never checked). The body names the dependency. The verdict is cached for 3 s. | Pod logs; reach the database and broker from the pod. | Restore the dependency. The chart's default readiness probe uses `/readyz`, so the pod is out of the Service meanwhile; `/healthz` stays `200`. |
| `/readyz` returns `503 {"status":"not_ready"}` | Graceful shutdown has started (SIGTERM). | Pod is `Terminating`? | Expected. If it lasts more than ~30 s the drain is stuck: look for `did not stop before the shutdown drain deadline` warnings. |
| Projector or reports exits immediately with `ANALYTICS_DATABASE_URL is required` | Analytics secret missing or key absent. | `analytics.database.existingSecret` / `projectorUrl`. | Provide `ANALYTICS_DATABASE_URL` (and `ANALYTICS_READER_DATABASE_URL` for reports). |
| Projector exits immediately with `KAFKA_BROKERS is required` | The projector has no `localhost:9092` default any more. | Env of the projector pod. | Set `kafka.brokers` (the chart then sets `KAFKA_BROKERS`). |
| `mcp` exits with `DATABASE_URL is required` | `cmd/mcp` needs the OLTP database (it has no in-memory store any more); it does not need `KAFKA_BROKERS`. | Env of the MCP pod. | Set `DATABASE_URL` (chart: `database.url` or `database.existingSecret`). |

### Migration failure

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Replicas crash-loop with `pq: unnamed prepared statement does not exist` or `pq: canceling statement due to statement timeout` during `run migrations` | Migrations went through PgBouncer transaction pooling, which breaks golang-migrate's `pg_advisory_lock` ([ADR 0023](../adr/0023-migrations-direct-postgres-connection.md)). | Is `MIGRATIONS_DATABASE_URL` set to a **direct** DSN in the secret? | Add the key; the chart mounts it with `optional: true`. |
| `Dirty database version N. Fix and force version.` | A previous migration failed half way. | `SELECT * FROM schema_migrations;` | Repair the schema by hand, then `migrate -path migrations -database <direct DSN> force <last good version>`. |
| `cmd/mcp` and `cmd/inventory` both run migrations | By design: both apply `migrations/` on start; golang-migrate makes the second run a no-op. | — | Nothing to fix. Point both at the same `MIGRATIONS_PATH`. |

## Kafka, outbox and DLQs

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| No events on `warehouse.inventory.events` at all | Events leave only through the outbox, so the relay is not delivering: broker unreachable (see `/readyz`), or no `cmd/inventory` process is running against this database. | `SELECT count(*) FROM outbox_events WHERE published_at IS NULL;` and the relay's WARN lines. | Restore the broker or run `cmd/inventory`. There is no `EVENT_PUBLISHER` switch to flip. |
| Events appear late or stop; `outbox relay: send failed, stopping this pass at the failed row` | The relay retries the oldest unpublished row and stops each pass at it, until the row reaches `OUTBOX_MAX_ATTEMPTS` (default 20; `0` = forever). | `SELECT id, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5;` | Fix the broker / topic problem in `last_error`; the relay resumes on the next pass. |
| ERROR `outbox relay: row reached the attempt cap and is excluded from future claims` | A row failed `OUTBOX_MAX_ATTEMPTS` sends ([ADR 0038](../adr/0038-outbox-relay-attempt-cap.md)); later rows, including same-key ones, now overtake it and consumers never got it. | `SELECT id, topic, event_type, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= 20;` | Fix the cause, then `UPDATE outbox_events SET attempts = 0 WHERE id = <id>;` ([Runbook](./runbook.md#an-outbox-row-hit-the-attempt-cap)). |
| MCP `revoke_reservation` never reaches Kafka | `cmd/mcp` only writes outbox rows; the relay runs in `cmd/inventory`. | Is a `cmd/inventory` pod (it runs the relay) running against the same DB? | Run one. |
| Consumer lag grows on `inventory-storage-transfer-allocation` or the product-master / inbound-receipt groups; logs repeat `... handling failed; retrying same message` | A transient (DB / transaction) error on one message: these consumers retry it in place without limit (200 ms → 5 s) and never dead-letter. | `attempt`, `partition`, `offset` in the WARN line; database health. | Fix the database problem; the same message is retried and the partition unblocks. |
| `warehouse.fulfillment.events.dlq` grows | TaskCompleted messages that were malformed (`task-completed message is malformed; dead-lettering`) or failed 5 times (`... dead-lettering after bounded retries`). | Read the DLQ with headers (`x-dlq-error`). | Fix the cause, then re-produce the original value onto `warehouse.fulfillment.events` ([Runbook](./runbook.md#inspect-and-re-drive-a-dlq)). |
| `warehouse.facility.events.dlq` grows | facility-layout facts the cache cannot parse. | `x-dlq-error` header. | Fix at the producer; restart the API to replay. |
| `skipping ... message that is not a valid CloudEvent` | A producer sent a non-CloudEvents (legacy flat) message, typically history replayed by a new group. Logged and committed past. | The message's headers / body. | None needed for history; a live producer must emit CloudEvents 1.0. |
| `warehouse.fulfillment.events.dlq` holds `RELOCATE` completions; `x-dlq-error` contains `malformed relocation task` | A RELOCATE `TaskCompleted` ([ADR 0039](../adr/0039-intra-site-relocation-stockrelocated-cloudevents.md)) that can never apply: a missing additive field (`sku`, `from_bin`, `to_bin`, `quantity` — expected until fulfillment-execution emits them), an unknown bin, a bin of another site, the same bin, too little **usable** stock at the source (reserved stock never moves) or a full destination. | The rest of `x-dlq-error`; `GET /bins/{binId}` for both bins; `get_bin_occupancy`. | Fix the cause and re-drive, or apply the move with `POST /relocations` ([Runbook](./runbook.md#inspect-and-re-drive-a-dlq)). |
| RELOCATE tasks complete but stock never moves, no DLQ entry | The message was a redelivery (`task-completed RELOCATE already applied; redelivery ignored`), or the consumer is lagging (the consumer always runs). | Startup line `task-completed confirm-pick consumer configured` with `task_types=PICK,RELOCATE,REPLENISH`; consumer-group lag on `TASK_COMPLETED_CONSUMER_GROUP`; `SELECT * FROM relocations WHERE source_ref = '<relocation_ref or request_ref>';` | A duplicate needs nothing; for lag, see the consumer rows above. |
| `warehouse.fulfillment.events.dlq` holds `REPLENISH` completions; `x-dlq-error` contains `malformed relocation task` | A REPLENISH `TaskCompleted` ([ADR 0041](../adr/0041-apply-replenish-task-as-replenishment-relocation.md)) that can never apply: the RELOCATE causes above, or a missing/blank `request_ref` (`request_ref is missing`; required, no task-id fallback). | The rest of `x-dlq-error`; `GET /bins/{binId}`; `get_bin_occupancy`. | Fix the producer or the bins and re-produce the original value ([Runbook](./runbook.md#inspect-and-re-drive-a-dlq)); `POST /relocations` cannot apply `REPLENISHMENT`. |
| Reservations stay `ACTIVE` after picks complete | `order_ref` is missing on the TaskCompleted payload (`task-completed ignored (not a PICK task or no order_ref)` at DEBUG), the fulfillment-execution task has not completed yet, or the consumer is lagging (it always runs). | Startup line `task-completed confirm-pick consumer configured`; consumer-group lag; DLQ `warehouse.fulfillment.events.dlq`. | Fix the producer payload; or confirm via `POST /reservations/{id}/confirm-pick`. |
| Analytics report is stale | Projector down or lagging on group `inventory-analytics`. | `GET /reports/flow-accuracy/freshness` → `lagSeconds`. | Restart / scale the projector (max 2). |

## REST problem types

| Status · type | Cause in code | Fix |
| --- | --- | --- |
| `400 idempotency-key-required` | `POST /stock/receive`, `POST /reservations`, `POST /transfers/{transferLineId}/receipt`, `.../stow` or `POST /relocations` without `Idempotency-Key`; enforced in every deployment because Postgres is the only runtime ([ADR 0018](../adr/0018-idempotency-key-middleware.md), [ADR 0042](../adr/0042-postgres-and-kafka-are-the-only-runtime.md)). | Send a unique `Idempotency-Key` per logical request. |
| `422 idempotency-key-reused` | The key was used before with a different body (SHA-256 of the body differs). | Use a new key for a different request. A retry with the same body gets the stored response replayed, including its status. |
| `500 internal-error` with detail `idempotency row committed with no recorded outcome — this should be impossible` | Stored row without a response. | Delete the `idempotency_keys` row for that key and retry. |
| `409 concurrent-modification` | Optimistic-concurrency `version` mismatch on `StockUnit`, `Bin` or `Reservation` ([ADR 0019](../adr/0019-optimistic-concurrency-version-column.md)). | Re-read and retry. |
| `409 insufficient-usable` | Requested quantity > usable (on-hand minus active reservations minus held / unlocated). | `GET /inventory/{sku}/usable`; revoke stale reservations. |
| `409 bin-full`, `409 capacity-below-occupancy`, `409 release-exceeds-occupancy` | Bin capacity rules. | `GET /bins/{binId}`; resize with `PUT /bins/{binId}`. |
| `404 bin-not-found` on stow | Bin was never registered. | `PUT /bins/{binId}` with a capacity first. |
| `409 reservation-already-resolved`, `409 reservation-expired`, `409 reservation-no-allocations` | Reservation lifecycle (expiry is lazy, resolved on read). | Create a new reservation. |
| `409 insufficient-reserved`, `409 unit-unlocated` | Pick exceeds the reserved quantity or the unit was flagged `Unlocated` by a cycle count. | Recount the bin. |
| `409 hazmat-zone-required`, `409 temperature-class-mismatch`, `409 hazmat-class-incompatible` | Stow placement rules from the local product classification and the bin's zone attributes. | Choose a compatible bin. |
| `409 location-classification-unavailable` | A hazmat or temperature-sensitive SKU was stowed while the location lookup returned an error (`http` mode, after its 3 attempts). Unclassified SKUs are never blocked. | facility-layout reachable? With the breaker **open** the lookup falls back to permissive and the stow is allowed (fail-open). |
| `409 bin-not-at-site`, `422 same-bin`, `422 invalid-relocation-reason`, `422 source-ref-too-long`, `404 relocation-not-found` | `POST /relocations` / `GET /relocations/{id}` rules ([ADR 0039](../adr/0039-intra-site-relocation-stockrelocated-cloudevents.md)): both bins must belong to the site (recorded custody, else the site segment of a seven-segment location code), differ, `reason` in `SLOTTING`/`CONSOLIDATION`/`MANUAL`, `sourceRef` ≤ 200 characters. `409 insufficient-usable` here means the source bin's **unreserved** quantity is too small. | Pick bins of one site; revoke the reservation holding the stock or move less. |
| `409 transfer-receipt-conflict`, `409 receipt-not-staged`, `409 bin-wrong-site`, `409 stow-quantity-mismatch` | Destination transfer custody rules ([ADR 0033](../adr/0033-destination-transfer-receipt-custody.md)). | Stage before stow; stow into bins of the destination site; quantities must sum to the received quantity. |
| `422 transfer-scan-quarantined` (body has an `exception` object) | The destination scan has no ALLOCATED ledger row for that line; an `inventory_exceptions` row was written instead of stock. | Check `transfer_allocations` for the line; the transfer-allocation consumer must be on at the origin. |
| `410 classification-moved` | `PUT /products/{sku}/classification` is retired: product-master owns classification ([ADR 0034](../adr/0034-product-master-owns-classification.md)). | Classify in product-master. |
| `404 product-classification-not-found` | The local copy has no row for the SKU. | Has product-master published `ProductClassified` for it, and is the `PRODUCT_MASTER_CONSUMER_GROUP` consumer keeping up? |
| `400 missing-demand-ref`, `400 invalid-line-no`, `400 counted-quantity-required`, `400 capacity-required`, `400 transfer-id-required`, `400 bins-required`, `400 malformed-request-body`, `400 empty-sku`, `400 empty-bin-id`, `400 empty-site-id`, `400 quantity-not-positive` | Request validation in the handlers. | Fix the request. |
| `422 capacity-out-of-range`, `422 negative-quantity`, `422 zero-quantity`, `422 invalid-bin-capacity` | Quantity / capacity value objects. | Fix the value. |
| `400 invalid-report-query`, `500 report-store-error` (reports binary) | Missing / non-RFC3339 `from`/`to`, `granularity` other than `hour`; analytical DB error. | Fix the query; check the projector's database. |

This service sends no `412`: there is no `If-Match` precondition on any route.

## MCP

| Symptom | Cause | Fix |
| --- | --- | --- |
| Tool calls fail with an unknown-session error after `initialize` | More than one `mcp` replica: sessions live in process memory and the Service has no affinity. | Keep `mcp.replicaCount: 1`. |
| `get_relocation` answers `relocation not found` | Unknown id. | Use the `relocation_id` of the `StockRelocated` event or the `id` `POST /relocations` returned; `cmd/mcp` and `cmd/inventory` must share the same `DATABASE_URL`. |
| `get_inventory_flow_accuracy_report` missing from `tools/list` | `REPORTS_BASE_URL` unset (the chart sets it only with `analytics.enabled`). | Set it. |
| Tool result `reservation not found` with `isError: true` | `revoke_reservation` on an unknown id, or `cmd/mcp` points at a different database than `cmd/inventory`. | Run both against the same `DATABASE_URL`. |
