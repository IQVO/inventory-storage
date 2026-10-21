---
id: runbook
title: Runbook
sidebar_label: Runbook
description: How inventory-storage is deployed, migrated and operated — Deployments, probes, Kafka topics and consumer groups, outbox relay, DLQs, housekeeping, scaling and routine procedures.
---

# Runbook

Everything here is derived from `charts/inventory-storage`, the four
composition roots under `cmd/` and the adapters they wire. Variable names are
explained in [Configuration](./configuration.md); failure modes in
[Troubleshooting](./troubleshooting.md).

## Deployment

The Helm chart is `charts/inventory-storage` (chart name `inventory-storage`).
The `release` CI job publishes it to `oci://ghcr.io/iqvo/inventory-storage` and
the image to `ghcr.io/iqvo/inventory-storage` on every push to `main`. One image
carries all four binaries; each Deployment picks its binary with `command:`.

| Deployment (component label) | Binary | Enabled by | Container port | Service | Probes |
| --- | --- | --- | --- | --- | --- |
| `<release>` (`api`) | `cmd/inventory` (image default) | always | `8080` (`http`) | `<release>`, ClusterIP `80 → http` | startup `GET /healthz` (2 s × 30), liveness `GET /healthz` (10 s), readiness `GET /readyz` (5 s) |
| `<release>-projector` (`analytics-projector`) | `/app/inventory-projector` | `analytics.enabled` | `8091` (`admin`) | none (it serves no traffic) | startup, liveness and readiness all `GET /healthz` on `admin` (the binary also serves `GET /readyz`, which the chart does not probe) |
| `<release>-reports` (`analytics-reports`) | `/app/inventory-reports` | `analytics.enabled` | `8092` (`http`) | `<release>-reports`, ClusterIP `80 → http` | all three `GET /healthz` |
| `<release>-mcp` (`mcp`) | `/app/mcp` | `mcp.enabled` | `8090` (`http`) | `<release>-mcp`, ClusterIP `8090 → 8090` | all three `GET /healthz` |
| `<release>-frontend` | nginx serving `web/` | `frontend.enabled` | `8080` | ClusterIP `80` | — (static assets; reached through warehouse-infra's Nginx gateway at `/mfes/inventory-storage/`, never through Kong) |

In the warehouse-infra kind cluster the REST API is reached through Kong at
`http://localhost:8000/api/inventory-storage/...` (prefix stripped), e.g.
`curl http://localhost:8000/api/inventory-storage/healthz`. REST and MCP are
unauthenticated.

`terminationGracePeriodSeconds` is `30` on the API Deployment: 10 s HTTP drain,
up to 10 s for the consumers and the outbox relay to stop, plus margin for the
readiness probe to observe the flip.

### What the probes mean

| Endpoint | Binary | Answers |
| --- | --- | --- |
| `GET /healthz` | all four | `200 {"status":"ok"}` as soon as the listener is up (liveness only). |
| `GET /readyz` | `cmd/inventory`, `cmd/inventory-projector` | `200 {"status":"ready"}` while the process is not draining **and** Postgres answers a ping **and** the Kafka broker answers a metadata request (topics are never checked; the verdict is cached for 3 s). `503 {"status":"not_ready"}` from the first step of graceful shutdown; `503` problem type `dependency-unavailable` (naming `postgres` and/or `kafka`) while a dependency is down (`internal/adapters/inbound/http/readiness.go`; the projector answers the same condition with a JSON body). |

The chart's default readiness probe for the API is `/readyz`, so a pod whose
Postgres or broker is unreachable leaves the Service while `/healthz` (liveness)
stays `200` and the pod is not restarted. `warehouse-infra`'s values may
override the probe; check them before relying on this. Everything the API
depends on is also verified *before* the listener opens, in this order
(`cmd/inventory/main.go`): configuration (`loadConfig` exits naming the first
missing mandatory variable), telemetry, migrations + pool ping (under boot
retry), the location lookup (with `LOCATION_LOOKUP_MODE=kafka` it blocks until
the facility cache has replayed `warehouse.facility.events`, up to 60 s), then
the four consumers and the outbox relay. A pod that cannot reach its database or
broker at boot therefore never answers `/healthz` and is restarted once the
startup probe's 60 s budget runs out. The startup probe exists for this fleet's
~10 s first-dial reset (Istio native sidecars), which `bootretry` absorbs. Once
running, a consumer that stops without a shutdown signal ends the
process non-zero (ADR 0042), so the orchestrator restarts it.

### Graceful shutdown (`cmd/inventory`)

On SIGTERM (`cmd/inventory/shutdown.go`, [ADR 0020](../adr/0020-resilience-circuit-breaker-retry-dlq-shutdown.md)):

1. `/readyz` flips to `503`.
2. The HTTP server stops accepting connections and drains for up to 10 s.
3. The shared consumer context is cancelled; the facility cache, transfer,
   product-master, TaskCompleted and inbound-receipt consumers get up to 10 s
   each to return, then their readers close.
4. Deferred: the outbox relay stops (10 s bound) and its writer closes, the
   Kafka writers close, the housekeeping sweeper stops (10 s bound), the pool
   closes, and telemetry flushes (5 s bound).

`cmd/mcp`, `cmd/inventory-reports` and `cmd/inventory-projector` stop their HTTP
server with a 10 s drain; the projector cancels its consumer first.

## Migrations

| Database | Files | Applied by | DSN used |
| --- | --- | --- | --- |
| OLTP | `migrations/*.sql` (golang-migrate, `0001`–`0008`, `0030`–`0037`) | `cmd/inventory` **and** `cmd/mcp`, at every start, before the pool opens | `MIGRATIONS_DATABASE_URL`, falling back to `DATABASE_URL` |
| Analytical | `migrations/analytics/*.sql` (`0001_report`) | `cmd/inventory-projector`, at every start | `ANALYTICS_DATABASE_URL` |

There is no separate migration Job. golang-migrate serializes concurrent runs
with a session-scoped `pg_advisory_lock`, which PgBouncer's transaction pooling
cannot honour, so in the cluster `MIGRATIONS_DATABASE_URL` must be a **direct**
connection ([ADR 0023](../adr/0023-migrations-direct-postgres-connection.md)).
The chart mounts it from the same secret with `optional: true`, so a secret
without that key still boots (and migrates through `DATABASE_URL`).

Tables created by the OLTP migrations: `bins`, `stock_units`, `reservations`,
`reservation_allocations`, `product_classifications`, `outbox_events`
(`0005` drops the original `events` table of `0001`), `idempotency_keys`,
`transfer_allocations`, `transfer_receipts`, `inventory_exceptions`,
`processed_events`, `order_pick_progress`, `relocations` (`0036`, one immutable row per applied `RelocateStock`) and `bin_sku_levels` (`0037`, the per-(bin, SKU) version counter behind `BinStockLevelChanged`, bumped inside the ledger transaction). Analytical tables:
`analytics_processed_events`, `analytics_consumed_events`,
`flow_accuracy_rollup`. Column-level detail is on
[Entity relationship](../ddd/entity-relationship.md).

A failed migration fails startup (after 5 boot-retry attempts) and the pod
crash-loops; see [Troubleshooting](./troubleshooting.md#migration-failure).

## Kafka

One broker serves the whole platform: the in-cluster Bitnami Kafka deployed by
warehouse-infra (`kafka:9092` inside the cluster, `localhost:9092` from the
host through its external listener). Every message produced or accepted is a
CloudEvents 1.0 **structured-mode** JSON event with the
`content-type: application/cloudevents+json; charset=UTF-8` header
(`internal/adapters/kafka/cloudevents/cloudevents.go`,
[ADR 0024](../adr/0024-cloudevents-mandatory-envelope.md)). Writers auto-create
topics and route by key hash ([ADR 0021](../adr/0021-kafka-producer-partition-key.md)).

### Produced

| Topic | Producer | Event `type`s (`com.warehouse.wms.inventory-storage.` …) | Key |
| --- | --- | --- | --- |
| `warehouse.inventory.events` | `cmd/inventory` (outbox relay), `cmd/mcp` (outbox rows, delivered by the `cmd/inventory` relay) | `reservation.StockReserved`, `reservation.ReservationRevoked`, `reservation.TransferStockAllocated`, `reservation.TransferStockAllocationRejected`, `stock.TransferReceiptStaged`, `stock.TransferStockStowed`, `stock.StockRelocated` (ADR 0039), `stock.BinStockLevelChanged` (ADR 0040), `bin.DiscrepancyDetected` / `stock.ItemUnlocated` / `stock.ItemStowed` (promoted byte-identical copies of the analytics feed, ADR 0043) | reservation id, transfer line id, relocation id, `<bin>:<sku>`, bin id, SKU |
| `warehouse.inventory.analytics` | same | `stock.StockReceived`, `stock.ItemStowed`, `stock.ItemUnlocated`, `reservation.StockReserved`, `reservation.StockPicked`, `reservation.ReservationExpired`, `reservation.ReservationRevoked`, `bin.CycleCountCompleted`, `bin.DiscrepancyDetected` | reservation id, SKU or bin id |
| `warehouse.fulfillment.events.dlq` | `cmd/inventory` TaskCompleted consumer | the original message, raw, plus `x-dlq-source-topic`, `x-dlq-error`, `x-dlq-failed-at` headers | original key |
| `warehouse.facility.events.dlq` | `cmd/inventory` facility cache consumer | the original message, raw, plus `x-dlq-*` headers | original key |

Payloads are on [Integration events](../api-reference/events.md).

### Consumed

| Topic | Event `type` handled | Binary / switch | Consumer group | Retry and poison handling |
| --- | --- | --- | --- | --- |
| `warehouse.facility.events` | `com.warehouse.wms.facility-layout.zone.ZoneRegistered`, `...locationslot.LocationSlotRegistered`, `...locationslot.LocationSlotDecommissioned` | `cmd/inventory`, `LOCATION_LOOKUP_MODE=kafka` | **unique per process**: `inventory-storage-facility-location-cache-<host>-<pid>-<nanos>`, full replay on every start | malformed messages go to `warehouse.facility.events.dlq` |
| `warehouse.network-inventory-planning.events` | `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested` | `cmd/inventory`, `TRANSFER_ALLOCATION_CONSUMER_GROUP` (mandatory, always on) | the configured value (chart default `inventory-storage-transfer-allocation`) | transient errors retry the same message forever (200 ms → 5 s); poison is logged and committed past; no DLQ |
| `warehouse.product-master.events` | `com.warehouse.wms.product-master.product.ProductClassified` | `cmd/inventory`, `PRODUCT_MASTER_CONSUMER_GROUP` (mandatory, always on) | the configured value (chart default `inventory-storage-product-master`) | same as transfer; dedupe through `processed_events` |
| `warehouse.inbound-receiving.events` | `com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived` | `cmd/inventory`, `INBOUND_RECEIPT_CONSUMER_GROUP` (mandatory, always on) | the configured value (chart default `inventory-storage-inbound-receipts`) | same as transfer; dedupe through `processed_events` |
| `warehouse.fulfillment.events` | `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` (`task_type` `PICK` → confirm-pick, `RELOCATE` → relocation, ADR 0039; `REPLENISH` → `REPLENISHMENT` relocation, ADR 0041) | `cmd/inventory`, `TASK_COMPLETED_CONSUMER_GROUP` (mandatory, always on) | the configured value (chart default `inventory-storage-confirm-pick`) | 5 attempts then DLQ; poison (incl. a RELOCATE or REPLENISH the ledger can never apply, or a REPLENISH without `request_ref`) straight to `warehouse.fulfillment.events.dlq`; offset committed only after the DLQ write; dedupe through `processed_events` (`task-completed-confirm-pick` / `task-completed-relocate` / `task-completed-replenish`) |
| `warehouse.inventory.analytics` | the analytics types above | `cmd/inventory-projector` | `inventory-analytics` | invalid CloudEvents are logged at WARN and skipped (no DLQ); dedupe through `analytics_consumed_events` |

Every group-based consumer starts a **new** group at the earliest offset
(`StartOffset: FirstOffset`) and commits only after the handler settles.
Messages whose `type` the consumer does not handle are committed past.

## Outbox relay

The outbox is the only publishing path: use cases write one
`outbox_events` row per (event, topic) inside their own transaction, and a
single relay goroutine in `cmd/inventory` (always on) drains them
(`internal/adapters/outbound/postgres/outbox_relay.go`):

- every `OUTBOX_RELAY_INTERVAL` (default 1 s) it claims up to 100 unpublished
  rows in id order with `FOR UPDATE SKIP LOCKED`, so several replicas can relay
  concurrently without double-sending a row;
- a full batch is followed immediately by the next pass;
- a send failure increments `attempts`, stores `last_error`, logs a WARN and
  **stops the pass at that row**: the next pass retries it, so a broken broker
  holds back everything behind it rather than reordering;
- **attempt cap** ([ADR 0038](../adr/0038-outbox-relay-attempt-cap.md)): once a
  row has failed `OUTBOX_MAX_ATTEMPTS` sends (default 20, `0` = unlimited) the
  relay logs it **at ERROR** and excludes it from future claims, so one poison
  row cannot stop the relay forever. Later rows, **including rows with the
  same key**, then overtake it. The row is not deleted or moved (there is no
  DLQ topic): it stays in `outbox_events` with `published_at IS NULL`,
  `attempts` and `last_error`, and the sweeper never removes it.

### An outbox row hit the attempt cap

Symptom: an ERROR log
`outbox relay: row reached the attempt cap and is excluded from future claims; ...`
carrying `outbox_id`, `topic`, `event_type`, `key`, `attempts`,
`max_attempts` and `error`. Consumers never received that event; later events
for the same key may already have been delivered.

1. List the excluded rows (use the configured cap; 20 by default):

   ```sql
   SELECT id, topic, event_type, convert_from(key, 'UTF8') AS key, attempts, last_error, created_at
   FROM outbox_events
   WHERE published_at IS NULL AND attempts >= 20
   ORDER BY id;
   ```

2. Fix the cause named in `last_error` (topic missing, message too large,
   broker ACL, ...).
3. Re-arm the row so the next relay pass claims it again:

   ```sql
   UPDATE outbox_events SET attempts = 0 WHERE id = <outbox_id>;
   ```

   It is delivered late, after later rows with the same key; consumers dedupe
   on the CloudEvents `id`, so a re-armed row is never applied twice. If the
   event must not be delivered at all any more, leave it excluded and record
   why: deleting an unpublished outbox row is a manual data change and is not
   part of this procedure.

`cmd/mcp` writes outbox rows but runs no relay: MCP-initiated revocations reach
Kafka only while a `cmd/inventory` pod (which runs the relay) is running
against the same database.

## Housekeeping

`cmd/inventory` runs one sweeper goroutine
(`internal/adapters/outbound/postgres/sweeper.go`,
[ADR 0026](../adr/0026-housekeeping-sweeper-idempotency-keys-and-outbox.md))
every `HOUSEKEEPING_INTERVAL` (1 h). It deletes `idempotency_keys` older than
`IDEMPOTENCY_KEY_TTL` (24 h), **published** `outbox_events` older than
`OUTBOX_RETENTION` (7 d) and `order_pick_progress` rows idle for
`ORDER_PICK_PROGRESS_RETENTION` (30 d). Unpublished outbox rows are never swept.

Reservation expiry has **no** sweeper: an expired reservation is resolved lazily
the next time it is read, which raises `ReservationExpired`
([Domain events](../ddd/domain-events.md)).

## Scaling

- HPAs exist for `api` (1–4), `projector` (1–2), `reports` (1–3) and `frontend`
  (1–3), all at 70 % CPU and all `enabled: false` by default. When an HPA is on,
  the Deployment omits `replicas:` so the HPA owns it
  ([ADR 0022](../adr/0022-horizontal-autoscaling-and-pgxpool-tuning.md)).
- `api` scales freely: its only in-process cache is the facility cache, built by
  a per-process consumer group; the stable-group consumers share partitions via
  normal rebalancing; the relay uses `SKIP LOCKED`.
- `projector` is capped at 2 because the analytics topic is keyed per SKU and
  the workload is light idempotent upserts.
- `mcp` has **no** HPA: the MCP SDK keeps sessions in process memory and the
  Service has no session affinity, so keep `mcp.replicaCount` at 1.
- Each `api` pod holds up to 10 Postgres connections, each `mcp` pod up to 10,
  the projector and reports 5 each: size PgBouncer / `max_connections` for
  `replicas × MaxConns`.

## Routine procedures

### Re-publish product classifications

Removed. The `republish-product-classifications` subcommand and the legacy
`ProductClassified` it emitted were retired at stage E (2026-10-17,
[ADR 0034](../adr/0034-product-master-owns-classification.md)):
`cmd/inventory` takes no arguments. To reload the local classification copy,
reset the offsets of the `PRODUCT_MASTER_CONSUMER_GROUP` group (see "Replay a
consumer" below).

### Replay a consumer

All group consumers commit offsets per message, start new groups at the
earliest offset and dedupe where re-delivery matters (`processed_events` for
product-master, inbound receipts and TaskCompleted; `analytics_consumed_events`
for the projector; the transfer ledger for allocation commands). To replay:

- **Facility cache** — restart the `api` pods; each start replays the topic
  from the beginning under a new group.
- **Group consumers** — stop the consumer (scale `api` to 0; the consumers
  cannot be switched off any more), reset the group with the Kafka CLI
  (`kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group> --topic <topic> --reset-offsets --to-earliest --execute`),
  then restart. Alternatively point the `*_CONSUMER_GROUP` variable at a fresh
  group id.
- **Analytics read model** — reset group `inventory-analytics`. Event ids
  already in `analytics_consumed_events` (consumer gate) and
  `analytics_processed_events` (projection claim) are skipped, so a full
  rebuild also needs those two tables and `flow_accuracy_rollup` truncated on
  the analytical database first.

### Inspect and re-drive a DLQ

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic warehouse.fulfillment.events.dlq --from-beginning \
  --property print.headers=true
```

`x-dlq-error` says why the message failed. Nothing consumes the DLQ topics;
after fixing the cause, re-produce the original value (unchanged, it is still a
valid CloudEvent) onto the source topic. TaskCompleted handling is idempotent
per event id through `processed_events`.

**A dead-lettered RELOCATE completion** (ADR 0039) was rejected by the ledger
or carried a bad field; `x-dlq-error` contains `malformed relocation task`
followed by the rule (for example `from_bin: ...`, `bin not found`,
`reserve quantity exceeds usable quantity`, `bin is full: capacity exceeded`). The floor already moved the
stock, so the ledger is now out of step with the bins:

1. Read the value's `data` (`sku`, `from_bin`, `to_bin`, `quantity`,
   `relocation_ref`) and check the bins with `GET /bins/{binId}` and
   `get_bin_occupancy`.
2. If the move is still valid after a fix (bin registered, reservation
   revoked, capacity raised), re-produce the original value: it was never
   claimed, so it applies once. Or apply it by hand with `POST /relocations`
   (`reason` `SLOTTING`, `sourceRef` = the `relocation_ref`) under a fresh
   `Idempotency-Key`; do not do both.
3. If the stock really is somewhere else, cycle-count both bins
   (`POST /bins/{binId}/cycle-count`) instead.

Until fulfillment-execution sends the additive fields, every RELOCATE
completion lands here with `malformed relocation task: sku: ...`; that is
expected and needs no action.

**A dead-lettered REPLENISH completion** (ADR 0041) is handled like the RELOCATE
one above (same `malformed relocation task` prefix in `x-dlq-error`, same checks
of the bins and usable stock), with two differences: a message
`request_ref is missing` means the producer did not send the replenishment
request id (there is no task-id fallback, because pick-face-replenishment
closes a request by `source_ref == request id`), so fix the producer and
re-produce the original value; and a manual `POST /relocations` is **not**
possible (`reason` `REPLENISHMENT` is rejected there), so re-produce the value.
Dedupe consumer name:
`task-completed-replenish`. No new environment variable.

### Inspect stuck outbox rows

```sql
SELECT id, topic, event_type, attempts, last_error, created_at
FROM outbox_events
WHERE published_at IS NULL
ORDER BY id
LIMIT 20;
```

The lowest id with `attempts > 0` is the row the relay keeps failing on.

### Rotate database credentials

DSNs are read once at startup. Update the secret named by
`database.existingSecret` (keys `DATABASE_URL` and `MIGRATIONS_DATABASE_URL`)
or the analytics secret, then `kubectl rollout restart` the affected
Deployments (`api` and `mcp` for OLTP, `projector` and `reports` for analytics).
