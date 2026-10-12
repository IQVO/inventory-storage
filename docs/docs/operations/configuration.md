---
id: configuration
title: Configuration
sidebar_label: Configuration
description: Every environment variable each of the four binaries reads, with its default, whether it is required, and the file that reads it.
---

# Configuration

The repository builds **four binaries**, and each one reads its own environment.
There are no configuration files and no flags (the one exception is the
`republish-product-classifications` subcommand of `cmd/inventory`, see
[Runbook](./runbook.md#re-publish-product-classifications)). An unset or empty
variable takes the default shown below: every reader uses the same
`getenv(key, fallback)` helper, which treats an empty string as unset.

Test-only variables (`*_TEST_*`) are not listed.

## `cmd/inventory` — the OLTP REST API

Read in `cmd/inventory/config.go` (`loadConfig`) unless the source column says
otherwise.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API (`/healthz`, `/readyz` and every route in `apis/openapi.yaml` except `/reports/*`). | `config.go` |
| `DATABASE_URL` | *(unset)* | no | Postgres DSN for the runtime `pgxpool`. Unset = in-memory repositories, no migrations, no idempotency middleware, no housekeeping sweeper, no outbox. Every Kafka consumer below refuses to start without it. | `config.go` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct (non-PgBouncer) DSN used **only** by the golang-migrate startup step; the pool always uses `DATABASE_URL` ([ADR 0023](../adr/0023-migrations-direct-postgres-connection.md)). | `config.go`, `postgres.go` |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP `*.up.sql` files, relative to the working directory. | `config.go` |
| `EVENT_PUBLISHER` | `log` | no | `kafka` (case-insensitive) switches the publisher to the integration + analytics Kafka fan-out: through the transactional outbox and relay when `DATABASE_URL` is set ([ADR 0017](../adr/0017-transactional-outbox.md)), directly to Kafka otherwise. Any other value logs events to stdout. | `config.go`, `adapters.go` |
| `KAFKA_BROKERS` | publishers: `localhost:9092`; consumers: *(none)* | for any consumer | Comma-separated broker list. Read twice: `adapters.go` falls back to `localhost:9092` for the publishers and the outbox relay, while `config.go` leaves the consumer broker list empty when unset, so `LOCATION_LOOKUP_MODE=kafka` and every consumer switch below fail startup with "... requires KAFKA_BROKERS to be set". | `config.go`, `adapters.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Poll interval of the outbox relay (only runs with `EVENT_PUBLISHER=kafka` and `DATABASE_URL`). An unparsable value logs a warning and uses `1s`. | `config.go` |
| `OUTBOX_MAX_ATTEMPTS` | `20` | no | Outbox relay attempt cap ([ADR 0038](../adr/0038-outbox-relay-attempt-cap.md)): a row that has failed this many sends is excluded from the relay's claims and logged at ERROR (`outbox_id`, `topic`, `event_type`, `key`). It stays in `outbox_events` until re-armed (see [Runbook](./runbook.md#outbox-relay)); later rows, including rows with the same key, overtake it. `0` = unlimited (one poison row blocks the relay forever). An invalid or negative value logs a warning and uses `20`. | `config.go`, `adapters.go` |
| `HOUSEKEEPING_INTERVAL` | `1h` | no | How often the housekeeping sweeper runs ([ADR 0026](../adr/0026-housekeeping-sweeper-idempotency-keys-and-outbox.md)). `0` disables the sweeper. Postgres mode only. | `housekeeping.go` |
| `IDEMPOTENCY_KEY_TTL` | `24h` | no | Age after which an `idempotency_keys` row is deleted. `0` keeps rows forever. | `housekeeping.go` |
| `OUTBOX_RETENTION` | `168h` | no | Age after which a **published** `outbox_events` row is deleted. `0` keeps rows forever. | `housekeeping.go` |
| `ORDER_PICK_PROGRESS_RETENTION` | `720h` | no | Age (since the last pick) after which an `order_pick_progress` counter row is deleted ([ADR 0035](../adr/0035-confirm-pick-from-task-completed.md)). `0` keeps rows forever. | `housekeeping.go` |
| `LOCATION_LOOKUP_MODE` | `permissive` | no | Where `StowStock` gets zone attributes for a bin: `kafka` (local cache fed by `warehouse.facility.events`; startup blocks until the replay finishes), `http` (synchronous facility-layout call behind a circuit breaker), anything else = `permissive` (every bin reports `Known=false`). | `config.go`, `lookup.go` |
| `FACILITY_LAYOUT_BASE_URL` | *(unset)* | when `LOCATION_LOOKUP_MODE=http` | Base URL of facility-layout; the client calls `GET <base>/locations/<binId>/classification`. | `config.go`, `internal/adapters/outbound/facilitylayout/client.go` |
| `TRANSFER_ALLOCATION_CONSUMER_MODE` | `off` | no | `kafka` starts the network-inventory-planning `TransferAllocationRequested` command consumer ([ADR 0030](../adr/0030-site-scoped-transfer-allocation.md)). Requires `DATABASE_URL` and `KAFKA_BROKERS`. | `config.go`, `transfer.go` |
| `TRANSFER_ALLOCATION_CONSUMER_GROUP` | `inventory-storage-transfer-allocation` | no | Consumer group of that consumer (the default is applied in `internal/adapters/inbound/kafka/transfer_consumer.go`). Override it when running a second process locally so it never steals the deployed group's partitions. | `config.go` |
| `PRODUCT_MASTER_CONSUMER_GROUP` | *(unset — consumer off)* | no | Setting it starts the product-master `ProductClassified` consumer under that group ([ADR 0034](../adr/0034-product-master-owns-classification.md)). Requires `DATABASE_URL` and `KAFKA_BROKERS`. There is no default group. | `config.go`, `productmaster.go` |
| `INBOUND_RECEIPT_CONSUMER_GROUP` | *(unset — consumer off)* | no | Setting it starts the inbound-receiving `ReceiptLineReceived` consumer under that group ([ADR 0037](../adr/0037-stock-receipts-from-inbound-receiving.md)). Requires `DATABASE_URL` and `KAFKA_BROKERS`; logs a warning when `EVENT_PUBLISHER` is not `kafka`, because the booked `StockReceived` then never leaves the process. | `config.go`, `inboundreceipt.go` |
| `TASK_COMPLETED_CONSUMER_MODE` | `off` | no | `kafka` starts the fulfillment-execution `TaskCompleted` confirm-pick consumer ([ADR 0035](../adr/0035-confirm-pick-from-task-completed.md), [ADR 0036](../adr/0036-per-line-confirm-pick.md)). Requires `DATABASE_URL` and `KAFKA_BROKERS`; dials the first broker under boot retry before starting. | `config.go`, `confirmpick.go` |
| `TASK_COMPLETED_CONSUMER_GROUP` | `inventory-storage-confirm-pick` | no | Consumer group of that consumer. | `config.go`, `confirmpick.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5182` | no | Comma-separated origins allowed by the CORS middleware (methods GET/POST/PUT/DELETE; headers `Accept`, `Content-Type`, `Authorization`, `Idempotency-Key`). The defaults are the console shell's and this context's micro-frontend dev servers. | `internal/adapters/inbound/http/server.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `warn` or `error` (case-insensitive); anything else is `info`. Logs are JSON on stdout. | `config.go` |
| `OTEL_SERVICE_NAME` | `inventory-storage` | no | `service.name` on every span, metric and log. | `config.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC collector for traces and metrics. A value without a scheme gets `http://` prepended. Export is non-blocking: an unreachable collector costs telemetry, never availability. | `config.go`, `internal/adapters/outbound/telemetry/telemetry.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. | `config.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` |

The `republish-product-classifications` subcommand
(`go run ./cmd/inventory republish-product-classifications`) reads only
`DATABASE_URL`, which is **required** there (`cmd/inventory/republish.go`). It
writes `outbox_events` rows; the relay of a running `cmd/inventory` with
`EVENT_PUBLISHER=kafka` delivers them.

## `cmd/inventory-projector` — the analytics writer

Read in `cmd/inventory-projector/main.go`.

| Variable | Default | Required? | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | *(none)* | **yes** | Read-write DSN of the analytical database. The process exits with `ANALYTICS_DATABASE_URL is required` when it is empty. Migrations and the pool both use it. |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical `*.up.sql` files, applied (under boot retry) before the pool opens. |
| `KAFKA_BROKERS` | `localhost:9092` | no | Brokers for the `warehouse.inventory.analytics` consumer (group `inventory-analytics`). |
| `ADMIN_ADDR` | `:8091` | no | Listen address of the admin server, which serves only `GET /healthz` (used by all three probes). |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`/`warning`, `error`; anything else is `info`. |
| `OTEL_SERVICE_NAME` | `inventory-projector` | no | `service.name`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC collector. |
| `SERVICE_VERSION` | `dev` | no | `service.version`. |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` (read by the shared telemetry adapter). |

## `cmd/inventory-reports` — the analytics reader

Read in `cmd/inventory-reports/main.go`.

| Variable | Default | Required? | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | *(none)* | **yes** | DSN of the analytical database. The pool forces `default_transaction_read_only=on`, `MaxConns=5` and `statement_timeout=15s`; the Helm chart feeds it the **reader** DSN (`ANALYTICS_READER_DATABASE_URL` key of the analytics secret). Exits with `ANALYTICS_DATABASE_URL is required` when empty. |
| `HTTP_ADDR` | `:8092` | no | Listen address for `GET /healthz`, `GET /reports/flow-accuracy` and `GET /reports/flow-accuracy/freshness`. |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`/`warning`, `error`; anything else is `info`. |
| `OTEL_SERVICE_NAME` | `inventory-reports` | no | `service.name`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC collector. |
| `SERVICE_VERSION` | `dev` | no | `service.version`. |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name`. |

The reports binary runs no migrations: the projector owns the analytical schema.

## `cmd/mcp` — the MCP server

Read in `cmd/mcp/main.go`. See [MCP tools](../mcp/tools.md) for what it serves.

| Variable | Default | Required? | Meaning |
| --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address of the Streamable HTTP server (`/mcp`, `/mcp/*`, `/*` and `GET /healthz`). |
| `DATABASE_URL` | *(unset)* | no | Same OLTP database as `cmd/inventory`. Unset = the binary's **own** in-memory repositories (not shared with a separately running `cmd/inventory`). |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step only (ADR 0023). `cmd/mcp` applies the same OLTP migrations on start; golang-migrate makes that idempotent. |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP migrations. |
| `EVENT_PUBLISHER` | `log` | no | `kafka`: with `DATABASE_URL`, `revoke_reservation` writes `outbox_events` rows that the **`cmd/inventory` relay** drains (this binary runs no relay); without `DATABASE_URL` it publishes directly to Kafka. |
| `KAFKA_BROKERS` | *(none)* | when `EVENT_PUBLISHER=kafka` and no `DATABASE_URL` | Comma-separated brokers for the direct publisher. Startup fails with `EVENT_PUBLISHER=kafka without DATABASE_URL requires KAFKA_BROKERS to be set`. |
| `REPORTS_BASE_URL` | *(unset)* | no | Base URL of `cmd/inventory-reports`. Setting it registers the read-only `get_inventory_flow_accuracy_report` tool (HTTP client timeout 10 s). |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`, `error`; anything else is `info`. |
| `OTEL_SERVICE_NAME` | `inventory-storage-mcp` | no | `service.name`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC collector. |
| `SERVICE_VERSION` | `dev` | no | `service.version`. |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name`. |

## Compiled-in limits (not configurable)

These are constants in code; changing them is a code change.

| Limit | Value | Source |
| --- | --- | --- |
| OLTP `pgxpool` `MaxConns` / `statement_timeout` | `10` / `5s` | `internal/adapters/outbound/postgres/pool.go` ([ADR 0022](../adr/0022-horizontal-autoscaling-and-pgxpool-tuning.md)) |
| Projector pool `MaxConns` / `statement_timeout` | `5` / `10s` | `internal/adapters/outbound/analyticsstore/pool.go` |
| Reports pool `MaxConns` / `statement_timeout` | `5` / `15s`, read-only | `internal/adapters/outbound/analyticsstore/pool.go` |
| Boot retry (migrations, DB ping, broker dials) | 5 attempts, waits 1 s, 2 s, 4 s, 8 s | `internal/adapters/outbound/bootretry/retry.go` ([ADR 0028](../adr/0028-bootretry-boot-time-dial-retry.md)) |
| Outbox relay batch size | 100 rows per pass; attempt cap `OUTBOX_MAX_ATTEMPTS` (default 20, ADR 0038) | `internal/adapters/outbound/postgres/outbox_relay.go`, `cmd/inventory/config.go` |
| Facility cache replay wait | 60 s (`WaitReadyTimeout`) | `internal/adapters/outbound/facilitycache/consumer.go` |
| facility-layout circuit breaker | trips at 5 consecutive failures or > 50 % failures over ≥ 10 requests; 30 s open; 1 half-open probe; 3 attempts per call (50 ms → 500 ms back-off) | `internal/resilience/breaker.go`, `internal/adapters/outbound/facilitylayout/breaker.go` |
| TaskCompleted handling attempts before DLQ | 5 | `internal/adapters/inbound/kafka/task_completed_consumer.go` |
| Consumer retry back-off (same message) | 200 ms doubling to a 5 s cap | `internal/adapters/inbound/kafka/transfer_consumer.go` (`retryBackoff`) |
| HTTP drain / consumer and relay drain on shutdown | 10 s / 10 s | `cmd/inventory/shutdown.go`, `cmd/inventory/adapters.go` |
| Telemetry flush on shutdown | 5 s | `cmd/*/main.go`, `cmd/inventory/telemetry.go` |
| OTel metric export interval | 15 s | `internal/adapters/outbound/telemetry/telemetry.go` |

## Helm values to environment

`charts/inventory-storage` renders these variables; anything else goes through
`extraEnv` (API) or `mcp.extraEnv` (MCP).

| Values key | Variable (Deployment) |
| --- | --- |
| `config.httpAddr`, `config.eventPublisher`, `config.migrationsPath`, `config.logLevel` | `HTTP_ADDR`, `EVENT_PUBLISHER`, `MIGRATIONS_PATH`, `LOG_LEVEL` (API; MCP reuses the last three) |
| `config.locationLookupMode`, `config.facilityLayoutBaseUrl` | `LOCATION_LOOKUP_MODE`, `FACILITY_LAYOUT_BASE_URL` (API) |
| `config.outboxRelayInterval`, `config.outboxMaxAttempts`, `config.housekeepingInterval`, `config.idempotencyKeyTtl`, `config.outboxRetention`, `config.orderPickProgressRetention` | `OUTBOX_RELAY_INTERVAL`, `OUTBOX_MAX_ATTEMPTS`, `HOUSEKEEPING_INTERVAL`, `IDEMPOTENCY_KEY_TTL`, `OUTBOX_RETENTION`, `ORDER_PICK_PROGRESS_RETENTION` (API) |
| `config.transferAllocationConsumerMode` / `...Group`, `config.productMasterConsumerGroup`, `config.inboundReceiptConsumerGroup`, `config.taskCompletedConsumerMode` / `...Group` | the matching consumer variables (API) |
| `database.existingSecret` + `existingSecretKey` / `migrationsExistingSecretKey` | `DATABASE_URL` / `MIGRATIONS_DATABASE_URL` (`optional: true`) (API and MCP) |
| `kafka.enabled`, `kafka.brokers` | `KAFKA_BROKERS` (API and MCP only when enabled; projector always) |
| `otel.enabled`, `otel.endpoint`, `otel.serviceName` | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` |
| `environment`, `image.tag` | `ENVIRONMENT`, `SERVICE_VERSION` |
| `analytics.projector.adminAddr`, `analytics.migrationsPath` | `ADMIN_ADDR`, `ANALYTICS_MIGRATIONS_PATH` (projector) |
| `analytics.reports.httpAddr` | `HTTP_ADDR` (reports) |
| `analytics.database.*` | `ANALYTICS_DATABASE_URL` (projector: read-write key; reports: reader key) |
| `mcp.httpAddr` | `MCP_ADDR`; with `analytics.enabled` the chart also sets `REPORTS_BASE_URL` to the reports Service |

`CORS_ALLOWED_ORIGINS` has no values key: set it through `extraEnv` when the
console is served from another origin.
