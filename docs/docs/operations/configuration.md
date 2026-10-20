---
id: configuration
title: Configuration
sidebar_label: Configuration
description: Every environment variable each of the four binaries reads, with its default, whether it is required, and the file that reads it. Postgres and Kafka are the only runtime.
---

# Configuration

The repository builds **four binaries**, and each one reads its own environment.
There are no configuration files and no flags (the one exception is the
`republish-product-classifications` subcommand of `cmd/inventory`, see
[Runbook](./runbook.md#re-publish-product-classifications)).

**Postgres and Kafka are the only runtime**
([ADR 0042](../adr/0042-postgres-and-kafka-are-the-only-runtime.md)). There is
no in-memory mode, no log-only publisher, no permissive location lookup and no
opt-in consumer. A variable marked **yes** in the *Required?* column has no
default: when it is missing the process exits before telemetry, the listener or
any dial, with an error that names the variable. The remaining variables take
the default shown below: every reader uses the same `getenv(key, fallback)`
helper, which treats an empty string as unset.

Test-only variables (`*_TEST_*`) are not listed.

## `cmd/inventory` — the OLTP REST API

Read in `cmd/inventory/config.go` (`loadConfig`) unless the source column says
otherwise. `loadConfig` validates everything first and reports the first
problem.

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API (`/healthz`, `/readyz` and every route in `apis/openapi.yaml` except `/reports/*`). | `config.go` |
| `DATABASE_URL` | *(none)* | **yes** | Postgres DSN for the runtime `pgxpool`. Migrations, the idempotency middleware, the transactional outbox and the housekeeping sweeper all run on it. | `config.go` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct (non-PgBouncer) DSN used **only** by the golang-migrate startup step; the pool always uses `DATABASE_URL` ([ADR 0023](../adr/0023-migrations-direct-postgres-connection.md)). | `config.go`, `postgres.go` |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP `*.up.sql` files, relative to the working directory. | `config.go` |
| `KAFKA_BROKERS` | *(none)* | **yes** | Comma-separated `host:port` broker list. There is **no** `localhost:9092` default. The outbox relay, the four consumers, the facility cache (`LOCATION_LOOKUP_MODE=kafka`) and the `/readyz` broker check all use it. | `config.go` |
| `LOCATION_LOOKUP_MODE` | *(none)* | **yes** | Where `StowStock` gets zone attributes for a bin: `kafka` (local cache fed by `warehouse.facility.events`; startup blocks until the replay finishes) or `http` (synchronous facility-layout call behind a circuit breaker, the documented rollback for `kafka`). Unset, `permissive` and anything else are a boot error. | `config.go`, `lookup.go` |
| `FACILITY_LAYOUT_BASE_URL` | *(none)* | when `LOCATION_LOOKUP_MODE=http` | Base URL of facility-layout; the client calls `GET <base>/locations/<binId>/classification`. | `config.go`, `internal/adapters/outbound/facilitylayout/client.go` |
| `TRANSFER_ALLOCATION_CONSUMER_GROUP` | *(none)* | **yes** | Stable consumer group of the network-inventory-planning `TransferAllocationRequested` command consumer ([ADR 0030](../adr/0030-site-scoped-transfer-allocation.md)). The consumer always runs. The id must be stable so a restarted pod resumes from its committed offsets; use a different id when running a second process locally so it never steals the deployed group's partitions. | `config.go`, `transfer.go` |
| `PRODUCT_MASTER_CONSUMER_GROUP` | *(none)* | **yes** | Stable consumer group of the product-master `ProductClassified` consumer ([ADR 0034](../adr/0034-product-master-owns-classification.md)). Always runs. | `config.go`, `productmaster.go` |
| `INBOUND_RECEIPT_CONSUMER_GROUP` | *(none)* | **yes** | Stable consumer group of the inbound-receiving `ReceiptLineReceived` consumer ([ADR 0037](../adr/0037-stock-receipts-from-inbound-receiving.md)). Always runs; the booked `StockReceived` leaves through the outbox like every other event. | `config.go`, `inboundreceipt.go` |
| `TASK_COMPLETED_CONSUMER_GROUP` | *(none)* | **yes** | Stable consumer group of the fulfillment-execution `TaskCompleted` confirm-pick consumer ([ADR 0035](../adr/0035-confirm-pick-from-task-completed.md), [ADR 0036](../adr/0036-per-line-confirm-pick.md)). Always runs; dials the first broker under boot retry before starting. | `config.go`, `confirmpick.go` |
| `EVENT_PUBLISHER` | *(unset)* | no (guard) | Kept only because `warehouse-infra` still sets it. Unset or `kafka` (case-insensitive) is the single behaviour: events leave through the transactional outbox and the relay, onto the integration and analytics topics ([ADR 0017](../adr/0017-transactional-outbox.md)). Any other value (`log`) is a boot error. | `config.go` |
| `TRANSFER_ALLOCATION_CONSUMER_MODE`, `TASK_COMPLETED_CONSUMER_MODE` | *(unset)* | no (guard) | Same guard for the two consumers that used to be opt-in: unset or `kafka`; `off` or anything else is a boot error. The consumers always run. | `config.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Poll interval of the outbox relay, which always runs. An unparsable value logs a warning and uses `1s`. | `config.go` |
| `OUTBOX_MAX_ATTEMPTS` | `20` | no | Outbox relay attempt cap ([ADR 0038](../adr/0038-outbox-relay-attempt-cap.md)): a row that has failed this many sends is excluded from the relay's claims and logged at ERROR (`outbox_id`, `topic`, `event_type`, `key`). It stays in `outbox_events` until re-armed (see [Runbook](./runbook.md#outbox-relay)); later rows, including rows with the same key, overtake it. `0` = unlimited (one poison row blocks the relay forever). An invalid or negative value logs a warning and uses `20`. | `config.go`, `adapters.go` |
| `HOUSEKEEPING_INTERVAL` | `1h` | no | How often the housekeeping sweeper runs ([ADR 0026](../adr/0026-housekeeping-sweeper-idempotency-keys-and-outbox.md)). `0` disables the sweeper. | `housekeeping.go` |
| `IDEMPOTENCY_KEY_TTL` | `24h` | no | Age after which an `idempotency_keys` row is deleted. `0` keeps rows forever. | `housekeeping.go` |
| `OUTBOX_RETENTION` | `168h` | no | Age after which a **published** `outbox_events` row is deleted. `0` keeps rows forever. | `housekeeping.go` |
| `ORDER_PICK_PROGRESS_RETENTION` | `720h` | no | Age (since the last pick) after which an `order_pick_progress` counter row is deleted ([ADR 0035](../adr/0035-confirm-pick-from-task-completed.md)). `0` keeps rows forever. | `housekeeping.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5182` | no | Comma-separated origins allowed by the CORS middleware (methods GET/POST/PUT/DELETE; headers `Accept`, `Content-Type`, `Authorization`, `Idempotency-Key`). The defaults are the console shell's and this context's micro-frontend dev servers. | `internal/adapters/inbound/http/server.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`/`warning` or `error` (case-insensitive); anything else is `info`. Logs are JSON on stdout. | `config.go` |
| `OTEL_SERVICE_NAME` | `inventory-storage` | no | `service.name` on every span, metric and log. | `config.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC collector for traces and metrics. A value without a scheme gets `http://` prepended. Export is non-blocking: an unreachable collector costs telemetry, never availability. | `config.go`, `internal/adapters/outbound/telemetry/telemetry.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. | `config.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` |

What this means at runtime:

- **Always on.** The outbox relay and all four consumers (transfer allocation,
  product-master classification, inbound-receiving receipts, `TaskCompleted`
  confirm-pick) start with the process. A consumer that returns for any reason
  other than a shutdown signal makes `run` drain and return its error, so the
  process **exits non-zero** and the orchestrator restarts it.
- **`/healthz`** is liveness only. **`/readyz`** is the drain gate plus a
  Postgres ping and a Kafka broker-metadata request; while a dependency is
  down it answers `503` `application/problem+json` of type
  `dependency-unavailable`, naming the dependency (topics are never checked).
  See [Troubleshooting](./troubleshooting.md).
- **A NUL byte never reaches Postgres**: `404` in the request path, `400` in the
  query string or the body (`internal/adapters/inbound/http/nul.go`).

The `republish-product-classifications` subcommand
(`go run ./cmd/inventory republish-product-classifications`) reads only
`DATABASE_URL`, which is **required** there (`cmd/inventory/republish.go`). It
writes `outbox_events` rows; the relay of a running `cmd/inventory` delivers
them.

## `cmd/inventory-projector` — the analytics writer

Read in `cmd/inventory-projector/main.go`.

| Variable | Default | Required? | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | *(none)* | **yes** | Read-write DSN of the analytical database. The process exits with `ANALYTICS_DATABASE_URL is required` when it is empty. Migrations and the pool both use it. |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical `*.up.sql` files, applied (under boot retry) before the pool opens. |
| `KAFKA_BROKERS` | *(none)* | **yes** | Brokers for the `warehouse.inventory.analytics` consumer (group `inventory-analytics`). There is no `localhost:9092` default; the process exits with `KAFKA_BROKERS is required` when it is empty. |
| `ADMIN_ADDR` | `:8091` | no | Listen address of the admin server: `GET /healthz` (liveness) and `GET /readyz` (the drain gate plus the analytical Postgres and Kafka; `503` `dependency-unavailable` while one is unreachable). The Helm chart points all three probes at `/healthz`. |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`/`warning`, `error`; anything else is `info`. |
| `OTEL_SERVICE_NAME` | `inventory-projector` | no | `service.name`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC collector. |
| `SERVICE_VERSION` | `dev` | no | `service.version`. |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` (read by the shared telemetry adapter). |

The analytics consumer is always on. If it dies without a shutdown signal the
projector drains and exits non-zero.

## `cmd/inventory-reports` — the analytics reader

Read in `cmd/inventory-reports/main.go`. Unchanged by ADR 0042: it was already
Postgres-only and read-only.

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
It needs Postgres only: it always publishes through the transactional outbox,
and the relay that drains those rows stays in `cmd/inventory`, so this binary
never dials Kafka and needs no `KAFKA_BROKERS`. It has no `/readyz`; its probes
use `GET /healthz`.

| Variable | Default | Required? | Meaning |
| --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address of the Streamable HTTP server (`/mcp`, `/mcp/*`, `/*` and `GET /healthz`). |
| `DATABASE_URL` | *(none)* | **yes** | The same OLTP database as `cmd/inventory`. The process exits with `DATABASE_URL is required` when it is empty. |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step only (ADR 0023). `cmd/mcp` applies the same OLTP migrations on start; golang-migrate makes that idempotent. |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP migrations. |
| `EVENT_PUBLISHER` | *(unset)* | no (guard) | Accepted only unset or `kafka`; any other value is a boot error. `revoke_reservation` always writes `outbox_events` rows that the **`cmd/inventory` relay** drains (this binary runs no relay). |
| `REPORTS_BASE_URL` | *(unset)* | no | Base URL of `cmd/inventory-reports`. Setting it registers the read-only `get_inventory_flow_accuracy_report` tool (HTTP client timeout 10 s). |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`/`warning`, `error` (case-insensitive); anything else is `info`. |
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
| Boot retry (migrations, DB ping, broker dials) | 5 attempts, waits 1 s, 2 s, 4 s, 8 s: 15 s of backoff in total (no wait after the fifth attempt) plus the attempts' own duration; the code comment said "~31s" until gap fix D22, and ADR 0028's body still does | `internal/adapters/outbound/bootretry/retry.go` ([ADR 0028](../adr/0028-bootretry-boot-time-dial-retry.md)) |
| Outbox relay batch size | 100 rows per pass; attempt cap `OUTBOX_MAX_ATTEMPTS` (default 20, ADR 0038) | `internal/adapters/outbound/postgres/outbox_relay.go`, `cmd/inventory/config.go` |
| Facility cache replay wait | 60 s (`WaitReadyTimeout`) | `internal/adapters/outbound/facilitycache/consumer.go` |
| `/readyz` dependency verdict cache | 3 s (`DefaultReadinessTTL`), detached from the caller's cancellation | `internal/adapters/inbound/http/readiness.go` |
| facility-layout circuit breaker (`LOCATION_LOOKUP_MODE=http`) | trips at 5 consecutive failures or > 50 % failures over ≥ 10 requests; 30 s open; 1 half-open probe; 3 attempts per call (50 ms → 500 ms back-off) | `internal/resilience/breaker.go`, `internal/adapters/outbound/facilitylayout/breaker.go` |
| TaskCompleted handling attempts before DLQ | 5 | `internal/adapters/inbound/kafka/task_completed_consumer.go` |
| Consumer retry back-off (same message) | 200 ms doubling to a 5 s cap | `internal/adapters/inbound/kafka/transfer_consumer.go` (`retryBackoff`) |
| HTTP drain / consumer and relay drain on shutdown | 10 s / 10 s | `cmd/inventory/shutdown.go`, `cmd/inventory/adapters.go` |
| Telemetry flush on shutdown | 5 s | `cmd/*/main.go`, `cmd/inventory/telemetry.go` |
| OTel metric export interval | 15 s | `internal/adapters/outbound/telemetry/telemetry.go` |

## Helm values to environment

`charts/inventory-storage` renders these variables; anything else goes through
`extraEnv` (API) or `mcp.extraEnv` (MCP). Every `config.*` key is rendered into
the chart ConfigMap, which the **API Deployment mounts with
`envFrom: configMapRef`** (explicit `env` entries win for the same key); the
MCP, projector and reports Deployments set their own explicit `env` only and do
not mount it. `charts/inventory-storage/tests/render_test.sh` asserts the mount.

**The chart refuses to render a release that cannot boot** (`helm template`,
`lint` and `install` fail, naming the value; see `requireRealRuntime` in
`templates/_helpers.tpl`): no `database.url` / `database.existingSecret`,
`kafka.enabled=false` or empty `kafka.brokers`, a `config.eventPublisher` other
than `kafka` (or empty), a `config.locationLookupMode` other than `kafka` /
`http` (or `http` without `config.facilityLayoutBaseUrl`), a consumer mode other
than `kafka` (or empty), an empty consumer-group id, analytics enabled without a
projector DSN, or an `extraEnv` / `mcp.extraEnv` entry that overrides one of
those variables with a value the binary rejects.

| Values key | Default | Required? | Variable (Deployment) |
| --- | --- | --- | --- |
| `config.httpAddr`, `config.migrationsPath`, `config.logLevel` | `:8080`, `migrations`, `info` | no | `HTTP_ADDR`, `MIGRATIONS_PATH`, `LOG_LEVEL` (API; MCP reuses the last two) |
| `config.eventPublisher` | `kafka` | no (guard: `kafka` or empty) | `EVENT_PUBLISHER` (API and MCP) |
| `config.locationLookupMode` | `kafka` | **yes** (`kafka` or `http`) | `LOCATION_LOOKUP_MODE` (API) |
| `config.facilityLayoutBaseUrl` | *(empty)* | when `config.locationLookupMode=http` | `FACILITY_LAYOUT_BASE_URL` (API) |
| `config.outboxRelayInterval`, `config.outboxMaxAttempts`, `config.housekeepingInterval`, `config.idempotencyKeyTtl`, `config.outboxRetention`, `config.orderPickProgressRetention` | `1s`, `20`, `1h`, `24h`, `168h`, `720h` | no | `OUTBOX_RELAY_INTERVAL`, `OUTBOX_MAX_ATTEMPTS`, `HOUSEKEEPING_INTERVAL`, `IDEMPOTENCY_KEY_TTL`, `OUTBOX_RETENTION`, `ORDER_PICK_PROGRESS_RETENTION` (API) |
| `config.transferAllocationConsumerGroup`, `config.productMasterConsumerGroup`, `config.inboundReceiptConsumerGroup`, `config.taskCompletedConsumerGroup` | `inventory-storage-transfer-allocation`, `inventory-storage-product-master`, `inventory-storage-inbound-receipts`, `inventory-storage-confirm-pick` | **yes** (non-empty) | `TRANSFER_ALLOCATION_CONSUMER_GROUP`, `PRODUCT_MASTER_CONSUMER_GROUP`, `INBOUND_RECEIPT_CONSUMER_GROUP`, `TASK_COMPLETED_CONSUMER_GROUP` (API) |
| `config.transferAllocationConsumerMode`, `config.taskCompletedConsumerMode` | `kafka` | no (guard: `kafka` or empty; `off` is refused) | `TRANSFER_ALLOCATION_CONSUMER_MODE`, `TASK_COMPLETED_CONSUMER_MODE` (API) |
| `database.url` or `database.existingSecret` + `existingSecretKey` | *(empty)*, key `DATABASE_URL` | **yes** | `DATABASE_URL` (API and MCP) |
| `database.migrationsExistingSecretKey` | *(empty)* | no | `MIGRATIONS_DATABASE_URL` (`optional: true`; API and MCP) |
| `kafka.enabled`, `kafka.brokers` | `true`, *(empty)* | `kafka.brokers` **yes**; `kafka.enabled=false` is refused | `KAFKA_BROKERS` (always set on the API, MCP and projector pods; the MCP binary ignores it) |
| `otel.enabled`, `otel.endpoint`, `otel.serviceName` | see `values.yaml` | no | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` |
| `environment`, `image.tag` | see `values.yaml` | no | `ENVIRONMENT`, `SERVICE_VERSION` |
| `analytics.projector.adminAddr`, `analytics.migrationsPath` | see `values.yaml` | no | `ADMIN_ADDR`, `ANALYTICS_MIGRATIONS_PATH` (projector) |
| `analytics.reports.httpAddr` | see `values.yaml` | no | `HTTP_ADDR` (reports) |
| `analytics.database.*` | *(empty)* | **yes** while `analytics.enabled` | `ANALYTICS_DATABASE_URL` (projector: read-write key; reports: reader key) |
| `mcp.httpAddr` | see `values.yaml` | no | `MCP_ADDR`; with `analytics.enabled` the chart also sets `REPORTS_BASE_URL` to the reports Service |

`CORS_ALLOWED_ORIGINS` has no values key: set it through `extraEnv` when the
console is served from another origin.

The chart's default `readinessProbe` for the API points at `/readyz`, so an
unreachable Postgres or broker takes the pod out of the Service. A deployment
that overrides it to `/healthz` (the `warehouse-infra` values file does at the
time of ADR 0042) keeps the pod in the Service during a broker outage until the
override is dropped.
