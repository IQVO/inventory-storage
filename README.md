# Inventory & Storage

The WMS-tier authoritative record of **what is held where, and what portion
is usable**. Implements e-commerce-retailer-style **chaotic (random) stow**: no fixed
product location — an item goes to any free bin, and the system records the
exact bin. Supplies "stock reality" to Work Planning and makes allocation a
**revocable reservation** so a failed physical delivery never strands an
order.

## Disclaimer

This project is a domain-driven design study. It is not associated with,
endorsed by or affiliated with any company, and it uses no company's code,
data, logos or trademarks. All names, figures and examples are illustrative
or synthetic.

## Documentation

Full documentation site: **https://iqvo.github.io/inventory-storage/**

Business context and domain vision, the DDD model (subdomain classification,
aggregates and invariants, domain events, use cases), the ddd-crew DDD
artifact pack (core domain chart, bounded context canvas, aggregate design
canvas, EventStorming, domain message flows, UML class / ER / sequence
diagrams — all derived from the code), an API reference
generated from `apis/openapi.yaml` plus a hand-authored Events page from
`apis/asyncapi.yaml`, the ecosystem context map, and the Architecture Decision
Records. Source lives in [`docs/`](docs/) (Docusaurus); it is built and
deployed to GitHub Pages by [`.github/workflows/docs.yml`](.github/workflows/docs.yml).

Page index (source paths under `docs/docs/`):

| Area | Pages |
| --- | --- |
| Overview | [Introduction](docs/docs/overview/introduction.md), [Architecture](docs/docs/overview/architecture.md) (binaries, ports, data stores), [Quickstart](docs/docs/overview/quickstart.md) |
| Operations | [Configuration](docs/docs/operations/configuration.md) (every env var per binary), [Runbook](docs/docs/operations/runbook.md), [Observability](docs/docs/operations/observability.md), [Troubleshooting](docs/docs/operations/troubleshooting.md) |
| Development | [Testing](docs/docs/development/testing.md) (test pyramid, `make` targets, CI jobs) |
| Ecosystem | [Context map](docs/docs/ecosystem/context-map.md), [Integration](docs/docs/ecosystem/integration.md) |
| DDD | [Use cases](docs/docs/ddd/use-cases.md), [Subdomain classification](docs/docs/ddd/subdomain-classification.md), and the artifact pack under [`docs/docs/ddd/`](docs/docs/ddd/) |
| APIs | [REST reference](docs/docs/api-reference/overview.md), [Integration events](docs/docs/api-reference/events.md), [MCP tools](docs/docs/mcp/tools.md), [MCP governance charter](docs/docs/mcp/governance-charter.md), [A2A inbound adapter (ADR-0048)](docs/docs/adr/0048-a2a-inbound-adapter.md), [Flow & Accuracy report](docs/docs/analytics/inventory-flow-accuracy-report.md) |
| Decisions | [ADR index](docs/docs/adr/about.md) (42 ADRs; [ADR-0042](docs/docs/adr/0042-postgres-and-kafka-are-the-only-runtime.md): Postgres and Kafka are the only runtime) |

## Layering (hexagonal / ports & adapters)

Strict dependency rule: **domain depends on nothing; application depends on
domain; adapters depend on application/domain.**

```
cmd/inventory/               composition root (main.go)
cmd/inventory-projector/     analytics WRITER: consumes the analytics topic, projects
cmd/inventory-reports/       analytics READER: read-only report REST
cmd/mcp/                      MCP inbound adapter (Streamable HTTP)
cmd/a2a/                      A2A agent (JSON-RPC over HTTP, Agent Card at /.well-known/agent-card.json)
internal/
  domain/                    pure Go — no framework, no SQL types
    location/                Bin aggregate (capacity, occupancy)
    stock/                   StockUnit aggregate (SKU@location, qty, state)
    reservation/              Reservation aggregate (revocable, timeout)
    product/                 ProductClassification aggregate + DOT segregation matrix
    shared/                  SKU, BinId, Quantity value objects; domain events
  analytics/report/          read-model region (depends on nothing) for the data product
  application/
    ports/                   outbound interfaces the application depends on
    usecases/                one struct per use case (ReceiveStock, StowStock, ...)
  adapters/
    inbound/http/            chi router, DTOs, domain-error -> HTTP mapping; reports REST
    inbound/kafka/           analytics consumer (projector)
    inbound/mcp/             MCP tools incl. the read-only report tool
    inbound/a2a/             A2A skills: check_availability (read), revoke_reservation (write)
    outbound/postgres/       pgxpool repos + golang-migrate migrations
    outbound/memory/         thread-safe in-memory repos: unit/BDD test doubles ONLY (no cmd/ binary imports them, ADR-0042)
    outbound/events/         log publisher + buffered + multi (fan-out) publisher: test doubles ONLY (events leave through the outbox)
    outbound/kafka/          Kafka integration + analytics publishers (see below)
    outbound/analyticsstore/ analytical Postgres projection + read-only reader
    outbound/facilitycache/  Kafka-fed local cache of facility-layout zone/slot data (ADR-0013)
    outbound/facilitylayout/ sync HTTP location-classification lookup (LOCATION_LOOKUP_MODE=http, the rollback for kafka)
    outbound/telemetry/      OTel setup, trace-aware slog, reservation metrics
  architecture/              arch-go fitness tests for the dependency rule
migrations/                  golang-migrate SQL files (OLTP)
migrations/analytics/        golang-migrate SQL files (analytical read model)
web/                         inventory_mfe — Module Federation remote (separate npm module)
```

The application layer never imports an adapter package — it depends only on
`application/ports` interfaces, which `adapters/outbound/*` implement. The
inbound HTTP adapter never leaks domain structs across the wire; every
response is a DTO.

## Design notes

- **Reservation is SKU-scoped, not bin-scoped.** `ReserveStock` draws from
  whichever `StockUnit`s have usable quantity (first-fit across bins) and
  records exactly which units/quantities it drew from as `Allocation`s on the
  `Reservation`. `RevokeReservation` returns quantity to those same units, but
  because a fresh `ReserveStock` call is free to draw from any unit with
  usable quantity, a subsequent reservation can be satisfied from a
  **different physical holding** — this is what makes a reservation
  revocable without stranding an order when a specific pick fails.
- **StockUnit lifecycle**: `AVAILABLE` -> `RESERVED` (any reserved quantity
  present) -> `PICKED` (physically removed, quantity remains) or `REMOVED`
  (quantity reached zero), or -> `UNLOCATED` (cycle count could not account
  for it). `Usable = on-hand - reserved`, and is zero for `UNLOCATED` /
  `REMOVED` units.
- **ReceiveStock does not create a `StockUnit`.** A `StockUnit` requires both
  a SKU and a Bin (item-scan + location-scan) by construction — that is the
  domain's stow-requires-both invariant. Receiving stages goods (publishes
  `StockReceived`) without persisting an aggregate; the durable record starts
  at `StowStock`.
- **Cycle count shortfall** marks whichever `StockUnit`s cover the shortfall
  fully `UNLOCATED` (not split into located/lost sub-quantities), publishing
  `ItemUnlocated` per unit touched, kept simple by design. An overage is
  reported as a `DiscrepancyDetected`/`CycleCountCompleted(discrepancy=true)`
  pair for a separate receiving/audit process to reconcile.

## Run it

Postgres and Kafka are the only runtime ([ADR-0042](docs/docs/adr/0042-postgres-and-kafka-are-the-only-runtime.md)):
there is no in-memory mode and no log-only publisher, and `cmd/inventory` exits
at boot naming the first missing variable. Start both services, then the app:

```sh
# the local kind cluster owns localhost:9092, so compose publishes Kafka on 19092
# (override with KAFKA_HOST_PORT and keep KAFKA_BROKERS in step)
docker compose up -d --wait postgres kafka

export DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable'
export KAFKA_BROKERS=localhost:19092
export LOCATION_LOOKUP_MODE=kafka
export TRANSFER_ALLOCATION_CONSUMER_GROUP=inventory-storage-transfer-allocation
export PRODUCT_MASTER_CONSUMER_GROUP=inventory-storage-product-master
export INBOUND_RECEIPT_CONSUMER_GROUP=inventory-storage-inbound-receipt
export TASK_COMPLETED_CONSUMER_GROUP=inventory-storage-confirm-pick
go run ./cmd/inventory
```

Migrations in `migrations/` run automatically on startup (via
`MIGRATIONS_PATH`, default `migrations`). Listens on `:8080` (override with
`HTTP_ADDR`); `GET /readyz` is `200` only while Postgres and the Kafka broker
both answer. Every variable is documented in
[Configuration](docs/docs/operations/configuration.md).

### Running the MCP server in Kubernetes

The MCP server (`cmd/mcp`, ADR-0008) ships in the same image as `/app/mcp`
and is deployed by the Helm chart as a separate Deployment + ClusterIP
Service named `<release>-mcp`, gated on `mcp.enabled` (off by default). It
serves MCP over Streamable HTTP at both `/` and `/mcp` on port `8090`
(`mcp.service.port` → `mcp.httpAddr`), and answers `GET /healthz` for the
liveness/readiness probes. The binary reads `DATABASE_URL` from the same
Secret as the HTTP service, and `REPORTS_BASE_URL` is wired to the
reports Service automatically when `analytics.enabled=true`.

```sh
helm upgrade --install inventory-storage charts/inventory-storage \
  --set database.url='postgres://…' \
  --set mcp.enabled=true
# in-cluster endpoint for MCP clients (e.g. warehouse-ops-agent):
#   http://inventory-storage-mcp.<namespace>.svc.cluster.local:8090/mcp
```

### Running the A2A agent in Kubernetes

The A2A agent (`cmd/a2a`, [ADR-0048](docs/docs/adr/0048-a2a-inbound-adapter.md))
ships in the same image as `/app/a2a` and is deployed by the Helm chart as a
separate Deployment + ClusterIP Service (`<release>-a2a`, port `8093`) when
`a2a.enabled=true` (default `false`). It runs the same use cases over the same
`DATABASE_URL` Secret as the HTTP service and reads `A2A_ADDR` (default
`:8093`) and the mandatory `A2A_AGENT_URL`, the absolute http(s) URL advertised
on the Agent Card (the chart defaults it to the in-cluster Service DNS; set
`a2a.agentUrl` to override). `GET /healthz` serves the probes, the Agent Card is
at `/.well-known/agent-card.json`, and the JSON-RPC endpoint is mounted at both
`/` and `/a2a`. Like `cmd/mcp` it never runs the outbox relay and never dials a
broker.

Two skills, called as a `SendMessage` data part
`{"skill": "<id>", "arguments": {...}}`:

- `check_availability` (read): `{sku}` returns the usable quantity of the SKU.
- `revoke_reservation` (write): `{reservationId}` revokes the reservation,
  returns its quantity to usable and publishes `ReservationRevoked` through the
  transactional outbox. A second call on the same reservation is a `409`.

Domain rejections come back as an in-band RFC 7807 `problem` with the same
status classification as REST (404/400/409/500). A2A carries delegation only;
the facts still leave as CloudEvents on Kafka.

```sh
helm upgrade --install inventory-storage charts/inventory-storage \
  --set database.url='postgres://…' \
  --set a2a.enabled=true
# in-cluster endpoint for A2A clients:
#   http://inventory-storage-a2a.<namespace>.svc.cluster.local:8093/
```

## Endpoints

| Method | Path | Use case |
|--------|------|----------|
| POST   | `/stock-receipts` | ReceiveStock — **deprecated** in `apis/openapi.yaml` (K24): inbound-receiving's `ReceiptLineReceived` is the production path (ADR-0037); still serves ad-hoc receipts. Requires `Idempotency-Key` (ADR-0018) |
| POST   | `/stowages` | StowStock — requires `Idempotency-Key` |
| POST   | `/reservations` | ReserveStock — requires `Idempotency-Key` (ADR-0018) |
| GET    | `/reservations?demandRef=` | GetReservationsByDemandRef — keyset-paged `{items,nextCursor}` (`limit` 1-500, default 100; `cursor`) |
| DELETE | `/reservations/{id}` | RevokeReservation |
| POST   | `/reservations/{id}/pick-confirmations` | ConfirmPick — requires `Idempotency-Key`; **deprecated** in `apis/openapi.yaml` (K37): fulfillment-execution's `TaskCompleted` consumer confirms picks (ADR-0035, ADR-0036); removal planned with the next breaking window |
| GET    | `/inventory/{sku}/usable` | GetUsable |
| PUT    | `/bins/{binId}` | RegisterBin — idempotent: 201 created / 200 unchanged or resized / 409 below occupancy (ADR-0025) |
| GET    | `/bins/{binId}` | GetBin — capacity, occupied, available |
| POST   | `/bins/{binId}/cycle-counts` | RunCycleCount — requires `Idempotency-Key` |
| GET    | `/bins/{binId}/containers` | ListBinContainers — keyset-paged `{items,nextCursor}` in LPN order (`limit`, `cursor`) |
| POST   | `/transfers/{transferLineId}/receipt` | StageTransferReceipt — requires `Idempotency-Key`; 201 staged / 200 replay / 422 quarantined / 409 conflicting scan (ADR-0033) |
| POST   | `/transfers/{transferLineId}/stowages` | StowTransferStock — requires `Idempotency-Key`; 200 stowed with allocations / 409 not staged, wrong-site bin, or quantity mismatch (ADR-0033) |
| GET    | `/healthz` | liveness |
| GET    | `/readyz` | readiness — 503 `dependency-unavailable` while Postgres or the Kafka broker is down, and 503 `{"status":"not_ready"}` once graceful shutdown has begun (ADR-0020, ADR-0042) |

Every `POST` is route-scoped behind a transactional `Idempotency-Key` HTTP
middleware (see [ADR-0018](docs/docs/adr/0018-idempotency-key-middleware.md)
and [ADR-0049](docs/docs/adr/0049-adopt-the-fleet-rest-api-guideline.md)): a
missing header is a 400 `idempotency-key-required`, a key reused with a
different method, path or body is a 409 `idempotency-key-reused`, and a
retried identical request returns the exact same stored response plus
`Idempotency-Replayed: true` without re-executing the handler — a client-side
retry after a dropped response can never apply a write twice. The middleware
is backed by Postgres (`idempotency_keys`) in every deployment: there is no
mode in which a `POST` is unprotected.

Every collection `GET` (`/reservations`, `/bins/{binId}/containers`) is
keyset-paginated: `limit` (1-500, default 100) and the opaque `cursor`, answer
`{"items": [...], "nextCursor": "..."}` with `nextCursor` absent on the last
page; a bad `limit` is 400 `invalid-limit`, a bad `cursor` 400
`invalid-cursor`. Clients loop on `nextCursor` until it is absent.

None of these routes is authenticated: the static-bearer-key layer added by
ADR-0014 was removed again by
[ADR-0015](docs/docs/adr/0015-remove-rest-identity-layer.md), for both the
REST and the MCP surface.

### curl walkthrough

```sh
curl -s localhost:8080/healthz

# Stow requires a bin to exist first. Register it (idempotent, declarative —
# ADR-0025): 201 the first time, 200 on a repeat; a capacity below what the
# bin already holds is a 409 capacity-below-occupancy.
curl -s -i -X PUT localhost:8080/bins/A-1-1 -d '{"capacity":20}'
# => 201 Created, Location: /bins/A-1-1
#    {"binId":"A-1-1","capacity":20,"occupied":0,"available":20}

curl -s localhost:8080/bins/A-1-1
# => {"binId":"A-1-1","capacity":20,"occupied":0,"available":20}

curl -s -X POST localhost:8080/stock-receipts \
  -H 'Idempotency-Key: 8b1a...' \
  -d '{"sku":"SKU-1","quantity":10}'
# => 202 Accepted (a staged receipt has no addressable resource yet)

curl -s -i -X POST localhost:8080/stowages \
  -H 'Idempotency-Key: 5c2e...' \
  -d '{"sku":"SKU-1","quantity":10,"binId":"A-1-1"}'
# => 201 Created, Location: /stock/<stock-unit-id>

curl -s -i -X POST localhost:8080/reservations \
  -H 'Idempotency-Key: 8b1a...' \
  -d '{"sku":"SKU-1","quantity":6,"demandRef":"order-42","lineNo":1}'
# => 201 Created, Location: /reservations/<id>, body {"id":"res-...", ...,
#    "lineNo":1, "allocations":[{"stockUnitId":"su-...","binId":"A-1-1","quantity":6}]}
#    Every allocation names its pick location (binId) — ADR-0025. lineNo is
#    optional (1..2147483647; omitted from the response when unknown) — ADR-0036.
# A retry with the SAME Idempotency-Key + body returns this exact response
# again (plus `Idempotency-Replayed: true`) without creating a second
# reservation (ADR-0018); omitting the header entirely on any POST is a 400,
# and reusing the key for a different request is a 409 idempotency-key-reused.

curl -s 'localhost:8080/reservations?demandRef=order-42&limit=100'
# => {"items":[{"id":"res-...", ...}], "nextCursor":"..."}  (nextCursor absent
#    on the last page; pass it back as &cursor=... with the same demandRef)

curl -s localhost:8080/inventory/SKU-1/usable

curl -s -X POST localhost:8080/reservations/<id>/pick-confirmations \
  -H 'Idempotency-Key: 9d4f...'

curl -s -X DELETE localhost:8080/reservations/<id>

curl -s -X POST localhost:8080/bins/A-1-1/cycle-counts \
  -H 'Idempotency-Key: 1e7a...' \
  -d '{"countedQuantity":9}'
```

Error responses are RFC 7807 (`application/problem+json`), with a status
mapped from the typed domain/application error (400 missing/malformed
input, 422 well-formed but semantically invalid values like a non-positive
quantity, 404 not found, 409 conflict — e.g. bin full, reservation exceeds
usable, reservation already resolved):

```sh
curl -s -i -X DELETE localhost:8080/reservations/does-not-exist
# HTTP/1.1 404 Not Found
# Content-Type: application/problem+json
#
# {
#   "type": "https://errors.inventory-storage.warehouse-systems.dev/reservation-not-found",
#   "title": "Reservation not found",
#   "status": 404,
#   "detail": "reservation not found",
#   "instance": "/reservations/does-not-exist"
# }
```

## Integration

This service publishes integration events to the shared warehouse-systems
Kafka broker so other bounded contexts (e.g. `wes-work-planning`) can project
their own read models from inventory reality. It also consumes six topics
(five consumers always run, each under a mandatory stable consumer group; the
facility cache is the sixth, with `LOCATION_LOOKUP_MODE=kafka`):
`warehouse.facility.events`, to keep a local read model of facility-layout's
zone classifications (see [Consumed](#consumed-facility-layouts-location-classifications)
below), `warehouse.network-inventory-planning.events`, the command side
of the site-scoped transfer allocation exchange (ADR-0030),
`warehouse.product-master.events`, `warehouse.inbound-receiving.events`,
`warehouse.returns-processing.events`, and
`warehouse.fulfillment.events` (the sections below).

- **Topic**: `warehouse.inventory.events`
- **Publishing**: always through the transactional outbox (ADR 0017,
  [ADR 0042](docs/docs/adr/0042-postgres-and-kafka-are-the-only-runtime.md)):
  each use case's aggregate save(s) and its domain event(s) commit together in
  one Postgres transaction as `outbox_events` rows, and a background relay
  (always on in `cmd/inventory`; `OUTBOX_RELAY_INTERVAL`, default `1s`) drains
  them onto Kafka. There is no log-only publisher and no direct publishing.
  `EVENT_PUBLISHER` is only a guard: unset or `kafka`, any other value is a boot
  error. A row that fails `OUTBOX_MAX_ATTEMPTS` sends (default `20`,
  `0` = unlimited; ADR 0038) is logged at ERROR and excluded from the relay
  until re-armed, so one poison row cannot block it forever. `cmd/mcp`
  reads `DATABASE_URL` only and always writes the same outbox rows, so an MCP
  `revoke_reservation` is published by the relay in `cmd/inventory`.
- **Housekeeping** (ADR 0026): a background sweeper in `cmd/inventory` deletes
  `idempotency_keys` older than `IDEMPOTENCY_KEY_TTL` (default `24h`) and
  *published* `outbox_events` older than `OUTBOX_RETENTION` (default `168h`),
  every `HOUSEKEEPING_INTERVAL` (default `1h`; `0` disables the sweeper, a `0`
  TTL/retention keeps that table's rows forever). Unpublished outbox rows are
  never deleted. The same sweeper deletes `order_pick_progress` rows (the
  confirm-pick consumer's per-order counter, ADR 0035) not updated for
  `ORDER_PICK_PROGRESS_RETENTION` (default `720h` = 30 days).
- **Broker**: `KAFKA_BROKERS`, comma-separated, **required** (no default; the
  process exits at boot without it). There is one broker platform-wide: the
  in-cluster Kafka deployed by `warehouse-infra`, whose external listener is
  reachable from the host at `localhost:9092`. The repo's `docker-compose.yml`
  starts a disposable one on `localhost:19092` (`KAFKA_HOST_PORT`) for local
  runs, so it never collides with the kind cluster.
- **Envelope: CloudEvents 1.0, mandatory** (structured mode, fleet-wide —
  [ADR-0024](docs/docs/adr/0024-cloudevents-mandatory-envelope.md)). Every
  message carries the Kafka header
  `content-type: application/cloudevents+json; charset=UTF-8`; the Kafka key
  is the reservation id:
  ```json
  {
    "specversion": "1.0",
    "id": "uuid-v4",
    "source": "/warehouse/inventory-storage",
    "type": "com.warehouse.wms.inventory-storage.reservation.StockReserved",
    "subject": "res-1",
    "time": "2026-08-21T22:00:00Z",
    "datacontenttype": "application/json",
    "dataschema": "urn:warehouse:inventory-storage:events:StockReserved:v1",
    "data": {}
  }
  ```
- **Events published** — `com.warehouse.wms.inventory-storage.reservation.StockReserved`
  (on a successful `ReserveStock`) and
  `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked` (on a
  successful `RevokeReservation`), both with the same `data` shape:
  ```json
  {"sku": "SKU-1", "quantity": 4, "demand_ref": "order-42"}
  ```
  Plus the transfer-allocation replies (ADR-0030, integration topic only,
  through the same transactional outbox):
  `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated`
  (key/subject = reservation id) with `data`:
  ```json
  {
    "transfer_id": "tr-77", "transfer_line_id": "tl-77-1",
    "origin_site_id": "SITE-A", "reservation_id": "res-tr-1",
    "sku": "SKU-T1", "quantity": 6,
    "allocations": [{"stock_unit_id": "su-1", "bin_id": "BIN-1", "quantity": 6}],
    "expires_at": "2026-10-06T12:30:00Z"
  }
  ```
  and `com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected`
  (key/subject = transfer_line_id) with `data`:
  ```json
  {
    "transfer_id": "tr-77", "transfer_line_id": "tl-77-2",
    "origin_site_id": "SITE-B", "sku": "SKU-T2",
    "requested_quantity": 9, "reason": "INSUFFICIENT_USABLE"
  }
  ```
  `reason` is the closed set `ORIGIN_SITE_UNKNOWN | INSUFFICIENT_USABLE |
  IDEMPOTENCY_CONFLICT`.
  The legacy `com.warehouse.wms.inventory-storage.product.ProductClassified`
  is **no longer published**: ADR-0034 stage E (2026-10-17) removed the type,
  the `republish-product-classifications` backfill and both
  `/products/{sku}/classification` routes. Classification is product-master's
  (see "Consumed: product-master's product classifications" below).
  (`ReservationRevoked`'s domain event only carries the reservation id; the
  Kafka adapter looks the reservation back up via `ReservationRepo` to fill in
  `sku`/`quantity`/`demand_ref`.) Every other domain event (`StockReceived`,
  `ItemStowed`, ...) is not part of this integration contract and is not
  forwarded to `warehouse.inventory.events` (`LocationRecorded` stays
  in-process: no consumer). (Those events DO feed the separate
  analytics data product on `warehouse.inventory.analytics` — see
  [Analytics](#analytics-data-product) below.)

Watch the topic while the app from [Run it](#run-it) is up (compose broker):

```sh
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic warehouse.inventory.events --from-beginning

# or, against the in-cluster broker (point KAFKA_BROKERS at it instead):
kubectl --context kind-warehouse -n warehouse-systems exec -it kafka-controller-0 -c kafka -- \
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic warehouse.inventory.events --from-beginning

# then drive a reservation + revoke through the API (see curl walkthrough above)
```

### Consumed: network-inventory-planning's transfer allocation commands

The command side of the site-scoped transfer allocation exchange
([ADR-0030](docs/docs/adr/0030-site-scoped-transfer-allocation.md)).
network-inventory-planning publishes
`com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested`
(`data`: `{transfer_id, transfer_line_id, origin_site_id, sku, quantity}`) per
transfer line; this service decides it exactly once — stock in the ORIGIN
SITE's custody is drawn into a revocable `Reservation`, the decision is
recorded in the `transfer_allocations` ledger (DB-unique on
`transfer_line_id`), and the reply event goes out through the transactional
outbox, all in ONE Postgres transaction. A replayed command returns the
original outcome without touching stock again; a same-line-id /
different-payload command is answered `IDEMPOTENCY_CONFLICT`.

Stock without a recorded site (rows persisted before migration 0030) is
**never transfer-allocatable** — such commands answer `ORIGIN_SITE_UNKNOWN`
while continuing to serve ordinary demand unchanged.

| Env var | Required | Meaning |
| --- | --- | --- |
| `TRANSFER_ALLOCATION_CONSUMER_GROUP` | yes | Stable consumer group id, e.g. `inventory-storage-transfer-allocation`. The consumer always runs; a missing value is a boot error. |
| `TRANSFER_ALLOCATION_CONSUMER_MODE` | no | Legacy guard: unset or `kafka`. Any other value (`off`) is a boot error. |

### Consumed: product-master's product classifications (ADR-0034)

product-master owns product classification. This service keeps a
version-guarded local copy in `product_classifications`, which `StowStock`
reads for the ADR-0009/0010 placement rules (unchanged). It is fed by
`com.warehouse.wms.product-master.product.ProductClassified` on
`warehouse.product-master.events`; the CloudEvents `id` is claimed in
`processed_events` in the same transaction as the upsert, and a message
applies only when its `version` is newer than the stored one. Other
product-master event types are ignored.

| Env var | Required | Meaning |
| --- | --- | --- |
| `PRODUCT_MASTER_CONSUMER_GROUP` | yes | Stable consumer group id, e.g. `inventory-storage-product-master`. The consumer always runs; a missing value is a boot error. |

Stage E (2026-10-17) removed `PUT` and `GET /products/{sku}/classification`:
the router registers neither, so both answer `404`
(`features/product_classification.feature`). The local copy is read only by
`StowStock`; classify products in product-master.

### Consumed: inbound-receiving's ReceiptLineReceived → staged stock (ADR-0037)

inbound-receiving owns ASNs, dock appointments and receipts. When it counts a
line it publishes `com.warehouse.wms.inbound-receiving.receipt.ReceiptLineReceived`
on `warehouse.inbound-receiving.events`. A line with `condition=Good` is booked
through the existing `ReceiveStock` use case, so the effect is the same as
`POST /stock-receipts`: `StockReceived` on the analytics topic, the quantity
**staged and not yet usable**, stow still the RF action (item scan plus location
scan). The CloudEvents `id` is claimed in `processed_events` in the same
transaction, so a redelivery books nothing twice. A `Damaged` line is **not
booked** in v1 (INFO log and
`inventory.inbound_receipt_units{outcome=damaged_not_booked}`); an invalid line
(empty `sku`, `quantity` <= 0, unknown `condition`, not a CloudEvent) is logged
and skipped. Other inbound-receiving event types are ignored. `POST /stock-receipts`
is unchanged and keeps serving ad-hoc receipts.

| Env var | Required | Meaning |
| --- | --- | --- |
| `INBOUND_RECEIPT_CONSUMER_GROUP` | yes | Stable consumer group id, e.g. `inventory-storage-inbound-receipt`. The consumer always runs; a missing value is a boot error. `StockReceived` leaves through the outbox like every other event. Chart value `config.inboundReceiptConsumerGroup`. |

### Consumed: returns-processing's ReturnLineStaged → staged stock (ADR-0044)

returns-processing owns the reverse flow of goods. When a disposition
decides RESTOCK it publishes
`com.warehouse.wms.returns-processing.disposition.ReturnLineStaged` (the
RESTOCK disposition's handover, `data` = inbound-receiving's
`ReceiptLineReceived` shape plus `rma_id`, `disposition_id`, `site_id`) on
`warehouse.returns-processing.events`. The line is booked through the SAME
`ReceiveStock` use case a receipt line uses, so the effect is identical to
`POST /stock-receipts` and to the dock path: `StockReceived` on the analytics
topic, the quantity **staged and not yet usable**, stow still the RF action.
The CloudEvents `id` is claimed in `processed_events` (consumer
`return-line-staged`, the flow's provenance record) in the same transaction,
so a redelivery books nothing twice; `condition` is the constant `Good` and
anything else, an empty `sku`, a non-positive `quantity` or an empty
`disposition_id` is logged (WARN) and skipped. Other returns-processing event
types are ignored.

| Env var | Required | Meaning |
| --- | --- | --- |
| `RETURN_LINE_CONSUMER_GROUP` | yes | Stable consumer group id, e.g. `inventory-storage-return-lines`. The consumer always runs; a missing value is a boot error. `StockReceived` leaves through the outbox like every other event. Chart value `config.returnLineConsumerGroup`. |

The one-shot `republish-product-classifications` backfill (product-master ADR
0003 stage B) was retired with stage E: `cmd/inventory` has no subcommands.

### Consumed: fulfillment-execution's TaskCompleted → confirm exactly the picked line (ADR-0036; counting fallback ADR-0035)

Nothing calls `POST /reservations/{id}/pick-confirmations` in production, so picks are
confirmed from an event instead (no synchronous call into this context). On
`com.warehouse.wes.fulfillment-execution.task.TaskCompleted` from
`warehouse.fulfillment.events`, for a `task_type` of `PICK` with a non-empty
`order_ref` (the OrderId, = a reservation's `demand_ref`):

- **With the additive optional `line_no`** (ADR-0036) the ACTIVE reservation of
  (`order_ref`, `line_no`) is confirmed through the existing `ConfirmPick` logic:
  stock decremented, bin capacity released, `StockPicked` raised. A `Reservation`
  stores its line (`line_no`, sent by order-management as `lineNo` on
  `POST /reservations`), so the order's other lines stay ACTIVE until their own pick
  arrives and nothing is counted (no `order_pick_progress` row).
- **Without `line_no`, or for reservations made before it existed** (NULL), the
  ADR-0035 fallback **counts** the pick for the order (`order_pick_progress`) and,
  when the count reaches the ACTIVE + CONFIRMED reservations (REVOKED and EXPIRED
  are not awaited), i.e. on the **last** pick, confirms every ACTIVE one. Earlier
  picks only record progress (confirming early would mark unpicked lines as picked
  with no undo; confirming late is safe).

The CloudEvents `id` claim (`processed_events`), the counter increment (fallback
only) and all confirmations commit in **one transaction**; a redelivery (or an
extra PICK event for an already confirmed line or order) confirms nothing new and
never double-counts. CONFIRMED/REVOKED reservations are skipped, an EXPIRED one
(ADR-0003) is skipped, logged and counted in
`inventory.pick_confirmations{outcome=expired}`, never an error; an order with
no reservations is a successful no-op; a `line_no` that is not an integer in 1..2147483647 is
dead-lettered. Counter rows older than
`ORDER_PICK_PROGRESS_RETENTION` (default `720h`) are swept (see Housekeeping).

**Limitation: short picks are not modelled.** A Task carries no SKU or
quantity, so the whole reserved quantity of a confirmed line is picked. Short
picks need per-line quantities in work-planning's WorkUnit and
fulfillment-execution's Task and a business rule for the remainder.

A transient failure retries the same message (capped backoff, 5 attempts), then
dead-letters it to `warehouse.fulfillment.events.dlq`; a malformed payload is
dead-lettered at once. `StockPicked` leaves through the outbox.

| Env var | Required | Meaning |
| --- | --- | --- |
| `TASK_COMPLETED_CONSUMER_GROUP` | yes | Stable consumer group id, e.g. `inventory-storage-confirm-pick`. The consumer always runs; a missing value is a boot error. |
| `TASK_COMPLETED_CONSUMER_MODE` | no | Legacy guard: unset or `kafka`. Any other value (`off`) is a boot error. |

### Destination transfer receiving: stage → quarantine → stow (ADR-0033)

When the truck arrives, custody at the destination is taken by a SCAN,
not by an event: `POST /transfers/{transferLineId}/receipt` records what
was physically counted against the transfer line the operator claims,
and `POST /transfers/{transferLineId}/stowages` places it into
destination-site bins. Both require an `Idempotency-Key`.

- **Stage** looks the line up in the `transfer_allocations` ledger. A
  recognized `ALLOCATED` row answers `201` with a `STAGED`
  `transfer_receipts` row — `expected_quantity` from the ledger,
  `received_quantity` as counted, `variance = received − expected`
  (SIGNED: over positive, short negative, never silently absorbed) — and
  publishes
  `com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged`
  (key/subject = transfer line id) through the same transactional
  outbox. **No usable stock moves at stage.**
- **Quarantine** — the line was never allocated here, was `REJECTED`,
  the `transferId` mismatches, or the destination IS the transfer's own
  origin site: an `inventory_exceptions` row is written, NOTHING that
  could raise availability is published, and the `422` problem body
  carries the exception's coordinates in its `exception` member for an
  operator to resolve.
- **Stow** verifies every bin belongs to the receipt's
  `destination_site_id` (site custody fails closed — another site's bin
  or a legacy site-less bin is a `409`), requires the bin quantities to
  sum exactly to `receivedQuantity`, creates one `StockUnit` per leg AT
  the destination site, moves the receipt `STAGED → STOWED`, and
  publishes
  `com.warehouse.wms.inventory-storage.stock.TransferStockStowed` — the
  **only** event that raises destination usable. A replayed stow returns
  the original allocations, creates no second StockUnit, republishes
  nothing (DB-unique receipt + guarded state transition).

`TransferArrived` from fulfillment-execution
(`warehouse.fulfillment.events`) is deliberately NOT consumed here: it
is a work-execution fact, not a custody fact — custody is taken by the
physical count behind a scan. See ADR-0033.

## Consumed: facility-layout's location classifications

`StowStock` enforces hazmat-zone and temperature-class placement for SKUs
classified `Hazmat` or `TemperatureSensitive` (ADR-0009), which needs the
target bin's zone attributes. Where they come from is selected by
`LOCATION_LOOKUP_MODE`, which is **required** and is `kafka` or `http` (unset
and `permissive` are boot errors: a lookup that answers "unknown" for every bin
validates no location at all):

| Mode | Behaviour |
| --- | --- |
| `kafka` | `internal/adapters/outbound/facilitycache` replays `warehouse.facility.events` (`ZoneRegistered`, `LocationSlotRegistered`, `LocationSlotHandlingUnitDeclared`, `LocationSlotDecommissioned`) from the earliest offset into an in-process cache, under a per-process consumer group, and startup blocks until that replay completes (60s timeout). Uses `KAFKA_BROKERS`. This is what the `warehouse-infra` cluster runs (ADR-0013). |
| `http` | Synchronous `GET /locations/{code}/classification` on facility-layout per stow, via `FACILITY_LAYOUT_BASE_URL` (required in this mode). Kept as the rollback for `kafka`; this REST edge to a sibling context is deliberately out of scope of ADR-0042. |

The same-bin DOT hazard-class segregation check (ADR-0010) needs no lookup —
it reads only this service's own stock and classification repositories.

### Synchronous callers

Other contexts call this service's REST API directly: `order-management`
reserves/revokes stock (`POST /reservations`, `DELETE /reservations/{id}`);
`network-fulfillment` reads `GET /inventory/{sku}/usable`;
`warehouse-ops-agent` reads `GET /reservations?demandRef=`, the reports REST
and the MCP tools. No sibling reads `GET /products/{sku}/classification` any
more: `order-management` (its ADR 0036), `wes-work-planning` (its ADR 0035) and
`fulfillment-execution` (its ADR 0039) each keep a local copy fed by
product-master's `ProductClassified`.

## Analytics (data product)

Alongside the OLTP service, Inventory & Storage owns an **analytical data
product** — the *Inventory Flow & Accuracy* report — built entirely from its own
domain events. It is a lightweight data mesh with no central data platform: a
dedicated analytics topic, a separate analytical database, and two extra
read-model processes. See [ADR-0011](docs/docs/adr/0011-analytical-data-product.md)
and the [report contract](docs/docs/analytics/inventory-flow-accuracy-report.md).

- **Analytics topic**: `warehouse.inventory.analytics` (separate from the
  integration topic; published by a NEW outbound adapter, fanned out alongside
  the integration publisher). Same CloudEvents
  envelope and `type` strings, with `dataschema`
  `urn:warehouse:inventory-storage:analytics:<EventName>:v1` (the old
  `schema_version` field is gone).
- **Analytical database**: its own `ANALYTICS_DATABASE_URL`, its own migrations
  (`migrations/analytics/`), and a read-only role for the reader.
- **Three processes, one writer**:
  - `cmd/inventory` — the OLTP binary (unchanged; additionally fans events onto
    the analytics topic through the outbox relay).
  - `cmd/inventory-projector` — the ONLY writer. Consumes the analytics topic
    (`StartOffset = FirstOffset`, so a fresh group replays history), applies
    idempotent projections, and runs the analytical migrations on start.
  - `cmd/inventory-reports` — a read-only reader serving the report over REST.
- **MCP**: when `REPORTS_BASE_URL` is set, `cmd/mcp` exposes the curated
  read-only tool `get_inventory_flow_accuracy_report`, which reads through the
  reports REST rather than the analytical database directly.

Run the analytics side locally (requires Postgres, Kafka and the analytical
database; `docker compose up -d --wait postgres kafka` provides the first two):

```sh
# 1. OLTP service, fanning events onto both topics through the outbox relay
#    (DATABASE_URL, KAFKA_BROKERS, LOCATION_LOOKUP_MODE and the four *_CONSUMER_GROUP
#    variables exactly as in "Run it" above)
go run ./cmd/inventory

# 2. Projector (the only writer of the analytical DB): ANALYTICS_DATABASE_URL and
#    KAFKA_BROKERS are both mandatory
export ANALYTICS_DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory_analytics?sslmode=disable'
export KAFKA_BROKERS=localhost:19092
go run ./cmd/inventory-projector       # admin: /healthz and /readyz on :8091

# 3. Reports reader (read-only)
export ANALYTICS_DATABASE_URL='postgres://inventory_ro:***@localhost:5432/inventory_analytics?sslmode=disable'
go run ./cmd/inventory-reports         # REST on :8092

# 4. Query the report
curl 'http://localhost:8092/reports/flow-accuracy?from=2026-01-01T00:00:00Z&to=2027-01-01T00:00:00Z'
curl 'http://localhost:8092/reports/flow-accuracy/freshness'
```

## Observability

Traces and metrics are exported over **OTLP/gRPC** to an OpenTelemetry
Collector; logs stay on stdout as JSON and carry the ids that tie them back to
a trace. There is no `/metrics` endpoint — Prometheus exposition is the
Collector's job, not this service's.

### Environment variables

| Variable | Default | What it does |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | Collector's OTLP/gRPC receiver. Accepts a bare `host:port` (plaintext) or a full URL (`https://…` for TLS). |
| `OTEL_SERVICE_NAME` | `inventory-storage` | `service.name` resource attribute, and the span/metric scope name. |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute. |
| `ENVIRONMENT` | `local` | `deployment.environment.name` resource attribute. |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error`, case-insensitive. Also gates the OTel SDK's own diagnostics, which are bridged onto the same JSON logger. |

A Collector is *expected* at `OTEL_EXPORTER_OTLP_ENDPOINT`, but is never
required: the exporters dial lazily and no blocking dial option is set, so a
Collector that is down or absent costs telemetry and nothing else. Startup,
request latency and exit code are all unaffected — a failed final flush is
logged at `WARN`, not returned. In the `warehouse-infra` kind cluster the
endpoint points at the in-cluster Collector Service
(`otel-collector.observability.svc.cluster.local:4317`, set by the Helm chart's
`otel` values block).

### What gets exported

**Traces** — one server span per HTTP request, named after the *chi route
pattern* rather than the raw path (`/reservations/{id}`, not one span name per
reservation id), with these as children:

- every Postgres query, prepare, batch, copy and pool acquire, via `otelpgx`.
  Statements are normalized, so bound arguments — SKUs, bin codes, demand
  references — never leave the process as span attributes;
- `kafka.publish warehouse.inventory.events` for each integration event, with
  the W3C `traceparent` injected into the message headers. A consumer that
  extracts from those headers joins *this* trace, which is what makes the
  reserve → release path visible end to end across services.

**Metrics**

- `http.server.request.duration` (histogram, seconds) — OTel HTTP semantic
  conventions, from `otelchi`;
- `inventory.reservations` (counter) — the business signal, with an
  `outcome` attribute of `created` or `revoked`. It is recorded in the
  `ReserveStock` / `RevokeReservation` use cases, not the HTTP handler, so it
  counts reservations that were actually bound and durably saved rather than
  requests that merely arrived;
- Go runtime metrics (goroutines, GC, memory) via
  `contrib/instrumentation/runtime`.

**Logs** stay `log/slog` JSON on stdout. Any record written with a
span-carrying context gains `trace_id` and `span_id`, so a log line pivots
straight to its trace:

```json
{"time":"2026-08-23T20:09:13.9-03:00","level":"INFO","msg":"http request","method":"POST","path":"/reservations","status":201,"duration_ms":52,"trace_id":"0a225d107cf073c4f1f4ea2cadeb2941","span_id":"818a21eae51ab9de"}
```

### Trying it locally

```sh
# a Collector that just prints what it receives
cat > /tmp/otelcol.yaml <<'YAML'
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    traces:  {receivers: [otlp], exporters: [debug]}
    metrics: {receivers: [otlp], exporters: [debug]}
YAML
docker run --rm -p 4317:4317 -v /tmp/otelcol.yaml:/etc/otelcol-contrib/config.yaml \
  otel/opentelemetry-collector-contrib:latest

# in another shell
go run ./cmd/inventory        # with the environment from "Run it"; then drive the curl walkthrough above
```

## Local development / quality gate

Every CI sensor is also a `make` target, so the same feedback is available
locally, before you commit. `make help` lists them all.

```sh
make check        # fast pre-commit loop: fmt-check, vet, build, lint, test (-race)
make check-all    # before pushing: check + coverage gate (90%), arch-test, bdd
make vuln         # govulncheck ./... — known CVEs in deps and the Go stdlib
make mutation     # fast gremlins subset (blocks in CI); mutation-full = exhaustive
make integration  # needs Docker: Postgres and Kafka tests boot their own containers (testcontainers)
```

Git hooks are managed with [lefthook](https://github.com/evilmartians/lefthook)
and configured in [`lefthook.yml`](lefthook.yml) — `pre-commit` runs
`make fmt-check vet lint`, `pre-push` runs `make check`. Hooks live in
`.git/hooks/`, which is not tracked, so activate them once per clone:

```sh
brew install lefthook     # or: go install github.com/evilmartians/lefthook@latest
lefthook install
```

`make lint` expects `golangci-lint` on your PATH at the version CI pins:

```sh
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1
go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
go install golang.org/x/vuln/cmd/govulncheck@v1.1.4
```

## Tests

```sh
go build ./...
go vet ./...
go test ./...
go test -race ./...

# Integration tests (build-tagged; needs Docker only): every suite boots its
# own infrastructure via testcontainers — Postgres (one container per package,
# migrations applied once into a template DB, each test clones it) and/or a
# Kafka broker (internal/testsupport/kafkatc). Never an external
# DATABASE_URL/KAFKA_BROKERS, never t.Skip — the same command CI's
# `integration` job runs.
go test -tags=integration ./... -race -count=1
```

Each of the four named invariants has a dedicated failing-path test:

| Invariant | Test |
|-----------|------|
| Bin-capacity rejection | `TestBin_Occupy_ExceedsCapacity_Rejected` (domain), `TestStowStock_ExceedsBinCapacity_Rejected` (use case) |
| Stow requires item + location | `TestNewStockUnit_RequiresSKU` / `_RequiresBin` (domain) |
| Reservation <= usable | `TestStockUnit_Reserve_ExceedsUsable_Rejected` (domain), `TestReserveStock_ExceedsUsable_Rejected` (use case) |
| Revoke returns to usable | `TestStockUnit_ReleaseReservation_ReturnsToUsable` (domain), `TestRevokeReservation_ReturnsQuantityToUsable` (use case) |

### Integration suites (testcontainers)

The `-tags=integration` suites (`make integration`) prove the use cases and
adapters against real infrastructure each suite boots itself. The shape is
shared: a `TestMain` starts ONE container set per package (containers are slow
to boot — Postgres, and for the Kafka suites a broker via
`internal/testsupport/kafkatc`), applies the migrations ONCE into a template
database, and each test then gets its own database cloned from that template
(`CREATE DATABASE ... TEMPLATE`, a file-level copy); per-test isolation for
Kafka comes from a unique timestamped topic plus unique SKUs/bins, never from
separate infrastructure. Highlights, all on real Postgres/Kafka:

- `internal/application/usecases/transfer_allocation_integration_test.go` —
  the ADR 0030 command/reply contract: a duplicate command yields ONE
  reservation and one decrement; other-site stock untouched; insufficient
  stock yields exactly one rejection; a forced outbox failure rolls
  everything back.
- `internal/application/usecases/inbound_receipt_handover_integration_test.go`
  — ADR 0037 end to end: an inbound-receiving `ReceiptLineReceived` on Kafka
  becomes a `StockReceived` analytics outbox row in the same transaction as
  the `processed_events` claim; damaged/unknown/redelivered messages leave
  nothing behind.
- `internal/adapters/inbound/mcp/streamable_http_integration_test.go` — the
  MCP server on the real Streamable HTTP transport (SDK client against
  `httptest`), with real Postgres-backed use cases behind it: initialize,
  tools/list, tools/call for every tool; domain rejections come back as TOOL
  errors, not transport errors.
- `internal/adapters/inbound/kafka/analytics_consumer_integration_test.go` —
  the analytics projector against a real broker and real analytical Postgres:
  analytics CloudEvents in, the Inventory Flow & Accuracy read model out
  (asserted through the same reader `cmd/inventory-reports` serves).
- `internal/application/usecases/transfer_race_and_error_branches_test.go`
  (unit, no container) — the scripted-race companion to the transfer suite:
  in-transaction replay/conflict re-checks, unique-constraint race recovery,
  and error propagation from every port the happy-path tests never fail.

## BDD / Acceptance tests

Executable specifications written in Gherkin and run with
[godog](https://github.com/cucumber/godog), the official Cucumber
implementation for Go.

The feature files live under [`features/`](features/) — one per
aggregate/bounded concept, using the ubiquitous language from `CLAUDE.md`
(StockUnit, Bin, Stow, Usable inventory, Reservation, Cycle count):

| Feature file | Covers |
|--------------|--------|
| `features/stow.feature` | `POST /stock-receipts`, `POST /stowages` — chaotic stow, bin-capacity rejection |
| `features/reservation.feature` | `POST /reservations`, `DELETE /reservations/{id}`, `POST /reservations/{id}/pick-confirmations` — reserve against usable, revoke, confirm pick |
| `features/cycle_count.feature` | `POST /bins/{binId}/cycle-counts` — clean count vs. discrepancy/Unlocated |
| `features/bin_registration.feature` | `PUT /bins/{binId}`, `GET /bins/{binId}` — create / no-op / resize / below-occupancy rejection, and pick location (`binId`) on reservation allocations |
| `features/usable_inventory.feature` | `GET /inventory/{sku}/usable` — on-hand minus active reservations |

The step definitions live in [`features_test.go`](features_test.go) at the repo
root. They are true black-box acceptance tests: the real chi router is wired to
the in-memory outbound adapters (unit/BDD test doubles — the production
binaries never use them, ADR-0042), served over `httptest.NewServer`, and driven
with plain `net/http` requests — no use case is called directly. Every scenario
gets a fresh server and fresh state via a godog `Before` hook.

Run them locally:

```sh
go test ./... -run TestFeatures -v
```

They also run as the `bdd` job in CI.

## Helm chart

`charts/inventory-storage` deploys the OLTP service with optional components behind
values flags: the analytics projector and reports reader (`analytics.enabled`), the read-only MCP server (`mcp.enabled`, ClusterIP on `:8090`) and the operator micro-frontend (`frontend.enabled`). Mandatory values are the Postgres secret and the Kafka
broker list; the deployed consumer-group ids and mode flags come from
`warehouse-infra`'s helm-values.

The chart is linted in CI (`helm-lint`, `chart-tests`) and its invariants are
asserted by the tests in `charts/inventory-storage/tests/` — `render_test.sh`,
`test_service_selectors.py` (every Service selects exactly one Deployment) and
`test_kong_config.py`.
## Operator micro-frontend (`web/`)

`web/` is `inventory_mfe`, this context's Module Federation remote. It talks only to
this service's own REST API and is never part of `make check`.

**Screens** (mounted by the console at `/inventory/*`; no console change is needed, the
`/inventory` remote is already registered):

| Route | Screen |
|---|---|
| `/inventory` | usable inventory by SKU, reservations by demand ref |
| `/inventory/containers?lpn=<lpn>` | **Container lookup** (ADR 0045): scan or type a carton SSCC / LPN and read `GET /containers/{lpn}` as a card: state badge (SEALED / OPEN / EMPTY), SKU, case GTIN, units per pack, quantity in eaches, bin, site, owner / seller (`Fleet-owned` when `ownerRef` is null or omitted), lot, expiry and a link to the receipt line in Inbound receiving |

The container screen is read-only: breaking and reserving cases belong to the floor client.
Failures are keyed by problem code with what / what-to-do copy (`container-not-found` for a
404; an LPN outside `^[A-Za-z0-9._-]{1,64}$` is rejected before any request; 500 and network
failures have their own copy). The service keeps no break history read model yet: a broken case
shows as `EMPTY` with no bin, and the `CaseBroken` event is the only record of the break.

Web checks, all run by the CI `web` job: `npm run lint`, `npm run typecheck`, `npm test`
(Vitest + Testing Library on jsdom), `npm run build`.

**Standalone development** is unchanged:

```bash
cd web && npm install && npm run dev     # http://localhost:5182
```

**Deployed to the kind cluster**, it is built into a static bundle and served
by its own `nginx-unprivileged` pod:

```bash
cd web
docker build --build-context uikit=../../warehouse-ui-kit \
  -t warehouse/inventory-storage-frontend:local .
```

The cluster's localhost topology separates the two kinds of traffic onto two
independent entrypoints, and neither proxies to the other:

| URL | Served by | Carries |
|---|---|---|
| `http://localhost/mfes/inventory-storage/` | Nginx web gateway → this remote's nginx pod | HTML, JS, CSS, fonts, `remoteEntry.js` |
| `http://localhost:8000/api/inventory-storage/` | Kong | this service's REST API |

Kong never serves frontend assets, and the Nginx gateway never proxies an API.
Enable the workload with `frontend.enabled=true` in the Helm chart; the Service
is deliberately `ClusterIP` with no Ingress/HTTPRoute, because frontend path
routing belongs to the Nginx web gateway in `warehouse-infra`.

Because one image must work in more than one environment, the remote reads its
API origin at runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin` (published
by the console shell) rather than baking a hostname in at build time. A
production build with no runtime config **fails loudly** instead of silently
falling back to a developer port; standalone `npm run dev` still uses
`http://localhost:8082`. See `web/src/config.ts`.

Chart invariants are asserted by:

```bash
python3 charts/inventory-storage/tests/test_service_selectors.py
```

which proves every Service selects exactly one Deployment — the OLTP Service
must never select the frontend, analytics or MCP pods.

## License

MIT — see [LICENSE](LICENSE).
