# Inventory & Storage

> **⚠️ Study project.** This repository is an educational exercise in
> Domain-Driven Design applied to warehouse management/execution systems. It
> follows real industry-standard patterns and terminology (WMS/WES/WCS,
> chaotic storage, CloudEvents, RFC 7807, hexagonal architecture) but is
> **not a production system** and is **not affiliated with, endorsed by, or
> representative of any real-world company**.

The WMS-tier authoritative record of **what is held where, and what portion
is usable**. Implements e-commerce-retailer-style **chaotic (random) stow**: no fixed
product location — an item goes to any free bin, and the system records the
exact bin. Supplies "stock reality" to Work Planning and makes allocation a
**revocable reservation** so a failed physical delivery never strands an
order.

## Documentation

Full documentation site: **https://claudioed.github.io/inventory-storage/**

Business context and domain vision, the DDD model (subdomain classification,
aggregates and invariants, domain events, use cases), an API reference
generated from `apis/openapi.yaml` plus a hand-authored Events page from
`apis/asyncapi.yaml`, the ecosystem context map, and the Architecture Decision
Records. Source lives in [`docs/`](docs/) (Docusaurus); it is built and
deployed to GitHub Pages by [`.github/workflows/docs.yml`](.github/workflows/docs.yml).

## Layering (hexagonal / ports & adapters)

Strict dependency rule: **domain depends on nothing; application depends on
domain; adapters depend on application/domain.**

```
cmd/inventory/               composition root (main.go)
cmd/inventory-projector/     analytics WRITER: consumes the analytics topic, projects
cmd/inventory-reports/       analytics READER: read-only report REST
cmd/mcp/                      MCP inbound adapter (Streamable HTTP)
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
    outbound/postgres/       pgxpool repos + golang-migrate migrations
    outbound/memory/         thread-safe in-memory repos (tests, local dev)
    outbound/events/         log publisher + buffered + multi (fan-out) publisher
    outbound/kafka/          Kafka integration + analytics publishers (see below)
    outbound/analyticsstore/ analytical Postgres projection + read-only reader
    outbound/facilitycache/  Kafka-fed local cache of facility-layout zone/slot data (ADR-0013)
    outbound/facilitylayout/ sync HTTP + permissive location-classification lookups (fallbacks)
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

### Option A — in-memory (no database)

```sh
go run ./cmd/inventory
```

Without `DATABASE_URL` set, the app wires the in-memory adapters and logs
events to stdout. Listens on `:8080` (override with `HTTP_ADDR`).

### Option B — Postgres

```sh
docker compose up -d postgres
export DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable'
go run ./cmd/inventory
```

Migrations in `migrations/` run automatically on startup (via
`MIGRATIONS_PATH`, default `migrations`).

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

## Endpoints

| Method | Path | Use case |
|--------|------|----------|
| POST   | `/stock/receive` | ReceiveStock — requires `Idempotency-Key` (ADR-0018) |
| POST   | `/stock/stow` | StowStock |
| POST   | `/reservations` | ReserveStock — requires `Idempotency-Key` (ADR-0018) |
| GET    | `/reservations?demandRef=` | GetReservationsByDemandRef |
| DELETE | `/reservations/{id}` | RevokeReservation |
| POST   | `/reservations/{id}/confirm-pick` | ConfirmPick |
| GET    | `/inventory/{sku}/usable` | GetUsable |
| PUT    | `/bins/{binId}` | RegisterBin — idempotent: 201 created / 200 unchanged or resized / 409 below occupancy (ADR-0025) |
| GET    | `/bins/{binId}` | GetBin — capacity, occupied, available |
| POST   | `/bins/{binId}/cycle-count` | RunCycleCount |
| PUT    | `/products/{sku}/classification` | ClassifyProduct |
| GET    | `/products/{sku}/classification` | current ProductClassification |
| GET    | `/healthz` | liveness |

`POST /stock/receive` and `POST /reservations` are the two true
resource-creation endpoints (server-generated id, no caller-supplied
identity), so they are route-scoped behind a transactional
`Idempotency-Key` HTTP middleware (see
[ADR-0018](docs/docs/adr/0018-idempotency-key-middleware.md)): a missing
header is a 400, a reused key with a different request body is a 422, and
a retried identical request returns the exact same cached response
without re-executing the handler — a client-side retry after a dropped
response can never double-create a resource. The middleware only applies
when the service is Postgres-backed (`DATABASE_URL` set); in-memory dev
mode leaves these two routes unprotected, matching this repo's existing
"nil = no transactional backing" convention.

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

curl -s -X POST localhost:8080/stock/receive \
  -H 'Idempotency-Key: 8b1a...' \
  -d '{"sku":"SKU-1","quantity":10}'
# => 202 Accepted (a staged receipt has no addressable resource yet)

curl -s -i -X POST localhost:8080/stock/stow \
  -d '{"sku":"SKU-1","quantity":10,"binId":"A-1-1"}'
# => 201 Created, Location: /stock/<stock-unit-id>

curl -s -i -X POST localhost:8080/reservations \
  -H 'Idempotency-Key: 8b1a...' \
  -d '{"sku":"SKU-1","quantity":6,"demandRef":"order-42"}'
# => 201 Created, Location: /reservations/<id>, body {"id":"res-...", ...,
#    "allocations":[{"stockUnitId":"su-...","binId":"A-1-1","quantity":6}]}
#    Every allocation names its pick location (binId) — ADR-0025.
# A retry with the SAME Idempotency-Key + body returns this exact response
# again without creating a second reservation (ADR-0018); omitting the
# header entirely on these two routes is a 400.

curl -s localhost:8080/inventory/SKU-1/usable

curl -s -X POST localhost:8080/reservations/<id>/confirm-pick

curl -s -X DELETE localhost:8080/reservations/<id>

curl -s -X POST localhost:8080/bins/A-1-1/cycle-count \
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
their own read models from inventory reality. It also consumes one topic,
`warehouse.facility.events`, to keep a local read model of facility-layout's
zone classifications (see [Consumed](#consumed-facility-layouts-location-classifications)
below).

- **Topic**: `warehouse.inventory.events`
- **Publisher selection**: `EVENT_PUBLISHER` env var — `log` (default:
  stdout logging with in-memory adapters, or, with `DATABASE_URL` set, the
  same stdout logging — the transactional outbox only activates under
  `kafka`, see below) or `kafka`. With `DATABASE_URL` set AND
  `EVENT_PUBLISHER=kafka`, publishing goes through a transactional outbox
  (ADR 0017): each use case's aggregate save(s) and its domain event(s)
  commit together in one Postgres transaction as `outbox_events` rows, and
  a background relay (`OUTBOX_RELAY_INTERVAL`, default `1s`) drains them
  onto Kafka. Without `DATABASE_URL`, `EVENT_PUBLISHER=kafka` publishes
  directly (no outbox, no transactional guarantee) — the in-memory repos
  have nothing to commit atomically with.
- **Broker**: `KAFKA_BROKERS` env var, comma-separated, default
  `localhost:9092`. There is one broker platform-wide: the in-cluster Kafka
  deployed by `warehouse-infra`, whose external listener is reachable from
  the host at `localhost:9092`.
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
  (`ReservationRevoked`'s domain event only carries the reservation id; the
  Kafka adapter looks the reservation back up via `ReservationRepo` to fill in
  `sku`/`quantity`/`demand_ref`.) Every other domain event (`StockReceived`,
  `ItemStowed`, ...) is not part of this integration contract and is not
  forwarded to `warehouse.inventory.events`. (Those events DO feed the separate
  analytics data product on `warehouse.inventory.analytics` — see
  [Analytics](#analytics-data-product) below.)

Run it against the real broker:

```sh
export EVENT_PUBLISHER=kafka
export KAFKA_BROKERS=localhost:9092
export DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable'
go run ./cmd/inventory

# in another shell, tail the topic on the in-cluster broker:
kubectl --context kind-warehouse -n warehouse-systems exec -it kafka-controller-0 -c kafka -- \
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic warehouse.inventory.events --from-beginning

# then drive a reservation + revoke through the API (see curl walkthrough above)
```

### Consumed: facility-layout's location classifications

`StowStock` enforces hazmat-zone and temperature-class placement for SKUs
classified `Hazmat` or `TemperatureSensitive` (ADR-0009), which needs the
target bin's zone attributes. Where they come from is selected by
`LOCATION_LOOKUP_MODE`:

| Mode | Behaviour |
| --- | --- |
| `permissive` (default) | No lookup; every location reports `Known=false`, so placement rules never block a stow. |
| `kafka` | `internal/adapters/outbound/facilitycache` replays `warehouse.facility.events` (`ZoneRegistered`, `LocationSlotRegistered`, `LocationSlotDecommissioned`) from the earliest offset into an in-memory cache, under a per-process consumer group, and startup blocks until that replay completes (60s timeout). Requires `KAFKA_BROKERS`. This is what the `warehouse-infra` cluster runs (ADR-0013). |
| `http` | Synchronous `GET /locations/{code}/classification` on facility-layout per stow, via `FACILITY_LAYOUT_BASE_URL`. Kept as the rollback for `kafka`. |

The same-bin DOT hazard-class segregation check (ADR-0010) needs no lookup —
it reads only this service's own stock and classification repositories.

### Synchronous callers

Other contexts call this service's REST API directly: `order-management`
reserves/revokes stock (`POST /reservations`, `DELETE /reservations/{id}`)
and, with `wes-work-planning` and `fulfillment-execution`, reads
`GET /products/{sku}/classification`; `warehouse-ops-agent` reads
`GET /reservations?demandRef=`, the reports REST and the MCP tools. Each
caller gates the edge behind its own `*_MODE` env var.

## Analytics (data product)

Alongside the OLTP service, Inventory & Storage owns an **analytical data
product** — the *Inventory Flow & Accuracy* report — built entirely from its own
domain events. It is a lightweight data mesh with no central data platform: a
dedicated analytics topic, a separate analytical database, and two extra
read-model processes. See [ADR-0011](docs/docs/adr/0011-analytical-data-product.md)
and the [report contract](docs/docs/analytics/inventory-flow-accuracy-report.md).

- **Analytics topic**: `warehouse.inventory.analytics` (separate from the
  integration topic; published by a NEW outbound adapter, fanned out alongside
  the integration publisher when `EVENT_PUBLISHER=kafka`). Same CloudEvents
  envelope and `type` strings, with `dataschema`
  `urn:warehouse:inventory-storage:analytics:<EventName>:v1` (the old
  `schema_version` field is gone).
- **Analytical database**: its own `ANALYTICS_DATABASE_URL`, its own migrations
  (`migrations/analytics/`), and a read-only role for the reader.
- **Three processes, one writer**:
  - `cmd/inventory` — the OLTP binary (unchanged; additionally fans events onto
    the analytics topic under `EVENT_PUBLISHER=kafka`).
  - `cmd/inventory-projector` — the ONLY writer. Consumes the analytics topic
    (`StartOffset = FirstOffset`, so a fresh group replays history), applies
    idempotent projections, and runs the analytical migrations on start.
  - `cmd/inventory-reports` — a read-only reader serving the report over REST.
- **MCP**: when `REPORTS_BASE_URL` is set, `cmd/mcp` exposes the curated
  read-only tool `get_inventory_flow_accuracy_report`, which reads through the
  reports REST rather than the analytical database directly.

Run the analytics side locally (requires Kafka and the analytical database):

```sh
# 1. OLTP service, fanning events onto both topics
export EVENT_PUBLISHER=kafka
export KAFKA_BROKERS=localhost:9092
export DATABASE_URL='postgres://inventory:***@localhost:5432/inventory?sslmode=disable'
go run ./cmd/inventory

# 2. Projector (the only writer of the analytical DB)
export ANALYTICS_DATABASE_URL='postgres://inventory:***@localhost:5432/inventory_analytics?sslmode=disable'
export KAFKA_BROKERS=localhost:9092
go run ./cmd/inventory-projector       # admin/health on :8091

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
go run ./cmd/inventory        # then drive the curl walkthrough above
```

## Local development / quality gate

Every CI sensor is also a `make` target, so the same feedback is available
locally, before you commit. `make help` lists them all.

```sh
make check        # fast pre-commit loop: fmt-check, vet, build, lint, test (-race)
make check-all    # before pushing: check + coverage gate (90%), arch-test, bdd
make vuln         # govulncheck ./... — known CVEs in deps and the Go stdlib
make mutation     # fast gremlins subset (blocks in CI); mutation-full = exhaustive
make integration  # Postgres tests need DATABASE_URL; Kafka tests need Docker (testcontainers)
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

# Postgres integration test (build-tagged, skipped without DATABASE_URL)
docker compose up -d postgres
DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable' \
  go test -tags integration ./internal/adapters/outbound/postgres/...

# Kafka integration test for the facility-layout cache: starts its own
# broker via testcontainers, so it needs Docker but no external Kafka
go test -tags integration ./internal/adapters/outbound/facilitycache/...
```

Each of the four named invariants has a dedicated failing-path test:

| Invariant | Test |
|-----------|------|
| Bin-capacity rejection | `TestBin_Occupy_ExceedsCapacity_Rejected` (domain), `TestStowStock_ExceedsBinCapacity_Rejected` (use case) |
| Stow requires item + location | `TestNewStockUnit_RequiresSKU` / `_RequiresBin` (domain) |
| Reservation <= usable | `TestStockUnit_Reserve_ExceedsUsable_Rejected` (domain), `TestReserveStock_ExceedsUsable_Rejected` (use case) |
| Revoke returns to usable | `TestStockUnit_ReleaseReservation_ReturnsToUsable` (domain), `TestRevokeReservation_ReturnsQuantityToUsable` (use case) |

## BDD / Acceptance tests

Executable specifications written in Gherkin and run with
[godog](https://github.com/cucumber/godog), the official Cucumber
implementation for Go.

The feature files live under [`features/`](features/) — one per
aggregate/bounded concept, using the ubiquitous language from `CLAUDE.md`
(StockUnit, Bin, Stow, Usable inventory, Reservation, Cycle count):

| Feature file | Covers |
|--------------|--------|
| `features/stow.feature` | `POST /stock/receive`, `POST /stock/stow` — chaotic stow, bin-capacity rejection |
| `features/reservation.feature` | `POST /reservations`, `DELETE /reservations/{id}`, `POST /reservations/{id}/confirm-pick` — reserve against usable, revoke, confirm pick |
| `features/cycle_count.feature` | `POST /bins/{binId}/cycle-count` — clean count vs. discrepancy/Unlocated |
| `features/bin_registration.feature` | `PUT /bins/{binId}`, `GET /bins/{binId}` — create / no-op / resize / below-occupancy rejection, and pick location (`binId`) on reservation allocations |
| `features/usable_inventory.feature` | `GET /inventory/{sku}/usable` — on-hand minus active reservations |

The step definitions live in [`features_test.go`](features_test.go) at the repo
root. They are true black-box acceptance tests: the real chi router is wired to
the in-memory outbound adapters, served over `httptest.NewServer`, and driven
with plain `net/http` requests — no use case is called directly. Every scenario
gets a fresh server and fresh state via a godog `Before` hook.

Run them locally:

```sh
go test ./... -run TestFeatures -v
```

They also run as the `bdd` job in CI.

## Operator micro-frontend (`web/`)

`web/` is `inventory_mfe`, this context's Module Federation remote. It talks only to
this service's own REST API and is never part of `make check`.

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
