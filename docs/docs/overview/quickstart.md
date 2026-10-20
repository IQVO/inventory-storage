---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
description: Run the service locally and walk every endpoint with curl.
---

# Quickstart

## Prerequisites

| Tool | Why |
| --- | --- |
| Go (version from `go.mod`) | build and run the four binaries |
| Docker | `docker compose up -d --wait postgres kafka` (the local runtime) and the testcontainers integration suite |
| `make` | the local quality gate (`make help`) |
| golangci-lint v2.13.1, gremlins v0.6.0, govulncheck, `schemathesis==4.28.0` | optional: `make lint`, `make mutation`, `make vuln`, `make contract` (the contract run also needs `DATABASE_URL` and `KAFKA_BROKERS`, see `scripts/contract-test.sh`) |
| The warehouse-infra kind cluster | optional: it owns `localhost:9092` (the platform's single broker), which is why this repo's `docker-compose.yml` publishes its own Kafka on `localhost:19092` instead |

## Build and check

```bash
make build        # go build ./...
make check        # fmt-check vet build lint test  (what the pre-push hook runs)
make check-all    # check + coverage gate + arch-test + bdd
```

## Run it

Postgres and Kafka are the only runtime
([ADR 0042](../adr/0042-postgres-and-kafka-are-the-only-runtime.md)): there is
no in-memory mode and no log-only publisher, and every binary exits at boot
naming the first missing variable. The repository's `docker-compose.yml`
provides both services for local runs.

### Start the API

```bash
# Postgres on :5432 and a disposable Kafka on :19092 (KAFKA_HOST_PORT overrides
# it; keep KAFKA_BROKERS in step)
docker compose up -d --wait postgres kafka

export DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable'
export KAFKA_BROKERS=localhost:19092
export LOCATION_LOOKUP_MODE=kafka
export TRANSFER_ALLOCATION_CONSUMER_GROUP=inventory-storage-transfer-allocation
export PRODUCT_MASTER_CONSUMER_GROUP=inventory-storage-product-master
export INBOUND_RECEIPT_CONSUMER_GROUP=inventory-storage-inbound-receipts
export TASK_COMPLETED_CONSUMER_GROUP=inventory-storage-confirm-pick
go run ./cmd/inventory
```

It listens on `:8080` (override with `HTTP_ADDR`). Migrations under
`migrations/` (golang-migrate) run automatically at startup; `MIGRATIONS_PATH`
defaults to `migrations`. The outbox relay and the four consumers start with
the process; with `LOCATION_LOOKUP_MODE=kafka` startup also waits for the
replay of `warehouse.facility.events` to finish (use `http` plus
`FACILITY_LAYOUT_BASE_URL` to call facility-layout instead). See
[Configuration](../operations/configuration.md) for every variable and
[Integration](/docs/ecosystem/integration) for what lands on the wire and what
is consumed.

:::note[The kind cluster owns localhost:9092]
The shared broker deployed by `warehouse-infra` is published on the host at
`localhost:9092`. This repository's compose Kafka therefore uses `19092`, so
both can run side by side. Point `KAFKA_BROKERS` at `localhost:9092` instead to
use the cluster's broker (and pick consumer-group ids that differ from the
deployed ones, so a local process never steals their partitions).
:::

:::note[Idempotency-Key]
`POST /stock/receive`, `POST /reservations`, `POST /transfers/{transferLineId}/receipt`,
`.../stow` and `POST /relocations` sit behind the transactional
`Idempotency-Key` middleware (ADR 0018) in every deployment: add
`-H 'Idempotency-Key: <any-unique-string>'` to those calls in the walk below,
or they answer `400 idempotency-key-required`.
:::

### The other three binaries

```bash
# MCP server on :8090 (needs DATABASE_URL only: it writes outbox rows that the
# cmd/inventory relay delivers, so it never dials Kafka)
go run ./cmd/mcp

# Analytics: needs a second Postgres database for the read model
export ANALYTICS_DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory_analytics?sslmode=disable'
KAFKA_BROKERS=localhost:19092 go run ./cmd/inventory-projector   # admin /healthz and /readyz on :8091
go run ./cmd/inventory-reports                                    # reports REST on :8092
curl -s 'localhost:8092/reports/flow-accuracy?from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z'
# {"rows":[...]}
```

The projector only sees events once `cmd/inventory` (whose relay publishes to
the analytics topic) runs against the same broker. Create the analytical
database first (`createdb`, or `CREATE DATABASE inventory_analytics;` in the
compose Postgres); the projector applies `migrations/analytics` itself.

### Through the cluster gateway

With warehouse-infra running, the deployed API is behind Kong at
`http://localhost:8000/api/inventory-storage/` (the prefix is stripped), e.g.
`curl http://localhost:8000/api/inventory-storage/healthz`.

## Seeding

There is no seed script: the walk below *is* the seed. Register bins with
`PUT /bins/{binId}`, then receive and stow stock. Product classifications
cannot be written here any more (`PUT /products/{sku}/classification` answers
`410 classification-moved`); they arrive from product-master through the
always-on `PRODUCT_MASTER_CONSUMER_GROUP` consumer, so a SKU product-master has
not classified yet is stowed without hazmat/temperature checks.

## Walk the API

:::note[Register a bin first]
`StowStock` returns `404 bin-not-found` for an unknown bin, so register one
before stowing. `PUT /bins/{binId}` is idempotent and declarative (ADR 0025):
`201` when it creates the bin, `200` when it already exists (unchanged or
resized), `409 capacity-below-occupancy` if you ask for less than it holds.
:::

```bash
# Liveness and readiness
curl -s localhost:8080/healthz
# {"status":"ok"}
curl -s localhost:8080/readyz
# {"status":"ready"}   (503 {"status":"not_ready"} once shutdown has begun,
#                        503 dependency-unavailable while Postgres or Kafka is down)

# 0. Register the bin (inventory control).
curl -s -i -X PUT localhost:8080/bins/A-1-1 \
  -H 'Content-Type: application/json' \
  -d '{"capacity":20}'
# HTTP/1.1 201 Created
# Location: /bins/A-1-1
# {"binId":"A-1-1","capacity":20,"occupied":0,"available":20}

# 1. Receive: goods are in the building, not yet located.
curl -s -i -X POST localhost:8080/stock/receive \
  -H 'Content-Type: application/json' \
  -d '{"sku":"SKU-1","quantity":10}'
# HTTP/1.1 202 Accepted   (a staged receipt has no addressable resource yet)

# 2. Stow: item-scan + location-scan. This is what creates a StockUnit.
curl -s -i -X POST localhost:8080/stock/stow \
  -H 'Content-Type: application/json' \
  -d '{"sku":"SKU-1","quantity":10,"binId":"A-1-1"}'
# HTTP/1.1 201 Created
# Location: /stock/<stock-unit-id>

# 3. Usable inventory for the SKU.
curl -s localhost:8080/inventory/SKU-1/usable
# {"sku":"SKU-1","usable":10}

# 4. Reserve against usable (not against on-hand).
curl -s -i -X POST localhost:8080/reservations \
  -H 'Content-Type: application/json' \
  -d '{"sku":"SKU-1","quantity":6,"demandRef":"order-42"}'
# HTTP/1.1 201 Created
# Location: /reservations/<id>
# {"id":"...","sku":"SKU-1","quantity":6,"demandRef":"order-42",
#  "status":"ACTIVE","allocations":[{"stockUnitId":"...","binId":"A-1-1","quantity":6}],
#  ...   <- binId is the pick location: where the picker goes
#  "expiresAt":"..."}

curl -s localhost:8080/inventory/SKU-1/usable
# {"sku":"SKU-1","usable":4}   <- usable dropped, on-hand did not

# 5a. The pick succeeded — consume the reservation.
curl -s -X POST localhost:8080/reservations/<id>/confirm-pick

# 5b. …or the pick failed. Revoke; quantity returns to usable.
curl -s -X DELETE localhost:8080/reservations/<id>
curl -s localhost:8080/inventory/SKU-1/usable
# {"sku":"SKU-1","usable":10}

# 6. Cycle count a bin.
curl -s -X POST localhost:8080/bins/A-1-1/cycle-count \
  -H 'Content-Type: application/json' \
  -d '{"countedQuantity":9}'
# {"binId":"A-1-1","countedQuantity":9,"systemQuantity":10,"discrepancy":true}
```

## Errors

Every error response is RFC 7807 `application/problem+json`:

```bash
curl -s -i -X DELETE localhost:8080/reservations/does-not-exist
```

```http
HTTP/1.1 404 Not Found
Content-Type: application/problem+json

{
  "type": "https://errors.inventory-storage.warehouse-systems.dev/reservation-not-found",
  "title": "Reservation not found",
  "status": 404,
  "detail": "reservation not found",
  "instance": "/reservations/does-not-exist"
}
```

The full status-code and problem-type table is in the
[API Reference overview](/docs/api-reference). See
[ADR 0005](/docs/adr/0005-rfc-7807-problem-details) for why.

## Tests

See [Testing](../development/testing.md) for every layer and CI job.

```bash
go build ./...
go vet ./...
go test ./...
go test ./... -race

# Integration tests (build-tagged; needs Docker only — every suite boots its
# own Postgres and/or Kafka via testcontainers; never an external
# DATABASE_URL/KAFKA_BROKERS, never skipped)
go test -tags=integration ./... -race -count=1

# Gherkin acceptance suite
go test -run TestFeatures ./...
```
