---
title: Quickstart
sidebar_label: Quickstart
description: Run the service locally and walk every endpoint with curl.
---

# Quickstart

## Run it

### Option A — in-memory, no database

```bash
go run ./cmd/inventory
```

With `DATABASE_URL` unset the composition root wires the in-memory adapters
and logs domain events to stdout. It listens on `:8080` (override with
`HTTP_ADDR`). This is the fastest way to poke at the API.

### Option B — Postgres

```bash
docker compose up -d postgres
export DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable'
go run ./cmd/inventory
```

Migrations under `migrations/` (golang-migrate) run automatically at startup;
`MIGRATIONS_PATH` defaults to `migrations`.

:::note[Idempotency-Key in Postgres mode]
With `DATABASE_URL` set, `POST /stock/receive` and `POST /reservations` sit
behind the transactional `Idempotency-Key` middleware (ADR 0018): add
`-H 'Idempotency-Key: <any-unique-string>'` to those two calls in the walk
below, or they answer `400 idempotency-key-required`. In-memory mode
(Option A) does not apply the middleware.
:::

### Option C — publishing integration events to Kafka

The shared broker lives outside this repository: it is the single in-cluster
Kafka deployed by `warehouse-infra`, whose external listener is published on
the host at `localhost:9092`. With that cluster running:

```bash
EVENT_PUBLISHER=kafka KAFKA_BROKERS=localhost:9092 go run ./cmd/inventory
```

Add `LOCATION_LOOKUP_MODE=kafka` to also build the facility-layout location
cache from `warehouse.facility.events` (startup then waits for the replay to
finish). See [Integration](/docs/ecosystem/integration) for what lands on the
wire and what is consumed.

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
# {"status":"ready"}   (503 {"status":"not_ready"} once shutdown has begun)

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

```bash
go build ./...
go vet ./...
go test ./...
go test ./... -race

# Postgres-backed integration tests (skipped without DATABASE_URL)
DATABASE_URL='postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable' \
  go test -tags=integration ./internal/adapters/outbound/postgres/...

# Kafka-backed integration test (starts its own broker via testcontainers; needs Docker)
go test -tags=integration ./internal/adapters/outbound/facilitycache/...

# Gherkin acceptance suite
go test -run TestFeatures ./...
```
