---
slug: /api-reference
title: API Reference
sidebar_label: Overview
description: REST conventions, the full endpoint matrix, and the RFC 7807 error catalog.
---

# API Reference

This service exposes two contracts, both kept as source-of-truth artefacts in
the repository and both linted by Spectral in CI:

| Contract | File | Rendered here |
| --- | --- | --- |
| REST (synchronous) | `apis/openapi.yaml` — OpenAPI 3.0.3 | **[REST API](./rest/inventory-storage-api.info.mdx)** — generated directly from the spec |
| Events (asynchronous) | `apis/asyncapi.yaml` — AsyncAPI 2.6.0 | **[Events](./events.md)** |

The REST pages under **REST API** are *generated* from `apis/openapi.yaml` by
`docusaurus-plugin-openapi-docs` at build time. They are not hand-transcribed,
so they cannot drift from the spec the service ships.

## Endpoint matrix

All 13 API routes registered in `internal/adapters/inbound/http/server.go`'s
`NewRouter` are documented — **13 / 13** (the `/readyz` readiness probe is
infrastructure, not part of the spec). (The separate `cmd/inventory-reports`
binary serves the analytics report routes — see
[Inventory Flow & Accuracy Report](/docs/analytics/inventory-flow-accuracy-report);
they are not part of `apis/openapi.yaml`.)

| Method | Path | Operation | Tag | Success | Errors |
| --- | --- | --- | --- | --- | --- |
| `GET` | `/healthz` | `getHealthz` | Health | `200` | — |
| `POST` | `/stock/receive` | `receiveStock` | Stock | `202` | `400` `422` `500` |
| `POST` | `/stock/stow` | `stowStock` | Stock | `201` | `400` `404` `409` `422` `500` |
| `POST` | `/reservations` | `reserveStock` | Reservations | `201` | `400` `409` `422` `500` |
| `GET` | `/reservations?demandRef=` | `getReservationsByDemandRef` | Reservations | `200` | `400` `500` |
| `DELETE` | `/reservations/{id}` | `revokeReservation` | Reservations | `204` | `404` `409` `500` |
| `POST` | `/reservations/{id}/confirm-pick` | `confirmPick` | Reservations | `204` | `404` `409` `500` |
| `GET` | `/inventory/{sku}/usable` | `getUsableInventory` | Inventory | `200` | `400` `500` |
| `PUT` | `/bins/{binId}` | `registerBin` | Bins | `200` / `201` | `400` `409` `422` `500` |
| `GET` | `/bins/{binId}` | `getBin` | Bins | `200` | `404` `500` |
| `POST` | `/bins/{binId}/cycle-count` | `runCycleCount` | Bins | `200` | `400` `404` `422` `500` |
| `PUT` | `/products/{sku}/classification` | `classifyProduct` | Products | `200` / `201` | `400` `500` |
| `GET` | `/products/{sku}/classification` | `getProductClassification` | Products | `200` | `404` `500` |

## Status-code conventions

The status codes are deliberate, not defaults. The reasoning was recorded
during the REST hardening pass:

| Code | Used for | Example |
| --- | --- | --- |
| `200 OK` | A read, or a command whose result *is* the response body | `GET /inventory/{sku}/usable`; `POST /bins/{binId}/cycle-count` returns the reconciliation result |
| `201 Created` | A new addressable resource, with a `Location` header | `POST /stock/stow` → `Location: /stock/{id}`; `POST /reservations` → `Location: /reservations/{id}` |
| `202 Accepted` | Accepted, but no addressable resource exists yet | `POST /stock/receive` — a staged receipt has no id and no `GET` route; the `StockUnit` is created later, at stow |
| `204 No Content` | A state transition with nothing useful to return | revoke, confirm-pick |
| `400 Bad Request` | Malformed or missing input | empty SKU, empty bin id, unparseable JSON |
| `404 Not Found` | The addressed resource does not exist | unknown reservation, unknown bin |
| `409 Conflict` | Well-formed and addressable, but conflicts with current state | bin full, reservation exceeds usable, reservation already resolved, reservation expired |
| `422 Unprocessable Entity` | Well-formed but semantically invalid *values* | quantity ≤ 0, negative quantity, non-positive bin capacity |

The `400` / `422` split is the one worth internalising: `400` means "I could
not understand the request," `422` means "I understood it perfectly and it is
not a legal thing to ask for."

## Errors: RFC 7807 Problem Details

Every error response uses `application/problem+json`:

```json
{
  "type": "https://errors.inventory-storage.warehouse-systems.dev/insufficient-usable",
  "title": "Requested quantity exceeds usable inventory",
  "status": 409,
  "detail": "requested quantity exceeds usable inventory",
  "instance": "/reservations"
}
```

- `type` is a stable, unique URI per error **category**. It is an identifier —
  it does not have to resolve to a page.
- `title` is a fixed human string for the category; safe to switch on for
  display, though `type` is the machine-readable key.
- `detail` is the dynamic message from the underlying typed error.
- `instance` is the request path.

### Problem-type catalog

Mapping is one-for-one with the typed domain and application errors, in
`internal/adapters/inbound/http/errors.go`.

| `type` slug | Status | Raised by |
| --- | --- | --- |
| `stock-unit-not-found` | 404 | `usecases.ErrStockUnitNotFound` |
| `bin-not-found` | 404 | `usecases.ErrBinNotFound` |
| `reservation-not-found` | 404 | `usecases.ErrReservationNotFound` |
| `product-classification-not-found` | 404 | `usecases.ErrProductClassificationNotFound` |
| `empty-sku` | 400 | `shared.ErrEmptySKU` |
| `empty-bin-id` | 400 | `shared.ErrEmptyBinID` |
| `stow-requires-item-and-location` | 400 | `stock.ErrStowRequiresItemAndLocation` |
| `unknown-handling-tag` | 400 | `product.ErrUnknownHandlingTag` |
| `unknown-temperature-class` | 400 | `product.ErrUnknownTemperatureClass` |
| `no-handling-tags` | 400 | `product.ErrNoHandlingTags` |
| `temperature-class-required` | 400 | `product.ErrTemperatureClassRequired` |
| `temperature-class-not-applicable` | 400 | `product.ErrTemperatureClassNotApplicable` |
| `duplicate-handling-tag` | 400 | `product.ErrDuplicateHandlingTag` |
| `invalid-dot-hazard-class` | 400 | `product.ErrInvalidDOTHazardClass` |
| `dot-hazard-class-not-applicable` | 400 | `product.ErrDOTHazardClassNotApplicable` |
| `negative-quantity` | 422 | `shared.ErrNegativeQuantity` |
| `zero-quantity` | 422 | `shared.ErrZeroQuantity` |
| `invalid-bin-capacity` | 422 | `location.ErrInvalidCapacity` |
| `bin-full` | 409 | `location.ErrBinFull` |
| `capacity-below-occupancy` | 409 | `location.ErrCapacityBelowOccupancy` (`PUT /bins/{binId}`, ADR 0025) |
| `release-exceeds-occupancy` | 409 | `location.ErrReleaseExceedsOccupancy` |
| `insufficient-usable` | 409 | `usecases.ErrInsufficientUsable`, `stock.ErrInsufficientUsable` |
| `insufficient-reserved` | 409 | `stock.ErrInsufficientReserved` |
| `unit-unlocated` | 409 | `stock.ErrUnitUnlocated` |
| `reservation-already-resolved` | 409 | `reservation.ErrAlreadyResolved` |
| `reservation-expired` | 409 | `reservation.ErrExpired` |
| `reservation-no-allocations` | 409 | `reservation.ErrNoAllocations` |
| `hazmat-zone-required` | 409 | `usecases.ErrHazmatZoneRequired` |
| `temperature-class-mismatch` | 409 | `usecases.ErrTemperatureClassMismatch` |
| `location-classification-unavailable` | 409 | `usecases.ErrLocationClassificationUnavailable` |
| `hazmat-class-incompatible` | 409 | `usecases.ErrHazmatClassIncompatible` |
| `missing-demand-ref` | 400 | written directly by the `GET /reservations` and `POST /reservations` handlers (empty `demandRef`) |
| `capacity-required` | 400 | written directly by the `PUT /bins/{binId}` handler when `capacity` is omitted |
| `capacity-out-of-range` | 422 | written directly by the `PUT /bins/{binId}` handler when `capacity` exceeds int32 |
| `malformed-request-body` | 400 | written directly when a request body is not valid JSON |
| `internal-error` | 500 | anything unmapped |

The domain never knows about any of this. It returns typed errors; the inbound
adapter is the only layer that translates them. See
[ADR 0005](/docs/adr/0005-rfc-7807-problem-details).

## DTOs never leak domain types

Request and response bodies are adapter-local structs in
`internal/adapters/inbound/http/dto.go`. A `stockUnitResponse` is not a
`stock.StockUnit`; a `reservationResponse` is not a `reservation.Reservation`.
That indirection is what lets the domain model evolve without breaking the wire
contract, and it is enforced by the arch-go fitness tests.

## Authentication

None. No route — REST or MCP — is authenticated at the application layer,
and `apis/openapi.yaml` declares no security schemes. A static-bearer-key
layer was adopted in [ADR 0014](/docs/adr/0014-rest-identity-adoption) and
removed again by
[ADR 0015](/docs/adr/0015-remove-rest-identity-layer). In the local cluster the
API is reached through Kong at `http://localhost:8000/api/inventory-storage`,
which does not add authentication either.
