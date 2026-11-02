---
id: 0049-adopt-the-fleet-rest-api-guideline
slug: /adr/0049
title: "ADR 0049: Adopt the fleet REST API guideline"
sidebar_label: "49. Adopt the fleet REST API guideline"
sidebar_position: 49
description: "ADR 0049: inventory-storage adopts the mandatory fleet REST API guideline in one hard cutover: noun routes instead of verbs and :custom-methods, an Idempotency-Key on EVERY POST with 409 on reuse and Idempotency-Replayed on replay, and keyset-paginated {items, nextCursor} collections, enforced by the rest-guideline Spectral ruleset."
---

# 0049. Adopt the fleet REST API guideline

## Status

Accepted (2026-11-01). Amends [ADR 0018](./0018-idempotency-key-middleware.md)
(the middleware now guards every `POST`, answers key reuse with `409`, and
sets `Idempotency-Replayed`). Part of the fleet-wide REST cutover: the
guideline is the *Warehouse REST API Guideline*, kept in warehouse-docs; this
ADR records how this service applies it.

## Context

The 2026-10-30 application portfolio assessment measured, across the fleet, two
spellings of the replay header, key reuse answered `422` in most services and
`409` in one, three command-URL styles (`:verb`, verb sub-paths, sub-resources),
unpaginated lists and writes that were not retry-safe. This service had all of
them: `POST /stock/receive`, `POST /stock/stow`, `POST /reservations/{id}/confirm-pick`,
`POST /bins/{binId}/cycle-count` and `POST /transfers/{transferLineId}/stow`
were verbs, `POST /containers/{lpn}:stow` and `:break` were custom methods,
three `POST`s (stow, confirm-pick, cycle-count) were not behind the idempotency
middleware at all, key reuse was `422`, and `GET /reservations` and
`GET /bins/{binId}/containers` answered a bare array and a `containers` object.

## Decision

A hard cutover: no aliases, no dual routes, no toggles. The old routes are
gone and answer 404/405 (`TestRetiredRoutesAreGone`).

### 1. Noun routes (guideline sections 1.1-1.5)

URLs name things; a command is a noun sub-resource. Status codes and bodies of
an operation do not change with its rename, and every `operationId` is
unchanged, so the MCP and A2A mappings do not move.

| Was | Now | operationId |
| --- | --- | --- |
| `POST /stock/receive` | `POST /stock-receipts` | `receiveStock` |
| `POST /stock/stow` | `POST /stowages` | `stowStock` |
| `POST /reservations/{id}/confirm-pick` | `POST /reservations/{id}/pick-confirmations` | `confirmPick` |
| `POST /bins/{binId}/cycle-count` | `POST /bins/{binId}/cycle-counts` | `runCycleCount` |
| `POST /transfers/{transferLineId}/stow` | `POST /transfers/{transferLineId}/stowages` | `stowTransferStock` |
| `POST /containers/{lpn}:stow` | `POST /containers/{lpn}/stowages` | `stowContainer` |
| `POST /containers/{lpn}:break` | `POST /containers/{lpn}/case-breaks` | `breakCase` |

### 2. Every `POST` is idempotent (sections 2.1-2.7)

- `Idempotency-Key` (1-128 printable ASCII characters, a fresh UUID per logical
  submit) is required on **every** `POST`. A missing key is
  `400 idempotency-key-required`, a malformed one `400 idempotency-key-invalid`.
- The fingerprint is **method + path + SHA-256(body)**. The same key with the
  same fingerprint replays the stored answer (status, body, `Content-Type`,
  `Location`) and adds `Idempotency-Replayed: true`; the first execution never
  carries it. The retired spelling `Idempotent-Replayed` appears nowhere.
- The same key with a **different** fingerprint is
  **`409` `idempotency-key-reused`** (it was `422`), told apart from a business
  `409` by its problem type.
- Deterministic answers (2xx and 4xx) are stored and replayed; a 5xx or a panic
  rolls back and is never stored, so the retry re-executes. Concurrent requests
  with one key are serialised by the unique index and the loser replays the
  winner.
- The middleware is route-scoped on every `POST` route, and the key row is
  written in the same Postgres transaction as the aggregate change and its
  outbox rows. **Newly keyed:** `POST /stowages`,
  `POST /reservations/{id}/pick-confirmations` and
  `POST /bins/{binId}/cycle-counts`. Their use cases (`StowStock`, `ConfirmPick`,
  `RunCycleCount`) already ran inside the `UnitOfWork`; under the middleware that
  unit of work joins the request transaction, so key row, stock/reservation
  change, bin-level facts and outbox rows still commit or roll back together.
- `apis/openapi.yaml`: every `POST` references `components/parameters/IdempotencyKey`,
  declares `400` and `409`, and documents the `Idempotency-Replayed` header on
  every 2xx; no `422` is described as the reuse answer.

### 3. Collections are keyset-paginated (section 3)

`GET /reservations?demandRef=` and `GET /bins/{binId}/containers` take `limit`
(1-500, default 100) and an opaque `cursor`, and answer
`{"items": [...], "nextCursor": "..."}` (`nextCursor` absent on the last page).
The cursor is the fleet's `page` package (base64url JSON of the last row's sort
key and id), copied into `internal/adapters/inbound/http/page`:

| List | Order (sort key, id) | Filter repeated on every page |
| --- | --- | --- |
| `GET /reservations` | `(created_at, id)` | `demandRef` |
| `GET /bins/{binId}/containers` | `(lpn, lpn)` (the LPN is unique) | `binId` (path) |

Both the Postgres and the in-memory repositories order by that key (never
`OFFSET`); a bad `limit` is `400 invalid-limit`, a cursor that does not decode
`400 invalid-cursor`. The bin-containers body no longer repeats `binId`.
**Exemptions: none.** Neither list is a composite view or a bounded report.

### 4. Enforcement

- `.spectral.yaml` and `.spectral/functions/restGuideline.js` (rules R1-R8) run
  in the `api-lint` CI job with `--fail-severity=warn`; `apis/openapi.yaml` passes.
- Tests with a real router and real Postgres (testcontainers): fresh key,
  byte-identical replay with `Idempotency-Replayed: true`, same key with another
  body or path `409`, missing key `400`, N concurrent same-key requests creating
  one row, a 4xx cached and replayed, a 5xx not stored, and for each list a page
  walk over more rows than `limit` (rows sharing a sort key included) with no
  duplicate and no skipped row.

## Consumers

Changed in this repository: the inventory micro-frontend
(`web/src/api.ts`, `web/src/useReservations.ts`, `InventoryScreen`) reads
`items` and follows `nextCursor` for the demandRef lookup (it makes no
`POST`/`PATCH`, so it sends no key); `apis/asyncapi.yaml`, `scripts/contract_hooks.py`
(Schemathesis now expects `409` for a reused key), the executive page, the
README and the docs. The MCP tools and A2A skills call the use cases directly,
not HTTP, and are unchanged.

Outside this repository, each in its own PR of the cutover set: order-management
(`POST /reservations`, `DELETE`), warehouse-ops-agent (lists reservations and must
read `items`), e2e-tests (seeds with `POST /stowages`, `/stock-receipts`),
network-fulfillment (reads `GET /inventory/{sku}/usable`, whose path is
unchanged), fulfillment-execution (`/containers/{lpn}/case-breaks`,
`/stowages`) and warehouse-infra helm values.

## Consequences

- Breaking by design: new URLs, a new list envelope, and the key is now
  required on three more routes. All PRs of the REST cutover merge as one set and
  are deployed together; `develop` is half-migrated until the last one lands.
- A client that reads only the first page of a list silently truncates data once
  the collection outgrows `limit`; every consumer must loop on `nextCursor`.
- `409` now has two meanings on a `POST`; clients distinguish them by the
  problem `type` (`idempotency-key-reused` is a client bug, never retried blindly).
- The deprecated `POST /stock-receipts` and `POST /reservations/{id}/pick-confirmations`
  keep their deprecation status; renaming them is not a reprieve.
