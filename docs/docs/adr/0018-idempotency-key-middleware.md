---
id: 0018-idempotency-key-middleware
slug: /adr/0018-idempotency-key-middleware
title: 0018. Transactional Idempotency-Key middleware
sidebar_label: 0018. Idempotency-Key middleware
sidebar_position: 18
description: POST /stock/receive and POST /reservations now require an Idempotency-Key header, enforced by a route-scoped, transactional HTTP middleware that shares the request's Postgres transaction with the wrapped use case, so a client-side retry after a dropped response can never double-create a resource.
---

# 0018. Transactional Idempotency-Key middleware

## Status

Accepted.

## Context

`POST /stock/receive` (`ReceiveStock`) and `POST /reservations`
(`ReserveStock`) are this service's only two true resource-creation
endpoints: the caller supplies no id of its own, and a server-generated
identity is minted on success (a `StagedReceipt` acknowledgment for
receive; a `Reservation` id for reserve). Before this change, neither
endpoint protected against a client retrying the exact same logical
request after a dropped response (a timeout, a proxy reset, a client
crash mid-read): `ReceiveStock` would publish a second `StockReceived`
event, and `ReserveStock` would allocate and persist a second
`Reservation` against the same `demandRef`, double-reserving inventory.
`ReserveStock` already has a best-effort, non-transactional guard (an
active-reservation-by-`demandRef` lookup — see its own doc comment) that
narrows this window but does not close it: two concurrent first attempts
for the same `demandRef` can both pass that check before either has
saved, and it says nothing at all about `ReceiveStock`.

This fleet already has a proven, merged reference implementation for
exactly this problem: order-management's `POST /orders` (PR #105, ADR
0023), plus the `internal/pgtx` extraction pattern this repo's own
transactional-outbox rollout (ADR 0017) makes reusable with zero changes
to any use case.

## Decision

Add a route-scoped (chi's `r.With(...)`, never global) transactional
`Idempotency-Key` HTTP middleware, `RequireIdempotencyKey`, applied only
to `POST /stock/receive` and `POST /reservations`. A caller must supply
an `Idempotency-Key` header on these two routes; the middleware persists
the request's outcome — success OR a business-validation error — keyed by
that header value plus a hash of the request body, in the SAME Postgres
transaction the wrapped use case's own `UnitOfWork.Execute` call runs in.

### `internal/pgtx` extraction

`internal/adapters/outbound/postgres/unit_of_work.go` used to own an
unexported `txKey`/`withTx`/`txFrom` trio. The idempotency middleware
lives in the INBOUND http adapter and must begin the outer transaction
and bind it into the exact same context slot `UnitOfWork.Execute` reads
— but this repo's `internal/architecture` fitness tests forbid an inbound
adapter from importing an outbound adapter package. Fix: the tiny
key/`WithTx`/`TxFrom` pair now lives in its own dependency-free package,
`internal/pgtx`, imported by both `internal/adapters/inbound/http` and
`internal/adapters/outbound/postgres`. `unit_of_work.go`'s `withTx`/
`txFrom` are now thin aliases over `pgtx.WithTx`/`pgtx.TxFrom`. This
needed **zero changes to any use case**: `UnitOfWork.Execute`'s existing
"already in a tx? join it" branch (`if _, ok := txFrom(ctx); ok { return
fn(ctx) }`) works unmodified once fed a `pgtx`-bound context from any
source, not just its own `Execute` — verified by the integration tests
below, which prove the join actually happens rather than silently
opening a second, invisible transaction.

### `idempotency_keys` schema (migration `0006`)

```sql
CREATE TABLE idempotency_keys (
    key              TEXT PRIMARY KEY,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    status_code      INTEGER,
    response_body    BYTEA,
    response_headers JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys (created_at);
```

`status_code`/`response_body`/`response_headers` start NULL and are only
ever populated by the SAME transaction that inserted the row, via one
`UPDATE` immediately before that transaction commits. A reader can
therefore never observe a committed row with a NULL `status_code`: either
the inserting transaction never committed at all (rolled back — the
wrapped handler panicked, or the update/commit itself failed), or it
committed only after the outcome was fully populated. This removes the
need for any "in progress" marker, timeout, or 409-retry-later state that
naive two-phase idempotency-key designs need.

### Middleware flow

1. Missing `Idempotency-Key` header → 400 (`idempotency-key-required`).
2. Read the full body, `requestHash = hex(sha256(body))`, restore
   `r.Body` via `io.NopCloser` so the wrapped handler's own JSON decoding
   is unaffected.
3. Begin a pgx transaction, `INSERT ... ON CONFLICT (key) DO NOTHING`.
   - 1 row (genuinely new key): call the wrapped handler with the
     transaction bound into `ctx` via `pgtx.WithTx`, capture its
     response in an `httptest.Recorder`, then — in the SAME transaction —
     `UPDATE` the row with the outcome and commit, and only THEN copy the
     recorder's output to the real `ResponseWriter`. Every normal
     response is cached, including a business-logic 4xx: a retry with
     the same key+body must get a deterministic answer, not re-run
     validation.
   - 0 rows (a row already exists): Postgres' own unique-index lock
     serializes this against whichever transaction inserted the existing
     row, so by the time 0 rows is observed that original transaction is
     guaranteed to have already resolved (commit or rollback) — this
     call's own empty transaction rolls back, then a plain,
     non-transactional read of the existing row follows: hash mismatch →
     422 (`idempotency-key-reused`); hash match → replay the stored
     response verbatim, never calling the real handler.
   - A panic from the wrapped handler rolls back the transaction (no
     idempotency row, no domain write survives) and re-panics so the
     outer chi `Recoverer` still produces the normal 500 — a panic's
     outcome is never cached, so a retry after one re-attempts the real
     work.

### `Server.IdempotencyPool`

`inboundhttp.Server` gained an `IdempotencyPool *pgxpool.Pool` field.
`NewRouter` wraps `POST /stock/receive` and `POST /reservations` with
`r.With(RequireIdempotencyKey(s.IdempotencyPool))` only when
`IdempotencyPool != nil`; a nil pool (in-memory / no-`DATABASE_URL`
configuration) leaves both routes unprotected, exactly this codebase's
existing convention for every other optional Postgres-backed capability
(`UnitOfWork`, the outbox relay). `cmd/inventory/main.go`'s
`buildAdapters` now also returns the raw `*pgxpool.Pool` (previously only
handed out indirectly via `ports.UnitOfWork`, an interface with no way to
recover the concrete pool) so the composition root can wire it onto
`Server.IdempotencyPool`.

## Scope: which endpoints, and why not more

Only `POST /stock/receive` and `POST /reservations` — this service's
only two endpoints that mint a new server-generated resource identity on
every call. Deliberately NOT applied to:

- `POST /stock/stow` — also creates a `StockUnit`, but is driven by a
  physical item-scan + location-scan workflow where the caller does not
  retry blind the way an upstream HTTP client does; left out of v1 scope
  per the same judgment call order-management's ADR 0023 documents for
  its own out-of-scope endpoints. Revisit if a real double-stow incident
  surfaces.
- `PUT /products/{sku}/classification` — idempotent by HTTP semantics
  already (a PUT to the same URI with the same body is a no-op replay by
  definition).
- `DELETE /reservations/{id}`, `POST /reservations/{id}/confirm-pick` —
  act on a caller-supplied, already-existing id; a retry re-targets the
  same resource rather than risking a duplicate create.

## `atomically()`/`UnitOfWork` wiring check (required by this task's scope)

Both `ReceiveStock.Execute` and `ReserveStock.Execute` already wrap their
`Save`+`Publish` calls in `atomically(ctx, uc.UnitOfWork, func(ctx) error
{...})` — this was already in place from the transactional-outbox rollout
(ADR 0017) before this change; **no additional wiring was needed** for
either use case. This was verified by reading both use cases' current
source (not assumed), and is what makes the idempotency bookkeeping and
the aggregate write commit together as one atomic unit: the middleware
begins the transaction and binds it via `pgtx.WithTx`; `UnitOfWork.Execute`
sees the ctx already carries a transaction (via `pgtx.TxFrom`) and joins
it instead of opening a second one.

## Consequences

- Both protected endpoints now require callers to generate and send an
  `Idempotency-Key` header — a breaking contract change for any existing
  caller of `POST /stock/receive` / `POST /reservations` that does not
  yet send one (it will get a 400). No caller of this service exists
  outside this fleet today; this is an accepted, one-time contract
  tightening.
- `idempotency_keys` grows unboundedly today (nothing deletes old rows);
  a follow-up housekeeping job is left as a known gap, consistent with
  how ADR 0017 left the same kind of gap open for `outbox_events`, and
  how order-management's ADR 0023 left it open for its own table. The
  `created_at` index exists solely to make that future job's query cheap.
- The middleware needs a real `*pgxpool.Pool`, so it is unavailable in
  the in-memory (no `DATABASE_URL`) dev/test configuration — both routes
  are simply unprotected in that mode, matching every other optional
  Postgres-backed capability's nil convention in this repo.

## Alternatives considered

- **Polling / "in progress" state with a client-facing retry-after**:
  rejected — unnecessary given Postgres' own unique-index lock already
  serializes concurrent identical requests, and the no-null-status-code
  invariant above means a reader never has to wait for an in-flight
  outcome to resolve.
- **Application-level dedup only (the existing `ReserveStock`
  active-reservation-by-`demandRef` check)**: rejected as sufficient —
  it is a best-effort narrowing, not a hard guarantee (two concurrent
  first attempts can both pass it before either saves), and it does not
  exist at all for `ReceiveStock`.
- **A distinct, dedicated `idempotency_keys`-per-route table**: rejected
  — one shared table keyed by `(key)` with `method`/`path` columns is
  simpler and matches order-management's reference design; nothing
  currently needs per-route partitioning.

## Verification performed

- `go build ./...`, `go vet ./...`, `golangci-lint run ./...` (0 issues,
  via `make check`).
- `gofmt -l .` clean.
- `go build -tags=integration ./...`, `go vet -tags=integration ./...`.
- `make arch-test`: hexagonal fitness green, including the
  inbound/outbound dependency rule the `internal/pgtx` extraction exists
  to satisfy.
- `go test -tags=integration ./internal/adapters/inbound/http/... -run TestIdempotency -race -count=1`
  against a throwaway `testcontainers-go/modules/postgres` container, 12
  scenarios (6 per protected route — fresh key creates the resource;
  replay with identical key+body returns the byte-identical cached
  response with no duplicate row / no re-execution of the handler; same
  key + different body → 422; missing header → 400; N real goroutines
  racing the same key+body → exactly one resource row created; a
  business-validation error response is itself cached and replayed
  verbatim on retry) — all green.
- `go test -tags=integration ./... -race -count=1`: full suite green,
  including the pre-existing outbox/repo integration tests (confirms the
  `internal/pgtx` extraction did not regress the outbox rollout's own
  transaction-join behavior).
- `make coverage`: 97.9% on `./internal/domain/...,./internal/application/...`
  (gate 90%), up from 96.2% on `origin/develop` before this change.
