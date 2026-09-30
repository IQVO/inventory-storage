---
id: 0019-optimistic-concurrency-version-column
slug: /adr/0019-optimistic-concurrency-version-column
title: 0019. Optimistic concurrency (version column) for StockUnit, Bin, Reservation
sidebar_label: 0019. Optimistic concurrency
sidebar_position: 19
description: A version column and version-guarded UPDATE close a confirmed lost-update bug where two concurrent Saves of the same aggregate silently clobbered each other; a stale write now fails fast with 409 Conflict instead of corrupting state.
---

# 0019. Optimistic concurrency (version column) for StockUnit, Bin, Reservation

## Status

Accepted.

## Context

`internal/adapters/outbound/postgres/stock_repo.go`'s `Save` did a blind

```sql
INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state)
VALUES (...)
ON CONFLICT (id) DO UPDATE SET
  quantity = EXCLUDED.quantity,
  reserved = EXCLUDED.reserved,
  state    = EXCLUDED.state;
```

with no `WHERE` clause and nothing checking whether the row had changed
since it was read. `location_repo.go` (`Bin`) and `reservation_repo.go`
(`Reservation`) had the identical shape: a full-column
`ON CONFLICT (id) DO UPDATE SET <every column> = EXCLUDED.<...>`, unconditional.

This is a real lost-update bug, not a theoretical one. Two concurrent
requests that each `FindByID`/`FindBySKU` the SAME aggregate, mutate a
private in-memory copy, then `Save`, race with no error to either side:

- Two `ReserveStock` calls against the same `StockUnit` — the second
  `Save` overwrites the first's `reserved` delta entirely rather than
  adding to it.
- A `ReserveStock` racing a `StowStock` against the same `StockUnit` —
  same clobber, in either direction depending on write order.
- Two racing `Bin.Occupy`/`Release` calls, or two racing reservation
  state transitions (e.g. `Revoke` racing `ConfirmPick` on the same
  `Reservation`) — same shape, same silent loss.

The caller that "lost" gets a `200`/`204` as if its write succeeded; the
persisted state reflects only the other write. There is no signal
anywhere that this happened.

This complements, and does **not** conflict with, the separately
in-flight idempotency-key middleware work (`feature/idempotency-key-middleware`,
migration `0006_idempotency_keys`): that work stops a **duplicate
create** on client retry of the same logical request; this ADR stops a
**lost update** when two *different* requests concurrently modify the
same *existing* row. Idempotency keys do nothing to protect a
read-modify-write race between two distinct requests — this needed its
own mechanism. The two migrations were renumbered to avoid a version
collision (this ADR is `0007_optimistic_concurrency_version`, since
`0006` was already claimed by the idempotency-key branch at the time
this was written) and touch disjoint files: idempotency touches inbound
HTTP + a new table; this touches domain aggregates + the three existing
repos' `Save` methods + a different new migration.

## Decision

Add a `version` column to each mutable aggregate's table
(`stock_units`, `bins`, `reservations`) and guard every `Save` on it.

### Domain: an unexported, unsettable `version`

Each aggregate (`StockUnit`, `Bin`, `Reservation`) gains:

```go
type StockUnit struct {
    // ...existing fields...
    version int
}

func (u *StockUnit) Version() int { return u.version }
```

`version` is infrastructure metadata the domain carries but never
reasons about — no business method reads or branches on it, and there
is deliberately no setter. The repo is the only thing that ever advances
it (via `Save`'s `RETURNING version`).

- **Rehydrate-style constructors** (`RehydrateStockUnit`, `RehydrateBin`,
  `reservation.Rehydrate`), called by a repo's `FindByID`/`FindBySKU`/etc.
  after scanning a row, take the loaded version as a new trailing
  parameter.
- **Fresh-aggregate constructors** (`NewStockUnit`, `NewBin`,
  `reservation.New`) implicitly start at version 1 — chosen (over 0) so
  that "version" and "row currently exists with N successful writes
  behind it" stay intuitively aligned: a freshly created, never-updated
  row is version 1, matching its `version INTEGER NOT NULL DEFAULT 1`
  column default.

### Migration `0007_optimistic_concurrency_version`

```sql
ALTER TABLE stock_units  ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE bins         ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE reservations ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
```

with the matching `.down.sql` dropping all three columns. Every
pre-existing row backfills to version 1 for free via `DEFAULT`, so no
data migration step is needed and no existing row is treated as
"already conflicted."

### Repo `Save`: version-guarded single-statement UPSERT

We investigated whether a single `INSERT ... ON CONFLICT DO UPDATE ...
WHERE <table>.version = $loaded_version` statement correctly reports
`RowsAffected = 0` when the `WHERE` fails to match, rather than
insert-or-no-op ambiguity — **tested directly against a real Postgres**
(both via `psql`'s row-count line and a small pgx probe reading
`pgconn.CommandTag.RowsAffected()`), not assumed:

```sql
INSERT INTO stock_units (id, sku, bin_id, quantity, reserved, state, version)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (id) DO UPDATE SET
  quantity = EXCLUDED.quantity,
  reserved = EXCLUDED.reserved,
  state    = EXCLUDED.state,
  version  = stock_units.version + 1
WHERE stock_units.version = $8
RETURNING version;
```

Confirmed: a matching `WHERE` reports 1 row affected and returns the
new version; a stale `$loaded_version` reports 0 rows affected and
leaves the row completely untouched (no `RETURNING` row, no partial
write). This holds for the genuine `ON CONFLICT` path (the row already
exists) — the plain `INSERT` path (first-ever save of a brand-new
aggregate) has no conflict to guard and always succeeds. Since the
single-statement form works cleanly, we use it everywhere rather than
an explicit `SELECT ... FOR UPDATE` + branch pair, keeping the change
mechanical and avoiding a second round trip per write.

On `RowsAffected() == 0` for a row that is confirmed to already exist
(distinguished from "insert never matched a conflict," which can't
happen here since `id` is always supplied), `Save` returns a new
sentinel:

```go
// internal/application/usecases/errors.go
var ErrConcurrentModification = errors.New("resource was modified by another request; re-fetch and retry")
```

alongside this package's other sentinel errors (`ErrInsufficientUsable`,
`ErrHazmatClassIncompatible`, etc.) — the existing convention for
domain/application-level errors that the HTTP layer maps by
`errors.Is`.

### Use cases: zero logic changes

Every use case that reads-then-saves one of these three aggregates
(`reserve_stock.go`, `stow_stock.go`, `confirm_pick.go`,
`revoke_reservation.go`, `run_cycle_count.go`, `reservation_expiry.go`)
needed **no code changes**. The version flows through transparently:
`FindBySKU`/`FindByID` populates it via `Rehydrate*`, the use case
mutates the aggregate exactly as before (it never touches `version`),
and `Save` reads `u.Version()` off the aggregate it was handed. This was
verified for real, not assumed: an end-to-end test runs two goroutines
through the real `ReserveStock.Execute` against a real (testcontainers)
Postgres, racing to reserve against the same `StockUnit` — exactly one
succeeds, the other surfaces `ErrConcurrentModification` unchanged from
the repo layer.

### HTTP: `ErrConcurrentModification` → `409 Conflict`

Per ADR 0005's RFC 7807 convention, both `statusFor` and `problemFor` in
`internal/adapters/inbound/http/errors.go` gained a case:

```json
{
  "type": "https://errors.inventory-storage.warehouse-systems.dev/concurrent-modification",
  "title": "The resource was modified by another request; re-fetch the latest version and retry",
  "status": 409,
  "detail": "resource was modified by another request; re-fetch and retry",
  "instance": "/reservations"
}
```

Documented in `apis/openapi.yaml` as an additional `409` example on
every write endpoint that can hit this path (`POST /stock/stow`,
`POST /reservations`, `DELETE /reservations/{id}`,
`POST /reservations/{id}/confirm-pick`), verified with
`spectral lint openapi.yaml --ruleset .spectral.yaml --fail-severity=warn`.

**API contract for callers:** on a `409` with this `type`, re-fetch the
resource (or its dependent read model) and retry the operation from
scratch — the previous in-memory view is stale and must not be reused.

## Alternatives considered

- **Pessimistic locking (`SELECT ... FOR UPDATE`)**: rejected as the
  default — it would hold a row lock for the duration of an entire use
  case (including, for `ReserveStock`, a second repo's write inside the
  same transaction), increasing contention and lock-wait latency under
  load for a conflict pattern that is expected to be rare. Optimistic
  concurrency fails fast and cheaply instead, at the cost of the caller
  needing to handle a `409` and retry.
- **Two-statement branch (`UPDATE ... WHERE version = $v` then check
  `RowsAffected`, separately from any `INSERT`)**: rejected once the
  single-statement `ON CONFLICT ... WHERE` form was confirmed to report
  `RowsAffected` correctly — it would have been strictly more code for
  no behavioral benefit.
- **Timestamp-based (`updated_at`) optimistic locking instead of an
  integer version**: rejected — clock skew and sub-millisecond
  concurrent writes make a timestamp a weaker uniqueness guarantee than
  a monotonically-incrementing integer guarded by the database itself.

## Consequences

### Easier

- The exact lost-update bug described above is closed for all three
  aggregates, with a clear, machine-readable signal (`409` +
  `concurrent-modification` type) instead of silent data loss.
- Every existing single-writer `Save` call site needed zero changes —
  the version threading is entirely inside `Rehydrate*`/`Save`, invisible
  to every use case.
- The full pre-existing unit and integration suites pass unmodified,
  confirming no observable behavior changed for the non-conflicting
  (overwhelmingly common) case.

### Harder

- Every write-path caller (today: `ReserveStock`, `StowStock`,
  `ConfirmPick`, `RevokeReservation`, `RunCycleCount`, lazy reservation
  expiry, and any future one) must be prepared to receive
  `ErrConcurrentModification`/`409` and either surface it to *its* own
  caller or retry with a fresh read — this ADR does not add automatic
  retry anywhere; a caller that ignores the `409` will simply drop its
  own change on the floor (a visible failure, at least, rather than a
  silent one).
- `outbox_events`/domain events published inside an `atomically()` scope
  that then hits `ErrConcurrentModification` on a *later* `Save` in the
  same use case (e.g. `ReserveStock`'s `Reservations.Save` after a
  successful `Stock.Save`) roll back the whole transaction per the
  existing `UnitOfWork` semantics (ADR 0017) — no event is ever
  published for a use case that ultimately failed, which is the
  behavior we want, but it means a conflict late in a multi-aggregate
  use case discards work that had already succeeded earlier in the same
  request.

## Verification performed

- `go build ./...`, `go vet ./...`, `gofmt -l .` (clean),
  `golangci-lint run ./...` (0 issues) via `make check`.
- `go build -tags=integration ./...`, `go vet -tags=integration ./...`
  (clean).
- `go test ./... -race -count=1` — every existing unit test green,
  unmodified, including `internal/architecture` (hexagonal fitness).
- `go test -tags=integration ./... -race -count=1` — every existing
  integration test green, unmodified.
- New tests, all against a real, throwaway `testcontainers-go/modules/postgres`
  container (never a fake, never `t.Skip`, never a hardcoded
  `localhost`):
  - `internal/adapters/outbound/postgres/optimistic_concurrency_integration_test.go`:
    per-aggregate (StockUnit, Bin, Reservation) stale-version-fails and
    current-version-succeeds-and-increments; one real two-goroutine
    concurrent `Save` race on a single `StockUnit` — exactly one
    succeeds, the other gets `ErrConcurrentModification`, verified
    stable across 5 repeated `-race -count=5` runs.
  - `internal/application/usecases/optimistic_concurrency_integration_test.go`:
    the same two-goroutine race through the real `ReserveStock.Execute`
    end to end, verified stable across 5 repeated runs.
  - `internal/adapters/inbound/http/errors_test.go`: pins the `409`
    status and dedicated `concurrent-modification` problem slug/title.
- `spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn`
  — 0 results.
- `make arch-test` — hexagonal fitness green.
- `make coverage`: see PR description for the before/after numbers on
  this branch.
