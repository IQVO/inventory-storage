---
id: 0023-migrations-direct-postgres-connection
slug: /adr/0023-migrations-direct-postgres-connection
title: "23. Run golang-migrate against a direct Postgres connection, not PgBouncer"
sidebar_label: "23. Migrations bypass PgBouncer"
sidebar_position: 23
description: "ADR 0023 — Phase 4 fleet-wide finding: golang-migrate's postgres driver takes a session-scoped pg_advisory_lock to serialize concurrent migration runs, which is incompatible with PgBouncer's transaction-pooling mode (warehouse-infra PR #43, already load-bearing for this service per ADR-0022). Two or more inventory-storage/mcp replicas starting concurrently (HPA scale-out, ADR-0022, or an ordinary rolling deploy) would crash-loop until one won the advisory-lock race. Fix: a second env var, MIGRATIONS_DATABASE_URL, carries a direct (non-pooled) connection string used ONLY for the migration step; DATABASE_URL/the runtime pgxpool is untouched and keeps going through PgBouncer. Ports order-management's reference implementation, ADR-0029, to this service."
---

# 23. Run golang-migrate against a direct Postgres connection, not PgBouncer

## Status

Accepted — implemented in the same change that introduces this record.
This is the inventory-storage fan-out of a fleet-wide bug found during
Phase 4 (k6/HPA load-test validation) cleanup, whose reference
implementation is order-management
[PR #115](https://github.com/claudioed/order-management/pull/115) and its
[ADR-0029](https://github.com/claudioed/order-management/blob/develop/docs/docs/adr/0029-migrations-direct-postgres-connection.md).
`warehouse-infra` [PR #44](https://github.com/claudioed/warehouse-infra/pull/44)
already provisions the `MIGRATIONS_DATABASE_URL` secret key server-side for
all 9 OLTP services (this one included), so no further `warehouse-infra`
work is needed for this fan-out — this PR is purely the Go code + chart
side for `inventory-storage`.

## Context

`warehouse-infra`'s PgBouncer rollout
([PR #43](https://github.com/claudioed/warehouse-infra/pull/43), Phase 3)
repointed every one of the fleet's 9 OLTP services' `DATABASE_URL` secret
at PgBouncer, in **transaction-pooling** mode (`pool_mode = "transaction"`).
This service's own ADR-0022 (per-workload HPA + pgxpool tuning) explicitly
calls out that it "builds directly on `warehouse-infra` PR #43" and is
sized against PgBouncer's transaction-pooling front end — so this service's
`DATABASE_URL` has been PgBouncer-fronted since Phase 3, and ADR-0022's
autoscaling is exactly the kind of event that triggers the bug below.

What PR #43 did not carve out: **migrations**. Both `cmd/inventory` and
`cmd/mcp` run golang-migrate's postgres driver
(`github.com/golang-migrate/migrate/v4/database/postgres`) against the same
`DATABASE_URL` at process startup (`buildAdapters` in each), before serving
any traffic. golang-migrate's postgres driver calls
`SELECT pg_advisory_lock($1)` to serialize concurrent migration runs — by
design: if two processes start at once and both try to run the same
migration, whichever loses the lock should block, not race.

`pg_advisory_lock` is **session-scoped**: the lock is held by whichever
physical backend connection issued it, and is expected to be released by
that same connection (or the session ending). PgBouncer's
transaction-pooling mode does not preserve that mapping — each statement in
a client's logical session can be routed to a different physical backend
connection, because the client's backend connection is returned to the
pool the instant its transaction commits. So:

- Pod A dials PgBouncer, gets backend connection #1, takes the advisory
  lock, runs migrations.
- Pod B dials PgBouncer *concurrently*, gets a **different** backend
  connection, and PgBouncer may freely reuse/rotate backend connections for
  either pod's subsequent statements mid-"session" from the application's
  point of view.
- The advisory lock never behaves as a real mutex across the two pods.
  Whichever pod's statements land on a backend connection with unexpected
  transaction/prepared-statement state gets errors like `pq: unnamed
  prepared statement does not exist` or `pq: canceling statement due to
  statement timeout`, and crash-loops for roughly 1-2 minutes until the
  race resolves.

This is a **latent, fleet-wide, production-blocking bug**, not a load-test
artifact: it fires on any ordinary rolling ArgoCD deploy with more than 1
replica of `inventory` or `mcp`, and on every ADR-0022 HPA scale-out event
for the `api` workload. order-management independently reproduced this
live before shipping its reference fix (PR #115); this repo's fix ports
that same pattern rather than re-deriving or re-verifying the root cause.

## Decision

Give this service a **second** connection string,
`MIGRATIONS_DATABASE_URL` — a direct (non-pooled, session-mode) Postgres
connection string, same user/password/dbname as `DATABASE_URL`, pointed at
Postgres itself rather than PgBouncer — used **only** for the golang-migrate
startup step. `DATABASE_URL` and the pgxpool built from it are completely
unchanged: every request `cmd/inventory` and `cmd/mcp` serve still goes
through PgBouncer in transaction-pooling mode, exactly as PR #43 and
ADR-0022 already assume.

This is architecturally identical to order-management's ADR-0029 and to
universal Postgres/PgBouncer operational guidance: **migrations need a
direct/session connection; steady-state application traffic goes through
the pooler.** We are not weakening or changing PgBouncer's `pool_mode`
(still `transaction`, still correct for this fleet's traffic, still what
ADR-0022's pgxpool sizing assumes) — this fix is entirely about routing one
specific, short-lived, startup-only operation around the pooler.

`warehouse-infra` PR #44 already provisions `MIGRATIONS_DATABASE_URL` as a
new key alongside the existing `DATABASE_URL` key in this service's
`inventory-storage-db` Secret (and the other 8 OLTP services' equivalents)
— no further `warehouse-infra` work is required for this PR.
`cmd/inventory/main.go` and `cmd/mcp/main.go` (both run migrations) now
read `MIGRATIONS_DATABASE_URL` for the migration step:

```go
migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
...
buildAdapters(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, ...)
...
postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)  // migrations only
// the pgxpool opened right after this still uses databaseURL, unchanged
```

The fallback to `databaseURL` when `MIGRATIONS_DATABASE_URL` is unset keeps
every environment that doesn't provision the split — local dev, CI
integration tests, or any cluster whose Terraform predates this fix —
working exactly as before, byte-for-byte. Nothing about local dev or CI
changes as a result of this PR.

`charts/inventory-storage`: a new `database.migrationsExistingSecretKey`
value (default `"MIGRATIONS_DATABASE_URL"`) renders a
`MIGRATIONS_DATABASE_URL` env var, sourced from the same
`existingSecret`, in both the `api` (`deployment.yaml`) and `mcp`
(`mcp-deployment.yaml`) Deployments — `optional: true` on the
`secretKeyRef` so a secret that predates this key still starts the pod.
Without this env wiring, the secret key existing server-side does nothing:
Kubernetes only injects env vars a Deployment's pod spec explicitly asks
for.

`cmd/inventory-projector` and `cmd/inventory-reports` are **out of
scope**: they read `ANALYTICS_DATABASE_URL`, a separate analytical
Postgres database that PR #43 deliberately left un-pooled (the same
carve-out this ADR's fix mirrors for migrations), so they were never
affected by this bug.

### Why not just make PgBouncer's pool_mode session for this fleet?

Rejected, for the same reason order-management's ADR-0029 rejected it:
session pooling would fix the advisory-lock problem but throws away the
entire point of PgBouncer for this fleet — transaction pooling is what
lets many short-lived HTTP-request-scoped OLTP connections share a small
number of physical Postgres backends. Switching to session mode
fleet-wide to accommodate a ~1-2 second startup-time lock call is the tail
wagging the dog, and would also undercut this service's own ADR-0022
pgxpool sizing, which assumes transaction pooling.

### Why not just remove the advisory lock / skip migrations on non-leader replicas?

Rejected, for the same reason order-management's ADR-0029 rejected it:
golang-migrate's advisory lock is exactly the right mechanism *given a
session-scoped connection* — the bug is the mismatch between that
mechanism and the pooling mode we run migrations through, not the
mechanism itself. An init-container Job that runs migrations exactly once
before any replica starts was considered but rejected: it's a bigger
architectural change (a new Kubernetes resource type per binary,
coordination with each Deployment's rollout strategy, one for `api` and
one for `mcp`) for the same outcome this two-line env-var fallback already
achieves, and it would still need a direct-vs-pooled connection decision
for the Job itself.

## Consequences

- **Fixes** the crash-loop bug for `inventory-storage`'s two migrating
  binaries (`cmd/inventory`, `cmd/mcp`), following the pattern
  order-management's PR #115 already proved live.
- **No runtime behavior change**: `DATABASE_URL` is untouched, so
  request-serving connection pooling, `pool_mode`, and PgBouncer's own
  configuration are all unaffected by this PR.
- **No behavior change for environments without the split**: the
  `getenv("MIGRATIONS_DATABASE_URL", databaseURL)` fallback means local
  dev and CI integration tests keep using `DATABASE_URL` for everything,
  exactly as before.
- **Makes ADR-0022's HPA safe to enable for the `api` workload**: an HPA
  scale-out is exactly the "2+ replicas start concurrently" trigger for
  this bug; this fix removes that blocker for this service, mirroring
  order-management's Phase-3-HPA unblock.
- One more secret key to keep in sync per service going forward, already
  handled by `warehouse-infra` PR #44's fleet-wide Terraform change — no
  new per-service manual step for this service.

## Verification

Ported directly from order-management's PR #115 / ADR-0029, which
reproduced the crash-loop live and then verified the fix live (forced
concurrent replica starts against a real PgBouncer-fronted cluster; 0
restarts, no `pq:` errors, across two repeated runs — see that ADR's
Verification section for the full transcript). This PR's own quality gate
(`make check`, `make check-all`, `make integration`, `helm lint` — see
below) is the verification performed for `inventory-storage` specifically;
a live cluster re-verification identical to order-management's was judged
unnecessary given the code and chart changes are a mechanical, tested port
of an already-live-verified pattern onto the same underlying bug.
