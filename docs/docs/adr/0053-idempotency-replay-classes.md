---
id: 0053-idempotency-replay-classes
slug: /adr/0053-idempotency-replay-classes
title: 0053. Every problem type has a replay class; a retryable refusal is not stored
sidebar_label: 0053. Idempotency replay classes
sidebar_position: 53
description: "ADR 0053: a retry under the same Idempotency-Key is answered from the store only when the first answer was final for that request. Every problem type declares FINAL, OUTCOME or RETRYABLE on its catalogue entry; a RETRYABLE refusal rolls the request back, key row included, so the retry executes. Supersedes the 'every 4xx is stored' sentence of ADR 0018 and ADR 0049."
---

# 0053. Every problem type has a replay class; a retryable refusal is not stored

## Status

Accepted (2026-11-19). Adopts the fleet decision of 2026-11-18
(`.claude/rules/fleet/idempotency-and-outbox.md`; standard and reference test in warehouse-harness-template
`docs/idempotency-replay-classes.md`). It supersedes only the sentences of
[ADR 0018](./0018-idempotency-key-middleware.md) and [ADR 0049](./0049-adopt-the-fleet-rest-api-guideline.md) that
say every 4xx answer is stored and replayed; their Decision text is untouched and carries a dated pointer here.

## Context

The middleware stored every non-5xx answer under the key for 24 hours. A refusal that a retry could pass was therefore
pinned: a `409 concurrent-modification` replayed to a client that did the right thing and retried, a `404 bin-not-found`
replayed after the bin was registered, a `409 receipt-not-staged` replayed after the stage. Classifying by status cannot
fix it (`409` is both `concurrent-modification` and `reservation-already-resolved`), and "store only 2xx" would lose the
refusals that committed state.

Phase-0 facts of this service (read from the code on 2026-11-19):

- **Store.** `idempotency_keys` (migration 0006), one row per key, global primary key, columns `method`, `path`,
  `request_hash`, `status_code`, `response_body`, `response_headers`. `RequireIdempotencyKey(pool)`
  (`internal/adapters/inbound/http/idempotency.go`) opens a pgx transaction, inserts the bare row
  (`ON CONFLICT (key) DO NOTHING`), binds the transaction into the context (`internal/pgtx`) so the use cases' unit of
  work JOINS it, runs the handler behind `SAVEPOINT handler_run` against a capture, then updates the row and commits in
  the same transaction. The key row, the aggregate rows and the outbox rows commit or roll back together.
- **What a 4xx does to the handler's writes.** Nothing by itself: a joined unit of work runs in its own savepoint
  (`unit_of_work.go` `executeInSavepoint`), rolled back when the use case returns an error; when the handler answers 4xx
  the handler savepoint is released and the scopes that committed commit WITH the key row. Only a statement that failed
  on the server (SQLSTATE 23505 mapped to a 409) aborts the transaction: `ROLLBACK TO SAVEPOINT` discards the handler's
  writes and the answer is stored anyway. An OUTCOME is therefore possible in this store: a use case that commits a scope
  and then returns an error produces one.
- **TTL and purge.** `IDEMPOTENCY_KEY_TTL`, default 24 h; the housekeeping `Sweeper` (ADR 0026, hourly) deletes older
  rows. A replay does NOT check the row's age: between expiry and the next sweep a row still replays (open point, not
  changed here).
- **Surfaces.** Every `POST` route is keyed (11 routes, `server.go`); `PUT` and `DELETE` are idempotent by method. The
  MCP tools and the A2A skill `revoke_reservation` have NO store of their own (A2A: "NOT idempotent", ADR 0048; MCP:
  `IdempotentHint` false), so there is nothing to classify there. A2A keeps its own slug table (`a2a/mapping.go`); it is
  not a keyed surface.
- **Catalogue shape.** Not a slice of `{error, problem}` with a status: a status switch (`statusFor`), a
  `problemCatalog()` slice of `{error, problemInfo{slug, title}}`, and ad hoc `problemInfo` literals at call sites.

## Decision

1. **Three classes, declared on the catalogue entry.** `problemInfo` gains `class` (`replayClass`: `FINAL`, `OUTCOME`,
   `RETRYABLE`). The `problemCatalog()` entries declare it; every problem type that is not a typed domain error is
   declared once with `register(slug, title, status, class)` in `problems.go` (it replaces the literals at call sites;
   slug, title and status on the wire are unchanged). `statusFor` stays the status switch. A `problemInfo` literal
   anywhere else, or one without a class, fails `TestEveryProblemInfoLiteralIsInTheCatalogueAndClassified`; a problem type
   with no class fails `TestEveryProblemTypeDeclaresAReplayClass`. There is no runtime default.
2. **The writer marks, the middleware reads.** `writeProblem` (so `writeError`) and `writeQuarantineProblem` mark the
   middleware's `capture` with the class. The middleware (`runFreshRequest`) stores a 2xx, never stores a 5xx or a panic,
   and stores any other answer only if its class is FINAL or OUTCOME. It holds no slug and no status range beyond 2xx and
   5xx.
3. **RETRYABLE is released exactly as the 5xx branch.** The whole transaction rolls back, key row included, and the
   client gets the answer without `Idempotency-Replayed`. A refusal that declares no class (a runtime path outside the
   catalogue) is released the same way, as a safe fallback.
4. **OUTCOME commits with the key row.** The command's unit of work already committed its scope; the middleware stores the
   answer and commits in the same transaction. If the handler savepoint cannot be released (a statement failed on the
   server, so the writes are gone) an OUTCOME answer is neither returned nor stored: it is a 500 and the key retries, as a
   2xx behind a failed release already was.
5. **FINAL and 2xx are byte-identical to before.**

What a **concurrent duplicate** sees: the second request blocks on the unique index until the first transaction resolves
(ADR 0018). When the first ends RETRYABLE (rolled back) the second's insert succeeds and it EXECUTES the command itself and
answers its own result (which may be a success). When the first ends FINAL, OUTCOME or 2xx the second replays it.
Releasing a RETRYABLE key also means a different body under the same key afterwards is a new request, not
`idempotency-key-reused`.

### Replay class of every problem type

`FINAL`: a function of the request alone. `OUTCOME`: the command committed state and then refused. `RETRYABLE`: nothing
committed and the answer depends on state that can change (or a server-side 5xx). Types that only a `GET` answers
(`invalid-cursor`, `invalid-limit`, `unknown-query-parameter`, `invalid-report-query`, `invalid-older-than-hours`,
`relocation-not-found`, `route-not-found`, `report-store-error`, `dependency-unavailable`, ...) carry the class of the same
question so that no problem type is unclassified. One problem type has one class: a slug that mixes causes
(`reservation-already-resolved`, `bin-not-found`) takes the class that is safe for the mix.

| Problem type | Status | Class | Why |
|---|---|---|---|
| `automated-storage-requires-container` | 409 | RETRYABLE | Loose stock into a bin whose facility-layout LocationType is `AutomatedStorage` (`automated_storage_guard.go`, ADR 0054): the type comes from facility-layout events and can change. |
| `bin-full` | 409 | RETRYABLE | `bin.Occupy` against the bin's current occupancy (`stow_stock.go`, `stow_container.go:99`): state that changes as stock leaves. |
| `bin-handling-unit-mismatch` | 409 | RETRYABLE | The bin's declared handling unit (`BinHandlingUnitDeclared` event) can change. |
| `bin-not-at-site` | 409 | RETRYABLE | `relocation.ErrBinNotAtSite`: the bin's site (facility-layout; may be unset until the event arrives). |
| `bin-not-found` | 404 | RETRYABLE | A referenced fact not known yet: bins are registered by `PUT /bins` and fed by facility-layout events, so the id can arrive later (`stow_stock.go:57`, `stow_transfer_stock.go:201`, relocation, container stow). A 404 on a referenced fact is RETRYABLE (owner rule). |
| `bin-wrong-site` | 409 | RETRYABLE | `stow_transfer_stock.go:204`: the bin's site custody comes from facility-layout events and can change. |
| `bins-required` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `break-not-conserving` | 409 | RETRYABLE | `break_case.go`: the unit's quantity differs from the container's: a data discrepancy a cycle count can repair. |
| `capacity-below-occupancy` | 409 | RETRYABLE | `PUT /bins` resize against current occupancy: state that changes. (PUT is not keyed; classified for completeness.) |
| `capacity-out-of-range` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `capacity-required` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `concurrent-modification` | 409 | RETRYABLE | A version-guarded `Save` found zero rows (`location_repo.go:44`, `stock_repo.go:58`): depends on another writer's commit; the unit of work's savepoint rolled the use case's writes back, nothing committed. A retry reads the new version and passes. |
| `container-not-at-site` | 409 | RETRYABLE | Container site against bin site (facility-layout and receiving events). |
| `container-not-found` | 404 | RETRYABLE | The LPN is minted upstream (inbound-receiving carton links, `container_linker.go`), not by this service: a referenced fact an event may deliver later (`stow_container.go` `loadContainer`). |
| `container-not-sealed` | 409 | RETRYABLE | Container state (`stow_container.go:44`, `break_case.go`): reached by earlier commands and events. |
| `container-not-stowed` | 409 | RETRYABLE | `break_case.go`: break before stow; the aggregate state not reached yet. |
| `container-quantity-mismatch` | 409 | RETRYABLE | Raised by the container aggregate when the receiving consumer builds it; no keyed route answers it. Unsure, so RETRYABLE. |
| `container-reserved` | 409 | RETRYABLE | `stow_container.go:139-141`, `break_case.go`: a reservation holds the case; released by revoke, confirm or expiry. |
| `counted-quantity-required` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `dependency-unavailable` | 503 | RETRYABLE | A 503 is a 5xx: never stored (`/readyz`). |
| `dot-hazard-class-not-applicable` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `duplicate-handling-tag` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `empty-bin-id` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `empty-site-id` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `empty-sku` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `hazmat-class-incompatible` | 409 | RETRYABLE | Depends on the SKUs already in the bin (`stow_stock.go` `checkSegregation`): state. |
| `hazmat-zone-required` | 409 | RETRYABLE | Depends on the SKU's classification (product-master events) and the bin's zone attributes (facility-layout lookup) (`stow_stock.go:144`): both change. |
| `held-unit-closed` | 409 | RETRYABLE | `held.Decide` on a unit already decided (`decide_held_disposition.go:41`): depends on the unit's state, nothing committed. Terminal today but not a function of the request alone: unsure, so RETRYABLE. |
| `held-unit-not-found` | 404 | FINAL | Same condition: ids `hu-<uuid>` minted here (`held_stock_repo.go:129`), never deleted, only this service creates held units. |
| `idempotency-key-invalid` | 400 | FINAL | Answered before the store is touched. |
| `idempotency-key-required` | 400 | FINAL | Answered before the store is touched. |
| `idempotency-key-reused` | 409 | FINAL | The store's own answer to a key seen with another method, path or body. No handler ran and the request writes no key row: it is never stored. |
| `insufficient-reserved` | 409 | RETRYABLE | `unit.Pick` against the unit's reserved quantity (`confirm_pick.go:85`): state; the savepoint rolled the partial pick back. |
| `insufficient-usable` | 409 | RETRYABLE | `reserve_stock.go:255,282`, `reserve_full_cases.go`: usable stock at the moment of the call; receipts and revocations change it. The lazy expiry that ran first (`expireAllIfDue`) is rolled back with the request and rediscovered by the next read: it is not the refusal's own outcome. |
| `internal-error` | 500 | RETRYABLE | A 5xx is never stored. |
| `invalid-bin-capacity` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-cursor` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-disposition` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-dot-hazard-class` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-hold-reason` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-limit` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-line-no` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-older-than-hours` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-owner-ref` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-relocation-reason` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `invalid-report-query` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `license-plate-taken` | 409 | FINAL | `PUT /totes/{lpn}` on a plate that names a sealed case (`register_tote.go`, `license_plates`, ADR 0054): a plate never changes kind and is never released. |
| `location-classification-unavailable` | 409 | RETRYABLE | A dependency outage: the facility-layout lookup failed (`stow_stock.go:136`). Nothing committed; the retry can succeed. Answered 409 today and SHOULD be 503 (open point; the status is not changed here). |
| `malformed-request-body` | 400 | FINAL | The body bytes alone (unreadable, NUL, invalid JSON). |
| `missing-demand-ref` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `negative-quantity` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `no-handling-tags` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `not-found` | 404 | FINAL | A NUL in the path: the path alone. |
| `owner-mismatch` | 409 | RETRYABLE | Stock owner against the reservation owner: state of the stock unit. |
| `product-classification-not-found` | 404 | RETRYABLE | Classifications are copied from product-master events (ADR 0034): a referenced fact an event may deliver later. Read side only today. |
| `quantity-and-cases-exclusive` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `quantity-not-positive` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `receipt-not-staged` | 409 | RETRYABLE | `stow_transfer_stock.go:107,147`: the aggregate state not reached yet (stage before stow). |
| `release-exceeds-occupancy` | 409 | RETRYABLE | `bin.Release` against current occupancy (`confirm_pick.go:107`, container break and restow): state. |
| `relocation-not-found` | 404 | FINAL | Same condition (`GET /relocations/{id}` only; no keyed route answers it). |
| `report-store-error` | 500 | RETRYABLE | A 5xx is never stored. |
| `reservation-already-resolved` | 409 | OUTCOME | `confirm_pick.go:47` runs `expireIfDue` (`reservation_expiry.go:75-87`): a timed-out reservation becomes EXPIRED, its quantity returns to usable and `ReservationExpired` is published, in a unit of work that COMMITS; `confirm_pick.go:53` then refuses the already EXPIRED reservation (`reservation.go:183`). Proven by `TestReplayClass_PickConfirmationOfATimedOutReservationCommitsTheExpiryAndReplays`. The slug also answers a reservation confirmed or revoked earlier (nothing committed, terminal state): judgement call 1. |
| `reservation-expired` | 409 | RETRYABLE | `reservation.go:185-186`: `Confirm` answers it only when the reservation is still ACTIVE after `expireIfDue` (`confirm_pick.go:47`) and the clock moved past `expiresAt` between the two `Clock.Now()` reads (:52). Nothing is committed: a reservation expired by `expireIfDue` is EXPIRED in memory and answers `reservation-already-resolved`. A retry hits the lazy expiry. The brief listed it as OUTCOME; the code does not support that: judgement call 2. |
| `reservation-no-allocations` | 409 | RETRYABLE | `reservation.NewForLine` with no allocation: unreachable from the routes (the allocator refuses first); an invariant, not a request property. Unsure, so RETRYABLE. |
| `reservation-not-found` | 404 | FINAL | 404 on the command's own target: ids are `res-<uuid>` minted here (`reservation_repo.go:119`), reservations are never deleted, no event delivers one. The owner's FINAL condition holds. |
| `route-not-found` | 404 | FINAL | The path alone. |
| `same-bin` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `source-ref-too-long` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `stock-unit-not-found` | 404 | RETRYABLE | Raised inside a use case for a unit that a reservation or container points at (`confirm_pick.go:80-82`, `stow_container.go:137`): an inconsistency of state, not the request's own target. Unsure, so RETRYABLE. |
| `stow-quantity-mismatch` | 409 | RETRYABLE | `stow_transfer_stock.go:210`: compares the request with the staged receipt's received quantity: state, not the request alone. |
| `stow-requires-item-and-location` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `temperature-class-mismatch` | 409 | RETRYABLE | Same inputs as `hazmat-zone-required` (`stow_stock.go:147`). |
| `temperature-class-not-applicable` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `temperature-class-required` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `tote-not-found` | 404 | RETRYABLE | A tote is registered by `PUT /totes/{lpn}` (ADR 0054): a registration can follow, as `container-not-found`. |
| `tote-not-placed` | 409 | RETRYABLE | `fill_tote.go`: a fill before the tote's first stow; the aggregate state not reached yet. |
| `tote-reserved` | 409 | RETRYABLE | `stow_tote.go` `movable`: a reservation holds stock in the tote; released by revoke, confirm or expiry. |
| `transfer-id-required` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `transfer-receipt-conflict` | 409 | RETRYABLE | `replayOrRefuseReceipt` (`stage_transfer_receipt.go`): the line already has a receipt for a different scan; depends on the stored receipt, not the request alone. |
| `transfer-scan-quarantined` | 422 | OUTCOME | `stage_transfer_receipt.go:146-180`: `quarantine` (:252-264) saves the inventory exception inside `atomically`, which succeeds; the problem is returned AFTER the commit as `decided.Problem`, and `transfer_receipt_handler.go:63-68` answers 422. The exception row is the record of the physical scan. Proven by `TestReplayClass_QuarantinedScanCommitsTheExceptionAndReplays`. Was not a `problemCatalog` entry (written by `writeQuarantineProblem`). |
| `unit-held` | 409 | RETRYABLE | Stock unit held pending a disposition: state until the disposition. |
| `unit-unlocated` | 409 | RETRYABLE | Stock unit state set by a cycle count and cleared by a later one: state. |
| `unknown-handling-tag` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `unknown-query-parameter` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `unknown-temperature-class` | 400 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |
| `zero-quantity` | 422 | FINAL | Validation of the request's own fields or parameters: a function of the request alone. |

(`invalid-hold-reason` is also answered 400 by the held-stock list filter; `quantity-not-positive`, `missing-demand-ref`
and `invalid-line-no` have more than one call site, all request validation.)

### Judgement calls

1. **`reservation-already-resolved` is OUTCOME although one of its causes commits nothing.** When the reservation timed
   out the pick confirmation commits the expiry and then refuses; storing the refusal with its writes keeps the expiry
   durable and the answer stable. The same slug answers a reservation confirmed or revoked earlier: nothing is committed,
   but those states are terminal and never change back, so a stored answer is never wrong. Splitting the expiry-driven
   refusal into its own problem type is a recorded follow-up, not part of this change.
2. **`reservation-expired` is RETRYABLE, not OUTCOME.** The brief listed it as an OUTCOME candidate; reading the use case
   shows the lazy expiry turns a timed-out reservation into `reservation-already-resolved`, and `reservation-expired` only
   appears in the clock-edge race where nothing was committed.
3. **`transfer-scan-quarantined` is OUTCOME.** The refusal commits the inventory exception. The allocation fact that would
   make the scan acceptable can arrive later, so a retry under the same key replays the 422 for the window; the scan stays
   an exception and the operator resolves it (a new physical scan uses a new key). Reading it as RETRYABLE would roll the
   exception back and lose the record of the scan.
4. **`bin-not-found`, `container-not-found`, `product-classification-not-found` are RETRYABLE** (referenced facts fed by
   events or `PUT`), while `reservation-not-found`, `held-unit-not-found` and `relocation-not-found` are FINAL (ids minted
   here, never deleted).
5. **`location-classification-unavailable` is RETRYABLE** and is answered 409 today although it is a dependency outage; its
   status should be 503 (open point, not changed here).
6. **Terminal-state refusals are RETRYABLE** (`held-unit-closed`, `transfer-receipt-conflict`, `stow-quantity-mismatch`):
   they depend on stored state, nothing is committed, and a released refusal costs one more execution.

## Consequences

- A client that retries a refusal now gets it re-executed under the same key; the tests prove the retry executes and is
  then stored and replayed. Callers that relied on a refusal being sticky must stop doing so: fulfillment-execution calls
  with the deterministic key `break-case:<taskId>` (no attempt part), so a `bin-not-found` or `concurrent-modification` on a
  case break no longer sticks for 24 h; order-management's attempt counter is unaffected. These are consumer-side
  observations only (no consumer repo is changed here).
- `Idempotency-Replayed: true` now appears only on FINAL, OUTCOME and 2xx replays.
- Tests: `replay_class_test.go` (classes, catalogue-vs-ADR table, literal scan, marking, the storable decision),
  `idempotency_replay_classes_template_integration_test.go` (the fleet template over the real middleware and Postgres),
  `idempotency_replay_classes_integration_test.go` (the real router: a genuine concurrent-modification race, an unknown bin,
  the expiring pick confirmation, the quarantined scan).
- Open points: the replay does not check the row's age (ADR 0026 sweeps hourly); `location-classification-unavailable`
  should be 503; the problem types that mix causes could be split.
