---
id: 0007-godog-bdd-acceptance-tests
slug: /adr/0007-godog-bdd-acceptance-tests
title: 0007. godog/Gherkin acceptance tests as executable specification
sidebar_label: 0007. godog BDD tests
description: ADR 0007 — express the context's behaviour as black-box Gherkin scenarios driven through the real HTTP router.
---

# 0007. godog/Gherkin acceptance tests as executable specification

## Status

Accepted. Delivered in commit *"Add godog (Cucumber/Gherkin) BDD acceptance
tests, wire bdd CI job"*.

## Context

By this point the service already had a deep test suite: table-driven unit
tests per aggregate and per invariant, use-case tests against in-memory
adapters, `httptest` tests per endpoint, Postgres integration tests per
outbound adapter, and a `gremlins` mutation pass over the domain. Coverage was
not the problem.

What was missing was a **statement of behaviour in the domain's own
vocabulary**. Every existing test is written for a developer and expressed in
Go: `TestReserveStock_ExceedsUsable_Rejected` is precise, but it does not say
*why* reserving more than usable is refused, and it cannot be read by anyone
who does not read Go.

That matters here more than usual, because this is a **Core subdomain** whose
correctness is defined by operational rules — a stow needs both scans, a full
bin rejects a stow, a revoke returns quantity to usable — rules that come from
the warehouse floor, not from the code. `CLAUDE.md` names the ubiquitous
language explicitly and asks that it be honoured; unit-test names are a poor
place for it to live.

There was also a structural gap: the existing tests each verify one layer.
Nothing exercised the *whole* stack — router, DTOs, error mapping, use cases,
aggregates, adapters — as a black box the way a real client would.

## Decision

**We will write executable specifications in Gherkin under `features/`, run
them with [godog](https://github.com/cucumber/godog) v0.16.0 (the official
Cucumber implementation for Go), and gate them in CI as a `bdd` job.**

1. **One feature file per aggregate/bounded concept**, using the ubiquitous
   language verbatim — StockUnit, Bin, Stow, Usable inventory, Reservation,
   Cycle count:

   | File | Covers |
   | --- | --- |
   | `features/stow.feature` | `POST /stock-receipts`, `POST /stowages` — chaotic stow, bin-capacity rejection |
   | `features/reservation.feature` | reserve against usable, revoke, confirm-pick |
   | `features/cycle_count.feature` | clean count vs. discrepancy/`Unlocated` |
   | `features/usable_inventory.feature` | on-hand minus active reservations |

2. **True black-box acceptance tests.** Step definitions live in
   `features_test.go` at the repo root. They wire the **real chi router** to
   the in-memory outbound adapters, serve it over `httptest.NewServer`, and
   drive it with plain `net/http` requests. **No use case is called directly** —
   if the HTTP status mapping or a DTO field name is wrong, the scenario fails.

3. **Fresh state per scenario.** A godog `Before` hook rebuilds the server and
   its adapters for every scenario, so ordering cannot create hidden coupling.

4. **In-memory adapters, not Postgres.** These specify *behaviour*, not
   persistence. The Postgres integration tests already cover storage, and
   keeping the acceptance suite broker-free and database-free means it runs in
   the ordinary `go test ./...` invocation.

5. **Blocking in CI** as the `bdd` job (`go test ./... -run TestFeatures -v`).

## Consequences

### Easier

- **The behaviour is readable by anyone.** A Gherkin scenario about a full bin
  rejecting a stow is reviewable by someone who has never opened a Go file, and
  is phrased in the language of the warehouse rather than of the codebase.
- **The full stack is covered end to end.** Status codes, `Location` headers,
  RFC 7807 bodies, DTO field names and error mapping are all exercised — the
  layer of bugs unit tests structurally cannot see.
- **The ubiquitous language gets enforced.** Scenario prose uses the domain's
  own terms, so a rename in the model shows up as prose that no longer matches
  reality.
- **It is honest documentation.** Unlike prose docs, these fail the build when
  they stop being true. Together with [ADR 0006](./0006-arch-go-fitness-tests.md),
  both the *structure* and the *behaviour* of the service are executable
  statements.

### Harder

- **A second vocabulary to maintain.** Step definitions are glue: a change to a
  DTO or route means updating `features_test.go` as well as the handler.
  `features_test.go` is a non-trivial file in its own right.
- **Step reuse needs discipline.** Left alone, Gherkin suites grow near-
  duplicate steps ("given a bin with capacity 10" vs "given bin A-1-1 holds
  10") until nobody can find the existing one.
- **Slower than a unit test.** Each scenario stands up an HTTP server. Still
  fast in absolute terms with in-memory adapters, but no longer microseconds —
  hence a separate `bdd` CI job with its own timing.
- **Overlap with the `httptest` suite.** Some assertions now exist in both
  places. Accepted deliberately: the `httptest` tests are exhaustive per
  endpoint including error paths, while the Gherkin scenarios are the readable
  specification of the important journeys. Deleting either would lose
  something.
