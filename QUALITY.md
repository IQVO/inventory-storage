# Quality Engineering — Task 10 (linting, coverage, integration tests, mutation tests, CI)

> **Historical build brief.** The runtime and the tests have moved on: Postgres
> and Kafka are the only runtime ([ADR 0042](docs/docs/adr/0042-postgres-and-kafka-are-the-only-runtime.md)),
> so the `memory` adapters named below are unit/BDD test doubles only, and the
> integration suites boot their own Postgres/Kafka with testcontainers instead
> of being skippable on `DATABASE_URL`. See
> [Testing](docs/docs/development/testing.md) for the current setup.

This is a large, multi-stage task. Work through the stages IN ORDER below and
do not skip ahead — each stage's Definition of Done gates the next. This is
STRICTLY ADDITIVE to the existing bounded context: do not modify any existing
aggregate, invariant, or use case's BEHAVIOR. You may (and should) add new
test files, and you may make small, behavior-preserving refactors ONLY where
needed to make code testable (e.g. extracting a clock/random seed for
determinism) — if in doubt, add a test rather than change source.

## Stage 1 — Linting

A `.golangci.yml` already exists at the repo root (golangci-lint v2 config,
committed). Do not rewrite it. Run:

```sh
golangci-lint run ./...
```

Fix every reported issue with the SMALLEST correct change (e.g. `defer func()
{ _ = tx.Rollback(ctx) }()` instead of restructuring error handling; `defer
func() { _ = m.Close() }()` for migration closers). Do not disable/suppress
rules to make issues disappear — fix the underlying code. Do not add new
`//nolint` comments unless a rule is a genuine false positive on this exact
line (rare) — justify it in a comment if you do.

**Definition of done for Stage 1:** `golangci-lint run ./...` exits 0 with
zero issues. `gofmt -l .` is empty. `go build ./...` and `go vet ./...` still
clean.

## Stage 2 — Unit test coverage: 90% minimum on domain + application layers

Target packages (business logic only — this is the coverage gate):
`internal/domain/...` and `internal/application/...`

NOT gated (still keep reasonably tested, but not blocked on 90%):
`cmd/...` (composition root, wiring only) and
`internal/adapters/outbound/postgres/...` (requires a live DB; already has
build-tagged integration tests, see Stage 3).

Check current coverage first:

```sh
go test ./internal/domain/... ./internal/application/... -coverprofile=/tmp/cover.out
go tool cover -func=/tmp/cover.out | tail -1
go tool cover -func=/tmp/cover.out | awk '$3+0 < 90 {print}'   # packages/funcs below 90%
```

For every package/function below 90%, add table-driven unit tests covering:
- every named invariant's failing path (if not already covered — check
  existing `_test.go` files first, this codebase already has strong invariant
  coverage from the original build; don't duplicate)
- every error return branch
- boundary conditions (zero values, empty collections, exact-equal
  comparisons like `plannedHeads == installedStations`)
- every exported constructor's validation branches

Use the SAME test style already established in this repo (table-driven,
`t.Run` subtests, in-memory adapters from `internal/adapters/outbound/memory`
for application-layer tests). Do not introduce a new test framework/library —
stick to stdlib `testing` as the existing suite does.

**Definition of done for Stage 2:** re-run the coverage command above; total
statement coverage across `internal/domain/...` AND `internal/application/...`
combined is >= 90%. Record the actual number achieved. `go test ./... -race`
still fully green (including all pre-existing tests — none may be weakened,
skipped, or deleted to hit the number).

## Stage 3 — Integration tests

This repo already has SOME build-tagged (`//go:build integration`) tests
against a live Postgres — check what exists first with:

```sh
grep -rl "go:build integration" --include=*.go .
```

Extend this coverage so every outbound Postgres adapter (every `*Repo`
implementation under `internal/adapters/outbound/postgres/`) has at least one
integration test exercising a real round-trip against a live Postgres
(save/find/update, and at least one query specific to that repo, e.g. a
capacity/queue-depth read). Use `docker-compose.yml`'s existing Postgres
service (same user/password/db already defined there) — do not invent new
credentials. Tests must be skippable without a live DB:

```go
if os.Getenv("DATABASE_URL") == "" {
    t.Skip("DATABASE_URL not set; skipping integration test")
}
```
(match whatever skip pattern the existing integration tests in this repo
already use, for consistency — check first).

Run them for real before declaring done:

```sh
docker compose up -d postgres   # or however this repo's compose file names it
# wait for healthy, then:
DATABASE_URL="<the URL from this repo's docker-compose.yml>" go test -tags=integration ./... -v
docker compose down
```

**Definition of done for Stage 3:** every Postgres adapter has a real,
passing integration test, run for real against a live container (not just
compiled) at least once during this task, with output captured. Tests remain
skip-clean when `DATABASE_URL` is unset (so `go test ./...` without the tag
still passes with no live DB, exactly as it does today).

## Stage 4 — Mutation testing (domain layer only)

Tool: `gremlins` (already installed on this machine). Scope: ONLY
`internal/domain/...` — this is where mutation testing has the highest signal
(aggregate invariants), not application/adapters.

```sh
gremlins unleash ./internal/domain/... --dry-run   # sanity check it discovers mutants first
gremlins unleash ./internal/domain/...
```

This is EXPLORATORY, not a hard gate for this task (mutation testing scores
below ~100% are normal and often reflect equivalent mutants, not real gaps) —
but for every SURVIVED ("lived") mutant gremlins reports, look at it: if it
reveals a genuinely untested behavior (a boundary you don't have a test for),
add a test that kills it. If it's an equivalent mutant (behaviorally
identical, e.g. mutating `i++` to `i+=1` in a context where both are truly
equivalent) or genuinely not worth chasing, leave it and move on — do not
contort the code to kill unkillable mutants.

Write a short `MUTATION.md` at the repo root: the gremlins command used, the
final efficacy/mutant-coverage numbers, and a one-line note per survived
mutant you decided not to chase and why.

**Definition of done for Stage 4:** `gremlins unleash ./internal/domain/...`
run at least once with real output captured in `MUTATION.md`. No requirement
to hit a specific score — the requirement is that you looked at every
survived mutant and made a documented judgment call.

## Stage 5 — GitHub Actions

Create `.github/workflows/ci.yml`:

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: "0 6 * * 1"   # weekly, Monday 6am UTC, for the mutation job only
  workflow_dispatch: {}

jobs:
  lint-test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: inventory
          POSTGRES_PASSWORD: inventory
          POSTGRES_DB: inventory
        ports: ["5432:5432"]
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 3s
          --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.26"
      - name: golangci-lint
        uses: golangci/golangci-lint-action@v6
        with:
          version: v2.13.1
      - name: Unit tests + coverage gate
        run: |
          go test ./internal/domain/... ./internal/application/... -coverprofile=coverage.out -race
          go tool cover -func=coverage.out | tail -1
          COVERAGE=$(go tool cover -func=coverage.out | tail -1 | awk '{print $3}' | tr -d '%')
          echo "Total coverage: ${COVERAGE}%"
          awk -v cov="$COVERAGE" 'BEGIN { exit (cov < 90) }'
      - name: Full test suite (build/vet/race, all packages)
        run: |
          go build ./...
          go vet ./...
          go test ./... -race
      - name: Integration tests
        env:
          DATABASE_URL: "postgres://inventory:inventory@localhost:5432/inventory?sslmode=disable"
        run: go test -tags=integration ./... -v

  mutation:
    runs-on: ubuntu-latest
    if: github.event_name == 'workflow_dispatch' || github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.26"
      - name: Install gremlins
        run: go install github.com/go-gremlins/gremlins/cmd/gremlins@latest
      - name: Mutation testing (domain layer)
        run: gremlins unleash ./internal/domain/...
```

Adapt the Postgres service credentials and `DATABASE_URL` to match THIS
repo's actual `docker-compose.yml` values (checked in Stage 3). The
`lint-test` job runs on every push/PR and is a real blocking gate (coverage
< 90% fails the build via the awk exit-code trick above). The `mutation` job
only runs on the weekly schedule or manual dispatch — the `if:` condition
means it's skipped on every push/PR, so it never blocks anyone's PR.

**Definition of done for Stage 5:** `.github/workflows/ci.yml` is valid YAML
— verify it AT LEAST syntactically (e.g. `python3 -c "import yaml;
yaml.safe_load(open('.github/workflows/ci.yml'))"` or equivalent) before
declaring done. If the `gh` CLI is available and authenticated in this
environment, also push a trivial commit and confirm via `gh run list` / `gh
run watch` that the workflow actually triggers and the lint-test job passes
for real on GitHub's runners — do this if you can, it's the strongest
verification, but syntactic validation is the hard minimum requirement.

## Final report

When all 5 stages are done, write a summary as your final message covering:
lint issues fixed (count), starting vs final coverage % on domain+application,
which Postgres adapters got new integration tests, mutation testing efficacy/
mutant-coverage numbers and how many survived mutants you triaged, and
confirmation the full existing test suite (all prior Tasks) is still green
with `go build/vet/test/test -race` and `gofmt -l .` clean. Do not stop until
every stage's Definition of Done is met.
