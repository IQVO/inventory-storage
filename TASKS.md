# Build Tasks — Inventory & Storage

> **Historical build brief.** Tasks 3, 5 and 7 describe the first build, when
> the service could run in memory and publish through a log or a direct Kafka
> writer. That is gone: Postgres and Kafka are the only runtime
> ([ADR 0042](docs/docs/adr/0042-postgres-and-kafka-are-the-only-runtime.md));
> the `memory` and log adapters remain as unit/BDD test doubles only. See the
> [README](README.md) and [Configuration](docs/docs/operations/configuration.md)
> for how the service runs today.

Build the full bounded context described in CLAUDE.md, in order. Keep
`go build ./...` and `go test ./...` green throughout. Read
/Users/claudioed/docs/amazon-fulfillment-ddd.md for the domain model first.

## Task 0 — Skeleton
- `go mod init github.com/claudioed/inventory-storage`; create the layout;
  .gitignore (bin/, .env); add chi + pgx deps.

## Task 1 — Domain (pure Go)
- shared: SKU, BinId, Quantity value objects; DomainEvent + the 10 events.
- location: Bin aggregate (capacity, occupancy; reject stow when full).
- stock: StockUnit aggregate (qty>=0, states, stow needs item+location, Unlocated).
- reservation: Reservation aggregate (revocable, timeout, <= usable, no double-consume).
- Unit tests for EVERY invariant incl. failing paths.

## Task 2 — Application
- ports: StockRepo, LocationRepo, ReservationRepo, EventPublisher, Clock.
- usecases: the 7 use cases from CLAUDE.md, depending only on domain + ports.
- Unit-test against in-memory adapter. Compute usable = on-hand - active reservations.

## Task 3 — Outbound adapters
- memory: thread-safe impls of every port.
- postgres: pgxpool repos + migrations (stock_unit, bin, reservation, events);
  build-tagged integration test (skip w/o DATABASE_URL).
- events: log/buffered publisher, kafka-ready interface.

## Task 4 — Inbound HTTP
- chi router + handlers for every endpoint, DTOs, domain-error->HTTP mapping, /healthz.
- httptest per endpoint against in-memory repos.

## Task 5 — Composition & ops
- cmd/inventory/main.go wires env -> adapters -> use cases -> router.
- docker-compose.yml (pg16); README.md with run steps + curl examples.

## Task 6 — Verify
- build, vet, test (and `-race`) all green; gofmt clean. Confirm the four named
  invariants each have a red-path test. Do not stop until DoD in CLAUDE.md is met.

## Task 7 — Cross-service integration (additive, see CLAUDE.md's new section)
- Add `github.com/segmentio/kafka-go` dependency.
- New Kafka outbound publisher adapter implementing the existing EventPublisher
  port, publishing StockReserved and ReservationRevoked to
  warehouse.inventory.events, selected via EVENT_PUBLISHER env (default "log").
- Unit test the envelope shape the adapter produces.
- README gains an Integration section. REAL smoke test against the shared
  broker (docker-compose.kafka.yml in ~/warehouse-systems, localhost:9092):
  call POST /reservations with EVENT_PUBLISHER=kafka and confirm the message
  is consumable on the topic before declaring done.
- Full existing suite (build/vet/test/-race) must still be green afterward.

## Task 10 — Quality engineering: linting, coverage, integration tests, mutation tests, CI
Full spec in QUALITY.md at the repo root. Five ordered stages, each gates the
next: (1) golangci-lint clean via the committed .golangci.yml, (2) unit test
coverage >= 90% on internal/domain/... + internal/application/... combined,
(3) real integration tests against live Postgres for every outbound Postgres
adapter, (4) gremlins mutation testing on internal/domain/... only
(exploratory, triaged not gated), (5) .github/workflows/ci.yml — lint+unit+
integration blocking on every push/PR, mutation testing on a weekly schedule/
manual dispatch only, never blocking PRs. Do not stop until every stage's
Definition of Done in QUALITY.md is met, then report the final numbers.

## Task 11 — REST API hardening + OpenAPI 3.0.3 docs + Spectral CI gate
Full spec in REST_API_TASK.md at the repo root. Four ordered stages: (1) audit
this service's HTTP adapter against REST/HTTP Level 2 maturity and fix real
violations (resource nouns, correct verbs/status codes, Location headers,
input validation), (2) migrate all error responses from the bespoke
{"error":...} shape to RFC 7807 application/problem+json, (3) write a very
detailed openapi.yaml (3.0.3) covering every route with full request/response
schemas and real domain-grounded examples, (4) add a new openapi-lint job to
the existing .github/workflows/ci.yml using Spectral, blocking on every
push/PR. Do not stop until every stage's Definition of Done in
REST_API_TASK.md is met, then report the final numbers.

## Task 12 — CI workflow restructure (user-provided template)
Full spec in CI_RESTRUCTURE_TASK.md at the repo root. Rewrite
.github/workflows/ci.yml to the given 4-job structure (lint, test,
integration, mutation) plus top-level permissions/concurrency/defaults,
while preserving Task 11's openapi-lint job as a 5th job. Requires adapting
placeholders to this repo's real values (postgres creds, DATABASE_URL,
whether integration tests self-migrate, current gremlins version), not
copy-pasting blindly. Every job's commands must be verified locally against
this repo before pushing, and the real GitHub Actions run must be confirmed
green via gh run watch. Do not stop until every requirement in
CI_RESTRUCTURE_TASK.md is met.

## Task 13 — Docker Hub publish job in CI
Added a `docker-publish` job to .github/workflows/ci.yml, gated on
`needs: [lint, test, integration, openapi-lint]` and
`if: github.event_name == 'push' && github.ref == 'refs/heads/main'` (never
runs on PRs, only after all quality gates pass on a real push to main).
Builds and pushes the existing repo-root Dockerfile to Docker Hub under the
`claudioed` namespace, tagged `latest` + short git SHA, linux/amd64 only,
using GitHub Actions cache (type=gha) to speed up rebuilds. Requires the
repo to have `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets configured
(user-provided, not committed anywhere).

## Task 14 — Architecture fitness tests (ArchUnit equivalent: arch-go)
Full spec in ARCH_TEST_TASK.md at the repo root. Add
internal/architecture/architecture_test.go using github.com/arch-go/arch-go
to encode the hexagonal dependency rule as executable Go tests (domain
depends on nothing internal, application depends only on domain, inbound and
outbound adapters never depend on each other, only cmd/ wires everything).
New arch-test CI job, added to docker-publish's needs list. Strictly
additive -- do not touch existing production code; if a real architecture
violation is found, report it explicitly rather than silently working around
it. Do not stop until every requirement in ARCH_TEST_TASK.md is met.

## Task 15 — Helm chart lint CI job
Added a `helm-lint` job to .github/workflows/ci.yml using
helm/chart-testing-action@v2.8.0 (`ct lint`) against
charts/inventory-storage. Runs on every push/PR (no gating condition, always
blocking), and wired into `docker-publish`'s `needs` list alongside the
existing gates -- a chart that fails helm/Chart.yaml validation or YAML
style rules never reaches Docker Hub either. Verified locally with
`ct lint --charts charts/inventory-storage --validate-maintainers=false
--check-version-increment=false` before pushing (chart-testing v3.14.0,
yamllint installed via Homebrew).
