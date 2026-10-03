---
paths:
  - "**/*.go"
  - "go.mod"
  - "go.sum"
  - "Makefile"
  - ".github/workflows/**"
---

# Go code standards, config and testing detail

Moved here from CLAUDE.md; the gate commands and the testcontainers rule stay there.

- Go 1.26, modules. chi (`go-chi/chi/v5`), pgx/v5 + pgxpool, golang-migrate.
- Config via env (`DATABASE_URL`, `HTTP_ADDR`, `ANALYTICS_DATABASE_URL`,
  `MIGRATIONS_DATABASE_URL`, `EVENT_PUBLISHER`, `OUTBOX_RELAY_INTERVAL`,
  `KAFKA_BROKERS`, `CORS_ALLOWED_ORIGINS`, `LOCATION_LOOKUP_MODE`,
  `FACILITY_LAYOUT_BASE_URL`, `REPORTS_BASE_URL`, `MCP_ADDR`). No hardcoded
  config.
- Typed domain errors mapped to HTTP status (RFC 7807 problem details) in the
  adapter. gofmt/go vet clean; every package has a doc comment.
- Table-driven tests: domain + application (in-memory adapter); one httptest
  per endpoint; build-tagged Postgres integration test (`-tags=integration`,
  skipped without `DATABASE_URL`). CI's `integration` job runs Postgres only,
  no Kafka service.
- `make check` (fmt-check, vet, build, lint, test — ~1 min, no DB) after
  every change, before committing.
- `make check-all` before pushing: adds the 90% coverage gate, `arch-test`
  (hexagonal fitness, enforces the dependency rule and that
  `internal/analytics/` imports nothing from OLTP domain/application), and
  `bdd` (godog/Gherkin acceptance, `features/*.feature`).
- `make vuln` after touching `go.mod`/`go.sum` — blocking CI job, flags known
  CVEs in the dependency graph and stdlib.
- lefthook git hooks (`lefthook install`) enforce fmt-check/vet/lint
  pre-commit and `make check` pre-push, but run `make check` proactively —
  hooks are per-clone and may not be installed.
- Definition of done: `go build ./...`, `go vet ./...`, `go test ./...` all
  green; README run steps + curl'd endpoints + layering note kept current;
  every domain invariant has a failing-path test (bin-capacity rejection,
  stow-requires-item-and-location, reservation <= usable, revoke returns to
  usable).

## Local dev

```sh
go run ./cmd/inventory                       # in-memory adapters, no DB; :8080 (HTTP_ADDR)

docker compose up -d postgres                # Postgres mode; migrations run automatically
export DATABASE_URL='postgres://inventory:***@localhost:5432/inventory?sslmode=disable'
go run ./cmd/inventory

cd docs && npm ci
npm run gen-api-docs                         # regenerate REST reference from apis/openapi.yaml
npm run build                                # full site build; onBrokenLinks: 'throw'
```
