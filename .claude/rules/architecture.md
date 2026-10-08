---
paths:
  - "internal/**"
  - "cmd/**"
---

# Architecture: hexagonal layout and dependency rule (ADR-0001)

Moved here from CLAUDE.md; the NON-NEGOTIABLE one-line rule stays there.

Hexagonal / Ports & Adapters. Strict dependency rule: **domain depends on
nothing; application depends on domain; adapters depend on application/domain.**
No framework or SQL types in the domain layer.

```
cmd/inventory/               main.go — composition root (OLTP REST API)
cmd/inventory-projector/     analytics WRITER: consumes analytics topic, projects
cmd/inventory-reports/       analytics READER: read-only report REST
cmd/mcp/                     MCP inbound adapter (Streamable HTTP)
internal/
  domain/
    location/                Bin/Location aggregate (capacity, occupancy)
    stock/                   StockUnit aggregate (SKU@location, qty, state)
    reservation/              Reservation aggregate (revocable, timeout)
    product/                  ProductClassification aggregate (SKU master data)
    shared/                  value objects: SKU, BinId, Quantity, events
  analytics/report/          read-model region (depends on nothing) — data product
  application/
    ports/                   OUT interfaces: StockRepo, LocationRepo, ReservationRepo,
                              ProductClassificationRepo, LocationClassificationLookup,
                              EventPublisher, UnitOfWork, ReservationMetrics, Clock
    usecases/                one struct per use case
  adapters/
    inbound/http/            chi handlers, DTOs, error mapping
    kafka/cloudevents/       the ONLY CloudEvents envelope builder/decoder (ADR-0024)
    inbound/kafka/           analytics consumer (projector)
    inbound/mcp/             MCP tools incl. the read-only report tool
    outbound/postgres/       pgxpool repos + migrations
    outbound/memory/         in-memory repos for tests/local
    outbound/events/         log/buffered/multi (fan-out) publisher
    outbound/kafka/          Kafka integration + analytics publishers
    outbound/analyticsstore/ analytical Postgres projection + read-only reader
    outbound/facilitycache/  Kafka-fed facility-layout location cache (ADR-0013)
    outbound/facilitylayout/ sync HTTP + permissive location lookups (fallbacks)
    outbound/telemetry/      OTel setup, trace-aware slog, reservation metrics
  architecture/              arch-go + fitness tests (dependency rule, no auth, Kafka rules)
migrations/                  golang-migrate SQL files (OLTP)
migrations/analytics/        golang-migrate SQL files (analytical read model)
web/                         inventory-mfe — Vite/React MFE remote (separate module)
```

The application layer never imports an adapter package — it depends only on
`application/ports` interfaces. The inbound HTTP adapter never leaks domain
structs across the wire; every response is a DTO.

## Processes and contracts

- Four binaries: `cmd/inventory` (OLTP REST API), `cmd/inventory-projector`
  (analytics writer), `cmd/inventory-reports` (analytics reader) and
  `cmd/mcp` (MCP inbound adapter, Streamable HTTP). Plus `web/` (a standalone
  Vite/React micro-frontend remote — see `.claude/rules/frontend-mfe.md`).
- Publishes `StockReserved`/`ReservationRevoked` to `warehouse.inventory.events`
  (plus the analytics stream `warehouse.inventory.analytics`), all as
  CloudEvents 1.0 (ADR-0024), and consumes sibling topics: facility-layout's
  `warehouse.facility.events` into a local location-classification cache
  (`LOCATION_LOOKUP_MODE=kafka`, ADR-0013), plus the transfer-command,
  product-master and fulfillment (`TaskCompleted`, confirm-pick, ADR-0035)
  topics — see `integration-events.md`. Every REST and MCP endpoint is
  unauthenticated (ADR-0015).
- API contracts are the single source of truth for generated docs:
  `apis/openapi.yaml` (REST, Spectral-linted) and `apis/asyncapi.yaml`
  (events, Spectral-linted). The Docusaurus site in `docs/` regenerates its
  REST reference from `apis/openapi.yaml` via `npm run gen-api-docs`.

## ADR index (check before re-litigating a decision)

`docs/docs/adr/0001..0036`: hexagonal layering ADR-0001, chaotic storage
ADR-0002, revocable reservations ADR-0003, DOT hazard segregation ADR-0010,
facility-layout events cache ADR-0013, why the REST identity/bearer-auth
layer was added then removed ADR-0014/0015, standard metrics convention
ADR-0016, transactional outbox ADR-0017, Idempotency-Key middleware
ADR-0018, optimistic concurrency ADR-0019, resilience (breaker, DLQ,
graceful shutdown) ADR-0020, Kafka partition key ADR-0021, CloudEvents
mandatory envelope ADR-0024, declarative bin registration & pick location
ADR-0025, housekeeping sweeper ADR-0026, MCP eval/governance suite ADR-0027,
bootretry ADR-0028, extra architecture fitness tests ADR-0029, site-scoped
transfer allocation ADR-0030, publish ProductClassified ADR-0031,
confirm-pick event-driven ADR-0032 (superseded by 0035), destination transfer
receipt custody ADR-0033, product-master owns classification ADR-0034,
confirm picks on the LAST pick from TaskCompleted ADR-0035 (the fallback),
reservations store the order line and picks are confirmed per line ADR-0036. The full
index with every Status is `docs/docs/adr/about.md`.
