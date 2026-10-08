---
paths:
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
---

# Cross-service integration events (Kafka)

This service PUBLISHES integration events over Kafka to the fleet's shared
broker. It CONSUMES four sibling topics: `warehouse.facility.events`
(facility-layout) into a local location-classification cache,
`warehouse.network-inventory-planning.events` (transfer commands),
`warehouse.product-master.events` (product-master, ADR-0034) into the local
copy of product classifications, and `warehouse.fulfillment.events`
(fulfillment-execution `TaskCompleted`, ADR-0035) to confirm picks — see
"Consumed" below.

## Envelope: CloudEvents 1.0, mandatory (ADR-0024)

Every message this service produces or consumes — integration
(`warehouse.inventory.events`), analytics (`warehouse.inventory.analytics`)
and facility-layout's `warehouse.facility.events` — is a CloudEvents 1.0
event in structured mode. There is NO flat envelope, NO dual-write/dual-read,
NO envelope toggle.

```json
{
  "specversion": "1.0",
  "id": "uuid-v4 (minted once in Encode, persisted in the outbox row)",
  "source": "/warehouse/inventory-storage",
  "type": "com.warehouse.wms.inventory-storage.reservation.StockReserved",
  "subject": "res-1",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:inventory-storage:events:StockReserved:v1",
  "data": { "sku": "SKU-1", "quantity": 5, "demand_ref": "order-42" }
}
```

- Build/decode ONLY via `internal/adapters/kafka/cloudevents` (`New`,
  `Decode`, `ContentTypeHeader`, `Type`, `DataSchema`), which wraps
  `github.com/cloudevents/sdk-go/v2/event`. Never hand-roll an envelope
  struct; never use the SDK's protocol/client packages (transport stays
  kafka-go).
- Every produced message carries `cloudevents.ContentTypeHeader()` plus the
  W3C trace headers. Kafka key = aggregate id, `kafkago.Hash{}` balancer.
- `data` shapes are frozen: a breaking change is a new `.v2` type + new
  `dataschema` version, never a mutation.
- Consumers dispatch on the FULL `type` (never a suffix), ignore unknown
  types, dedupe on `id`, and DLQ (facility cache) or WARN-and-skip
  (analytics projector) anything `cloudevents.Decode` rejects.

### Required attributes (moved from CLAUDE.md)

- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- `specversion=1.0`, `id` (UUID, stable across outbox redelivery),
  `source=/warehouse/inventory-storage`, `type`, `subject` (aggregate id),
  `time` (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:inventory-storage:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wms.inventory-storage.<entity>.<EventName>`.
- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write, no
  dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Consumers DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.
- Full standard and the fleet's cross-service type catalogue: ADR-0024
  (`docs/docs/adr/`).

## Kafka

- Client library: `github.com/segmentio/kafka-go`.
- Broker: `KAFKA_BROKERS` env var (default `localhost:9092`). There is ONE
  broker platform-wide — the in-cluster Kafka deployed by `warehouse-infra`,
  exposed on the host at `localhost:9092`. Do not add a Kafka service to this
  repo's own `docker-compose.yml`.
- Adapter package: `internal/adapters/outbound/kafka/`, implementing
  `ports.EventPublisher`. Selected via `EVENT_PUBLISHER=kafka|log` (default
  `log`).
- Topic: `warehouse.inventory.events`.

## Published today: 5 of 13 catalog events

**`StockReserved`, `ReservationRevoked`, `TransferStockAllocated`,
`TransferStockAllocationRejected` and the legacy `ProductClassified` cross
the service boundary.** Since ADR-0034 `ProductClassified` is raised by no
write path: only the one-shot `republish-product-classifications` backfill
re-emits it (integration topic only, through the outbox); its encoder
mapping and goldens are kept for that command and removed at product-master
ADR 0003 stage E.
The Kafka adapter's `switch` has a `default: return nil` branch that
silently drops every other domain event — deliberate, not an oversight.
`apis/asyncapi.yaml` documents the full 13-message catalog (all four
aggregates: StockUnit, Reservation, Bin/Location, ProductClassification —
`LocationRecorded` stays in-process) plus the consumed product-master
message, and marks every catalog-only message as such in its own
`description`, so a downstream team cannot mistake a documented event for a
wired one.

- **StockReserved** — `data`: `{"sku": "...", "quantity": N, "demand_ref": "..."}`.
  Raised by `ReserveStock` when a reservation is successfully created
  against usable inventory. Downstream: `wes-work-planning` decrements its
  observed usable count for that SKU (`UsableInventoryObserved` read model).
- **ReservationRevoked** — `data`: `{"sku": "...", "quantity": N, "demand_ref": "..."}`.
  Raised by `RevokeReservation`. The domain event itself carries only the
  reservation id; the Kafka adapter **enriches** it by re-reading the
  reservation via `ports.ReservationRepo` at publish time (fails rather than
  emitting a partial payload if the lookup misses).
  Downstream: `wes-work-planning` increments its observed usable count back.

- **TransferStockAllocated** — `data`: `{"transfer_id", "transfer_line_id", "origin_site_id", "reservation_id", "sku", "quantity", "allocations": [{"stock_unit_id", "bin_id", "quantity"}], "expires_at"}`.
  Reply leg of the transfer-allocation command/reply (ADR-0030): raised by
  `AllocateTransferStock` when a network transfer line's stock was held.
  Key/subject = reservation id. Integration topic only (no analytics variant).
- **TransferStockAllocationRejected** — `data`: `{"transfer_id", "transfer_line_id", "origin_site_id", "sku", "requested_quantity", "reason"}`.
  `reason` is the closed set `ORIGIN_SITE_UNKNOWN | INSUFFICIENT_USABLE |
  IDEMPOTENCY_CONFLICT`. Key/subject = transfer_line_id. Integration topic only.

Both events already exist in the domain event list above — do not invent
new event names when wiring a publisher; carry them through with this exact
`data` shape.

Wire `type`s: `com.warehouse.wms.inventory-storage.reservation.StockReserved`
and `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked`
(wes-work-planning dispatches on these exact strings). The analytics topic
carries ten types (entity `stock`/`reservation`/`bin`/`product`, see ADR-0024
and ADR-0033).

Consumers should ignore unknown `type` values (the catalog will grow),
deduplicate on `(source, id)` (Kafka delivery is at-least-once). Every
message is keyed by the reservation id (`ReservationID`), so per-reservation
ordering (StockReserved before its later ReservationRevoked) is guaranteed
regardless of the topic's partition count (ADR-0021) — cross-reservation/
cross-SKU ordering is still not guaranteed, and the authoritative answer
for correctness-sensitive reads is always `GET /inventory/{sku}/usable`,
not the event stream.

## Consumed: `warehouse.product-master.events` (ADR-0034)

- Adapter: `internal/adapters/inbound/kafka/product_master_consumer.go`
  (`ProductMasterTopic`). Started only when `PRODUCT_MASTER_CONSUMER_GROUP`
  is set (a stable group id from configuration; unset = not started). Also
  requires `DATABASE_URL` and `KAFKA_BROKERS`, else boot fails.
- Dispatches ONLY on the full type
  `com.warehouse.wms.product-master.product.ProductClassified`, `data`
  `{sku, handling_tags[], temperature_class?, dot_hazard_class?, classification_source, version}`;
  `ProductRegistered`, `ProductDescriptionChanged`,
  `ProductDimensionsDeclared`, `ProductMeasured` (and anything else) are
  committed past untouched.
- `ApplyProductClassification` claims the CloudEvents `id` in
  `processed_events` (consumer `product-master-classification`) and upserts
  `product_classifications` in ONE UnitOfWork; the upsert applies only when
  `version` > stored version (legacy rows are 0). It raises NO domain event
  (no publish loop back to product-master).
- At-least-once checklist: `FetchMessage`, commit after the handler settles,
  capped-backoff retry of the SAME message on transient (DB) errors; not a
  CloudEvent / undecodable payload / invariant violation -> WARN and commit
  past.
- Integration test: `internal/application/usecases/product_master_handover_integration_test.go`
  (testcontainers Kafka + Postgres: event -> local copy -> hazmat stow
  placement).

## Consumed: `warehouse.fulfillment.events` (ADR-0035)

- Adapter: `internal/adapters/inbound/kafka/task_completed_consumer.go`.
  Selected by `TASK_COMPLETED_CONSUMER_MODE=kafka` (default `off`). Consumer
  group `TASK_COMPLETED_CONSUMER_GROUP` (default `inventory-storage-confirm-pick`).
  Malformed messages go to `warehouse.fulfillment.events.dlq` at once; transient
  failures retry with capped backoff, then dead-letter.
- Dispatches ONLY on the full type
  `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` with `task_type`
  `PICK` and a non-empty `order_ref` (the ORDER id = a reservation's `demand_ref`;
  fulfillment-execution's order-ref decision, documented in that repo);
  everything else is committed past.
- The event may carry an optional positive integer `line_no` (a non-positive one
  is dead-lettered as malformed). `ConfirmPicksForOrder` claims the CloudEvents
  `id` in `processed_events` (consumer `task-completed-confirm-pick`) and
  decides in ONE UnitOfWork:
  - PER-LINE path (ADR-0036): the event names a line and a reservation of the
    order stores that `line_no` -> confirm exactly that line's ACTIVE
    reservation via `ConfirmPick` (outcome `LINE_SETTLED`); NO
    `order_pick_progress` row is written. Reservations carry `line_no` because
    order-management sends `lineNo` when it reserves.
  - COUNTING fallback (ADR-0035): no `line_no` on the event, or only line-less
    (pre-`line_no`) reservations -> count the pick in `order_pick_progress` and
    confirm the order's ACTIVE reservations only when the count reaches the
    confirmable ones (ACTIVE + CONFIRMED), i.e. on the LAST pick: late is safe
    (the reservation holds the stock), early would mark unpicked lines as
    picked. A line no reservation carries and no line-less reservation left is
    `LINE_NOT_FOUND`, a successful no-op.
  Short picks are not modelled. Expired reservations are skipped and counted
  (`inventory.pick_confirmations{outcome=expired}`).
- The sweeper deletes `order_pick_progress` rows older than
  `ORDER_PICK_PROGRESS_RETENTION` (default 720h; chart
  `config.orderPickProgressRetention`; `0` disables).

## Consumed: `warehouse.network-inventory-planning.events` (ADR-0030)

- Adapter: `internal/adapters/inbound/kafka/transfer_consumer.go` — the
  command side of the transfer-allocation exchange. Selected by
  `TRANSFER_ALLOCATION_CONSUMER_MODE=kafka` (default `off`; also requires
  `DATABASE_URL` — the decision is only atomic through the Postgres
  UnitOfWork/outbox, so an in-memory run must not consume real commands).
  Consumer group: `TRANSFER_ALLOCATION_CONSUMER_GROUP` (default
  `inventory-storage-transfer-allocation`).
- Dispatches ONLY on the full type
  `com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested`
  with `data` `{transfer_id, transfer_line_id, origin_site_id, sku, quantity}`;
  every other type on the topic is acknowledged untouched.
- Run loop follows the at-least-once atomicity checklist: `FetchMessage`
  (never auto-committing `ReadMessage`), commit only after the handler
  settles, capped-backoff retry of the SAME message on transient errors,
  deterministic poison (not CloudEvents, malformed command) logged and
  committed past — never retried, never blocking.
- Idempotency is the `transfer_allocations` ledger (DB-unique
  `transfer_line_id`): a replayed command returns the original outcome
  without touching stock or republishing; the same line id with a
  DIFFERENT payload answers `IDEMPOTENCY_CONFLICT` while the original
  decision stands.

## Consumed: `warehouse.facility.events` (ADR-0013)

- Adapter: `internal/adapters/outbound/facilitycache/`, implementing
  `ports.LocationClassificationLookup` for `StowStock`'s hazmat /
  temperature-class placement rules. Selected by
  `LOCATION_LOOKUP_MODE=kafka` (requires `KAFKA_BROKERS`); `http` is the
  synchronous facility-layout rollback, `permissive` (default) does no lookup.
- Applies `com.warehouse.wms.facility-layout.zone.ZoneRegistered`,
  `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` and
  `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned`
  (FULL type match) into an in-memory zone/slot map. A message that is not a
  valid CloudEvent is dead-lettered to `<topic>.dlq` and readiness still
  advances past it. Replays from `FirstOffset` on every start under a
  per-process-unique consumer group (`consumerGroupPrefix` +
  `uniqueConsumerGroup()`), and `cmd/inventory` blocks startup until the
  replay completes (`WaitReadyTimeout`, 60s).
- Integration test uses testcontainers
  (`consumer_integration_test.go`), never an external broker.

## Definition of done for any new/changed publisher

- New/changed adapter compiles and has a golden exact-JSON test per
  published type (all CloudEvents attributes + `content-type` header — see
  `internal/adapters/outbound/kafka/golden_test.go`); every consumer has a
  legacy-flat-message-rejected test.
- Existing full suite (`go build ./...`, `go vet ./...`, `go test ./...`,
  `go test ./... -race`) stays green.
- README's "Integration" section stays current: topic published, exact JSON
  schemas, the `KAFKA_BROKERS`/`EVENT_PUBLISHER` env vars.
- Do a REAL smoke test: with the shared broker running and
  `EVENT_PUBLISHER=kafka`, call the relevant endpoint against the running
  binary and confirm the message lands on `warehouse.inventory.events` via
  `kafka-console-consumer.sh --from-beginning` (or an equivalent one-off Go
  consumer) before declaring done.
