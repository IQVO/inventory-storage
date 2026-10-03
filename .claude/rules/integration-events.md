---
paths:
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
---

# Cross-service integration events (Kafka)

This service PUBLISHES integration events over Kafka to the fleet's shared
broker. It CONSUMES exactly one sibling topic, `warehouse.facility.events`
(facility-layout), into a local location-classification cache — see
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

## Published today: 2 of 10 catalog events

**Only `StockReserved` and `ReservationRevoked` cross the service boundary.**
The Kafka adapter's `switch` has a `default: return nil` branch that
silently drops every other domain event — deliberate, not an oversight.
`apis/asyncapi.yaml` documents the full 10-event catalog (three of the four
aggregates: StockUnit, Reservation, Bin/Location — `ProductClassified` is
domain-only and is NOT in the AsyncAPI catalog, see
`docs/docs/api-reference/events.md`) and marks every catalog-only message as
such in its own `description`, so a downstream team cannot mistake a
documented event for a wired one.

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

Both events already exist in the domain event list above — do not invent
new event names when wiring a publisher; carry them through with this exact
`data` shape.

Wire `type`s: `com.warehouse.wms.inventory-storage.reservation.StockReserved`
and `com.warehouse.wms.inventory-storage.reservation.ReservationRevoked`
(wes-work-planning dispatches on these exact strings). The analytics topic
carries nine types (entity `stock`/`reservation`/`bin`, see ADR-0024).

Consumers should ignore unknown `type` values (the catalog will grow),
deduplicate on `(source, id)` (Kafka delivery is at-least-once). Every
message is keyed by the reservation id (`ReservationID`), so per-reservation
ordering (StockReserved before its later ReservationRevoked) is guaranteed
regardless of the topic's partition count (ADR-0021) — cross-reservation/
cross-SKU ordering is still not guaranteed, and the authoritative answer
for correctness-sensitive reads is always `GET /inventory/{sku}/usable`,
not the event stream.

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
