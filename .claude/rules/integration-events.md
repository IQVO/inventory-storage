# Cross-service integration events (Kafka)

This service PUBLISHES integration events over Kafka to the fleet's shared
broker. It CONSUMES exactly one sibling topic, `warehouse.facility.events`
(facility-layout), into a local location-classification cache — see
"Consumed" below.

## Envelope shipped today: the legacy flat envelope

`internal/adapters/outbound/kafka/publisher.go` currently emits the
**legacy flat warehouse envelope** (identical across all warehouse-systems
services), not the CloudEvents attributes `apis/asyncapi.yaml` documents as
the platform target:

```json
{
  "event_id": "uuid-v4",
  "event_type": "StockReserved",
  "occurred_at": "2026-08-21T22:00:00Z",
  "source": "inventory-storage",
  "data": { }
}
```

The `data` payloads for `StockReserved` and `ReservationRevoked` match the
AsyncAPI document field-for-field; the CloudEvents context attributes
(`specversion`, `id`, `source`, `type`, `subject`, `time`,
`datacontenttype`) describe the target the platform is standardising on.
`apis/asyncapi.yaml`'s own `info.description` states this gap explicitly —
it is documented, not a surprise. See `docs/docs/api-reference/events.md`
and `docs/docs/ddd/domain-events.md` for the full narrative (both envelopes
shown in full, the CloudEvents `type` reverse-DNS convention, and the
"What ships today" caveat).

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

## Published today: 2 of 11 catalog events

**Only `StockReserved` and `ReservationRevoked` cross the service boundary.**
The Kafka adapter's `switch` has a `default: return nil` branch that
silently drops every other domain event — deliberate, not an oversight.
`apis/asyncapi.yaml` documents the full 11-event catalog (all four
aggregates: StockUnit, Reservation, Bin/Location, ProductClassification) and
marks every catalog-only message as such in its own `description`, so a
downstream team cannot mistake a documented event for a wired one.

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

Consumers should tolerate unknown `type` values (the catalog will grow),
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
- Applies `ZoneRegistered`, `LocationSlotRegistered`,
  `LocationSlotDecommissioned` (matched on the trailing event name) into an
  in-memory zone/slot map. Replays from `FirstOffset` on every start under a
  per-process-unique consumer group (`consumerGroupPrefix` +
  `uniqueConsumerGroup()`), and `cmd/inventory` blocks startup until the
  replay completes (`WaitReadyTimeout`, 60s).
- Integration test uses testcontainers
  (`consumer_integration_test.go`), never an external broker.

## Definition of done for any new/changed publisher

- New/changed adapter compiles and is unit-tested (e.g. against an
  in-memory kafka-go writer fake, or by asserting the envelope shape
  produced).
- Existing full suite (`go build ./...`, `go vet ./...`, `go test ./...`,
  `go test ./... -race`) stays green.
- README's "Integration" section stays current: topic published, exact JSON
  schemas, the `KAFKA_BROKERS`/`EVENT_PUBLISHER` env vars.
- Do a REAL smoke test: with the shared broker running and
  `EVENT_PUBLISHER=kafka`, call the relevant endpoint against the running
  binary and confirm the message lands on `warehouse.inventory.events` via
  `kafka-console-consumer.sh --from-beginning` (or an equivalent one-off Go
  consumer) before declaring done.
