---
title: Sequence Diagrams
sidebar_label: Sequence Diagrams
sidebar_position: 10
description: UML sequence diagrams for every command use case exposed over REST, MCP and Kafka, plus the outbox relay and the Kafka consumers — derived from the use-case function bodies, with idempotency, version checks, transaction boundaries and error branches.
---

# Sequence Diagrams

One diagram per command use case (REST and MCP), plus the background flows
(outbox relay, facility location cache, analytics projector) and the one
read with side effects (lazy expiry). Each follows the actual function body:
the order of calls, the `UnitOfWork` transaction boundary (shaded), and the
error branches as `alt`. Unless stated otherwise the diagrams show the
deployed configuration: Postgres (`DATABASE_URL` set) and
`EVENT_PUBLISHER=kafka`, so `Publish` means "insert an `outbox_events` row
in the current transaction". In-memory mode skips the idempotency middleware
and the transaction and logs events instead.

Participants: **Client**, the inbound adapter (**HTTP**, **MCP** or a Kafka
consumer), the **use case**, the **aggregate**, the **repository** port,
**Outbox** (`postgres.OutboxPublisher`, running the Kafka encoders) and the
downstream system.

## 1. ReceiveStock — `POST /stock/receive`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant MW as RequireIdempotencyKey
    participant H as HTTP handler
    participant UC as ReceiveStock
    participant OB as Outbox
    participant DB as Postgres
    C->>MW: POST /stock/receive with Idempotency-Key
    alt header missing
        MW-->>C: 400 idempotency-key-required
    else header present
        MW->>DB: BEGIN, INSERT idempotency_keys ON CONFLICT DO NOTHING
        alt key already committed
            MW->>DB: ROLLBACK, SELECT stored response
            alt same body hash
                MW-->>C: replay stored status, headers and body
            else different body
                MW-->>C: 422 idempotency-key-reused
            end
        else new key
            MW->>H: serve into a recorder, tx on the context
            H->>UC: Execute(sku, qty)
            Note over UC: rejects empty SKU 400 and qty below 1 with 422
            UC->>OB: Publish StockReceived, joins the middleware tx
            OB->>DB: INSERT outbox_events, analytics topic only
            UC-->>H: StagedReceipt
            H-->>MW: 202 sku, quantity, receivedAt
            MW->>DB: UPDATE idempotency_keys with the response, COMMIT
            MW-->>C: 202 Accepted
        end
    end
```

Source: `internal/adapters/inbound/http/idempotency.go`, `server.go`
(`handleReceiveStock`), `internal/application/usecases/receive_stock.go`,
`internal/adapters/outbound/postgres/outbox_publisher.go`.
Omitted: the 500 branches of the middleware; no aggregate is persisted —
a staged receipt is not an aggregate.

## 2. StowStock — `POST /stock/stow`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as HTTP handler
    participant UC as StowStock
    participant LR as LocationRepo
    participant PCR as ProductClassificationRepo
    participant LK as LocationClassificationLookup
    participant SR as StockRepo
    participant AG as Bin and StockUnit
    participant OB as Outbox
    C->>H: POST /stock/stow sku, quantity, binId
    H->>UC: Execute(sku, qty, binId)
    UC->>LR: FindByID(binId)
    alt bin unknown
        UC-->>H: ErrBinNotFound
        H-->>C: 404 bin-not-found
    else bin found
        UC->>PCR: FindBySKU(sku)
        opt SKU classified Hazmat or TemperatureSensitive
            UC->>LK: GetSlotAttributes(binId)
            alt lookup error
                H-->>C: 409 location-classification-unavailable
            else zone known and rule violated
                H-->>C: 409 hazmat-zone-required or temperature-class-mismatch
            end
        end
        opt SKU has a DOT hazard class
            UC->>SR: FindByBin(binId)
            UC->>PCR: FindBySKU for each other occupant SKU
            alt incompatible occupant
                H-->>C: 409 hazmat-class-incompatible
            end
        end
        UC->>SR: NextID()
        UC->>AG: stock.NewStockUnit(id, sku, binId, qty)
        UC->>AG: bin.Occupy(qty)
        alt bin full
            H-->>C: 409 bin-full
        else space available
            rect rgb(235, 235, 235)
                UC->>LR: Save(bin), version-guarded
                UC->>SR: Save(unit)
                UC->>OB: Publish ItemStowed, analytics row
                UC->>OB: Publish LocationRecorded, no row
            end
            alt version conflict
                H-->>C: 409 concurrent-modification
            else committed
                H-->>C: 201 Created, Location /stock/id
            end
        end
    end
```

Source: `internal/application/usecases/stow_stock.go`,
`internal/adapters/outbound/postgres/location_repo.go`, `stock_repo.go`.
Omitted: the 400 and 422 validation in the handler, and the in-memory
`facilitycache` versus HTTP lookup detail (the cache never errors).

## 3. ReserveStock — `POST /reservations`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client e.g. order-management
    participant MW as RequireIdempotencyKey
    participant H as HTTP handler
    participant UC as ReserveStock
    participant RR as ReservationRepo
    participant SR as StockRepo
    participant RES as Reservation
    participant OB as Outbox
    participant K as Kafka relay
    C->>MW: POST /reservations sku, quantity, demandRef, Idempotency-Key
    Note over MW: same key handling as diagram 1, the tx stays on the context
    MW->>H: serve with tx
    alt demandRef empty
        H-->>C: 400 missing-demand-ref
    else
        H->>UC: Execute(sku, qty, demandRef)
        UC->>RR: FindByDemandRef(demandRef)
        UC->>UC: expireAllIfDue, see diagram 9
        alt ACTIVE reservation with same sku and qty
            UC-->>H: existing reservation, replay guard
            H-->>C: 201 same reservation
        else no replay
            UC->>SR: FindBySKU(sku)
            UC->>UC: allocate first-fit, StockUnit.Reserve per unit
            alt sum of usable below qty
                H-->>C: 409 insufficient-usable
            else covered
                UC->>RR: NextID()
                UC->>RES: reservation.New(id, sku, qty, demandRef, allocations, now, 30m)
                rect rgb(235, 235, 235)
                    UC->>SR: Save each touched StockUnit
                    UC->>RR: Save(reservation)
                    UC->>OB: Publish StockReserved
                    Note over OB: two rows, warehouse.inventory.events and warehouse.inventory.analytics, key = reservation id
                end
                UC->>UC: Metrics.ReservationCreated
                H-->>C: 201 Created with allocations and binId
                Note over MW: UPDATE idempotency_keys, COMMIT
                K-)K: relay publishes the rows later, see diagram 10
            end
        end
    end
```

Source: `internal/application/usecases/reserve_stock.go`,
`reservation_expiry.go`, `internal/adapters/inbound/http/server.go`
(`handleReserveStock`), `internal/adapters/outbound/kafka/publisher.go`.
Note: the touched `StockUnit` saves, the reservation save and the publish all
run inside the use case's own `UnitOfWork` scope, so they commit or roll back
together whether or not an outer (middleware) transaction is on the context.
The grey block is that scope.
Omitted: `ErrConcurrentModification` (409) on a racing unit save.

## 4. RevokeReservation — `DELETE /reservations/{id}` and MCP `revoke_reservation`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client or MCP host
    participant IN as HTTP handler or MCP tool
    participant UC as RevokeReservation
    participant RR as ReservationRepo
    participant RES as Reservation
    participant SR as StockRepo
    participant OB as Outbox
    C->>IN: DELETE /reservations/id or tools/call revoke_reservation
    IN->>UC: Execute(reservationId)
    UC->>RR: FindByID(id)
    alt not found
        IN-->>C: 404 reservation-not-found or tool error
    else found
        UC->>UC: expireIfDue
        UC->>RES: Revoke()
        alt not ACTIVE, including just expired
            IN-->>C: 409 reservation-already-resolved
        else ACTIVE
            rect rgb(235, 235, 235)
                loop each allocation
                    UC->>SR: FindByID, ReleaseReservation(qty), Save
                end
                UC->>RR: Save(reservation)
                UC->>OB: Publish ReservationRevoked
                OB->>RR: FindByID, enrich sku, quantity, demand_ref
                Note over OB: rows for warehouse.inventory.events and warehouse.inventory.analytics
            end
            UC->>UC: Metrics.ReservationRevoked
            IN-->>C: 204 No Content or revoked true
        end
    end
```

Source: `internal/application/usecases/revoke_reservation.go`,
`internal/adapters/inbound/mcp/tools.go`, `internal/adapters/outbound/kafka/publisher.go`
(`Encode` enrichment). Omitted: `ErrStockUnitNotFound` and version
conflicts inside the loop (404 and 409, transaction rolled back).

## 5. ConfirmPick — `POST /reservations/{id}/confirm-pick`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as HTTP handler
    participant UC as ConfirmPick
    participant RR as ReservationRepo
    participant RES as Reservation
    participant SR as StockRepo
    participant LR as LocationRepo
    participant OB as Outbox
    Note over C,H: Decided 2026-10-06 (ADR 0035): in production the trigger is fulfillment-execution's<br/>TaskCompleted, consumed here (diagram 13). This REST route stays for operators<br/>and the simulator. No sync call from sibling contexts.
    C->>H: POST /reservations/id/confirm-pick
    H->>UC: Execute(reservationId)
    UC->>RR: FindByID(id)
    alt not found
        H-->>C: 404 reservation-not-found
    else found
        UC->>UC: expireIfDue
        UC->>RES: Confirm(now)
        alt not ACTIVE
            H-->>C: 409 reservation-already-resolved
        else past expiresAt
            H-->>C: 409 reservation-expired
        else confirmed
            rect rgb(235, 235, 235)
                loop each allocation
                    UC->>SR: FindByID, StockUnit.Pick(qty), Save
                    UC->>LR: FindByID(unit bin), Bin.Release(qty), Save
                end
                UC->>RR: Save(reservation)
                UC->>OB: Publish StockPicked, analytics row
            end
            H-->>C: 204 No Content
        end
    end
```

Source: `internal/application/usecases/confirm_pick.go`.
Omitted: `ErrStockUnitNotFound` / `ErrBinNotFound` (404) and version
conflicts (409) inside the transaction.

## 6. RunCycleCount — `POST /bins/{binId}/cycle-count`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as HTTP handler
    participant UC as RunCycleCount
    participant SR as StockRepo
    participant SU as StockUnit
    participant OB as Outbox
    C->>H: POST /bins/binId/cycle-count countedQuantity
    alt countedQuantity missing
        H-->>C: 400 counted-quantity-required
    else present
        H->>UC: Execute(binId, counted)
        UC->>SR: FindByBin(binId)
        UC->>UC: system qty = sum of units not UNLOCATED or REMOVED
        rect rgb(235, 235, 235)
            alt counted equals system
                UC->>OB: Publish CycleCountCompleted, discrepancy false
            else counted above system, overage
                UC->>OB: Publish DiscrepancyDetected
                UC->>OB: Publish CycleCountCompleted, discrepancy true
            else counted below system, shortfall
                UC->>OB: Publish DiscrepancyDetected
                loop units until the shortfall is covered
                    UC->>SU: MarkUnlocated()
                    UC->>SR: Save(unit)
                    UC->>OB: Publish ItemUnlocated
                end
                UC->>OB: Publish CycleCountCompleted, discrepancy true
            end
        end
        H-->>C: 200 binId, countedQuantity, systemQuantity, discrepancy
    end
```

Source: `internal/application/usecases/run_cycle_count.go`.
Omitted: an unknown bin is not an error here — it simply has system
quantity 0.

## 7. ApplyProductClassification — product-master's `ProductClassified` (ADR 0034)

```mermaid
sequenceDiagram
    autonumber
    participant K as warehouse.product-master.events
    participant CON as ProductMasterConsumer
    participant UC as ApplyProductClassification
    participant PE as processed_events
    participant PCR as product_classifications
    K->>CON: FetchMessage
    alt not a CloudEvent, other type, bad payload or invariant
        CON->>K: CommitMessages, skip
    else ProductClassified
        CON->>UC: Execute(eventId, sku, tags, classes, source, version)
        rect rgb(235, 235, 235)
            UC->>PE: Claim(consumer, eventId)
            UC->>PCR: upsert WHERE stored version < version
        end
        alt transient DB error
            CON->>CON: capped backoff, retry same message
        else applied or no-op
            CON->>K: CommitMessages
        end
    end
```

`PUT /products/{sku}/classification` answers `410 classification-moved`
without reading the body. Source:
`internal/adapters/inbound/kafka/product_master_consumer.go`,
`internal/application/usecases/apply_product_classification.go`,
`internal/adapters/outbound/postgres/product_classification_repo.go`.
Omitted: the deprecated read `GET /products/{sku}/classification`, a direct
repository lookup on the local copy with no use case.

## 8. RegisterBin — `PUT /bins/{binId}`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as HTTP handler
    participant UC as RegisterBin
    participant LR as LocationRepo
    participant B as Bin
    C->>H: PUT /bins/binId capacity
    alt capacity missing
        H-->>C: 400 capacity-required
    else above int32
        H-->>C: 422 capacity-out-of-range
    else valid
        H->>UC: Execute(binId, capacity)
        rect rgb(235, 235, 235)
            UC->>LR: FindByID(binId)
            alt absent
                UC->>B: location.NewBin(id, capacity)
                UC->>LR: Save(bin)
            else same capacity
                Note over UC: BinUnchanged, nothing written
            else different capacity
                UC->>B: Resize(capacity)
                alt below occupancy
                    H-->>C: 409 capacity-below-occupancy
                else resized
                    UC->>LR: Save(bin), version-guarded
                end
            end
        end
        H-->>C: 201 created with Location, or 200 unchanged or resized
    end
```

Source: `internal/application/usecases/register_bin.go`,
`internal/adapters/inbound/http/server.go` (`handleRegisterBin`).
Omitted: 422 `invalid-bin-capacity` for capacity below 1, and 409
`concurrent-modification` when a stow races the resize. No event is
published (ADR 0025).

## 9. Lazy expiry — `GET /reservations?demandRef=`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client e.g. ops-agent or inventory-mfe
    participant H as HTTP handler
    participant UC as GetReservationsByDemandRef
    participant RR as ReservationRepo
    participant SR as StockRepo
    participant RES as Reservation
    participant OB as Outbox
    C->>H: GET /reservations?demandRef=ref
    alt demandRef empty
        H-->>C: 400 missing-demand-ref
    else
        H->>UC: Execute(ref)
        UC->>RR: FindByDemandRef(ref)
        loop each reservation, expireAllIfDue
            opt ACTIVE and past expiresAt
                rect rgb(235, 235, 235)
                    UC->>SR: release each allocation, Save units
                    UC->>RES: Expire()
                    UC->>RR: Save(reservation)
                    UC->>OB: Publish ReservationExpired, analytics row
                end
            end
        end
        H-->>C: 200 array, empty for an unknown demandRef
    end
```

Source: `internal/application/usecases/get_reservations_by_demand_ref.go`,
`reservation_expiry.go`. The same `expireIfDue` runs inside diagrams 3, 4
and 5. Each expired reservation gets its own transaction. Omitted: the
read-only `GET /inventory/{sku}/usable` and `GET /bins/{binId}` (no side
effects).

## 10. Outbox relay to Kafka

```mermaid
sequenceDiagram
    autonumber
    participant RL as OutboxRelay in cmd/inventory
    participant DB as Postgres outbox_events
    participant RS as RelaySink
    participant K as Kafka
    participant WP as wes-work-planning
    loop every OUTBOX_RELAY_INTERVAL, default 1s
        RL->>DB: BEGIN, SELECT unpublished ORDER BY id LIMIT batch FOR UPDATE SKIP LOCKED
        loop each row in id order
            RL->>RS: Send(topic, key, value, headers)
            RS->>K: WriteMessages, Hash balancer on key
            alt send failed
                RL->>DB: UPDATE attempts + 1, last_error, COMMIT, stop this pass
            else sent
                RL->>DB: UPDATE published_at = now()
            end
        end
        RL->>DB: COMMIT
    end
    K-)WP: StockReserved and ReservationRevoked on warehouse.inventory.events
```

Source: `internal/adapters/outbound/postgres/outbox_relay.go`,
`internal/adapters/outbound/kafka/relay_sink.go`, `cmd/inventory/main.go`.
Omitted: the housekeeping `Sweeper` that later deletes published rows older
than `OUTBOX_RETENTION` (ADR 0026). `cmd/mcp` writes outbox rows but runs no
relay — the relay in `cmd/inventory` drains them.

## 11. Facility location cache — consuming `warehouse.facility.events`

```mermaid
sequenceDiagram
    autonumber
    participant MAIN as cmd/inventory startup
    participant FC as facilitycache.Consumer
    participant K as Kafka warehouse.facility.events
    participant DLQ as warehouse.facility.events.dlq
    participant UC as StowStock
    MAIN->>FC: NewConsumer, per-process group, FirstOffset
    FC->>K: read end offsets as the readiness target
    MAIN->>FC: WaitReady, 60s timeout
    loop Run
        K->>FC: message
        FC->>FC: apply, cloudevents.Decode then switch on type
        alt ZoneRegistered
            FC->>FC: zones by zoneId, hazmat and temperature class
        else LocationSlotRegistered
            FC->>FC: slots by locationCode, to zoneId
        else LocationSlotDecommissioned
            FC->>FC: delete slot
        else any other type
            FC->>FC: ignore
        else apply failed, invalid CloudEvent or bad data
            FC->>DLQ: raw payload with x-dlq-source-topic, x-dlq-error, x-dlq-failed-at
        end
        FC->>FC: observe offset, mark ready when targets reached
    end
    Note over MAIN: HTTP starts serving only after WaitReady
    UC->>FC: GetSlotAttributes(binId), in memory, never errors
```

Source: `internal/adapters/outbound/facilitycache/consumer.go`,
`cmd/inventory/main.go` (`buildLocationLookup`).
Omitted: the `bootretry` around the initial dial and the empty-cache
warning.

## 12. Analytics projector — consuming `warehouse.inventory.analytics`

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka warehouse.inventory.analytics
    participant AC as AnalyticsConsumer in cmd/inventory-projector
    participant PE as ConsumedEventsRepo
    participant PR as PostgresProjection
    participant RD as inventory-reports
    participant OA as warehouse-ops-agent
    loop Run, group inventory-analytics
        K->>AC: ReadMessage
        AC->>AC: cloudevents.Decode
        alt not a valid CloudEvent
            AC->>AC: WARN and skip
        else type outside the projection
            AC->>AC: acknowledge, nothing written
        else projecting type
            AC->>PE: MarkProcessed(event id)
            alt already processed
                AC->>AC: skip
            else new
                AC->>PR: Apply by type, upsert flow_accuracy_rollup per sku, bin, hour
            end
        end
    end
    OA->>RD: GET /reports/flow-accuracy
    RD->>PR: read-only query
```

Source: `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/consumed_events_repo.go`,
`postgres_projection.go`, `internal/adapters/inbound/http/reports_handler.go`.
Omitted: tracing spans and the freshness endpoint.

## 13. ConfirmPicksForOrder — fulfillment-execution's `TaskCompleted`, confirm exactly the picked line (ADR 0036; counting fallback ADR 0035)

```mermaid
sequenceDiagram
    autonumber
    participant K as warehouse.fulfillment.events
    participant CON as TaskCompletedConsumer
    participant UC as ConfirmPicksForOrder
    participant CP as ConfirmPick
    participant PE as processed_events
    participant PP as order_pick_progress
    participant RR as ReservationRepo
    participant OB as Outbox
    participant DLQ as warehouse.fulfillment.events.dlq
    K->>CON: FetchMessage
    alt not a CloudEvent
        CON->>K: CommitMessages, skip with a sampled WARN
    else other type
        CON->>K: CommitMessages
    else TaskCompleted with an undecodable payload (incl. a non-integer line_no)
        CON->>DLQ: raw message plus x-dlq headers
        CON->>K: CommitMessages, only after the DLQ write succeeded
    else TaskCompleted
        CON->>UC: Execute(eventId, taskType, orderRef, lineNo?)
        alt task_type is not PICK or order_ref is empty
            UC-->>CON: IGNORED, nothing claimed
        else line_no outside 1..2147483647
            UC-->>CON: malformed, dead-lettered like any poison message
        else PICK for an order
            rect rgb(235, 235, 235)
                UC->>PE: Claim(task-completed-confirm-pick, eventId)
                alt already claimed
                    UC-->>CON: DUPLICATE, nothing is touched
                else first delivery
                    UC->>RR: FindByDemandRef(orderRef)
                    alt no reservation for the order
                        UC-->>CON: NO_RESERVATIONS, a no-op
                    else line_no present and some reservation has that line_no
                        Note over UC: PER-LINE path (ADR 0036): only that line, nothing is counted
                        loop each reservation of (orderRef, line_no), in id order
                            alt CONFIRMED or REVOKED
                                UC->>UC: skip and count
                            else EXPIRED, or ACTIVE past its timeout
                                UC->>UC: lazy expiry returns the stock, skip, count, WARN
                            else ACTIVE
                                UC->>CP: Execute(reservationId)
                                CP->>OB: StockUnit.Pick, Bin.Release, Publish StockPicked
                            end
                        end
                        UC-->>CON: LINE_SETTLED, other lines stay ACTIVE
                    else line_no present, none has it, and no reservation lacks a line_no
                        UC-->>CON: LINE_NOT_FOUND, a no-op
                    else no line_no, or line_no present and only legacy reservations (line_no NULL) can serve it
                        Note over UC: COUNTING fallback (ADR 0035), over all reservations when the event has no line_no, over the line-less ones only when it has one
                        Note over UC: needed = ACTIVE + CONFIRMED (REVOKED and EXPIRED do not count)
                        alt needed is 0
                            UC-->>CON: NOTHING_TO_CONFIRM, no progress row is left
                        else
                            UC->>PP: upsert picked_tasks + 1 (same transaction as the claim)
                            alt picked_tasks is below needed
                                UC-->>CON: AWAITING_LAST_PICK, nothing is confirmed
                            else picked_tasks reached needed but no ACTIVE reservation is left
                                UC-->>CON: NOTHING_TO_CONFIRM
                            else the LAST pick
                                loop each counted reservation, in id order
                                    alt CONFIRMED or REVOKED
                                        UC->>UC: skip and count
                                    else EXPIRED, or ACTIVE past its timeout
                                        UC->>UC: lazy expiry returns the stock, skip, count, WARN
                                    else ACTIVE
                                        UC->>CP: Execute(reservationId)
                                        CP->>OB: StockUnit.Pick, Bin.Release, Publish StockPicked
                                    end
                                end
                            end
                        end
                    end
                end
            end
        end
        alt transient failure, rolled back with the claim (and the counter)
            CON->>CON: capped backoff, retry the same message, 5 attempts
            CON->>DLQ: then dead-letter, and only then commit
        else settled
            CON->>K: CommitMessages
        end
    end
```

A PICK task is per order **line**. Since ADR 0036 the `Reservation` stores the
order line it was made for (`reservations.line_no`, sent by order-management as
`lineNo`) and `TaskCompleted` carries the task's `line_no`, so a pick confirms
**exactly its own line**: lines 1 and 3 picked, line 2 not, confirms 1 and 3 and
leaves 2 `ACTIVE` with its stock still reserved. Nothing is counted on that path
and no `order_pick_progress` row is written, so ADR 0035's "one pick early"
edge (a revoked or expired line shifting the count) is closed for line-aware
events. The counting of ADR 0035 stays as the backward-compatible fallback: for
an event without `line_no`, or for reservations made before `line_no` existed
(`NULL`), the consumer counts the order's completed PICK tasks and confirms only
on the **last** one (confirming early would mark unpicked lines as picked with no
undo, confirming late is safe: the reservation keeps the stock unavailable, and
expires lazily if it is never confirmed). The counter row is deleted by the
housekeeping sweeper after `ORDER_PICK_PROGRESS_RETENTION` (default 30 days). A
redelivered event id is stopped by the claim before anything is touched. No
reservation for the order (a transfer or a non-inventory order) settles as a
successful no-op. Short picks are not modelled (a Task carries no SKU or
quantity). Source:
`internal/adapters/inbound/kafka/task_completed_consumer.go`,
`internal/application/usecases/confirm_picks_for_order.go`, `confirm_pick.go`,
`internal/adapters/outbound/postgres/order_pick_progress_repo.go`, `sweeper.go`,
`cmd/inventory/confirmpick.go`. Omitted: the `bootretry` around the first
broker dial and tracing spans.
