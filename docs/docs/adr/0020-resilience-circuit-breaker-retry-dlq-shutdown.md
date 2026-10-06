---
id: 0020-resilience-circuit-breaker-retry-dlq-shutdown
slug: /adr/0020-resilience-circuit-breaker-retry-dlq-shutdown
title: "20. Circuit breaker, read-only retry, Kafka DLQ, and graceful shutdown hardening"
sidebar_label: "20. Circuit breaker, retry, DLQ, shutdown"
sidebar_position: 20
description: "ADR 0020 — Phase 2 resilience for inventory-storage, mirroring order-management's ADR-0025/PR #107 verbatim: a sony/gobreaker/v2 circuit breaker around the facilitylayout HTTP client that reuses its EXISTING fail-open behaviour as the OPEN-state fallback rather than inventing a new one; cenkalti/backoff/v4 jittered retry on the read-only GetSlotAttributes call; a dead-letter topic for the facility-location-cache Kafka consumer so one malformed message cannot block its partition; and a readiness-flip-first graceful shutdown sequence."
---

# 20. Circuit breaker, read-only retry, Kafka DLQ, and graceful shutdown hardening

## Status

Accepted — implemented in the same change that introduced this record.
This is Phase 2 (resilience) of the fleet production-readiness plan,
mirroring order-management's ADR-0025 (PR #107, merged into
`develop`), the fleet's reference implementation for this phase,
verbatim in design.

## Context

Before this change, inventory-storage had exactly one synchronous
cross-context HTTP client, `facilitylayout.Client`
(`GET /locations/{locationCode}/classification`), selected via
`LOCATION_LOOKUP_MODE=http` (default `permissive`; a third mode,
`kafka`, replaces the HTTP call entirely with a local read model fed by
facility-layout's own `warehouse.facility.events` topic — see
ADR-0013). The HTTP client had **no circuit breaker, no bounded retry,
and no context-deadline propagation**: a slow or failing
facility-layout deployment would hang every `http`-mode caller until a
fresh, hardcoded per-call timeout (`facilitylayout.DefaultTimeout`, 5s)
elapsed, on every single request, with no mechanism to stop hammering a
struggling dependency.

The `kafka`-mode consumer (`facilitycache.Consumer`, ADR-0013) had no
dead-letter handling for a message its `apply()` cannot parse
(malformed envelope/data JSON, a required field missing): it logged the
error and silently moved on, no different from a network partition
tearing the message off the wire — there was no durable, replayable
record of WHICH message failed or WHY, only a log line that scrolls
away.

Graceful shutdown already existed
(`signal.NotifyContext`+`httpServer.Shutdown`, `cmd/inventory/main.go`)
from Phase 0/1 work, but had no readiness-flip step ahead of the HTTP
`Shutdown` call, and no bounded wait for the `kafka`-mode consumer's
`Run` goroutine to actually stop before the process exits.

## Decision

### 1. One circuit breaker for the ONE synchronous cross-context dependency

`internal/resilience` (new, tiny, dependency-free top-level package —
the same "outbound adapters may depend on other top-level
`internal/*` helper packages" pattern already established by
`internal/pgtx`/`internal/adapters/outbound/bootretry`) holds the same
shared tuning order-management's `internal/resilience` uses, byte-for-
byte comparable across the two repos:

```go
const (
    DefaultMaxRequests = 1                // 1 probe per half-open cycle
    DefaultInterval    = 30 * time.Second // closed-state rolling-Counts reset window
    DefaultTimeout     = 30 * time.Second // open-state cooldown before a half-open probe
)

func ReadyToTrip(counts gobreaker.Counts) bool {
    if counts.ConsecutiveFailures >= 5 {
        return true
    }
    if counts.Requests < 10 { // minimum sample size before the error-rate leg engages
        return false
    }
    return float64(counts.TotalFailures)/float64(counts.Requests) > 0.5
}
```

This service has exactly one synchronous cross-context HTTP client
today (`facilitylayout.Client`), so `facilitylayout.NewBreakerClient`
is the only call site that constructs a breaker using this shared
tuning — still designed, like order-management's, as one breaker PER
downstream dependency rather than one global breaker, so a future
second HTTP dependency gets its own independent breaker/bulkhead
without touching this one.

### 2. The breaker's OPEN-state fallback REUSES the existing fail-open behaviour — it does not invent a new one

`facilitylayout.BreakerClient`, while OPEN, calls
`PermissiveLookup.GetSlotAttributes` — the SAME fail-OPEN contract
(`Known=false`, `nil` error) `Client.GetSlotAttributes` already
produces for a 404 (a legitimate "this location isn't modeled yet"
answer). A location classification lookup is a soft
routing/enrichment input to `StowStock`'s placement check, never a
mutation of real state, so a missing or failed lookup must never block
a stow — exactly the fail-open contract this client already had before
any breaker existed.

`isBreakerRejection(err)` distinguishes gobreaker refusing to even
ATTEMPT the call (`gobreaker.ErrOpenState`/`ErrTooManyRequests`) from a
real error a call gobreaker DID let through; only the former routes to
the fallback — a real error from an attempted call (a transport error,
`facilitylayout.ErrUnexpectedStatus`) propagates unchanged, exactly as
before this breaker existed. `StowStock`'s own `checkPlacement` is what
decides fail-open vs. fail-closed based on whether the SKU is
classified — this adapter's only job is to surface the real error once
retry/breaker are exhausted (see `client.go`), unchanged from before.

### 3. Context deadline propagation: `resilience.CallTimeout`

```go
func CallTimeout(ctx context.Context, maxPerCall time.Duration) (context.Context, context.CancelFunc)
```

`facilitylayout.BreakerClient.GetSlotAttributes` derives its timeout
from the inbound request's own remaining `ctx.Deadline()`, capped at
`resilience.DefaultTimeout` (30s) when that remaining budget is larger
or absent — never a fresh, hardcoded timeout that could outlast the
caller's own patience.

### 4. Bulkhead: confirmed, not newly built

`facilitylayout.NewClient` already constructs its own `*http.Client`
(defaulting when a nil `HTTPDoer` is passed) — this is the only
synchronous cross-context HTTP client in the service, so there was
never a shared client to begin with. This ADR only confirms that
invariant.

**The default client carries no `http.Client.Timeout`.** The original
default (`&http.Client{Timeout: facilitylayout.DefaultTimeout}`, 5s) was
a fixed per-request wall clock that silently overrode the §3 context
deadline — the very thing §3 exists to prevent — because
`cmd/inventory` passes a nil doer. `facilitylayout.DefaultTimeout` is
removed; `Client.GetSlotAttributes` now derives each ATTEMPT's bound
with `resilience.CallTimeout(ctx, resilience.DefaultTimeout)` (the
caller's deadline when sooner, else the 30s cap), and
`BreakerClient` additionally bounds the whole retry loop the same way.
Proven by `client_deadline_test.go` (default doer has `Timeout == 0`;
request context carries the caller's exact deadline, or ~30s when none).

### 5. Retry (`cenkalti/backoff/v4`, jittered, max 3 attempts) on `GetSlotAttributes`

`facilitylayout.BreakerClient.retryingFetch` retries
`Client.GetSlotAttributes` (a pure GET, safe to retry) up to 3 total
attempts (`backoff.WithMaxRetries(policy, 2)`), jittered exponential
backoff (50ms–500ms), bounded by the same `callCtx` `CallTimeout`
derived. A 404 (a legitimate `Known=false` answer, not a failure)
returns on the FIRST attempt, exactly like a 200 does — it never
consumes retry budget. The retry loop runs INSIDE one
`breaker.Execute` call, so a retry storm against an already-degraded
dependency still only ever counts as ONE success/failure toward the
breaker's trip condition, not three.

Unlike order-management's `productclassification.Client` (which needed
a `GetClassification`/`fetch` split to separate its fail-open port
contract from a raw, retry-facing call), inventory-storage's
`facilitylayout.Client.GetSlotAttributes` already has the "raw, no
fail-open conversion for a real error" shape this retry loop needs — a
404 legitimately returns `Known=false`/`nil`, but a transport error or
`ErrUnexpectedStatus` is returned as a real error already. No client
split was needed here; `retryingFetch` calls the existing exported
method directly.

The `kafka`-mode path (`facilitycache.Consumer`) is a full-replay local
cache, never a per-call synchronous dependency, so it gets no
breaker/retry treatment — see §6 for its own, different resilience
mechanism.

### 6. Dead-letter topic for the facility-location-cache consumer

`facilitycache.Consumer.Run` now dead-letters a message its `apply()`
cannot parse to `<its own source topic>+".dlq"` (derived per-instance,
never a fixed constant — an isolated integration-test topic
automatically gets its own isolated DLQ topic, mirroring the existing
constructor's isolation pattern), preserving the raw payload plus
`x-dlq-source-topic`/`x-dlq-error`/`x-dlq-failed-at` headers for
operator inspection/replay, then continues (`observe()` still runs
unconditionally) — offsets keep advancing exactly as they did before
this change, so one malformed message was never able to block the
partition either before or after.

This consumer's DLQ path is intentionally SIMPLER than order-
management's `RepromiseConsumer` (ADR-0025 §6): there is no
in-process bounded retry before dead-lettering. `apply()`'s only
failure mode is a permanently-malformed JSON payload (bad
envelope/data shape, an empty required field) — a deterministic parse
failure that would fail identically on every retry attempt, never a
transient infrastructure error `RepromiseConsumer`'s handler can hit
(e.g. a database write racing another writer). Retrying a guaranteed-
deterministic failure would only burn CPU for zero benefit. The value
this DLQ still adds is pure visibility/replay: a malformed message is
still logged and applied-past exactly as before, but now ALSO
preserved on the `.dlq` topic so an operator can inspect it and, if
facility-layout ever fixes a publish-side bug, replay it.

The DLQ writer follows the same synchronous-writer contract as the
producer adapters (`outbound/kafka/writer_config.go`): `Balancer:
&kafka.Hash{}` (so the source key keeps routing to one DLQ partition),
`RequiredAcks: RequireAll` (a nil return really means the broker stored
the dead letter before the source offset is committed) and a 10ms
`BatchTimeout`; pinned by `TestNewDLQWriter_AutoCreatesTopic`.

Proven end to end with a real testcontainers Kafka
(`consumer_integration_test.go`,
`TestConsumer_MalformedMessage_GoesToDeadLetterTopicWithoutBlockingPartition`):
a `ZoneRegistered` event published with an empty `zoneId` (a
deterministic parse failure per `apply()`'s own validation) lands on
the `.dlq` topic with the raw original JSON payload and the
error-context headers intact, and — published right after the
malformed one, on the SAME topic — a well-formed pair of
`ZoneRegistered`/`LocationSlotRegistered` events is applied to the
cache without delay, proving the partition was never blocked.

### 7. Circuit breaker state as a Prometheus gauge

`telemetry.CircuitBreakerMetrics`
(`internal/adapters/outbound/telemetry/circuit_breaker_metrics.go`)
registers ONE OTel `Int64Gauge`, `circuit_breaker.state` — re-exported
by this fleet's OTel Collector prometheus exporter as
`circuit_breaker_state{dependency="facility-layout"}` (0=closed,
1=half-open, 2=open, gobreaker's own numbering verbatim, no translation
table) — on the SAME global `otel.Meter` this service's other metrics
already use (ADR-0016), not a second, parallel registry.
`resilience.RecordStateChange(dependency, recorder)` adapts a
`resilience.StateRecorder` (the interface `CircuitBreakerMetrics`
implements) into `gobreaker.Settings.OnStateChange`'s signature; a
`nil` recorder is a documented no-op, so a test that does not care
about the metric never needs to construct one.

### 8. Graceful shutdown hardening

`cmd/inventory/main.go`'s pre-existing `signal.NotifyContext` +
`httpServer.Shutdown(shutdownCtx)` sequence is extended, not
rewritten, into this order:

1. **Flip readiness to not-ready FIRST**
   (`inboundhttp.Readiness.SetNotReady`, backing a new `GET /readyz`,
   distinct from the pre-existing `GET /healthz` which stays a pure
   liveness signal and is never flipped by shutdown) — before anything
   else stops, so a Kubernetes `readinessProbe` polling `/readyz` has a
   window to observe the flip and stop routing NEW traffic to this pod
   before step 2 ever closes the listener.
2. **Stop accepting new HTTP connections and drain in-flight
   requests** — `httpServer.Shutdown(shutdownCtx)`, unchanged from
   before.
3. **Stop the `kafka`-mode facility-location-cache consumer's `Run`
   loop cleanly** (when `LOCATION_LOOKUP_MODE=kafka`) — cancel its own
   context (no NEW message is fetched/handled after this) and WAIT,
   bounded by `shutdownDrainTimeout` (10s), for the goroutine to
   actually finish, rather than merely firing the cancel and moving on.
   The pre-existing outbox relay's stop sequence is extended with the
   same bounded wait for consistency.
4. **Close the pgx pool LAST** — `closeAdapters` is `defer`red near the
   top of `run()`, so by `defer`'s LIFO order it runs AFTER every
   consumer/relay goroutine has already stopped touching the pool, not
   before.

`Readiness`'s zero value (and a `nil *Readiness`) is always ready —
every existing test and any caller that predates this type behaves
exactly as before.

`charts/inventory-storage/values.yaml`'s `readinessProbe` now points at
`/readyz` (was `/healthz`) — `startupProbe`/`livenessProbe` are
UNCHANGED, still `/healthz`, because liveness must never be flipped by
a graceful drain or Kubernetes would SIGKILL the pod mid-drain instead
of letting it finish. A new `terminationGracePeriodSeconds: 30` was
added (previously unset) — the HTTP `Shutdown` budget (10s) plus the
relay/consumer stop budget (`shutdownDrainTimeout`, 10s) plus margin
for the `readinessProbe`'s `periodSeconds: 5` to have actually observed
the not-ready flip before traffic fully stops arriving.

## Consequences

- The `http`-mode `facilitylayout` call now derives its timeout from
  the caller's remaining budget rather than a fresh hardcoded one — a
  caller with a short deadline gets a short-lived outbound call, not
  one that outlives its own patience.
- A failing facility-layout deployment (in `http` mode) now trips ONE
  breaker after 5 consecutive failures (or a sustained >50% error rate
  with enough volume) and stops sending real traffic to it for
  `DefaultTimeout` (30s) before probing again — bounded load on a
  struggling dependency instead of unbounded retry-forever pressure —
  while every call still resolves with the SAME fail-open answer
  `StowStock` already tolerated.
- The `kafka`-mode consumer can no longer silently drop a malformed
  message with only a scrolling log line as evidence: the `.dlq` topic
  is a new, durable, replayable operational surface. It needs
  monitoring/alerting (out of scope here — the ERROR-level log line is
  the interim signal) and a manual replay tool (also out of scope).
- `GET /readyz` is a new, distinct endpoint SRE tooling should point
  `readinessProbe`s at going forward — `/healthz` alone is no longer
  sufficient for a pod that participates in a graceful drain.
- `sony/gobreaker/v2` and `cenkalti/backoff/v4` are new direct
  dependencies, matching the exact versions order-management's
  ADR-0025 adopted.
- `facilitylayout.Client.GetSlotAttributes`'s OBSERVABLE behaviour (its
  port contract: `Known=false`/`nil` on a 404, a real error otherwise)
  is entirely unchanged; only `http`-mode callers now go through
  `BreakerClient` instead of `Client` directly.

## Alternatives considered

- **One global circuit breaker for all outbound calls:** rejected for
  the same isolation reasoning as order-management's ADR-0025 — even
  though this service has only one synchronous cross-context
  dependency today, a future second one must not share a breaker with
  this one.
- **A brand-new fallback behaviour when the breaker opens (e.g. a
  cached last-known classification):** rejected — the breaker only
  decides WHEN to fall back, not WHAT the fallback is; inventing new
  fallback semantics here would diverge from the pre-existing,
  already-understood `permissive`-mode contract `StowStock` already
  reasons about.
- **Bounded in-process retry before dead-lettering the facility-
  location-cache consumer's malformed messages, mirroring
  `RepromiseConsumer` exactly:** rejected — see §6; `apply()`'s failure
  mode is a deterministic parse failure that retrying cannot fix. Retry
  is reserved for the genuinely transient failures the HTTP client
  path can hit.
- **Dropping a DLQ message instead of publishing it:** rejected — a
  replayable, inspectable record is strictly more operationally useful
  than a silent drop, at negligible extra cost.
- **Retrying the `kafka`-mode consumer's `newTargetOffsets` broker dial
  through the breaker too:** rejected as out of scope — that dial is
  already covered by the separate, pre-existing
  `internal/adapters/outbound/bootretry` boot-time retry (Phase 0),
  which exists specifically for this fleet's known ~10s
  post-start first-outbound-dial reset; the circuit breaker is a
  steady-state-traffic mechanism, not a boot-time one.

## References

- Reference design: order-management's ADR-0025
  (`docs/docs/adr/0025-resilience-circuit-breakers-retry-dlq-shutdown.md`,
  PR #107, merged into `develop`) — this ADR mirrors its design
  verbatim, adapted to this service's single `facilitylayout` HTTP
  dependency and its `facilitycache` Kafka consumer.
- ADR-0013 — the `kafka`-mode local cache design this ADR's DLQ
  addition extends, not re-derives.
- ADR-0016 — the standard metrics convention this ADR's
  `circuit_breaker.state` gauge follows.

## Verification performed

- `go build ./...`, `go vet ./...`, `gofmt -l .` (clean),
  `golangci-lint run ./...` (0 issues) via `make check`.
- `go build -tags=integration ./...`, `go vet -tags=integration ./...`
  (clean).
- `go test ./... -race -count=1` — every existing unit test green,
  including new `facilitylayout.BreakerClient`/`http.Readiness` tests
  and `internal/architecture` (hexagonal fitness, including the
  fleet-wide "Kafka integration tests must use testcontainers" fitness
  check against the new DLQ test).
- `go test -tags=integration ./internal/adapters/outbound/facilitycache/... -race -count=1`
  — every existing integration test green, plus the new
  `TestConsumer_MalformedMessage_GoesToDeadLetterTopicWithoutBlockingPartition`,
  against a real, throwaway `testcontainers-go/modules/kafka` broker
  (never a fake, never `t.Skip`, never a hardcoded `localhost`).
- `make coverage`: 97.9% (gate: 90%) — unchanged in shape from before
  this branch, since `COVERAGE_PKGS` scopes to
  `./internal/domain/...,./internal/application/...`, neither of which
  this change touches.
- `make arch-test` — hexagonal fitness green.
