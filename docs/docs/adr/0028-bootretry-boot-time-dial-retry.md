---
id: 0028-bootretry-boot-time-dial-retry
slug: /adr/0028-bootretry-boot-time-dial-retry
title: "0028. Boot-time dial retry (bootretry)"
sidebar_label: "28. Boot-time dial retry"
sidebar_position: 28
description: "ADR 0028 — records internal/adapters/outbound/bootretry: a bounded exponential-backoff retry (5 attempts, ~31s) applied only to the boot-time dials (migrations, pool ping, Kafka startup dial) to survive the Istio native-sidecar first-dial reset, while still failing closed."
---

# 0028. Boot-time dial retry (bootretry)

## Status

Accepted — records a mechanism already in the code
(`internal/adapters/outbound/bootretry`, used by `cmd/inventory` and `cmd/mcp`).

## Context

In the `warehouse` cluster every injected pod's **first** outbound TCP dial
(Postgres, Kafka) fails with `read: connection reset by peer` roughly 10s after
the app starts: Istio runs native sidecars (istio-proxy as an init container
with `restartPolicy: Always`), so `holdApplicationUntilProxyStarts` is a no-op
and the app cannot be made to wait for the proxy. A composition root that dials
once and fails closed turns that transient window into a permanent
`CrashLoopBackOff`.

## Decision

A tiny package, `bootretry`, provides `Retry` / `RetryWithDelay`: exponential
backoff starting at 1s, **5 attempts (~31s total: 1+2+4+8+16)** — comfortably
past the ~10s reset and inside the liveness probe's tolerance. It returns the
**last real error** (wrapped with the op name and attempt count), honours
`ctx` cancellation, and logs each retry.

It wraps **only** the boot-time dials that must succeed before the process can
call itself ready:

- running migrations (`postgres.RunMigrations`), in `cmd/inventory` and `cmd/mcp`;
- pinging the pool (`pgxpool` does not connect in `NewWithConfig`, so without
  the ping the reset would surface inside the first request);
- the facility-location Kafka consumer's synchronous startup dial
  (`LOCATION_LOOKUP_MODE=kafka`).

It is **not** a request-retry helper and must never be used on the request hot
path; request-time resilience is [ADR 0020](./0020-resilience-circuit-breaker-retry-dlq-shutdown.md)'s
breaker/retry.

## Consequences

- Pods survive the sidecar reset instead of crash-looping, at the cost of up to
  ~31s of extra boot time when a dependency is genuinely unreachable (it still
  fails closed after the budget).
- `RetryWithDelay` takes the base delay so tests exercise the give-up path
  without sleeping out the real budget (`retry_test.go`).
- A new boot-time dial added to a composition root should be wrapped the same
  way; reviewers should flag an unwrapped one.
