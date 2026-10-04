---
id: 0029-extra-architecture-fitness-tests
slug: /adr/0029-extra-architecture-fitness-tests
title: "0029. Additional architecture fitness tests beyond the dependency rule"
sidebar_label: "29. Extra fitness tests"
sidebar_position: 29
description: "ADR 0029 — records the fitness tests added to internal/architecture beyond ADR 0006's arch-go dependency rule: MCP adapter dependency rule, no reintroduced auth, CloudEvents-only envelopes, unique consumer groups, replay commit interval, and testcontainers-only Kafka integration tests."
---

# 0029. Additional architecture fitness tests beyond the dependency rule

## Status

Accepted — records tests that already exist in `internal/architecture/`.
Extends [ADR 0006](./0006-arch-go-fitness-tests.md), which introduced the
arch-go hexagonal dependency rule (`TestHexagonalArchitecture`).

## Context

Several fleet rules were violated, or nearly violated, in ways the import-graph
check cannot see: a flat Kafka envelope sneaking back in, a bearer-auth layer
being "helpfully" reintroduced after ADR 0015 removed it, a Kafka consumer group
hardcoded inline, an integration test pointing at `localhost:9092`. Each cost
real incidents or fleet-wide rework, so each became an executable rule.

## Decision

`internal/architecture` carries these Go AST/text fitness tests (each failing
with a uniform `archViolation(...)` message naming the rule, the file and the
fix), all run by `go test ./...` and `make arch-test`:

| Test | Rule |
|---|---|
| `TestMCPAdapterDependencyRule` | The MCP inbound adapter depends only inward (ADR 0008); no MCP/SDK type leaks into `domain`/`application`. |
| `TestNoAuthMiddlewareReintroduced` | No bearer/JWT/API-key pattern in REST/MCP code — both surfaces are unauthenticated by decision (ADR 0015). |
| `TestCloudEventsOnly` | CloudEvents 1.0 is the only Kafka envelope (ADR 0024): no `EVENT_ENVELOPE_MODE` toggle anywhere, and any package using `kafka-go` must build envelopes with `github.com/cloudevents/sdk-go`. |
| `TestKafkaConsumerGroupNeverHardcodedInline` | A consumer `GroupID` is never a string literal inline (a past incident); it comes from a named constant or a unique-group helper. |
| `TestReplayConsumersSetCommitInterval` | A full-replay cache consumer (process-unique group) must set `CommitInterval`, else kafka-go commits synchronously after every message. |
| `TestKafkaIntegrationTestsUseTestcontainers` | Kafka integration tests use testcontainers — never an env-gated skip or a hardcoded broker address (fleet rule). |

## Consequences

- These rules fail the PR that breaks them, not a later incident review; the
  failure message states the fix.
- Being AST/text heuristics they can false-positive; the escape hatch is to
  change the test **together with the ADR** documenting the new decision (the
  auth test says so explicitly), never to silence it.
- New fleet-wide rules should land here as tests, with a row in this table.
