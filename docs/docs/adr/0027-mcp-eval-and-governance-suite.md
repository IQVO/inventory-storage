---
id: 0027-mcp-eval-and-governance-suite
slug: /adr/0027-mcp-eval-and-governance-suite
title: "0027. MCP governance gate and eval suite (E1–E3)"
sidebar_label: "27. MCP eval & governance suite"
sidebar_position: 27
description: "ADR 0027 — records the computational governance tests (tool count, naming, annotations, descriptions) and the three-layer MCP eval suite (E1 schema/registry, E2 wire conformance, E3 Gherkin behaviour) that gate the MCP adapter in CI, including the dedicated evals-tests job."
---

# 0027. MCP governance gate and eval suite (E1–E3)

## Status

Accepted — records tests and a CI gate that already exist; written to document
them as a decision. Complements [ADR 0008](./0008-mcp-inbound-adapter.md),
which promised a "planned CI lint on tool count/annotations".

## Context

An MCP tool surface is **model-controlled**: nothing in the compiler stops a PR
from adding a tool per endpoint, renaming a tool an agent prompt depends on, or
shipping a tool without annotations or a description. The estate-wide
[MCP Governance Charter](../mcp/governance-charter.md) states the rules; rules
that are only prose degrade.

## Decision

Enforce the charter with plain `go test`s inside
`internal/adapters/inbound/mcp/` — no new infrastructure — in two groups:

1. **Governance gate** (`governance_test.go`, package-internal). Boots the real
   server and lists its tools:
   `TestGovernance_ToolCountWithinBudget` (`maxTools = 8`),
   `TestGovernance_ToolNaming` (snake_case `verb_noun`),
   `TestGovernance_ToolsAreAnnotated`, `TestGovernance_ToolsHaveDescriptions`.
2. **Eval suite** (`eval_*_test.go`, `evalsuite_test.go`, package `mcp_test`),
   all over the real Streamable HTTP handler with a real SDK client
   (`eval_harness_test.go`), never in-process values:
   - **E1 — schema & registry** (`eval_governance_test.go`): every advertised
     tool's input schema is a resolvable, constraining JSON Schema with
     described parameters, and the advertised surface equals the golden
     registries in `testdata/tool_registry.golden` /
     `testdata/fleet_tool_snapshot.golden` (fleet-wide tool-name uniqueness).
   - **E2 — wire conformance** (`eval_conformance_test.go`): the initialize
     handshake, error shapes for unknown tools / bad argument types / unknown
     resources and prompts, discovery of resource templates and prompts, session
     lifecycle after close.
   - **E3 — behaviour** (`evalsuite_test.go` + `testdata/features/mcp_tools.feature`):
     Gherkin scenarios driven by godog against a canonical state (10 units of
     SKU-A at BIN-1, 4 reserved).

They run in the normal `test` job **and** as their own named, individually
gating CI job `evals-tests`
(`go test ./internal/adapters/inbound/mcp/... -race -run '^TestEval|^TestMCPEvalSuite'`).

## Consequences

- Adding a ninth tool, a badly named tool, or an unannotated tool fails CI;
  changing a tool's schema or name requires updating the golden registry in the
  same PR, which makes the change visible in review.
- The golden registries couple this repo to the fleet snapshot; a fleet-wide
  rename needs a coordinated update.
- The suites exercise the adapter, not `cmd/mcp`'s wiring; the outbox wiring of
  `revoke_reservation` is covered separately by
  `TestMCPRevokeReservation_WritesOutboxRowsOnBothTopics` (ADR 0017).
