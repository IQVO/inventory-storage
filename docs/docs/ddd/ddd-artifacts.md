---
title: DDD Artifacts
sidebar_label: DDD Artifacts (ddd-crew)
sidebar_position: 0
description: Index of the ddd-crew DDD artifact pack for inventory-storage — one line per artifact, the ddd-crew tool it follows, and where its facts come from.
---

# DDD Artifacts

The Domain-Driven Design artifact pack for **Inventory & Storage**, following
the [ddd-crew](https://github.com/ddd-crew) tools plus UML. Every diagram is
Mermaid, derived from the code on `develop`, and carries a "Source:" line
naming the files it came from and a note on what it omits.

## Strategic design

| Artifact | ddd-crew tool | What it answers |
| --- | --- | --- |
| [Subdomain Classification](./subdomain-classification.md) | — (reference model) | Why this is a Core subdomain in the WMS tier. |
| [Core Domain Chart](./core-domain-chart.md) | [Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) | Differentiation vs model complexity, with the evolution note. |
| [Bounded Context Canvas](./bounded-context-canvas.md) | [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) | Purpose, roles, every inbound and outbound message, decisions, open questions. |
| [Context Map](/docs/ecosystem/context-map) | [Context Mapping](https://github.com/ddd-crew/context-mapping) | Every upstream/downstream relationship, pattern, technology and wiring status. |
| [Context Relationships](./context-relationships.md) | — (narrative) | The strategic story behind each edge of the map. |
| [Ubiquitous Language](/docs/business-context/ubiquitous-language) | — (glossary) | Every term with its code identifier, and where the two differ. |

## Tactical design

| Artifact | ddd-crew tool | What it answers |
| --- | --- | --- |
| [Aggregate Design Canvas](./aggregate-design-canvas.md) | [Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) | One canvas per aggregate root: states, invariants with their `Err` values, commands, events, throughput, size. |
| [Domain Events](./domain-events.md) | — (event catalogue) | Every event published and consumed: full CloudEvents type, topic, key, payload, producer, consumers. |
| [Domain Message Flow](./domain-message-flow.md) | [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) | Four business scenarios as numbered command / query / event flows between contexts. |
| [EventStorming](./eventstorming.md) | [EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet) | Design-level process maps with the standard sticky colours and real hotspots. |
| [Use Cases](./use-cases.md) | — | The eleven application use cases, their collaborators and algorithms. |

## UML and data

| Artifact | Notation | What it answers |
| --- | --- | --- |
| [Class Diagrams](./class-diagram.md) | UML class diagram | Domain types, enumerations, events, ports and the hexagonal adapter map. |
| [ER Diagram](./entity-relationship.md) | Entity-relationship | The final OLTP and analytical Postgres schemas and the table-to-aggregate mapping. |
| [Sequence Diagrams](./sequence-diagrams.md) | UML sequence diagram | Every command use case plus the outbox relay and both Kafka consumers, following the function bodies. |

:::note[Sources of truth]
Code wins over every page here. The facts come from:

- `internal/domain/**` — aggregates, value objects, invariants, events.
- `internal/application/usecases/**` and `ports/ports.go` — use cases and ports.
- `internal/adapters/**` and `cmd/*/main.go` — routes, MCP tools, topics,
  consumer groups, environment variables and wiring.
- `migrations/*.up.sql` and `migrations/analytics/*.up.sql` — the schema.
- `apis/openapi.yaml` and `apis/asyncapi.yaml` — the published contracts
  (pinned to the code by the `contract` and `arch-test` CI jobs).
- `docs/docs/adr/` — the decisions (ADR 0001-0029).
- Sibling repositories' outbound adapters on `develop` — for who calls this
  service and how (see the [Context Map](/docs/ecosystem/context-map)).

Throughput and size figures on the Aggregate Design Canvas, and the
coordinates on the Core Domain Chart, are estimates and are labelled so.
:::
