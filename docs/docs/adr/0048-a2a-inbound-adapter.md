---
id: 0048-a2a-inbound-adapter
slug: /adr/0048
title: "ADR 0048: A2A inbound adapter (third driving adapter)"
sidebar_label: "48. A2A inbound adapter"
sidebar_position: 48
description: "ADR 0048: inventory-storage is an Agent2Agent (A2A) agent through a third driving adapter over the same use cases as REST and MCP. Two curated goal-shaped skills derived from the MCP tools, check_availability (read) and revoke_reservation (write), in a message-only agent served by cmd/a2a on port 8093. A2A carries delegation only; facts still leave as CloudEvents through the transactional outbox."
---

# 0048. A2A inbound adapter (third driving adapter)

## Status

Accepted (2026-10-29). Mirrors the reference rollouts in workforce-management
(ADR-0039) and wes-work-planning (ADR-0046); follows
[ADR-0008](./0008-mcp-inbound-adapter.md) (the MCP adapter this one is modelled
on), [ADR-0015](./0015-remove-rest-identity-layer.md) (REST and MCP are
unauthenticated),
[ADR-0017](./0017-transactional-outbox.md) (the transactional outbox),
[ADR-0024](./0024-cloudevents-mandatory-envelope.md) (CloudEvents on every Kafka
message) and
[ADR-0042](./0042-postgres-and-kafka-are-the-only-runtime.md) (Postgres and
Kafka are the only runtime).

## Context

The fleet is adopting the Agent2Agent protocol so that a bounded context can
**delegate** a goal to another context's agent. This context already exposes its
use cases over REST and MCP. The two things a delegating agent would want to ask
of the stock ledger are exactly the two the MCP surface already offers as its
core read and write: *how much of this SKU can I promise?* and *give that
reservation's quantity back*.

Three transports now carry three different things and must not overlap:

| Transport | Carries |
|---|---|
| Kafka + CloudEvents | **facts** between contexts ([ADR-0024](./0024-cloudevents-mandatory-envelope.md)) |
| MCP | an agent's **tools** (reads and single actions) |
| A2A | **delegation** of a goal to another context's agent |

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same**
use cases (`GetUsable`, `RevokeReservation`) as the HTTP and MCP adapters, built
on the official `a2aproject/a2a-go/v2` SDK (pinned v2.7.0) and served by a new
`cmd/a2a` binary (JSON-RPC 2.0 over HTTP, port **8093**, the Agent Card at
`/.well-known/agent-card.json`, the endpoint mounted at `/` and `/a2a`).

### Two curated skills, derived from the MCP tools

| Skill | Kind | What it does |
|---|---|---|
| `check_availability` | read | the usable quantity of a SKU (on-hand minus active reservations and held or unlocated stock, fleet-owned stock). Persists nothing; an unknown SKU answers 0. |
| `revoke_reservation` | write | revokes an active reservation, returning its quantity to usable; publishes `ReservationRevoked` (and the bin stock-level facts) through the transactional outbox. |

The same curation discipline as ADR-0008 and the
[MCP governance charter](../mcp/governance-charter.md): goal-shaped, never one
skill per endpoint.

**Write gate.** A skill may change state only if the context's MCP surface
already offers that write. The MCP charter offers exactly one write,
`revoke_reservation` (revocable by design: it returns quantity to usable and
never strands or destroys stock), so that is the only write skill; a test pins
that the card carries exactly one `write` skill and that it is that one. Reads
that the MCP surface offers but this ADR does not carry over (`get_bin_occupancy`,
`get_relocation`, `get_container`, the flow-and-accuracy report) stay MCP-only:
they are data access for an agent's own tooling, not goals to delegate. Nothing
the context does not already expose over MCP is exposed over A2A.

### Message-only, answered in-band

Both skills are transactional request/response work, so the agent is
message-only (no Task lifecycle). Domain rejections and malformed requests are
**answered** as RFC 7807-shaped `problem`s inside the message envelope
([ADR-0005](./0005-rfc-7807-problem-details.md)), never raised as JSON-RPC
transport errors, with the **same classification and `type` URIs as the REST
adapter** (`statusFor` / `problemCatalog`) for every error these two use cases
can raise: `404` reservation or stock unit not found, `400` empty SKU (and any
malformed request or argument), `409` reservation already resolved or expired,
insufficient reserved quantity, release exceeding occupancy, or concurrent
modification, `500` anything else. The 500 answer carries adapter-owned detail,
never the raw error (a database error may name a DSN); the cause is logged. The
REST status classification has no `503` for these use cases, so the A2A one does
not either.

### Strict input

Unknown argument names are **rejected**, not ignored (a misspelled argument must
not silently change what a delegated write does), and trailing data after the
JSON document is rejected too. Go's `encoding/json` matches names
case-insensitively, so only genuinely unknown names are refused. The skill
request is `{"skill":"<id>","arguments":{...}}`, as a data part or as a text part
holding the same JSON. An unknown skill id is answered in-band and starts no
OTel span (the name is caller-controlled); every known skill runs in an
`a2a.skill <id>` span carrying the `a2a.skill` attribute, mirroring the MCP
adapter's `mcp.tool <name>` spans.

This context has no deprecated key alias on these two skills, so (unlike a
sibling that must keep a legacy key off the new surface) there is nothing to
withhold from the card.

### Facts still travel on Kafka

A2A never becomes a data-sync channel. `revoke_reservation` writes the same
outbox rows `DELETE /reservations/{id}` writes, through the same
`UnitOfWork` and publisher; `cmd/a2a` never runs the relay (only `cmd/inventory`
drains `outbox_events`) and, like `cmd/mcp`, never dials a broker, so it needs no
`KAFKA_BROKERS`. Proven on real Postgres by
`TestA2ARevokeReservation_WritesUnpublishedOutboxRowsOnBothTopics` (build tag
`integration`): a delegated revoke leaves exactly the two **unpublished** rows
(integration and analytics topic), and a rejected delegation stores nothing.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP
([ADR-0015](./0015-remove-rest-identity-layer.md)). Schemes are added to the
card, not a redesign, if that decision is ever reversed; re-introducing an
identity layer is a new ADR.

## Consequences

- A fourth deployable (`/app/a2a`) in the same image and an `a2a.*` chart block
  (`<release>-a2a`, default **off**), exposed through Kong at
  `/api/a2a/inventory-storage` rewritten to `/`, so
  `/.well-known/agent-card.json` stays reachable under the same prefix. The card
  advertises `A2A_AGENT_URL`, which is mandatory (an absolute http(s) URL; the
  chart defaults it to the in-cluster Service DNS and `a2a.agentUrl` overrides
  it).
- `revoke_reservation` is **not idempotent** in effect: a second call on the same
  reservation is a `409` (`reservation-already-resolved`), which a delegating
  agent can treat as "already done". The card says so. The MCP tool has the same
  property today.
- Delegating *to* this agent does not change who decides: this context still
  owns what is usable and what a revocation does to it; whether a caller may
  *ask* for a revocation is that caller's governance.
- Port 8093 is the fleet's A2A port (8090 is this chart's MCP server, 8091 its
  projector admin port, 8092 the reports API).
- Not in this change: write-skill rate limiting and a per-call audit record (the
  MCP charter's §8.2 and §9 gaps apply equally here), an A2A `messageId` as an
  idempotency key, and any outbound A2A client.
