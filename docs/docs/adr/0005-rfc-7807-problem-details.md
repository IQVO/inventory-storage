---
id: 0005-rfc-7807-problem-details
slug: /adr/0005-rfc-7807-problem-details
title: 0005. RFC 7807 Problem Details for all error responses
sidebar_label: 0005. RFC 7807 errors
description: ADR 0005 — replace the bespoke error shape with application/problem+json across the whole API.
---

# 0005. RFC 7807 Problem Details for all error responses

## Status

Accepted. Delivered in the REST hardening pass (`Task 11`, commit *"Task 11:
REST API hardening, RFC 7807 errors, OpenAPI 3.0.3 docs, Spectral CI gate"*).
The audit that motivated it is recorded in `REST_AUDIT.md`.

## Context

The service originally returned a bespoke error body:

```json
{ "error": "requested quantity exceeds usable inventory" }
```

That shape has three concrete problems once the API has real consumers:

- **Nothing is machine-readable.** The only signal is an English sentence.
  A consumer that wants to distinguish "bin is full" from "reservation exceeds
  usable" — both `409 Conflict` — has to match on prose, which breaks the first
  time someone improves the wording.
- **It is one more dialect to learn.** Every service inventing its own error
  envelope means every client writing per-service error handling.
- **The status code alone is not enough.** This API returns `409` for at least
  six distinct conditions and `422` for three, each needing a different
  reaction from the caller.

At the same time, the REST audit found status-code inaccuracies worth fixing in
the same pass: semantically-invalid values were returning `400` where `422` was
correct, `ReceiveStock` returned `201` for something with no addressable
resource, and resource creation was missing `Location` headers.

Constraints:

- The domain must not learn about HTTP. Aggregates return typed errors; only
  the inbound adapter may know about status codes.
- Whatever is chosen has to be describable in OpenAPI and pass Spectral.

## Decision

**We will use RFC 7807 *Problem Details for HTTP APIs* for every error
response, with `Content-Type: application/problem+json`.**

```json
{
  "type": "https://errors.inventory-storage.warehouse-systems.dev/insufficient-usable",
  "title": "Requested quantity exceeds usable inventory",
  "status": 409,
  "detail": "requested quantity exceeds usable inventory",
  "instance": "/reservations"
}
```

1. **`type` is the machine-readable key** — a stable URI, unique per error
   *category*. It is an identifier and deliberately need not resolve to a
   page; the base URI
   `https://errors.inventory-storage.warehouse-systems.dev/` namespaces it.
2. **`title` is a fixed human string per category**; **`detail`** carries the
   dynamic message from the underlying typed error; **`instance`** is the
   request path.
3. **Mapping happens exactly once, in the adapter.** `statusFor(err)` chooses
   the status code and `problemFor(err)` chooses the `(type, title)` pair,
   both in `internal/adapters/inbound/http/errors.go`, both switching on
   `errors.Is` against typed domain and application errors. `statusFor` is a
   `switch`; `problemFor` is a lookup table (`problemCatalog()`, an ordered
   slice of `(error, problemInfo)` pairs that `problemFor` walks, first
   `errors.Is` match wins — order matters, not a map). The two mirror each
   other's groupings one-for-one.
4. **Status codes were corrected in the same pass:**
   - `422 Unprocessable Entity` for well-formed but semantically invalid
     *values* (quantity ≤ 0, negative quantity, invalid bin capacity), leaving
     `400 Bad Request` for genuinely malformed or missing input;
   - `202 Accepted` for `ReceiveStock` — a staged receipt has no id and no
     `GET` route, so `201 Created` would be a lie;
   - `201 Created` **with a `Location` header** for `StowStock` and
     `ReserveStock`;
   - `409 Conflict` for state conflicts (bin full, exceeds usable, already
     resolved, expired).
5. **The catalog is documented and enforced.** Every problem type and every
   status code appears in `apis/openapi.yaml` as a response example on the
   operation(s) that can return it — including the slugs written outside the
   catalog (`idempotency-key-required` / `idempotency-key-reused`,
   `counted-quantity-required`, `invalid-report-query`, `report-store-error`)
   — and the `api-lint` CI job Spectral-lints the spec on every push and
   pull request.

## Consequences

### Easier

- **Clients can branch on `type`.** Six different `409`s become six
  distinguishable conditions without parsing English.
- **One dialect across the platform.** RFC 7807 is a standard with existing
  client-side support; nobody has to learn a bespoke envelope.
- **The mapping is auditable in one place.** A function and a table, side by
  side, one case/row per typed error. Adding a domain error without mapping it is visible in
  review — and falls back to `internal-error` / `500` rather than leaking.
- **The domain stayed clean.** No aggregate learned about HTTP; the entire
  change was inside the inbound adapter.
- **Errors are documented like successes.** Each operation's error responses
  are in the OpenAPI spec, so they render in the generated
  [API Reference](/docs/api-reference).

### Harder

- **`type` URIs are now a contract.** Renaming a slug is a breaking change for
  any consumer switching on it, even though the URI resolves to nothing.
- **A status `switch` and a problem table to keep in sync.** `statusFor`
  and `problemFor`/`problemCatalog()` must stay aligned; they are
  deliberately written in the same order with the same groupings, but
  nothing mechanically enforces it.
- **`detail` leaks internal error strings.** It is currently `err.Error()`
  verbatim. For an internal service that is a feature (fast debugging); for a
  public API it would need sanitising.
- **The `400`/`422` distinction requires judgement per error.** "Malformed" vs
  "well-formed but illegal" is a real line, and every new domain error has to
  be placed on the right side of it.
