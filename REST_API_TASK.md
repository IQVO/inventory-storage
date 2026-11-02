# REST API Hardening & OpenAPI Documentation — Task 11

You are acting as a REST API specialist. Four ordered stages, strictly
additive to the bounded context's business logic — this task touches ONLY
the inbound HTTP adapter layer (`internal/adapters/inbound/http/`) and adds
new docs/CI files. Do NOT modify any domain aggregate, invariant, use case,
or the application layer's behavior. Every stage gates the next.

## Stage 1 — REST principles audit and fixes

Audit this service's HTTP adapter against REST/HTTP semantics (Richardson
Maturity Level 2 is the bar — proper resource nouns, correct HTTP verbs,
correct status codes; NOT HATEOAS, that's explicitly out of scope). Fix real
violations found. Specifically check for:

1. **Resource nouns, not verbs, in URLs** — `/tasks`, `/stations` are
   correct; a bare RPC-style endpoint with no resource identity is not.
   KNOWN ISSUE across this codebase to check for and fix: any endpoint like
   `POST /admin/expire-leases` that isn't scoped to a resource collection —
   rename to a properly resource-scoped action, e.g. `POST
   /tasks/expire-leases` (a collection-level action on the tasks resource,
   analogous to how `/tasks/{id}/complete` is an action on a single task).
   Keep verb-suffixed action endpoints for genuine domain commands (e.g.
   `/tasks/{id}/complete`, `/associates/{id}/start-shift`,
   `/reservations/{id}/pick-confirmations`) — this is correct DDD/REST practice for
   non-CRUD commands (see Stripe/GitHub APIs), do NOT force these into
   PUT/PATCH.
2. **Correct HTTP methods**: GET must be side-effect-free (check every `Get`
   handler doesn't mutate state), POST for creation/commands, DELETE for
   revocation (already used correctly for reservation revocation in
   inventory-storage — keep that pattern). PUT/PATCH are not required given
   the command-oriented domain, but if any handler mutates via GET, that's a
   real bug — fix it.
3. **Correct status codes**: 201 Created (with a Location header pointing at
   the created resource, if not already present — ADD Location headers to
   every 201 response that doesn't have one) for resource creation, 200 OK
   for successful reads/idempotent updates, 204 No Content for successful
   deletes/actions with no response body, 404 for not-found, 409 Conflict
   for genuine state conflicts (double-claim, double-complete, capacity
   exceeded), 422 Unprocessable Entity for semantically invalid input,
   400 Bad Request for malformed JSON/missing required fields. Audit every
   handler against this table and fix any mismatches found (check
   particularly for 200 where 201 is correct, or missing 400 handling for
   malformed request bodies — check every handler actually validates its
   decoded DTO before calling the use case, add validation + 400 if missing).
4. **Idempotency semantics documented, not necessarily changed** — note
   which endpoints are naturally idempotent (PUT-like POSTs, DELETE) vs which
   are not (POST creating a new resource each time) — this feeds Stage 2's
   documentation, no code change needed here unless you find a genuine bug
   (e.g. a "create" endpoint that should be idempotent by client-supplied ID
   but isn't checking for an existing resource first — flag this, but only
   fix it if it's a clear bug, not a design preference).
5. **Consistent JSON casing**: confirm every request/response field is
   camelCase (check — this codebase already does this consistently, just
   verify, don't rename working fields without cause).
6. **Content negotiation**: every response already sets
   `Content-Type: application/json` — after Stage 2 (RFC 7807), error
   responses specifically must set `Content-Type: application/problem+json`
   instead (see Stage 2).

Document every fix you make in a short `REST_AUDIT.md` at the repo root:
what was wrong, what you changed, why. If you find nothing wrong for a given
check above, say so explicitly (don't leave silent gaps).

**Definition of done for Stage 1:** REST_AUDIT.md written covering all 6
checks above. `go build ./...`, `go vet ./...`, `go test ./... -race`,
`golangci-lint run ./...`, `gofmt -l .` all clean. Existing httptest coverage
for the HTTP adapter still passes (update test assertions for any status
code or Location-header changes you made — do not delete tests to make them
pass).

## Stage 2 — RFC 7807 (application/problem+json) migration

This service currently returns errors as a bespoke `{"error": "..."}` JSON
body. Migrate ALL error responses to RFC 7807 (Problem Details for HTTP
APIs): https://www.rfc-editor.org/rfc/rfc7807

Response shape:
```json
{
  "type": "https://errors.<service-name-kebab-case>.warehouse-systems.dev/<error-slug>",
  "title": "Human-readable summary of the error category",
  "status": 409,
  "detail": "The specific error message for this occurrence (existing err.Error() text)",
  "instance": "/tasks/abc-123"
}
```

- `type` is a URI (does not need to resolve to a real page — it's an
  identifier) unique per distinct error CATEGORY in this service (e.g.
  `.../task-already-claimed`, `.../station-not-found`) — derive the slug
  from the existing sentinel error names (`ErrAlreadyClaimed` →
  `task-already-claimed`).
- `title` is a fixed, category-level human string (e.g. "Task already
  claimed by another station").
- `status` duplicates the HTTP status code as an integer (RFC 7807
  requirement — yes, it's redundant with the actual HTTP status, that's by
  design).
- `detail` is the existing dynamic `err.Error()` text (preserves whatever
  specific data the current error message carries).
- `instance` is the request path (`r.URL.Path`) that produced the error —
  omit if there's no natural resource path (e.g. a validation error on the
  request body with no path segment identifying a resource).

Implementation:
- Replace the existing `errorResponse` DTO and `writeError` function with an
  RFC 7807 version. Set `Content-Type: application/problem+json` (not
  `application/json`) on every error response.
- Keep the EXISTING `statusFor`/`errorStatus` mapping logic (the
  error → HTTP status switch) completely intact — only change what gets
  written to the body and the Content-Type header.
- Build the `type`/`title` pair as a small lookup table keyed by sentinel
  error (a map or switch alongside the existing status-code switch is fine —
  match this codebase's existing style).
- Update EVERY existing httptest that asserts on the old `{"error":...}`
  shape to assert on the new RFC 7807 shape instead (field names: type,
  title, status, detail, instance). Do not leave any test asserting the old
  shape — that would mean Stage 2 isn't actually done.
- Update this repo's `README.md` wherever it shows an example error response
  (curl examples, etc.) to reflect the new shape.

**Definition of done for Stage 2:** grep this repo for the old
`errorResponse`/`{"error"` pattern — zero remaining production code
references (test fixtures asserting the NEW shape are fine and expected).
Full suite green (`go build/vet/test -race`, `golangci-lint run ./...`,
`gofmt -l .`). Manually curl at least 2 different error scenarios against the
running binary (e.g. a 404 and a 409) and confirm the response body is valid
RFC 7807 JSON with `Content-Type: application/problem+json` — paste the raw
curl output into REST_AUDIT.md as evidence.

## Stage 3 — OpenAPI 3.0.3 documentation

Write `openapi.yaml` at the repo root: OpenAPI 3.0.3, EVERY endpoint in this
service's router, exhaustively detailed. This is the deliverable the user
called "very detailed" — do not write a thin stub. Required for every
operation:

- `operationId` (camelCase, unique, e.g. `createTask`, `claimNextTask`)
- `summary` (one line) AND `description` (2-4 sentences of real domain
  context — pull this from CLAUDE.md/the domain code, not generic filler;
  e.g. for claim-next: explain the PULL dispatch model, earliest-CPT-first
  selection, at-most-once lease semantics)
- `tags` grouping operations by aggregate/resource (e.g. "Tasks",
  "Stations", "Packages" for fulfillment-execution)
- full `requestBody` schema (every field, type, required/optional, format
  where applicable e.g. `format: date-time` for timestamps, a realistic
  `example`) for every POST/PUT/PATCH
- full response schema for EVERY status code the handler can actually return
  (200/201/204 success shapes AND every 4xx/5xx from Stage 1's status-code
  audit) — reference a shared RFC 7807 `Problem` schema component for all
  error responses (define it once in `components/schemas`, `$ref` it
  everywhere) rather than repeating it per-endpoint
- path/query parameters fully typed with description and example
- realistic `example` values throughout — use the actual ubiquitous-language
  terms from this service's domain (real path IDs, SKUs, task types, station
  IDs matching the patterns already used in this repo's own README/tests,
  not "foo"/"bar"/"string")

Also include:
- `info.title`, `info.version` (match this repo's go.mod-adjacent version or
  use "1.0.0"), `info.description` (a real paragraph explaining this bounded
  context's role, pulled from CLAUDE.md/the DDD docs — what subdomain, what
  aggregates, what it does NOT do / where its boundary is)
- `servers` (at minimum `http://localhost:8080` for local dev)
- `components/schemas/Problem` — the RFC 7807 shape from Stage 2, reused via
  `$ref` for every error response
- tag descriptions in a top-level `tags:` array

Validate the file is well-formed before declaring done:
```sh
python3 -c "import yaml; yaml.safe_load(open('openapi.yaml'))"
```
If `npx` and internet access are available, ALSO validate it's a genuinely
valid OpenAPI 3.0.3 document (not just valid YAML) with:
```sh
npx --yes @redocly/cli lint openapi.yaml
```
Fix any errors it reports (warnings are fine to leave, but note them in
REST_AUDIT.md). If `npx` isn't available or has no network access, note that
in REST_AUDIT.md and rely on the YAML syntax check plus your own careful
review against the OpenAPI 3.0.3 spec.

**Definition of done for Stage 3:** `openapi.yaml` exists at repo root,
covers every single route in `router.go` (cross-check: list every route from
the router, confirm each has a corresponding `paths` entry — do this
explicitly and report the count, e.g. "9/9 routes documented"), passes the
YAML syntax check, and (if tooling available) passes Redocly lint with zero
errors.

## Stage 4 — GitHub Actions: add an openapi-lint job using Spectral

Add a NEW job to the EXISTING `.github/workflows/ci.yml` (do not replace the
file, do not touch the existing `lint-test` or `mutation` jobs — add
alongside them). Spectral is a CLI tool (`@stoplight/spectral-cli`) for
linting OpenAPI documents.

```yaml
  openapi-lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - name: Install Spectral
        run: npm install -g @stoplight/spectral-cli
      - name: Lint OpenAPI spec
        run: spectral lint openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
```

Also add `.spectral.yaml` at the repo root:
```yaml
extends: ["spectral:oas"]
rules:
  operation-operationId: error
  operation-description: error
  operation-tags: error
  info-description: error
  oas3-api-servers: error
```
(This extends Spectral's built-in OpenAPI ruleset and escalates the specific
rules that matter most for this doc — operationId, description, tags,
servers — from warning to error, so CI actually fails on their absence
rather than just warning. Extend further if you find this repo's
openapi.yaml has other systematic gaps worth hard-gating.)

This new job runs on the SAME triggers as the existing `lint-test` job
(push/PR to main — it inherits the workflow-level `on:` block already there,
you don't need to duplicate it) and should be a REAL blocking gate (no `if:`
condition skipping it, unlike the `mutation` job).

Verify locally before pushing:
```sh
spectral lint openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
```
Fix whatever it reports — this must exit 0 before you declare Stage 4 done,
otherwise you're shipping a CI job that will immediately fail on every PR.

Then verify `.github/workflows/ci.yml` is still valid YAML with the new job
added:
```sh
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"
```

If `gh` is available and authenticated, commit (same branch strategy as
Task 10 — this repo's established convention: check `git log` for whether
prior tasks pushed straight to main or used a PR; match it, EXCEPT
fulfillment-execution which has an open PR #1 from Task 10 — for
fulfillment-execution specifically, branch off `task-10-quality-engineering`
or open a fresh branch/PR for Task 11, do not push directly to main), push,
and watch the new `openapi-lint` job actually run and pass on GitHub's real
runners via `gh run watch`. This is the strongest verification — do it if
credentials allow.

**Definition of done for Stage 4:** `.spectral.yaml` + updated
`.github/workflows/ci.yml` committed, `spectral lint` passes locally with
exit 0, YAML valid, and (if push access available) the new job confirmed
green on GitHub's actual runners.

## Final report

When all 4 stages are done, report: REST violations found and fixed (count
+ one-line list), confirmation error responses are RFC 7807 across the
board, route-count documented in openapi.yaml (N/N), Spectral lint result
(0 errors), and confirmation the full existing test suite (all prior Tasks)
remains green. Do not stop until every stage's Definition of Done is met.
