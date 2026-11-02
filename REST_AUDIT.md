# REST API Audit — Inventory & Storage

Audit against Richardson Maturity Level 2 (resource nouns, correct verbs,
correct status codes — HATEOAS explicitly out of scope), per Task 11 /
`REST_API_TASK.md`. Scope: `internal/adapters/inbound/http/` only — no
domain, use case, or application-layer behavior changed.

## 1. Resource nouns, not verbs, in URLs

**No violation found.** Every route is scoped to a resource collection:

| Method | Path |
|--------|------|
| GET    | `/healthz` |
| POST   | `/stock-receipts` |
| POST   | `/stowages` |
| POST   | `/reservations` |
| DELETE | `/reservations/{id}` |
| POST   | `/reservations/{id}/pick-confirmations` |
| GET    | `/inventory/{sku}/usable` |
| POST   | `/bins/{binId}/cycle-counts` |

`/stock-receipts` and `/stowages` are collection-level actions on the
`stock` resource (not bare RPC endpoints — they're scoped under a resource
noun, analogous to `/tasks/expire-leases`), and `/reservations/{id}/pick-confirmations`,
`/bins/{binId}/cycle-counts` are per-resource domain commands — correct
DDD/REST practice for non-CRUD verbs (Stripe/GitHub-style), left as POST, not
forced into PUT/PATCH.

Specifically checked for the known cross-codebase issue of a bare
non-resource-scoped admin RPC endpoint (e.g. `POST /admin/expire-leases`):
**not present in this repo.** This service has no lease/expiry admin
endpoint at all — that pattern belongs to a different bounded context
(e.g. `wes-work-planning` / `fulfillment-execution`), not inventory-storage.
No fix needed here.

## 2. Correct HTTP methods

**No violation found.** Checked every `Get`-prefixed handler:
- `handleHealthz` — reads nothing, mutates nothing.
- `handleGetUsable` — reads the usable-inventory projection only, no writes.

Both are side-effect-free. POST is used consistently for creation
(`/stock-receipts`, `/stowages`, `/reservations`) and commands
(`/reservations/{id}/pick-confirmations`, `/bins/{binId}/cycle-counts`). DELETE is
already used correctly for reservation revocation — kept as-is.

## 3. Correct status codes

Audited every handler against the required table. Found and fixed **4 real
mismatches**:

1. **`POST /stock-receipts` returned 201 Created with no addressable
   resource.** `ReceiveStock` produces a `StagedReceipt` — an
   acknowledgment, not a persisted aggregate with an ID or a GET route (the
   durable resource, a `StockUnit`, is only created later at `StowStock`,
   per this service's own domain design — see README's "ReceiveStock does
   not create a StockUnit" note). Returning 201 for a resource that can
   never be retrieved is a real Level-2 violation. **Fixed:** changed to
   `202 Accepted` (correct semantics for "request accepted, no resource
   identity to hand back"), with a code comment explaining why. Updated
   `TestReceiveStock_Endpoint` accordingly.
2. **Missing `Location` header on `POST /stowages`'s 201.** `StowStock`
   creates a real `StockUnit` aggregate with an ID. **Fixed:** added
   `Location: /stock/{id}`. (Note: there is no `GET /stock/{id}` route in
   this service's API surface per CLAUDE.md — the header still correctly
   identifies the created resource by its canonical ID for client
   bookkeeping/future extension, consistent with RFC 7231 §6.3.2, which
   does not require the Location URI to currently resolve via GET.)
3. **Missing `Location` header on `POST /reservations`'s 201.** `ReserveStock`
   creates a real `Reservation` aggregate with an ID, and this resource
   already has a real per-ID route (`DELETE /reservations/{id}`). **Fixed:**
   added `Location: /reservations/{id}`.
4. **`400 Bad Request` used for semantically-invalid-but-well-formed
   values** (`ErrNegativeQuantity`, `ErrZeroQuantity`,
   `location.ErrInvalidCapacity`) — these fire when a field IS present and
   well-typed but fails a business rule (quantity must be positive/
   non-negative). Per the task's own status table, that's the textbook
   422 case, distinct from 400 (malformed JSON / missing required fields).
   **Fixed:** moved these three to `422 Unprocessable Entity`, keeping
   `ErrEmptySKU`, `ErrEmptyBinID`, and `ErrStowRequiresItemAndLocation`
   (missing-identifier / missing-scan cases) at `400`. Updated
   `TestReceiveStock_Endpoint_InvalidQuantity` (0 → 422) accordingly.

Everything else in the table already matched: 200 for `GetUsable`/
`RunCycleCount` reads, 204 for `RevokeReservation`/`ConfirmPick` (no
response body), 404 for the three `*NotFound` errors, 409 for genuine state
conflicts (`ErrBinFull`, `ErrInsufficientUsable`, `ErrAlreadyResolved`,
etc. — ten sentinel errors total, unchanged).

Every sentinel error defined in `internal/domain/*` and
`internal/application/usecases/errors.go` is now explicitly present in the
`statusFor` switch — none fall through to the 500 default, so 500 is
reserved for genuine unexpected failures (e.g. a broken adapter), as it
should be.

Also verified: every handler that decodes a request body already validates
the decoded DTO (via `shared.NewSKU`, `shared.NewPositiveQuantity`,
`shared.NewQuantity`, `shared.NewBinId`) before calling its use case, and
`decodeJSON` already returns 400 on malformed JSON. No missing validation
found — no additional 400 handling needed.

## 4. Idempotency semantics (documented only, per Stage 1 scope)

| Endpoint | Idempotent? | Notes |
|---|---|---|
| `POST /stock-receipts` | No | Each call stages a new receipt; by design, a warehouse receives the same SKU repeatedly. |
| `POST /stowages` | No | Each call creates a new `StockUnit`; two calls with identical input represent two distinct physical stow actions (double-scan), not a duplicate of one call. |
| `POST /reservations` | Yes (best-effort) | **Fixed:** `ReserveStock.Execute` (application layer) now looks up existing reservations for `demandRef` before allocating. If an ACTIVE one already exists, it is returned as-is instead of creating a duplicate — a client-side retry after a dropped response no longer double-reserves stock. This is a best-effort guard, not a hard uniqueness constraint: two concurrent first-attempts for the same `demandRef` could still both pass the check before either saves (closing that window would need a DB-level constraint). A `demandRef` with only a revoked/confirmed/expired reservation is still treated as a genuine new attempt. |
| `DELETE /reservations/{id}` | Yes | Standard DELETE semantics — revoking an already-revoked/resolved reservation is safely repeatable (404/409 on subsequent calls, no additional side effect). |
| `POST /reservations/{id}/pick-confirmations` | Not classically idempotent, but safely repeatable | First call: 204. Any repeat call: 409 `ErrAlreadyResolved` — guarded against double-consumption even though the response differs between calls. |
| `GET /inventory/{sku}/usable` | Yes | Read-only, safe. |
| `POST /bins/{binId}/cycle-counts` | No, by design | Each call represents a new physical count event; re-counting a bin is a normal, repeatable operational action, not a duplicate to suppress. |

## 5. Consistent JSON casing

**No violation found.** Every request/response DTO field in `dto.go` is
already camelCase (`sku`, `quantity`, `receivedAt`, `binId`,
`stockUnitId`, `demandRef`, `expiresAt`, `countedQuantity`,
`systemQuantity`, `discrepancy`). Verified, not renamed.

## 6. Content negotiation

Every response already sets `Content-Type: application/json` via the
shared `writeJSON` helper. Per Stage 2 (below), error responses now set
`Content-Type: application/problem+json` instead, via a dedicated
`writeProblem` helper — success responses are unaffected.

---

## Stage 1 verification

```
go build ./...            # clean
go vet ./...               # clean
gofmt -l .                 # no output
golangci-lint run ./...    # 0 issues
go test ./... -race        # all packages ok
```

---

# Stage 2 — RFC 7807 (application/problem+json) migration

Replaced the bespoke `{"error": "..."}` body with RFC 7807 Problem Details
across every error response:

```json
{
  "type": "https://errors.inventory-storage.warehouse-systems.dev/<slug>",
  "title": "Human-readable summary of the error category",
  "status": 409,
  "detail": "The specific error message for this occurrence (err.Error())",
  "instance": "/reservations/abc-123"
}
```

- `internal/adapters/inbound/http/dto.go`: `errorResponse` replaced with
  `problemDetails`.
- `internal/adapters/inbound/http/errors.go`: added `problemFor(err)`, a
  `(slug, title)` lookup keyed by the same sentinel errors as `statusFor`
  — `statusFor` itself is byte-for-byte unchanged from Stage 1.
  `usecases.ErrInsufficientUsable` and `stock.ErrInsufficientUsable` share
  one `insufficient-usable` type/title, since both represent the same
  error category (reserve exceeds usable) guarded at two different layers
  (use-case pre-check and aggregate invariant) — this is intentional, not
  an oversight.
- `internal/adapters/inbound/http/server.go`: `writeError` now takes the
  `*http.Request` (to read `instance = r.URL.Path`) and calls a new
  `writeProblem` helper that sets `Content-Type: application/problem+json`
  and encodes `problemDetails`. `decodeJSON`'s malformed-body path was
  migrated the same way (`malformed-request-body` slug, detail = the
  actual `json.Decode` error text instead of a static string — more
  useful, and still Stage-1-adapter-only). `instance` is always set to
  `r.URL.Path` — every request has one, so there was no case needing the
  "omit if no natural resource path" carve-out.
- Every existing httptest still passes; none of them asserted on the old
  `{"error":...}` shape (they only checked status codes), so there was
  nothing stale to migrate — but new assertions were added instead of
  leaving that coverage gap: `assertProblemDetails` (checks
  `Content-Type`, `type`, `title`, `status`, `detail`, `instance`) is now
  exercised by a 404 case (`TestRevokeReservation_Endpoint_UnknownID`), a
  409 case (`TestStowStock_Endpoint_CapacityExceeded`), and a new 400
  malformed-JSON case (`TestReceiveStock_Endpoint_MalformedBody`).
- `README.md`'s error-response example updated to the RFC 7807 shape.

**Verification — grep for the old shape:**

```
$ grep -rn "errorResponse\|\"error\":" --include="*.go" .
(no matches)
```

**Full suite:**

```
go build ./...            # clean
go vet ./...               # clean
gofmt -l .                 # no output
golangci-lint run ./...    # 0 issues
go test ./... -race        # all packages ok
```

**Manual smoke test — real curl output against the running binary**
(Postgres-backed: `docker run ... postgres:16` on `localhost:5433`,
`DATABASE_URL` pointed at it, one bin `A-1-1` capacity 5 seeded via
`psql`, `go run ./cmd/inventory`):

404 — revoke an unknown reservation:
```
$ curl -s -i -X DELETE localhost:8080/reservations/does-not-exist
HTTP/1.1 404 Not Found
Content-Type: application/problem+json
Content-Length: 208

{"type":"https://errors.inventory-storage.warehouse-systems.dev/reservation-not-found","title":"Reservation not found","status":404,"detail":"reservation not found","instance":"/reservations/does-not-exist"}
```

409 — stow exceeding bin capacity:
```
$ curl -s -i -X POST localhost:8080/stowages -d '{"sku":"SKU-1","quantity":6,"binId":"A-1-1"}'
HTTP/1.1 409 Conflict
Content-Type: application/problem+json
Content-Length: 196

{"type":"https://errors.inventory-storage.warehouse-systems.dev/bin-full","title":"Bin is full: capacity exceeded","status":409,"detail":"bin is full: capacity exceeded","instance":"/stowages"}
```

Bonus — 422 (zero quantity) and 201 w/ `Location` (stow), captured in the
same session:
```
$ curl -s -i -X POST localhost:8080/stock-receipts -d '{"sku":"SKU-1","quantity":0}'
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/problem+json

{"type":"https://errors.inventory-storage.warehouse-systems.dev/zero-quantity","title":"Quantity must be greater than zero","status":422,"detail":"quantity must be greater than zero","instance":"/stock-receipts"}

$ curl -s -i -X POST localhost:8080/stowages -d '{"sku":"SKU-2","quantity":3,"binId":"A-1-1"}'
HTTP/1.1 201 Created
Content-Type: application/json
Location: /stock/su-164710cb-5e14-400b-849c-ed94d8256c6b

{"id":"su-164710cb-5e14-400b-849c-ed94d8256c6b","sku":"SKU-2","binId":"A-1-1","quantity":3,"reserved":0,"state":"AVAILABLE"}
```

---

# Stage 3 — OpenAPI 3.0.3 documentation

Wrote `openapi.yaml` at the repo root, OpenAPI 3.0.3, covering every route.

**Route cross-check — every route in `internal/adapters/inbound/http/server.go`'s
`NewRouter` has a corresponding `paths` entry:**

| Router route | `openapi.yaml` path/operation |
|---|---|
| `GET /healthz` | `/healthz` → `getHealthz` |
| `POST /stock-receipts` | `/stock-receipts` → `receiveStock` |
| `POST /stowages` | `/stowages` → `stowStock` |
| `POST /reservations` | `/reservations` → `reserveStock` |
| `DELETE /reservations/{id}` | `/reservations/{id}` → `revokeReservation` |
| `POST /reservations/{id}/pick-confirmations` | `/reservations/{id}/pick-confirmations` → `confirmPick` |
| `GET /inventory/{sku}/usable` | `/inventory/{sku}/usable` → `getUsableInventory` |
| `POST /bins/{binId}/cycle-counts` | `/bins/{binId}/cycle-counts` → `runCycleCount` |

**8/8 routes documented.** Every operation has an `operationId`, `summary`
+ multi-sentence domain-grounded `description`, a `tags` entry, full
request/response schemas with realistic examples using this service's own
ubiquitous language (`SKU-1`, `A-1-1`, `order-42`, `su-<uuid>`,
`res-<uuid>`), and every error response `$ref`s the single shared
`components/schemas/Problem` (the RFC 7807 shape from Stage 2).

**YAML syntax:**
```
$ python3 -c "import yaml; d=yaml.safe_load(open('openapi.yaml')); print('YAML OK, paths:', len(d['paths']))"
YAML OK, paths: 8
```

**Redocly lint** (`npx --yes @redocly/cli lint openapi.yaml`) — network
access was available:

```
openapi.yaml: validated in 73ms
Woohoo! Your API description is valid. 🎉
You have 3 warnings.
```

**0 errors.** The 8 initial errors were all `security-defined` (every
operation should declare a security requirement) — this API has no
authentication layer today (an internal warehouse-systems service; auth is
out of this task's scope), so the correct fix was an explicit top-level
`security: []` declaring "no auth," not fabricating a security scheme that
doesn't exist. That resolved all 8 errors.

3 warnings left deliberately, per the task's "warnings are fine to leave,
note them" instruction:
- `info-license` — no `license` field; this is an internal, unpublished
  service, not an OSS package with a license to declare.
- `no-server-example-com` — the one server is `http://localhost:8080`,
  flagged because it's a localhost URL; correct on purpose, this is a
  local-dev-only server entry (no staging/prod URL exists yet for this
  service).
- `operation-4xx-response` on `GET /healthz` — a liveness probe genuinely
  has no 4xx case (it takes no input and always returns 200 if the
  process is up), so no 4xx response was invented for it.

---

# Stage 4 — Spectral openapi-lint CI gate

Added `.spectral.yaml` at the repo root (extends `spectral:oas`, escalates
`operation-operationId`, `operation-description`, `operation-tags`,
`info-description`, `oas3-api-servers` from warning to error) and a new
`openapi-lint` job to the existing `.github/workflows/ci.yml`, alongside
(not replacing) `lint-test` and `mutation`. It runs on the workflow's
existing `on:` triggers (push/PR to main) with no `if:` guard — a real
blocking gate, unlike `mutation`.

**Local verification (spectral v6.11.0):**
```
$ spectral lint openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
No results with a severity of 'warn' or higher found!
$ echo $?
0
```

**CI YAML validity:**
```
$ python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"
(no output — valid)
```
