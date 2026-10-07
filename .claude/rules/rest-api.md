---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
---

# REST API (inbound adapter)

- `POST /stock/receive`                        -> ReceiveStock
- `POST /stock/stow`                           -> StowStock
- `POST /reservations`                         -> ReserveStock
- `GET  /reservations?demandRef=`              -> GetReservationsByDemandRef
- `DELETE /reservations/{id}`                  -> RevokeReservation
- `POST /reservations/{id}/confirm-pick`       -> ConfirmPick
- `GET  /inventory/{sku}/usable`               -> GetUsable
- `PUT  /bins/{binId}`                         -> RegisterBin (idempotent; 201 created,
                                                  200 unchanged/resized, 409
                                                  capacity-below-occupancy — ADR-0025)
- `GET  /bins/{binId}`                         -> GetBin ({binId, capacity, occupied, available})
- `POST /bins/{binId}/cycle-count`             -> RunCycleCount
- `PUT  /products/{sku}/classification`        -> 410 `classification-moved` (retired,
                                                  ADR-0033: classify in product-master)
- `GET  /products/{sku}/classification`        -> DEPRECATED: the local copy of
                                                  product-master's classification
                                                  (removed at product-master ADR 0003 stage E)
- `GET  /healthz`
- `GET  /readyz`                               -> 200 ready / 503 not_ready during
                                                  graceful shutdown (ADR-0020)

JSON DTOs live in the http adapter; never leak domain structs.

Every reservation allocation (`POST /reservations`, `GET /reservations`)
carries its pick location as `allocations[].binId` — the bin of the
StockUnit it drew from, captured at reserve time and persisted in
`reservation_allocations.bin_id` (migration 0008, ADR-0025). It is
`omitempty` only for legacy rows the backfill could not resolve.
`POST /reservations` rejects an empty `demandRef` with 400
`missing-demand-ref` (it is the idempotency/lookup key).

`GET /reservations?demandRef=` is the read side backing the fleet's
cross-service Order Lifecycle console screen — see ADR-0002 in
`warehouse-ops-agent`'s docs and this repo's own adoption-record ADR under
`docs/docs/adr/`. It returns every Reservation ever created against a
caller-supplied `demandRef` (array, since a demandRef can have multiple
reservations across its lifetime — a revoke followed by a retry), never
404s on an unknown demandRef (200 + empty array instead), and is
side-effect-free.

## CORS

`go-chi/cors` middleware is enabled on every route, allowing
`CORS_ALLOWED_ORIGINS` (env, default
`http://localhost:5173,http://localhost:5182` — the `warehouse-console`
shell and this service's own `inventory-mfe` remote).

## Source of truth for the generated reference

`apis/openapi.yaml` is Spectral-linted (`.spectral.yaml`) and is the ONLY
source `docs/docs/api-reference/rest/*.api.mdx` should ever be regenerated
from (`npm run gen-api-docs` in `docs/`, wired to `docusaurus.config.ts`'s
`docusaurus-plugin-openapi-docs` plugin, `specPath: '../apis/openapi.yaml'`).
Never hand-edit the generated `*.api.mdx` / `*.ParamsDetails.json` /
`*.RequestSchema.json` / `*.StatusCodes.json` files under
`docs/docs/api-reference/rest/` — they are build output.
