---
paths:
  - "web/**"
---

# Frontend micro-frontend remote (`web/`)

This repo also owns `web/`: `inventory-mfe`, a Vite + React Module
Federation **remote** consumed by the separate `warehouse-console` shell
repo. It is a plain browser client of this service's own REST API (SKU/
usable-inventory lookup, reservation-by-demandRef search) — nothing in
`web/` talks to any other bounded context, and nothing in `internal/` knows
`web/` exists.

- Own `package.json`, build, and dev server (`:5182`).
- Does NOT participate in this repo's Go quality gate (`make check` /
  `make check-all`) and is not part of the Go module.
- See ADR-0012 (`docs/docs/adr/0012-adopt-mfe-console-architecture.md`) for
  the adoption record.
